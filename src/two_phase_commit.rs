//! Parte 3 - Transacciones Distribuidas: Two-Phase Commit.
//!
//! Comprar una entrada involucra a dos servicios: `ServicioPagos` (debita el
//! precio de la cuenta) y `ServicioInventario` (asigna el asiento). O se hacen
//! las dos cosas o ninguna. Para eso el `Coordinador` ejecuta un 2PC:
//!
//! ```text
//! Coordinador                                       Participantes
//!   │ ── Prepare(compra) ────────────────────────────► │ retienen fondos / reservan asiento
//!   │ ◄───────────────────────────────── Voto YES/NO ─ │
//!   │ anota la decisión en el log:
//!   │   todos YES → Commit, algún NO → Rollback
//!   │ ── Commit(tx) / Rollback(tx) ──────────────────► │ cobran y venden / devuelven y liberan
//!   │ ◄────────────────────────────────────────── Ok ─ │
//! ```
//!
//! Los participantes usan los actores de la Parte 1, que ya separan cada
//! operación en prepare, commit y rollback:
//!
//! | Participante         | Prepare            | Commit             | Rollback          |
//! |----------------------|--------------------|--------------------|-------------------|
//! | `ServicioPagos`      | `SolicitarDebito`  | `ConfirmarDebito`  | `CancelarDebito`  |
//! | `ServicioInventario` | `SolicitarReserva` | `ConfirmarReserva` | `LiberarReserva`  |
//!
//! Para probar fallos de red, `Enlace` simula la conexión con un participante
//! y permite cortarla. Si la decisión no llega, la transacción queda
//! `Incompleta` y `Coordinador::reenviar_decision` la termina cuando vuelve
//! la red.

use crate::actores::{CuentaHandle, IdAsiento, IdTransaccion, InventarioHandle, Monto};
use std::cell::Cell;
use std::fmt;
use std::thread;
use std::time::Duration;

/// Cantidad de veces que el coordinador manda la decisión a un participante
/// antes de darlo por no disponible.
pub const INTENTOS_FASE2: u32 = 3;

/// Pausa entre un intento y el siguiente en la fase 2.
pub const ESPERA_ENTRE_INTENTOS: Duration = Duration::from_millis(50);

/// Datos de la compra que viajan en el `Prepare`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Compra {
    pub tx: IdTransaccion,
    pub asiento: IdAsiento,
    pub precio: Monto,
}

/// Respuesta de un participante al `Prepare`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Voto {
    /// Reservó lo necesario y se compromete a aplicar lo que decida el
    /// coordinador. A partir de acá no puede abortar por su cuenta.
    Yes,
    /// No puede hacer su parte. Trae el motivo.
    No(String),
}

/// Decisión global del coordinador para una transacción.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Commit,
    Rollback,
}

/// Operaciones que el coordinador le pide a cada participante.
///
/// `commit` y `rollback` tienen que ser idempotentes, porque el coordinador
/// los reintenta cuando no recibe respuesta.
pub trait Participante {
    fn nombre(&self) -> &str;

    /// Fase 1: reserva los recursos de la compra y vota.
    fn prepare(&self, compra: &Compra) -> Voto;

    /// Fase 2: hace definitivo lo reservado por `tx`.
    fn commit(&self, tx: IdTransaccion) -> Result<(), String>;

    /// Fase 2: deshace lo reservado por `tx`. Tiene que aceptar una `tx` que
    /// nunca preparó (por ejemplo, si votó NO).
    fn rollback(&self, tx: IdTransaccion) -> Result<(), String>;
}

/// Participante que cobra la entrada con el actor `CuentaUsuario` del comprador.
#[derive(Debug, Clone)]
pub struct ServicioPagos {
    cuenta: CuentaHandle,
}

impl ServicioPagos {
    pub fn new(cuenta: CuentaHandle) -> Self {
        ServicioPagos { cuenta }
    }
}

impl Participante for ServicioPagos {
    fn nombre(&self) -> &str {
        "ServicioPagos"
    }

    /// Retiene el precio. Vota NO si no alcanza el saldo o si el actor no
    /// responde.
    fn prepare(&self, compra: &Compra) -> Voto {
        match self.cuenta.solicitar_debito(compra.tx, compra.precio) {
            Ok(()) => Voto::Yes,
            Err(e) => Voto::No(e.to_string()),
        }
    }

    fn commit(&self, tx: IdTransaccion) -> Result<(), String> {
        self.cuenta.confirmar_debito(tx).map_err(|e| e.to_string())
    }

    fn rollback(&self, tx: IdTransaccion) -> Result<(), String> {
        self.cuenta.cancelar_debito(tx).map_err(|e| e.to_string())
    }
}

/// Participante que asigna el asiento con el actor `InventarioEntradas`.
#[derive(Debug, Clone)]
pub struct ServicioInventario {
    inventario: InventarioHandle,
}

impl ServicioInventario {
    pub fn new(inventario: InventarioHandle) -> Self {
        ServicioInventario { inventario }
    }
}

impl Participante for ServicioInventario {
    fn nombre(&self) -> &str {
        "ServicioInventario"
    }

    /// Reserva el asiento. Vota NO si no existe, si ya está tomado o si el
    /// actor no responde.
    fn prepare(&self, compra: &Compra) -> Voto {
        match self.inventario.solicitar_reserva(compra.tx, compra.asiento) {
            Ok(()) => Voto::Yes,
            Err(e) => Voto::No(e.to_string()),
        }
    }

    fn commit(&self, tx: IdTransaccion) -> Result<(), String> {
        self.inventario
            .confirmar_reserva(tx)
            .map_err(|e| e.to_string())
    }

    fn rollback(&self, tx: IdTransaccion) -> Result<(), String> {
        self.inventario
            .liberar_reserva(tx)
            .map_err(|e| e.to_string())
    }
}

/// Entrada del log del coordinador.
///
/// En un sistema real el log se guarda en disco antes de mandar los mensajes
/// de la fase siguiente. Si el coordinador se cae, al volver lee el log y sabe
/// qué decisión tiene que reenviar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntradaLog {
    /// Se mandó el `Prepare` y se esperan los votos.
    EsperandoVotos(IdTransaccion),
    /// Se tomó la decisión. Ya no puede cambiar.
    Decision(IdTransaccion, Decision),
    /// Todos los participantes aplicaron la decisión.
    Fin(IdTransaccion),
}

impl EntradaLog {
    /// Transacción a la que pertenece la entrada.
    pub fn tx(&self) -> IdTransaccion {
        match *self {
            EntradaLog::EsperandoVotos(tx) | EntradaLog::Decision(tx, _) | EntradaLog::Fin(tx) => {
                tx
            }
        }
    }
}

/// Cómo terminó una ejecución del 2PC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultadoTransaccion {
    /// Todos votaron YES y todos aplicaron el commit.
    Confirmada,
    /// Algún participante votó NO y todos deshicieron lo reservado.
    Abortada { motivos: Vec<String> },
    /// La decisión está tomada y anotada en el log, pero estos participantes
    /// no la confirmaron después de todos los intentos.
    Incompleta {
        decision: Decision,
        sin_respuesta: Vec<String>,
    },
}

/// Errores del coordinador.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCoordinador {
    /// La `tx` ya se usó en otra ejecución de este coordinador. Volver a
    /// votarla podría terminar en una decisión distinta a la anotada.
    TransaccionRepetida(IdTransaccion),
    /// No hay ninguna decisión anotada para la `tx`.
    TransaccionDesconocida(IdTransaccion),
}

impl fmt::Display for ErrorCoordinador {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCoordinador::TransaccionRepetida(tx) => write!(f, "{tx} ya fue ejecutada"),
            ErrorCoordinador::TransaccionDesconocida(tx) => {
                write!(f, "{tx} no tiene una decisión anotada")
            }
        }
    }
}

impl std::error::Error for ErrorCoordinador {}

/// Coordinador del 2PC. Guarda en su log el avance de cada transacción.
#[derive(Debug, Default)]
pub struct Coordinador {
    log: Vec<EntradaLog>,
}

impl Coordinador {
    pub fn new() -> Self {
        Self::default()
    }

    /// Entradas del log, en el orden en que se escribieron.
    pub fn log(&self) -> &[EntradaLog] {
        &self.log
    }

    /// Decisión anotada para `tx`, si ya se tomó.
    pub fn decision(&self, tx: IdTransaccion) -> Option<Decision> {
        self.log.iter().find_map(|entrada| match *entrada {
            EntradaLog::Decision(t, decision) if t == tx => Some(decision),
            _ => None,
        })
    }

    /// Ejecuta el 2PC de `compra` entre los `participantes`.
    ///
    /// Devuelve `TransaccionRepetida`, sin contactar a ningún participante, si
    /// la `tx` ya figura en el log.
    pub fn ejecutar(
        &mut self,
        compra: &Compra,
        participantes: &[&dyn Participante],
    ) -> Result<ResultadoTransaccion, ErrorCoordinador> {
        let tx = compra.tx;
        if self.log.iter().any(|entrada| entrada.tx() == tx) {
            return Err(ErrorCoordinador::TransaccionRepetida(tx));
        }

        // Fase 1 (Prepare): se le pide el voto a todos los participantes.
        self.log.push(EntradaLog::EsperandoVotos(tx));
        let mut motivos = Vec::new();
        for participante in participantes {
            if let Voto::No(motivo) = participante.prepare(compra) {
                motivos.push(format!("{}: {motivo}", participante.nombre()));
            }
        }

        // Commit solo si todos votaron YES. La decisión se anota antes de
        // comunicarla, así que a partir de acá ya no cambia.
        let decision = if motivos.is_empty() {
            Decision::Commit
        } else {
            Decision::Rollback
        };
        self.log.push(EntradaLog::Decision(tx, decision));

        // Fase 2 (Commit/Rollback): la decisión se manda a todos, incluso a
        // los que votaron NO. Así, si a alguno le llega tarde el Prepare, lo
        // rechaza porque la tx ya figura como cancelada.
        let sin_respuesta = self.fase2(tx, decision, participantes);
        if !sin_respuesta.is_empty() {
            return Ok(ResultadoTransaccion::Incompleta {
                decision,
                sin_respuesta,
            });
        }
        Ok(match decision {
            Decision::Commit => ResultadoTransaccion::Confirmada,
            Decision::Rollback => ResultadoTransaccion::Abortada { motivos },
        })
    }

    /// Vuelve a mandar la decisión anotada para `tx`. Sirve para cerrar una
    /// transacción `Incompleta` cuando los participantes vuelven a estar
    /// accesibles. Devuelve los que siguen sin responder (vacío si la
    /// transacción quedó terminada).
    ///
    /// Se le manda a todos, incluso a los que ya respondieron: como commit y
    /// rollback son idempotentes, recibirla dos veces no cambia nada.
    pub fn reenviar_decision(
        &mut self,
        tx: IdTransaccion,
        participantes: &[&dyn Participante],
    ) -> Result<Vec<String>, ErrorCoordinador> {
        let decision = self
            .decision(tx)
            .ok_or(ErrorCoordinador::TransaccionDesconocida(tx))?;
        if self.log.contains(&EntradaLog::Fin(tx)) {
            return Ok(Vec::new());
        }
        Ok(self.fase2(tx, decision, participantes))
    }

    /// Fase 2: manda la decisión a cada participante y, si todos la
    /// confirmaron, anota el `Fin`. Devuelve los que no respondieron.
    fn fase2(
        &mut self,
        tx: IdTransaccion,
        decision: Decision,
        participantes: &[&dyn Participante],
    ) -> Vec<String> {
        let mut sin_respuesta = Vec::new();
        for participante in participantes {
            if !enviar_decision(*participante, tx, decision) {
                sin_respuesta.push(participante.nombre().to_string());
            }
        }
        if sin_respuesta.is_empty() {
            self.log.push(EntradaLog::Fin(tx));
        }
        sin_respuesta
    }
}

/// Manda la decisión a un participante hasta `INTENTOS_FASE2` veces.
/// Devuelve `true` si el participante respondió `Ok`.
///
/// Reintentar no tiene riesgo porque `commit` y `rollback` son idempotentes.
fn enviar_decision(participante: &dyn Participante, tx: IdTransaccion, decision: Decision) -> bool {
    for intento in 1..=INTENTOS_FASE2 {
        let respuesta = match decision {
            Decision::Commit => participante.commit(tx),
            Decision::Rollback => participante.rollback(tx),
        };
        if respuesta.is_ok() {
            return true;
        }
        if intento < INTENTOS_FASE2 {
            thread::sleep(ESPERA_ENTRE_INTENTOS);
        }
    }
    false
}

/// Motivo que devuelve un `Enlace` cortado.
const SIN_CONEXION: &str = "sin conexión";

/// Estado de un `Enlace`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EstadoEnlace {
    Conectado,
    /// Deja pasar el próximo `Prepare` y su voto, y se corta justo después.
    CortarTrasPrepare,
    Cortado,
}

/// Simula la red entre el coordinador y un participante, para probar
/// particiones.
///
/// Mientras está cortado, los mensajes no llegan al participante y el
/// coordinador no recibe respuesta: un voto que no llega cuenta como NO, y un
/// commit o rollback que no llega se informa como error.
pub struct Enlace<'a> {
    participante: &'a dyn Participante,
    estado: Cell<EstadoEnlace>,
}

impl<'a> Enlace<'a> {
    pub fn new(participante: &'a dyn Participante) -> Self {
        Enlace {
            participante,
            estado: Cell::new(EstadoEnlace::Conectado),
        }
    }

    /// Corta la red a partir de ahora.
    pub fn cortar(&self) {
        self.estado.set(EstadoEnlace::Cortado);
    }

    /// Deja pasar el próximo `Prepare` y corta la red apenas llega el voto.
    pub fn cortar_tras_prepare(&self) {
        self.estado.set(EstadoEnlace::CortarTrasPrepare);
    }

    /// Restablece la red.
    pub fn restablecer(&self) {
        self.estado.set(EstadoEnlace::Conectado);
    }

    fn cortado(&self) -> bool {
        self.estado.get() == EstadoEnlace::Cortado
    }
}

impl Participante for Enlace<'_> {
    fn nombre(&self) -> &str {
        self.participante.nombre()
    }

    fn prepare(&self, compra: &Compra) -> Voto {
        if self.cortado() {
            return Voto::No(SIN_CONEXION.to_string());
        }
        let voto = self.participante.prepare(compra);
        if self.estado.get() == EstadoEnlace::CortarTrasPrepare {
            self.cortar();
        }
        voto
    }

    fn commit(&self, tx: IdTransaccion) -> Result<(), String> {
        if self.cortado() {
            return Err(SIN_CONEXION.to_string());
        }
        self.participante.commit(tx)
    }

    fn rollback(&self, tx: IdTransaccion) -> Result<(), String> {
        if self.cortado() {
            return Err(SIN_CONEXION.to_string());
        }
        self.participante.rollback(tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    const COMPRA: Compra = Compra {
        tx: IdTransaccion(1),
        asiento: 1,
        precio: 1_000,
    };

    /// Participante de prueba: vota lo que se le indica, falla la fase 2 la
    /// cantidad de veces pedida y anota cada mensaje que recibe.
    struct ParticipanteDePrueba {
        nombre: &'static str,
        voto: Voto,
        fallas_fase2: Cell<u32>,
        recibidos: RefCell<Vec<&'static str>>,
    }

    impl ParticipanteDePrueba {
        fn new(nombre: &'static str, voto: Voto) -> Self {
            Self::que_falla(nombre, voto, 0)
        }

        fn que_falla(nombre: &'static str, voto: Voto, fallas_fase2: u32) -> Self {
            ParticipanteDePrueba {
                nombre,
                voto,
                fallas_fase2: Cell::new(fallas_fase2),
                recibidos: RefCell::new(Vec::new()),
            }
        }

        fn recibidos(&self) -> Vec<&'static str> {
            self.recibidos.borrow().clone()
        }

        fn responder_fase2(&self, mensaje: &'static str) -> Result<(), String> {
            self.recibidos.borrow_mut().push(mensaje);
            let fallas = self.fallas_fase2.get();
            if fallas > 0 {
                self.fallas_fase2.set(fallas - 1);
                return Err("sin respuesta".to_string());
            }
            Ok(())
        }
    }

    impl Participante for ParticipanteDePrueba {
        fn nombre(&self) -> &str {
            self.nombre
        }

        fn prepare(&self, _compra: &Compra) -> Voto {
            self.recibidos.borrow_mut().push("prepare");
            self.voto.clone()
        }

        fn commit(&self, _tx: IdTransaccion) -> Result<(), String> {
            self.responder_fase2("commit")
        }

        fn rollback(&self, _tx: IdTransaccion) -> Result<(), String> {
            self.responder_fase2("rollback")
        }
    }

    #[test]
    fn todos_votan_yes_y_se_confirma() {
        let a = ParticipanteDePrueba::new("A", Voto::Yes);
        let b = ParticipanteDePrueba::new("B", Voto::Yes);
        let mut coordinador = Coordinador::new();

        let resultado = coordinador.ejecutar(&COMPRA, &[&a, &b]);

        assert_eq!(resultado, Ok(ResultadoTransaccion::Confirmada));
        assert_eq!(a.recibidos(), ["prepare", "commit"]);
        assert_eq!(b.recibidos(), ["prepare", "commit"]);
        let tx = COMPRA.tx;
        assert_eq!(
            coordinador.log(),
            [
                EntradaLog::EsperandoVotos(tx),
                EntradaLog::Decision(tx, Decision::Commit),
                EntradaLog::Fin(tx),
            ]
        );
    }

    #[test]
    fn un_voto_no_aborta_y_todos_reciben_rollback() {
        let a = ParticipanteDePrueba::new("A", Voto::Yes);
        let b = ParticipanteDePrueba::new("B", Voto::No("sin stock".to_string()));
        let mut coordinador = Coordinador::new();

        let resultado = coordinador.ejecutar(&COMPRA, &[&a, &b]);

        assert_eq!(
            resultado,
            Ok(ResultadoTransaccion::Abortada {
                motivos: vec!["B: sin stock".to_string()]
            })
        );
        assert_eq!(a.recibidos(), ["prepare", "rollback"]);
        assert_eq!(b.recibidos(), ["prepare", "rollback"]);
        assert_eq!(coordinador.decision(COMPRA.tx), Some(Decision::Rollback));
    }

    #[test]
    fn la_fase2_se_reintenta_hasta_que_responde() {
        let a = ParticipanteDePrueba::que_falla("A", Voto::Yes, INTENTOS_FASE2 - 1);
        let mut coordinador = Coordinador::new();

        let resultado = coordinador.ejecutar(&COMPRA, &[&a]);

        assert_eq!(resultado, Ok(ResultadoTransaccion::Confirmada));
        let commits = a.recibidos().iter().filter(|m| **m == "commit").count();
        assert_eq!(commits, INTENTOS_FASE2 as usize);
    }

    #[test]
    fn participante_sin_respuesta_deja_la_transaccion_incompleta() {
        let a = ParticipanteDePrueba::que_falla("A", Voto::Yes, u32::MAX);
        let b = ParticipanteDePrueba::new("B", Voto::Yes);
        let mut coordinador = Coordinador::new();

        let resultado = coordinador.ejecutar(&COMPRA, &[&a, &b]);

        assert_eq!(
            resultado,
            Ok(ResultadoTransaccion::Incompleta {
                decision: Decision::Commit,
                sin_respuesta: vec!["A".to_string()]
            })
        );
        // Que A no responda no impide que B reciba el commit.
        assert_eq!(b.recibidos(), ["prepare", "commit"]);
        // La decisión queda anotada, pero la transacción no se cierra.
        assert_eq!(coordinador.decision(COMPRA.tx), Some(Decision::Commit));
        assert!(!coordinador.log().contains(&EntradaLog::Fin(COMPRA.tx)));
    }

    #[test]
    fn una_tx_repetida_se_rechaza_sin_contactar_participantes() {
        let a = ParticipanteDePrueba::new("A", Voto::Yes);
        let mut coordinador = Coordinador::new();
        coordinador.ejecutar(&COMPRA, &[&a]).unwrap();

        assert_eq!(
            coordinador.ejecutar(&COMPRA, &[&a]),
            Err(ErrorCoordinador::TransaccionRepetida(COMPRA.tx))
        );
        assert_eq!(a.recibidos(), ["prepare", "commit"]);
    }

    #[test]
    fn particion_tras_el_voto_retiene_la_decision_hasta_que_vuelve_la_red() {
        let a = ParticipanteDePrueba::new("A", Voto::Yes);
        let b = ParticipanteDePrueba::new("B", Voto::Yes);
        let enlace_a = Enlace::new(&a);
        let enlace_b = Enlace::new(&b);
        enlace_a.cortar_tras_prepare();
        enlace_b.cortar_tras_prepare();
        let mut coordinador = Coordinador::new();

        let resultado = coordinador.ejecutar(&COMPRA, &[&enlace_a, &enlace_b]);

        // Los votos llegaron, la decisión se tomó, pero el commit no llegó.
        assert_eq!(
            resultado,
            Ok(ResultadoTransaccion::Incompleta {
                decision: Decision::Commit,
                sin_respuesta: vec!["A".to_string(), "B".to_string()]
            })
        );
        assert_eq!(a.recibidos(), ["prepare"]);
        assert_eq!(b.recibidos(), ["prepare"]);

        // Mientras la red siga cortada, reenviar no sirve.
        assert_eq!(
            coordinador.reenviar_decision(COMPRA.tx, &[&enlace_a, &enlace_b]),
            Ok(vec!["A".to_string(), "B".to_string()])
        );
        assert!(!coordinador.log().contains(&EntradaLog::Fin(COMPRA.tx)));

        // Al volver la red, la decisión anotada llega y la transacción termina.
        enlace_a.restablecer();
        enlace_b.restablecer();
        assert_eq!(
            coordinador.reenviar_decision(COMPRA.tx, &[&enlace_a, &enlace_b]),
            Ok(Vec::new())
        );
        assert_eq!(a.recibidos(), ["prepare", "commit"]);
        assert_eq!(b.recibidos(), ["prepare", "commit"]);
        assert!(coordinador.log().contains(&EntradaLog::Fin(COMPRA.tx)));
    }

    #[test]
    fn particion_antes_del_voto_aborta() {
        let a = ParticipanteDePrueba::new("A", Voto::Yes);
        let b = ParticipanteDePrueba::new("B", Voto::Yes);
        let enlace_a = Enlace::new(&a);
        enlace_a.cortar();
        let mut coordinador = Coordinador::new();

        let resultado = coordinador.ejecutar(&COMPRA, &[&enlace_a, &b]);

        // El voto de A no llega y cuenta como NO: se decide Rollback.
        assert_eq!(
            resultado,
            Ok(ResultadoTransaccion::Incompleta {
                decision: Decision::Rollback,
                sin_respuesta: vec!["A".to_string()]
            })
        );
        assert_eq!(a.recibidos(), Vec::<&str>::new());
        assert_eq!(b.recibidos(), ["prepare", "rollback"]);
    }

    #[test]
    fn reenviar_decision_de_tx_terminada_no_contacta_a_nadie() {
        let a = ParticipanteDePrueba::new("A", Voto::Yes);
        let mut coordinador = Coordinador::new();
        coordinador.ejecutar(&COMPRA, &[&a]).unwrap();

        assert_eq!(
            coordinador.reenviar_decision(COMPRA.tx, &[&a]),
            Ok(Vec::new())
        );
        assert_eq!(a.recibidos(), ["prepare", "commit"]);
    }

    #[test]
    fn reenviar_decision_de_tx_desconocida_es_error() {
        let a = ParticipanteDePrueba::new("A", Voto::Yes);
        let mut coordinador = Coordinador::new();

        assert_eq!(
            coordinador.reenviar_decision(COMPRA.tx, &[&a]),
            Err(ErrorCoordinador::TransaccionDesconocida(COMPRA.tx))
        );
        assert_eq!(a.recibidos(), Vec::<&str>::new());
    }
}
