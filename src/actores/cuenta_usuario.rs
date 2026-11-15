//! Actor `CuentaUsuario`: saldo de un usuario y débitos en dos fases.
//!
//! Ciclo de vida de un débito (una transacción `tx`):
//!
//! ```text
//!   SolicitarDebito ──► [retenido] ──ConfirmarDebito──► [confirmado]
//!                           │
//!                           └──────CancelarDebito───► [cancelado]
//! ```
//!
//! Mientras está retenido, el monto deja de estar disponible (nadie más puede
//! gastarlo) pero todavía no se cobró, se puede devolver con `CancelarDebito`.

use super::{Desenlace, IdTransaccion, Monto};
use std::collections::HashMap;
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

/// Mensajes que acepta el actor `CuentaUsuario`.
///
/// Los que esperan respuesta traen su propio canal `responder`
#[derive(Debug)]
pub enum MensajeCuenta {
    /// Consulta el estado del saldo.
    ConsultarSaldo { responder: Sender<SaldoCuenta> },
    /// Acredita dinero en la cuenta (carga de saldo).
    Acreditar {
        monto: Monto,
        responder: Sender<Result<(), ErrorCuenta>>,
    },
    /// Fase 1 (prepare): retiene `monto` para la transacción `tx`.
    SolicitarDebito {
        tx: IdTransaccion,
        monto: Monto,
        responder: Sender<Result<(), ErrorCuenta>>,
    },
    /// Fase 2 (commit): cobra definitivamente lo retenido por `tx`.
    ConfirmarDebito {
        tx: IdTransaccion,
        responder: Sender<Result<(), ErrorCuenta>>,
    },
    /// Fase 2 (rollback): devuelve al saldo lo retenido por `tx`.
    CancelarDebito {
        tx: IdTransaccion,
        responder: Sender<Result<(), ErrorCuenta>>,
    },
    /// Termina el loop del actor.
    Detener,
}

/// Foto del saldo de una cuenta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SaldoCuenta {
    /// Dinero que se puede gastar.
    pub disponible: Monto,
    /// Dinero apartado por débitos pendientes de confirmar o cancelar.
    pub retenido: Monto,
    /// Total cobrado por débitos confirmados.
    pub debitado: Monto,
}

/// Errores que puede devolver el actor `CuentaUsuario`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCuenta {
    /// El saldo disponible no alcanza para retener el monto.
    SaldoInsuficiente { disponible: Monto, requerido: Monto },
    /// Monto cero (no tiene sentido debitar ni acreditar nada).
    MontoInvalido,
    /// Acreditar desbordaría el saldo.
    Desbordamiento,
    /// La misma `tx` ya retuvo un monto distinto.
    ConflictoTransaccion(IdTransaccion),
    /// La `tx` ya terminó (confirmada o cancelada) y no se puede reabrir.
    TransaccionFinalizada(IdTransaccion),
    /// Se pidió confirmar una `tx` que nunca retuvo fondos.
    TransaccionDesconocida(IdTransaccion),
    /// Se pidió confirmar una `tx` ya cancelada.
    TransaccionYaCancelada(IdTransaccion),
    /// Se pidió cancelar una `tx` ya confirmada.
    TransaccionYaConfirmada(IdTransaccion),
    /// El actor ya no está corriendo.
    ActorDetenido,
}

impl fmt::Display for ErrorCuenta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCuenta::SaldoInsuficiente {
                disponible,
                requerido,
            } => write!(
                f,
                "saldo insuficiente (disponible {}, requerido {})",
                super::formatear_monto(*disponible),
                super::formatear_monto(*requerido)
            ),
            ErrorCuenta::MontoInvalido => write!(f, "monto inválido"),
            ErrorCuenta::Desbordamiento => write!(f, "el saldo se desbordaría"),
            ErrorCuenta::ConflictoTransaccion(tx) => write!(f, "{tx} ya retuvo otro monto"),
            ErrorCuenta::TransaccionFinalizada(tx) => write!(f, "{tx} ya finalizó"),
            ErrorCuenta::TransaccionDesconocida(tx) => write!(f, "{tx} desconocida"),
            ErrorCuenta::TransaccionYaCancelada(tx) => write!(f, "{tx} ya fue cancelada"),
            ErrorCuenta::TransaccionYaConfirmada(tx) => write!(f, "{tx} ya fue confirmada"),
            ErrorCuenta::ActorDetenido => write!(f, "el actor de la cuenta está detenido"),
        }
    }
}

impl std::error::Error for ErrorCuenta {}

/// Estado privado del actor. Solo lo toca el hilo del actor.
#[derive(Debug, Default)]
pub struct CuentaUsuario {
    saldo_disponible: Monto,
    debitado: Monto,
    /// Débitos en fase 1: `tx` → monto retenido.
    retenciones: HashMap<IdTransaccion, Monto>,
    /// Transacciones terminadas, para responder de forma idempotente.
    finalizadas: HashMap<IdTransaccion, Desenlace>,
}

impl CuentaUsuario {
    /// Crea una cuenta con un saldo inicial (en centavos).
    pub fn new(saldo_inicial: Monto) -> Self {
        CuentaUsuario {
            saldo_disponible: saldo_inicial,
            ..Default::default()
        }
    }

    /// Lanza el actor en su propio hilo.
    ///
    /// `self` se mueve al hilo, a partir de acá el único acceso al estado
    /// es a través del `CuentaHandle` devuelto, mandando mensajes.
    pub fn iniciar(self) -> (CuentaHandle, JoinHandle<()>) {
        let (buzon, rx) = mpsc::channel();
        let hilo = thread::spawn(move || self.ejecutar(rx));
        (CuentaHandle { buzon }, hilo)
    }

    /// Loop del actor: procesa los mensajes de a uno, en orden de llegada.
    ///
    /// Termina con `Detener` o cuando se sueltan todos los handles (el canal
    /// se cierra y el iterador termina).
    fn ejecutar(mut self, rx: Receiver<MensajeCuenta>) {
        for msg in rx {
            if !self.manejar(msg) {
                break;
            }
        }
    }

    /// Procesa un mensaje. Devuelve `false` si el actor debe detenerse.
    ///
    /// Si quien pidió la respuesta ya no la espera, el `send` falla y se
    /// ignora: el efecto sobre el estado igual quedó aplicado.
    fn manejar(&mut self, msg: MensajeCuenta) -> bool {
        match msg {
            MensajeCuenta::ConsultarSaldo { responder } => {
                let _ = responder.send(self.saldo());
            }
            MensajeCuenta::Acreditar { monto, responder } => {
                let _ = responder.send(self.acreditar(monto));
            }
            MensajeCuenta::SolicitarDebito {
                tx,
                monto,
                responder,
            } => {
                let _ = responder.send(self.solicitar_debito(tx, monto));
            }
            MensajeCuenta::ConfirmarDebito { tx, responder } => {
                let _ = responder.send(self.confirmar_debito(tx));
            }
            MensajeCuenta::CancelarDebito { tx, responder } => {
                let _ = responder.send(self.cancelar_debito(tx));
            }
            MensajeCuenta::Detener => return false,
        }
        true
    }

    // Lógica pura (testeable sin hilos)

    pub fn saldo(&self) -> SaldoCuenta {
        SaldoCuenta {
            disponible: self.saldo_disponible,
            retenido: self.retenciones.values().sum(),
            debitado: self.debitado,
        }
    }

    pub fn acreditar(&mut self, monto: Monto) -> Result<(), ErrorCuenta> {
        if monto == 0 {
            return Err(ErrorCuenta::MontoInvalido);
        }
        self.saldo_disponible = self
            .saldo_disponible
            .checked_add(monto)
            .ok_or(ErrorCuenta::Desbordamiento)?;
        Ok(())
    }

    /// Fase 1: chequea y retiene el saldo en un solo paso.
    ///
    /// El "si alcanza, descontar" no puede intercalarse con otro débito porque
    /// el actor procesa un mensaje por vez.
    pub fn solicitar_debito(&mut self, tx: IdTransaccion, monto: Monto) -> Result<(), ErrorCuenta> {
        if monto == 0 {
            return Err(ErrorCuenta::MontoInvalido);
        }
        if self.finalizadas.contains_key(&tx) {
            return Err(ErrorCuenta::TransaccionFinalizada(tx));
        }
        if let Some(&retenido) = self.retenciones.get(&tx) {
            // Reintento del mismo mensaje: idempotente.
            return if retenido == monto {
                Ok(())
            } else {
                Err(ErrorCuenta::ConflictoTransaccion(tx))
            };
        }
        if self.saldo_disponible < monto {
            return Err(ErrorCuenta::SaldoInsuficiente {
                disponible: self.saldo_disponible,
                requerido: monto,
            });
        }
        self.saldo_disponible -= monto;
        self.retenciones.insert(tx, monto);
        Ok(())
    }

    /// Fase 2 (commit): lo retenido pasa a estar cobrado.
    pub fn confirmar_debito(&mut self, tx: IdTransaccion) -> Result<(), ErrorCuenta> {
        if let Some(monto) = self.retenciones.remove(&tx) {
            self.debitado += monto;
            self.finalizadas.insert(tx, Desenlace::Confirmada);
            return Ok(());
        }
        match self.finalizadas.get(&tx) {
            Some(Desenlace::Confirmada) => Ok(()), // reintento: idempotente
            Some(Desenlace::Cancelada) => Err(ErrorCuenta::TransaccionYaCancelada(tx)),
            None => Err(ErrorCuenta::TransaccionDesconocida(tx)),
        }
    }

    /// Fase 2 (rollback): lo retenido vuelve al saldo disponible.
    ///
    /// Cancelar una `tx` desconocida es válido (aborto presunto): se registra
    /// como cancelada para que un `SolicitarDebito` tardío no la reviva.
    pub fn cancelar_debito(&mut self, tx: IdTransaccion) -> Result<(), ErrorCuenta> {
        if let Some(monto) = self.retenciones.remove(&tx) {
            self.saldo_disponible += monto;
            self.finalizadas.insert(tx, Desenlace::Cancelada);
            return Ok(());
        }
        match self.finalizadas.get(&tx) {
            Some(Desenlace::Confirmada) => Err(ErrorCuenta::TransaccionYaConfirmada(tx)),
            Some(Desenlace::Cancelada) => Ok(()), // reintento: idempotente
            None => {
                self.finalizadas.insert(tx, Desenlace::Cancelada);
                Ok(())
            }
        }
    }
}

/// Referencia clonable al actor: lo único que comparten los demás hilos.
///
/// Envuelve el `Sender` del buzón. Cada método arma el mensaje con un canal de
/// respuesta nuevo, lo envía y se bloquea esperando la respuesta.
#[derive(Debug, Clone)]
pub struct CuentaHandle {
    buzon: Sender<MensajeCuenta>,
}

impl CuentaHandle {
    /// Envía un mensaje y espera la respuesta.
    fn pedir<T>(&self, armar: impl FnOnce(Sender<T>) -> MensajeCuenta) -> Result<T, ErrorCuenta> {
        let (responder, respuesta) = mpsc::channel();
        self.buzon
            .send(armar(responder))
            .map_err(|_| ErrorCuenta::ActorDetenido)?;
        respuesta.recv().map_err(|_| ErrorCuenta::ActorDetenido)
    }

    pub fn consultar_saldo(&self) -> Result<SaldoCuenta, ErrorCuenta> {
        self.pedir(|responder| MensajeCuenta::ConsultarSaldo { responder })
    }

    pub fn acreditar(&self, monto: Monto) -> Result<(), ErrorCuenta> {
        self.pedir(|responder| MensajeCuenta::Acreditar { monto, responder })?
    }

    pub fn solicitar_debito(&self, tx: IdTransaccion, monto: Monto) -> Result<(), ErrorCuenta> {
        self.pedir(|responder| MensajeCuenta::SolicitarDebito {
            tx,
            monto,
            responder,
        })?
    }

    pub fn confirmar_debito(&self, tx: IdTransaccion) -> Result<(), ErrorCuenta> {
        self.pedir(|responder| MensajeCuenta::ConfirmarDebito { tx, responder })?
    }

    pub fn cancelar_debito(&self, tx: IdTransaccion) -> Result<(), ErrorCuenta> {
        self.pedir(|responder| MensajeCuenta::CancelarDebito { tx, responder })?
    }

    /// Pide al actor que termine. No espera respuesta.
    pub fn detener(&self) {
        let _ = self.buzon.send(MensajeCuenta::Detener);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TX1: IdTransaccion = IdTransaccion(1);
    const TX2: IdTransaccion = IdTransaccion(2);

    #[test]
    fn retener_reduce_disponible_sin_cobrar() {
        let mut c = CuentaUsuario::new(1_000);
        c.solicitar_debito(TX1, 300).unwrap();
        assert_eq!(
            c.saldo(),
            SaldoCuenta {
                disponible: 700,
                retenido: 300,
                debitado: 0
            }
        );
    }

    #[test]
    fn saldo_insuficiente_no_modifica_estado() {
        let mut c = CuentaUsuario::new(100);
        assert_eq!(
            c.solicitar_debito(TX1, 101),
            Err(ErrorCuenta::SaldoInsuficiente {
                disponible: 100,
                requerido: 101
            })
        );
        assert_eq!(c.saldo().disponible, 100);
        assert_eq!(c.saldo().retenido, 0);
    }

    #[test]
    fn confirmar_cobra_lo_retenido() {
        let mut c = CuentaUsuario::new(1_000);
        c.solicitar_debito(TX1, 300).unwrap();
        c.confirmar_debito(TX1).unwrap();
        assert_eq!(
            c.saldo(),
            SaldoCuenta {
                disponible: 700,
                retenido: 0,
                debitado: 300
            }
        );
    }

    #[test]
    fn cancelar_devuelve_lo_retenido() {
        let mut c = CuentaUsuario::new(1_000);
        c.solicitar_debito(TX1, 300).unwrap();
        c.cancelar_debito(TX1).unwrap();
        assert_eq!(
            c.saldo(),
            SaldoCuenta {
                disponible: 1_000,
                ..Default::default()
            }
        );
    }

    #[test]
    fn operaciones_repetidas_son_idempotentes() {
        let mut c = CuentaUsuario::new(1_000);
        c.solicitar_debito(TX1, 300).unwrap();
        c.solicitar_debito(TX1, 300).unwrap();
        assert_eq!(c.saldo().disponible, 700);
        c.confirmar_debito(TX1).unwrap();
        c.confirmar_debito(TX1).unwrap();
        assert_eq!(c.saldo().debitado, 300);

        c.solicitar_debito(TX2, 100).unwrap();
        c.cancelar_debito(TX2).unwrap();
        c.cancelar_debito(TX2).unwrap();
        assert_eq!(c.saldo().disponible, 700);
    }

    #[test]
    fn misma_tx_con_otro_monto_es_conflicto() {
        let mut c = CuentaUsuario::new(1_000);
        c.solicitar_debito(TX1, 300).unwrap();
        assert_eq!(
            c.solicitar_debito(TX1, 400),
            Err(ErrorCuenta::ConflictoTransaccion(TX1))
        );
    }

    #[test]
    fn transiciones_invalidas_se_rechazan() {
        let mut c = CuentaUsuario::new(1_000);
        assert_eq!(
            c.confirmar_debito(TX1),
            Err(ErrorCuenta::TransaccionDesconocida(TX1))
        );

        c.solicitar_debito(TX1, 100).unwrap();
        c.confirmar_debito(TX1).unwrap();
        assert_eq!(
            c.cancelar_debito(TX1),
            Err(ErrorCuenta::TransaccionYaConfirmada(TX1))
        );
        assert_eq!(
            c.solicitar_debito(TX1, 100),
            Err(ErrorCuenta::TransaccionFinalizada(TX1))
        );

        c.solicitar_debito(TX2, 100).unwrap();
        c.cancelar_debito(TX2).unwrap();
        assert_eq!(
            c.confirmar_debito(TX2),
            Err(ErrorCuenta::TransaccionYaCancelada(TX2))
        );
    }

    #[test]
    fn cancelar_tx_desconocida_impide_retener_despues() {
        let mut c = CuentaUsuario::new(1_000);
        c.cancelar_debito(TX1).unwrap();
        assert_eq!(
            c.solicitar_debito(TX1, 100),
            Err(ErrorCuenta::TransaccionFinalizada(TX1))
        );
        assert_eq!(c.saldo().disponible, 1_000);
    }

    #[test]
    fn montos_cero_y_desbordamiento() {
        let mut c = CuentaUsuario::new(Monto::MAX);
        assert_eq!(c.solicitar_debito(TX1, 0), Err(ErrorCuenta::MontoInvalido));
        assert_eq!(c.acreditar(0), Err(ErrorCuenta::MontoInvalido));
        assert_eq!(c.acreditar(1), Err(ErrorCuenta::Desbordamiento));
    }
}
