//! Actor `InventarioEntradas`: estado de los asientos de un evento.
//!
//! Ciclo de vida de un asiento:
//!
//! ```text
//!            SolicitarReserva(tx)           ConfirmarReserva(tx)
//!   Libre ───────────────────► Reservado(tx) ──────────────────► Vendido(tx)
//!     ▲                              │
//!     └──────LiberarReserva(tx)──────┘
//! ```
//!
//! Un asiento `Vendido` no vuelve nunca a `Libre`: así se garantiza que no se
//! vende dos veces.

use super::{Desenlace, IdAsiento, IdTransaccion};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

/// Mensajes que acepta el actor `InventarioEntradas`.
#[derive(Debug)]
pub enum MensajeInventario {
    /// Consulta el estado de un asiento (`None` si no existe).
    ConsultarAsiento {
        asiento: IdAsiento,
        responder: Sender<Option<EstadoAsiento>>,
    },
    /// Devuelve el estado de todos los asientos, ordenados por id.
    ListarAsientos {
        responder: Sender<Vec<(IdAsiento, EstadoAsiento)>>,
    },
    /// Fase 1 (prepare): bloquea `asiento` para la transacción `tx`.
    SolicitarReserva {
        tx: IdTransaccion,
        asiento: IdAsiento,
        responder: Sender<Result<(), ErrorInventario>>,
    },
    /// Fase 2 (commit): el asiento reservado por `tx` pasa a vendido.
    ConfirmarReserva {
        tx: IdTransaccion,
        responder: Sender<Result<(), ErrorInventario>>,
    },
    /// Fase 2 (rollback): el asiento reservado por `tx` vuelve a estar libre.
    LiberarReserva {
        tx: IdTransaccion,
        responder: Sender<Result<(), ErrorInventario>>,
    },
    /// Termina el loop del actor.
    Detener,
}

/// Estado de un asiento.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstadoAsiento {
    Libre,
    Reservado(IdTransaccion),
    Vendido(IdTransaccion),
}

/// Errores que puede devolver el actor `InventarioEntradas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorInventario {
    /// El asiento no existe en este inventario.
    AsientoInexistente(IdAsiento),
    /// El asiento está reservado o vendido por otra transacción.
    AsientoNoDisponible(IdAsiento),
    /// La misma `tx` ya reservó otro asiento.
    ConflictoTransaccion(IdTransaccion),
    /// La `tx` ya terminó (confirmada o cancelada) y no se puede reabrir.
    TransaccionFinalizada(IdTransaccion),
    /// Se pidió confirmar una `tx` que nunca reservó.
    TransaccionDesconocida(IdTransaccion),
    /// Se pidió confirmar una `tx` cuya reserva ya se liberó.
    TransaccionYaCancelada(IdTransaccion),
    /// Se pidió liberar una `tx` ya confirmada (el asiento ya se vendió).
    TransaccionYaConfirmada(IdTransaccion),
    /// El actor ya no está corriendo (su buzón está cerrado).
    ActorDetenido,
}

impl fmt::Display for ErrorInventario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorInventario::AsientoInexistente(a) => write!(f, "el asiento {a} no existe"),
            ErrorInventario::AsientoNoDisponible(a) => {
                write!(f, "el asiento {a} no está disponible")
            }
            ErrorInventario::ConflictoTransaccion(tx) => write!(f, "{tx} ya reservó otro asiento"),
            ErrorInventario::TransaccionFinalizada(tx) => write!(f, "{tx} ya finalizó"),
            ErrorInventario::TransaccionDesconocida(tx) => write!(f, "{tx} desconocida"),
            ErrorInventario::TransaccionYaCancelada(tx) => write!(f, "{tx} ya fue cancelada"),
            ErrorInventario::TransaccionYaConfirmada(tx) => write!(f, "{tx} ya fue confirmada"),
            ErrorInventario::ActorDetenido => write!(f, "el actor del inventario está detenido"),
        }
    }
}

impl std::error::Error for ErrorInventario {}

/// Estado privado del actor. Solo lo toca el hilo del actor.
#[derive(Debug, Default)]
pub struct InventarioEntradas {
    asientos: BTreeMap<IdAsiento, EstadoAsiento>,
    /// Reservas en fase 1: `tx` → asiento reservado.
    reservas: HashMap<IdTransaccion, IdAsiento>,
    /// Transacciones terminadas, para responder de forma idempotente.
    finalizadas: HashMap<IdTransaccion, Desenlace>,
}

impl InventarioEntradas {
    /// Crea un inventario con los asientos dados, todos libres.
    pub fn new(asientos: impl IntoIterator<Item = IdAsiento>) -> Self {
        InventarioEntradas {
            asientos: asientos
                .into_iter()
                .map(|a| (a, EstadoAsiento::Libre))
                .collect(),
            ..Default::default()
        }
    }

    /// Lanza el actor en su propio hilo. `self` se mueve al hilo.
    pub fn iniciar(self) -> (InventarioHandle, JoinHandle<()>) {
        let (buzon, rx) = mpsc::channel();
        let hilo = thread::spawn(move || self.ejecutar(rx));
        (InventarioHandle { buzon }, hilo)
    }

    /// Loop del actor: un mensaje por vez, en orden de llegada.
    fn ejecutar(mut self, rx: Receiver<MensajeInventario>) {
        for msg in rx {
            if !self.manejar(msg) {
                break;
            }
        }
    }

    /// Procesa un mensaje. Devuelve `false` si el actor debe detenerse.
    fn manejar(&mut self, msg: MensajeInventario) -> bool {
        match msg {
            MensajeInventario::ConsultarAsiento { asiento, responder } => {
                let _ = responder.send(self.consultar_asiento(asiento));
            }
            MensajeInventario::ListarAsientos { responder } => {
                let _ = responder.send(self.listar_asientos());
            }
            MensajeInventario::SolicitarReserva {
                tx,
                asiento,
                responder,
            } => {
                let _ = responder.send(self.solicitar_reserva(tx, asiento));
            }
            MensajeInventario::ConfirmarReserva { tx, responder } => {
                let _ = responder.send(self.confirmar_reserva(tx));
            }
            MensajeInventario::LiberarReserva { tx, responder } => {
                let _ = responder.send(self.liberar_reserva(tx));
            }
            MensajeInventario::Detener => return false,
        }
        true
    }

    // ----- Lógica pura (testeable sin hilos) -----

    pub fn consultar_asiento(&self, asiento: IdAsiento) -> Option<EstadoAsiento> {
        self.asientos.get(&asiento).copied()
    }

    pub fn listar_asientos(&self) -> Vec<(IdAsiento, EstadoAsiento)> {
        self.asientos.iter().map(|(&a, &e)| (a, e)).collect()
    }

    /// Fase 1: chequea que el asiento esté libre y lo reserva en un solo paso.
    pub fn solicitar_reserva(
        &mut self,
        tx: IdTransaccion,
        asiento: IdAsiento,
    ) -> Result<(), ErrorInventario> {
        if self.finalizadas.contains_key(&tx) {
            return Err(ErrorInventario::TransaccionFinalizada(tx));
        }
        if let Some(&reservado) = self.reservas.get(&tx) {
            // Reintento del mismo mensaje: idempotente.
            return if reservado == asiento {
                Ok(())
            } else {
                Err(ErrorInventario::ConflictoTransaccion(tx))
            };
        }
        let estado = self
            .asientos
            .get_mut(&asiento)
            .ok_or(ErrorInventario::AsientoInexistente(asiento))?;
        if *estado != EstadoAsiento::Libre {
            return Err(ErrorInventario::AsientoNoDisponible(asiento));
        }
        *estado = EstadoAsiento::Reservado(tx);
        self.reservas.insert(tx, asiento);
        Ok(())
    }

    /// Fase 2 (commit): el asiento reservado por `tx` queda vendido.
    pub fn confirmar_reserva(&mut self, tx: IdTransaccion) -> Result<(), ErrorInventario> {
        if let Some(asiento) = self.reservas.remove(&tx) {
            self.asientos.insert(asiento, EstadoAsiento::Vendido(tx));
            self.finalizadas.insert(tx, Desenlace::Confirmada);
            return Ok(());
        }
        match self.finalizadas.get(&tx) {
            Some(Desenlace::Confirmada) => Ok(()), // reintento: idempotente
            Some(Desenlace::Cancelada) => Err(ErrorInventario::TransaccionYaCancelada(tx)),
            None => Err(ErrorInventario::TransaccionDesconocida(tx)),
        }
    }

    /// Fase 2 (rollback): el asiento reservado por `tx` vuelve a estar libre.
    ///
    /// Liberar una `tx` desconocida es válido (aborto presunto) y la registra
    /// como cancelada para que una reserva tardía no la reviva.
    pub fn liberar_reserva(&mut self, tx: IdTransaccion) -> Result<(), ErrorInventario> {
        if let Some(asiento) = self.reservas.remove(&tx) {
            self.asientos.insert(asiento, EstadoAsiento::Libre);
            self.finalizadas.insert(tx, Desenlace::Cancelada);
            return Ok(());
        }
        match self.finalizadas.get(&tx) {
            Some(Desenlace::Confirmada) => Err(ErrorInventario::TransaccionYaConfirmada(tx)),
            Some(Desenlace::Cancelada) => Ok(()), // reintento: idempotente
            None => {
                self.finalizadas.insert(tx, Desenlace::Cancelada);
                Ok(())
            }
        }
    }
}

/// Referencia clonable al actor (envuelve el `Sender` de su buzón).
#[derive(Debug, Clone)]
pub struct InventarioHandle {
    buzon: Sender<MensajeInventario>,
}

impl InventarioHandle {
    /// Envía un mensaje y espera la respuesta.
    fn pedir<T>(
        &self,
        armar: impl FnOnce(Sender<T>) -> MensajeInventario,
    ) -> Result<T, ErrorInventario> {
        let (responder, respuesta) = mpsc::channel();
        self.buzon
            .send(armar(responder))
            .map_err(|_| ErrorInventario::ActorDetenido)?;
        respuesta.recv().map_err(|_| ErrorInventario::ActorDetenido)
    }

    pub fn consultar_asiento(
        &self,
        asiento: IdAsiento,
    ) -> Result<Option<EstadoAsiento>, ErrorInventario> {
        self.pedir(|responder| MensajeInventario::ConsultarAsiento { asiento, responder })
    }

    pub fn listar_asientos(&self) -> Result<Vec<(IdAsiento, EstadoAsiento)>, ErrorInventario> {
        self.pedir(|responder| MensajeInventario::ListarAsientos { responder })
    }

    pub fn solicitar_reserva(
        &self,
        tx: IdTransaccion,
        asiento: IdAsiento,
    ) -> Result<(), ErrorInventario> {
        self.pedir(|responder| MensajeInventario::SolicitarReserva {
            tx,
            asiento,
            responder,
        })?
    }

    pub fn confirmar_reserva(&self, tx: IdTransaccion) -> Result<(), ErrorInventario> {
        self.pedir(|responder| MensajeInventario::ConfirmarReserva { tx, responder })?
    }

    pub fn liberar_reserva(&self, tx: IdTransaccion) -> Result<(), ErrorInventario> {
        self.pedir(|responder| MensajeInventario::LiberarReserva { tx, responder })?
    }

    /// Pide al actor que termine. No espera respuesta.
    pub fn detener(&self) {
        let _ = self.buzon.send(MensajeInventario::Detener);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TX1: IdTransaccion = IdTransaccion(1);
    const TX2: IdTransaccion = IdTransaccion(2);

    #[test]
    fn reservar_confirmar_vende_el_asiento() {
        let mut inv = InventarioEntradas::new([1, 2]);
        inv.solicitar_reserva(TX1, 1).unwrap();
        assert_eq!(
            inv.consultar_asiento(1),
            Some(EstadoAsiento::Reservado(TX1))
        );
        inv.confirmar_reserva(TX1).unwrap();
        assert_eq!(inv.consultar_asiento(1), Some(EstadoAsiento::Vendido(TX1)));
        assert_eq!(inv.consultar_asiento(2), Some(EstadoAsiento::Libre));
    }

    #[test]
    fn asiento_reservado_o_vendido_no_se_puede_reservar() {
        let mut inv = InventarioEntradas::new([1]);
        inv.solicitar_reserva(TX1, 1).unwrap();
        assert_eq!(
            inv.solicitar_reserva(TX2, 1),
            Err(ErrorInventario::AsientoNoDisponible(1))
        );
        inv.confirmar_reserva(TX1).unwrap();
        assert_eq!(
            inv.solicitar_reserva(TX2, 1),
            Err(ErrorInventario::AsientoNoDisponible(1))
        );
    }

    #[test]
    fn liberar_devuelve_el_asiento() {
        let mut inv = InventarioEntradas::new([1]);
        inv.solicitar_reserva(TX1, 1).unwrap();
        inv.liberar_reserva(TX1).unwrap();
        assert_eq!(inv.consultar_asiento(1), Some(EstadoAsiento::Libre));
        inv.solicitar_reserva(TX2, 1).unwrap();
    }

    #[test]
    fn asiento_inexistente() {
        let mut inv = InventarioEntradas::new([1]);
        assert_eq!(
            inv.solicitar_reserva(TX1, 99),
            Err(ErrorInventario::AsientoInexistente(99))
        );
        assert_eq!(inv.consultar_asiento(99), None);
    }

    #[test]
    fn operaciones_repetidas_son_idempotentes() {
        let mut inv = InventarioEntradas::new([1, 2]);
        inv.solicitar_reserva(TX1, 1).unwrap();
        inv.solicitar_reserva(TX1, 1).unwrap();
        assert_eq!(
            inv.solicitar_reserva(TX1, 2),
            Err(ErrorInventario::ConflictoTransaccion(TX1))
        );
        inv.confirmar_reserva(TX1).unwrap();
        inv.confirmar_reserva(TX1).unwrap();

        inv.solicitar_reserva(TX2, 2).unwrap();
        inv.liberar_reserva(TX2).unwrap();
        inv.liberar_reserva(TX2).unwrap();
        assert_eq!(inv.consultar_asiento(2), Some(EstadoAsiento::Libre));
    }

    #[test]
    fn transiciones_invalidas_se_rechazan() {
        let mut inv = InventarioEntradas::new([1, 2]);
        assert_eq!(
            inv.confirmar_reserva(TX1),
            Err(ErrorInventario::TransaccionDesconocida(TX1))
        );

        inv.solicitar_reserva(TX1, 1).unwrap();
        inv.confirmar_reserva(TX1).unwrap();
        assert_eq!(
            inv.liberar_reserva(TX1),
            Err(ErrorInventario::TransaccionYaConfirmada(TX1))
        );
        assert_eq!(inv.consultar_asiento(1), Some(EstadoAsiento::Vendido(TX1)));

        inv.solicitar_reserva(TX2, 2).unwrap();
        inv.liberar_reserva(TX2).unwrap();
        assert_eq!(
            inv.confirmar_reserva(TX2),
            Err(ErrorInventario::TransaccionYaCancelada(TX2))
        );
    }

    #[test]
    fn liberar_tx_desconocida_impide_reservar_despues() {
        let mut inv = InventarioEntradas::new([1]);
        inv.liberar_reserva(TX1).unwrap();
        assert_eq!(
            inv.solicitar_reserva(TX1, 1),
            Err(ErrorInventario::TransaccionFinalizada(TX1))
        );
        assert_eq!(inv.consultar_asiento(1), Some(EstadoAsiento::Libre));
    }
}
