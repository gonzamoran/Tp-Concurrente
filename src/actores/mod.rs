//! Parte 1 - Modelos de Concurrencia y Actores.
//!
//! Las entidades `CuentaUsuario` e `InventarioEntradas` se modelan como
//! **actores**: cada una corre en su propio hilo, es dueña exclusiva de su
//! estado y solo se comunica mediante mensajes (`std::sync::mpsc`).
//!
//! Nadie más que el hilo del actor puede tocar su estado: el estado se *mueve*
//! al hilo al iniciarlo y los demás solo tienen un `Handle` que envuelve el
//! `Sender` de su buzón. Por eso no hacen falta locks sobre el estado.

pub mod cuenta_usuario;
pub mod inventario_entradas;

pub use cuenta_usuario::{CuentaHandle, CuentaUsuario, ErrorCuenta, MensajeCuenta, SaldoCuenta};
pub use inventario_entradas::{
    ErrorInventario, EstadoAsiento, InventarioEntradas, InventarioHandle, MensajeInventario,
};

use std::fmt;

/// Identificador único de una transacción de compra.
///
/// Es la clave de idempotencia: repetir un mensaje con el mismo `IdTransaccion`
/// no duplica su efecto (necesario para reintentos en el 2PC).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IdTransaccion(pub u64);

impl fmt::Display for IdTransaccion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tx#{}", self.0)
    }
}

/// Identificador de un asiento dentro del inventario.
pub type IdAsiento = u32;

/// Montos de dinero expresados en centavos.
///
/// Se usa un entero y nunca `f64`: los flotantes no representan exactamente
/// valores como 0.10 y acumulan error de redondeo, lo que haría que los cobros
/// no sean exactos.
pub type Monto = u64;

/// Formatea un monto en centavos como `$1234.56`.
pub fn formatear_monto(centavos: Monto) -> String {
    format!("${}.{:02}", centavos / 100, centavos % 100)
}

/// Cómo terminó una transacción que ya no está pendiente.
///
/// Los actores recuerdan el desenlace para responder de forma idempotente y
/// para rechazar transiciones inválidas (ej.: confirmar algo ya cancelado).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Desenlace {
    Confirmada,
    Cancelada,
}

/// Error de una compra orquestada con [`comprar_entrada`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCompra {
    Inventario(ErrorInventario),
    Cuenta(ErrorCuenta),
}

impl fmt::Display for ErrorCompra {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCompra::Inventario(e) => write!(f, "inventario: {e}"),
            ErrorCompra::Cuenta(e) => write!(f, "cuenta: {e}"),
        }
    }
}

impl std::error::Error for ErrorCompra {}

/// Compra de una entrada orquestando los dos actores.
///
/// Flujo: reservar el asiento → retener los fondos → confirmar ambos.
/// Si la retención falla, se libera la reserva y se cancela el débito.
///
/// Es una orquestación simple y local: no tolera la caída de quien la
/// ejecuta entre la fase provisoria y la definitiva.
pub fn comprar_entrada(
    inventario: &InventarioHandle,
    cuenta: &CuentaHandle,
    tx: IdTransaccion,
    asiento: IdAsiento,
    precio: Monto,
) -> Result<(), ErrorCompra> {
    // Fase provisoria (equivale al "prepare" del 2PC).
    inventario
        .solicitar_reserva(tx, asiento)
        .map_err(ErrorCompra::Inventario)?;

    if let Err(e) = cuenta.solicitar_debito(tx, precio) {
        // Deshacer en ambos lados. Cancelar un débito que nunca se retuvo es
        // válido y evita que un mensaje tardío lo reviva.
        let _ = inventario.liberar_reserva(tx);
        let _ = cuenta.cancelar_debito(tx);
        return Err(ErrorCompra::Cuenta(e));
    }

    inventario
        .confirmar_reserva(tx)
        .map_err(ErrorCompra::Inventario)?;
    cuenta.confirmar_debito(tx).map_err(ErrorCompra::Cuenta)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formatea_montos_en_centavos() {
        assert_eq!(formatear_monto(0), "$0.00");
        assert_eq!(formatear_monto(5), "$0.05");
        assert_eq!(formatear_monto(15_050), "$150.50");
    }
}
