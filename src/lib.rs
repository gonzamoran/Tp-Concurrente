//! TP2 - Motor Distribuido de Reservas y Pagos.
//!
//! Cada parte del enunciado vive en su propio módulo para que la lógica
//! se pueda testear desde `tests/` y usar desde los binarios de `src/bin/`.

/// Parte 1: actores `CuentaUsuario` e `InventarioEntradas` y sus mensajes.
pub mod actores;

/// Parte 2: detección de fallos con heartbeats (std::sync::mpsc + recv_timeout).
pub mod heartbeat;

/// Parte 3: coordinador de Two-Phase Commit (ServicioPagos + ServicioInventario).
pub mod two_phase_commit;
