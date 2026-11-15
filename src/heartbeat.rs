//! Parte 2 - Coordinación y Tolerancia a Fallos.
//!
//! TODO:
//! - 3 nodos que envían un heartbeat cada 500ms a un Monitor (std::sync::mpsc).
//! - El Monitor usa `recv_timeout`: SOSPECHOSO a 1s sin latido, CAIDO a 2s.
//! - Simular la caída de un nodo aleatorio.
//! - Si cae el líder, disparar la elección (Bully; explicación en el informe).
