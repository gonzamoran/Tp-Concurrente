# TP2 - Motor Distribuido de Reservas y Pagos

Programación Concurrente - FIUBA.

## Estructura

| Ruta | Contenido |
|------|-----------|
| `src/actores/` | Parte 1: actores `CuentaUsuario` e `InventarioEntradas` y sus enums de mensajes |
| `src/heartbeat.rs` | Parte 2: heartbeats, Monitor, detección de fallos |
| `src/two_phase_commit.rs` | Parte 3: coordinador 2PC |
| `src/bin/` | Un ejecutable por parte |
| `tests/` | Tests de integración |
| `docs/informe.md` | Borrador del informe (Partes 1-4, plan de pruebas, Petri) |

## Uso

```sh
cargo build
cargo run --bin parte1_actores
cargo run --bin parte2_heartbeat
cargo run --bin parte3_2pc
cargo test
```
