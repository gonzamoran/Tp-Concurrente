//! Demo de la Parte 1: actores CuentaUsuario e InventarioEntradas.
//! Ejecutar con: cargo run --bin parte1_actores
//!
//! Muchos hilos "cliente" compiten por pocos asientos al mismo tiempo. Cada
//! cliente usa la cuenta de un usuario (varios clientes comparten cuenta) y
//! prueba los asientos en orden hasta conseguir uno o quedarse sin saldo.
//!
//! Al final se verifican los invariantes del negocio:
//! - ningún asiento se vendió dos veces y no quedan reservas colgadas;
//! - el dinero se conserva: disponible + cobrado == saldo inicial total;
//! - lo cobrado es exactamente (asientos vendidos × precio).

use std::thread;
use tp2::actores::{
    CuentaHandle, CuentaUsuario, ErrorCompra, ErrorInventario, EstadoAsiento, IdAsiento,
    IdTransaccion, InventarioEntradas, Monto, comprar_entrada, formatear_monto,
};

const CANT_ASIENTOS: IdAsiento = 5;
const CANT_CLIENTES: u64 = 50;
const PRECIO: Monto = 15_000; // $150.00
/// Saldos iniciales de las cuentas, en centavos. Algunas alcanzan para varias
/// entradas, otras para una sola y otras para ninguna.
const SALDOS_INICIALES: [Monto; 5] = [100_000, 30_000, 15_000, 10_000, 0];

/// Resultado de un cliente.
enum Resultado {
    Compro { asiento: IdAsiento, cuenta: usize },
    SinSaldo,
    SinAsientos,
}

fn main() {
    let (inventario, hilo_inventario) = InventarioEntradas::new(1..=CANT_ASIENTOS).iniciar();
    let (cuentas, hilos_cuentas): (Vec<CuentaHandle>, Vec<_>) = SALDOS_INICIALES
        .iter()
        .map(|&saldo| CuentaUsuario::new(saldo).iniciar())
        .unzip();

    println!(
        "{CANT_CLIENTES} clientes compiten por {CANT_ASIENTOS} asientos a {} cada uno",
        formatear_monto(PRECIO)
    );

    // Lanzar todos los clientes a la vez. Solo comparten handles (Senders).
    let clientes: Vec<_> = (0..CANT_CLIENTES)
        .map(|i| {
            let inventario = inventario.clone();
            let id_cuenta = i as usize % cuentas.len();
            let cuenta = cuentas[id_cuenta].clone();
            thread::spawn(move || {
                let tx = IdTransaccion(i);
                // Cada cliente empieza por un asiento distinto y sigue en orden.
                for k in 0..CANT_ASIENTOS {
                    let asiento = (i as IdAsiento + k) % CANT_ASIENTOS + 1;
                    match comprar_entrada(&inventario, &cuenta, tx, asiento, PRECIO) {
                        Ok(()) => {
                            return Resultado::Compro {
                                asiento,
                                cuenta: id_cuenta,
                            };
                        }
                        // Ocupado: probar el siguiente con la misma tx (no quedó registrada).
                        Err(ErrorCompra::Inventario(ErrorInventario::AsientoNoDisponible(_))) => {}
                        Err(ErrorCompra::Cuenta(_)) => return Resultado::SinSaldo,
                        Err(e) => panic!("error inesperado en {tx}: {e}"),
                    }
                }
                Resultado::SinAsientos
            })
        })
        .collect();

    let mut compras = Vec::new();
    let (mut sin_saldo, mut sin_asientos) = (0, 0);
    for (i, cliente) in clientes.into_iter().enumerate() {
        match cliente.join().expect("un cliente entró en pánico") {
            Resultado::Compro { asiento, cuenta } => {
                println!("  cliente {i:>2} (cuenta {cuenta}) compró el asiento {asiento}");
                compras.push(asiento);
            }
            Resultado::SinSaldo => sin_saldo += 1,
            Resultado::SinAsientos => sin_asientos += 1,
        }
    }
    println!(
        "{} compras, {sin_saldo} rechazadas por saldo, {sin_asientos} sin asientos libres",
        compras.len()
    );

    // Verificación de invariantes
    let asientos = inventario.listar_asientos().unwrap();
    println!("\nEstado final del inventario:");
    for (asiento, estado) in &asientos {
        println!("  asiento {asiento}: {estado:?}");
    }
    let vendidos: Vec<IdTransaccion> = asientos
        .iter()
        .filter_map(|(_, e)| match e {
            EstadoAsiento::Vendido(tx) => Some(*tx),
            _ => None,
        })
        .collect();
    let reservados = asientos
        .iter()
        .filter(|(_, e)| matches!(e, EstadoAsiento::Reservado(_)))
        .count();

    let mut txs_unicas = vendidos.clone();
    txs_unicas.sort();
    txs_unicas.dedup();
    compras.sort();
    let compras_sin_repetir = {
        let mut c = compras.clone();
        c.dedup();
        c.len()
    };

    println!("\nSaldos finales:");
    let (mut disponible, mut retenido, mut debitado) = (0, 0, 0);
    for (i, cuenta) in cuentas.iter().enumerate() {
        let s = cuenta.consultar_saldo().unwrap();
        println!(
            "  cuenta {i}: inicial {:>9}  disponible {:>9}  cobrado {:>9}",
            formatear_monto(SALDOS_INICIALES[i]),
            formatear_monto(s.disponible),
            formatear_monto(s.debitado)
        );
        disponible += s.disponible;
        retenido += s.retenido;
        debitado += s.debitado;
    }
    let total_inicial: Monto = SALDOS_INICIALES.iter().sum();

    println!("\nInvariantes:");
    verificar(
        "cada compra exitosa corresponde a un asiento vendido distinto",
        compras.len() == vendidos.len() && compras_sin_repetir == compras.len(),
    );
    verificar(
        "ninguna transacción tiene más de un asiento",
        txs_unicas.len() == vendidos.len(),
    );
    verificar("no quedan reservas colgadas", reservados == 0);
    verificar("no quedan fondos retenidos", retenido == 0);
    verificar(
        "el dinero se conserva (disponible + cobrado == inicial)",
        disponible + debitado == total_inicial,
    );
    verificar(
        "lo cobrado es exactamente vendidos × precio",
        debitado == vendidos.len() as Monto * PRECIO,
    );

    // Apagado ordenado: Detener a cada actor y esperar sus hilos.
    inventario.detener();
    cuentas.iter().for_each(CuentaHandle::detener);
    hilo_inventario.join().unwrap();
    hilos_cuentas.into_iter().for_each(|h| h.join().unwrap());
}

fn verificar(descripcion: &str, cumple: bool) {
    println!("  [{}] {descripcion}", if cumple { "OK" } else { "FALLA" });
    assert!(cumple, "invariante violado: {descripcion}");
}
