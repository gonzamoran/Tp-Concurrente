//! Tests de integración de la Parte 1: los actores bajo concurrencia real.
//!
//! Cada test lanza muchos hilos que mandan mensajes al mismo actor a la vez y
//! verifica que el resultado sea el de una ejecución serial (sin data races ni
//! check-and-act intercalados).

use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tp2::actores::{
    CuentaUsuario, ErrorCompra, ErrorCuenta, ErrorInventario, EstadoAsiento, IdTransaccion,
    InventarioEntradas, Monto, comprar_entrada,
};

/// 1000 hilos piden el mismo asiento a la vez: exactamente uno lo consigue.
#[test]
fn mil_hilos_un_solo_asiento() {
    let (inventario, hilo) = InventarioEntradas::new([1]).iniciar();

    let resultados: Vec<_> = thread::scope(|s| {
        let pedidos: Vec<_> = (0..1000)
            .map(|i| {
                let inventario = inventario.clone();
                s.spawn(move || inventario.solicitar_reserva(IdTransaccion(i), 1))
            })
            .collect();
        pedidos.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let exitos: Vec<_> = resultados.iter().filter(|r| r.is_ok()).collect();
    assert_eq!(exitos.len(), 1);
    assert!(
        resultados
            .iter()
            .filter(|r| r.is_err())
            .all(|r| *r == Err(ErrorInventario::AsientoNoDisponible(1)))
    );
    assert!(matches!(
        inventario.consultar_asiento(1).unwrap(),
        Some(EstadoAsiento::Reservado(_))
    ));

    inventario.detener();
    hilo.join().unwrap();
}

/// 100 hilos intentan debitar $5 de una cuenta que tiene saldo para 10 débitos.
#[test]
fn debitos_concurrentes_no_sobregiran() {
    const MONTO: Monto = 500;
    const ALCANZA_PARA: u64 = 10;
    let (cuenta, hilo) = CuentaUsuario::new(MONTO * ALCANZA_PARA).iniciar();

    let exitos = thread::scope(|s| {
        let pedidos: Vec<_> = (0..100)
            .map(|i| {
                let cuenta = cuenta.clone();
                s.spawn(move || {
                    let tx = IdTransaccion(i);
                    match cuenta.solicitar_debito(tx, MONTO) {
                        Ok(()) => {
                            cuenta.confirmar_debito(tx).unwrap();
                            true
                        }
                        Err(ErrorCuenta::SaldoInsuficiente { .. }) => false,
                        Err(e) => panic!("error inesperado: {e}"),
                    }
                })
            })
            .collect();
        pedidos
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|&ok| ok)
            .count() as u64
    });

    assert_eq!(exitos, ALCANZA_PARA);
    let saldo = cuenta.consultar_saldo().unwrap();
    assert_eq!(saldo.disponible, 0);
    assert_eq!(saldo.retenido, 0);
    assert_eq!(saldo.debitado, MONTO * ALCANZA_PARA);

    cuenta.detener();
    hilo.join().unwrap();
}

/// Acreditaciones y débitos mezclados desde muchos hilos: el saldo final es
/// exacto (ninguna actualización se pierde).
#[test]
fn no_se_pierden_actualizaciones() {
    let (cuenta, hilo) = CuentaUsuario::new(0).iniciar();

    thread::scope(|s| {
        for i in 0..200 {
            let cuenta = cuenta.clone();
            s.spawn(move || {
                cuenta.acreditar(300).unwrap();
                let tx = IdTransaccion(i);
                cuenta.solicitar_debito(tx, 100).unwrap();
                if i % 2 == 0 {
                    cuenta.confirmar_debito(tx).unwrap();
                } else {
                    cuenta.cancelar_debito(tx).unwrap();
                }
            });
        }
    });

    let saldo = cuenta.consultar_saldo().unwrap();
    // 200 × $3 acreditados; 100 débitos de $1 confirmados.
    assert_eq!(saldo.debitado, 100 * 100);
    assert_eq!(saldo.disponible, 200 * 300 - 100 * 100);
    assert_eq!(saldo.retenido, 0);

    drop(cuenta);
    hilo.join().unwrap();
}

/// Compras completas concurrentes, no hay doble venta y el dinero se conserva.
#[test]
fn compras_concurrentes_conservan_invariantes() {
    const PRECIO: Monto = 1_000;
    const ASIENTOS: u32 = 20;
    let saldos: [Monto; 4] = [5_000, 7_000, 2_500, 0];

    let (inventario, hilo_inv) = InventarioEntradas::new(1..=ASIENTOS).iniciar();
    let (cuentas, hilos_cuentas): (Vec<_>, Vec<_>) = saldos
        .iter()
        .map(|&s| CuentaUsuario::new(s).iniciar())
        .unzip();

    let compras: u64 = thread::scope(|s| {
        let clientes: Vec<_> = (0..200u64)
            .map(|i| {
                let inventario = inventario.clone();
                let cuenta = cuentas[i as usize % cuentas.len()].clone();
                s.spawn(move || {
                    let tx = IdTransaccion(i);
                    for k in 0..ASIENTOS {
                        let asiento = (i as u32 + k) % ASIENTOS + 1;
                        match comprar_entrada(&inventario, &cuenta, tx, asiento, PRECIO) {
                            Ok(()) => return 1,
                            Err(ErrorCompra::Inventario(ErrorInventario::AsientoNoDisponible(
                                _,
                            ))) => continue,
                            Err(ErrorCompra::Cuenta(ErrorCuenta::SaldoInsuficiente { .. })) => {
                                return 0;
                            }
                            Err(e) => panic!("error inesperado: {e}"),
                        }
                    }
                    0
                })
            })
            .collect();
        clientes.into_iter().map(|h| h.join().unwrap()).sum()
    });

    let asientos = inventario.listar_asientos().unwrap();
    let vendidos = asientos
        .iter()
        .filter(|(_, e)| matches!(e, EstadoAsiento::Vendido(_)))
        .count() as u64;
    let reservados = asientos
        .iter()
        .filter(|(_, e)| matches!(e, EstadoAsiento::Reservado(_)))
        .count();

    let (mut disponible, mut retenido, mut debitado) = (0, 0, 0);
    for c in &cuentas {
        let s = c.consultar_saldo().unwrap();
        disponible += s.disponible;
        retenido += s.retenido;
        debitado += s.debitado;
    }

    // Plata total = 14.500 → alcanza para 14 entradas (cada cuenta por separado:
    // 5 + 7 + 2 + 0), y hay 20 asientos.
    assert_eq!(compras, 14);
    assert_eq!(vendidos, compras);
    assert_eq!(reservados, 0);
    assert_eq!(retenido, 0);
    assert_eq!(debitado, vendidos * PRECIO);
    assert_eq!(disponible + debitado, saldos.iter().sum::<Monto>());

    inventario.detener();
    cuentas.iter().for_each(|c| c.detener());
    hilo_inv.join().unwrap();
    hilos_cuentas.into_iter().for_each(|h| h.join().unwrap());
}

/// El actor termina con `Detener` o al soltar todos los handles, y después de
/// eso los pedidos devuelven `ActorDetenido` en lugar de colgarse.
#[test]
fn apagado_del_actor() {
    // Detener explícito.
    let (cuenta, hilo) = CuentaUsuario::new(100).iniciar();
    cuenta.detener();
    unir_con_timeout(hilo);
    assert_eq!(cuenta.consultar_saldo(), Err(ErrorCuenta::ActorDetenido));

    // Soltar todos los handles cierra el buzón y termina el loop.
    let (inventario, hilo) = InventarioEntradas::new([1]).iniciar();
    let copia = inventario.clone();
    drop(inventario);
    drop(copia);
    unir_con_timeout(hilo);
}

/// `join` con límite de tiempo, para que un actor colgado haga fallar el test
/// en lugar de bloquearlo para siempre.
fn unir_con_timeout(hilo: thread::JoinHandle<()>) {
    let (avisar, aviso) = mpsc::channel();
    thread::spawn(move || {
        hilo.join().unwrap();
        let _ = avisar.send(());
    });
    aviso
        .recv_timeout(Duration::from_secs(2))
        .expect("el actor no terminó");
}
