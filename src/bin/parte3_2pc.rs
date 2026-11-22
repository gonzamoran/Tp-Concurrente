//! Demo de la Parte 3: Two-Phase Commit de una compra.
//! Ejecutar con: cargo run --bin parte3_2pc
//!
//! Se ejecutan cinco compras, cada una con un 2PC entre `ServicioPagos` y
//! `ServicioInventario`:
//! 1. Compra normal: los dos votan YES y se confirma.
//! 2. Saldo insuficiente: Pagos vota NO y se libera el asiento.
//! 3. Asiento ya vendido: Inventario vota NO y se devuelve el dinero.
//! 4. Cuenta caída: su voto cuenta como NO. El rollback a Pagos no se puede
//!    confirmar y la transacción queda incompleta.
//! 5. Partición de red después de que los dos votan YES (Parte 3.2): los
//!    servicios quedan bloqueados con el asiento reservado y el dinero
//!    retenido, y otra compra de ese asiento, hecha desde otro coordinador, se
//!    rechaza. Al volver la red, el coordinador reenvía la decisión de su log
//!    y la compra se completa.
//!
//! Después de cada compra se verifica que haya pasado todo o nada.

use tp2::actores::{
    CuentaHandle, CuentaUsuario, EstadoAsiento, IdTransaccion, InventarioEntradas,
    InventarioHandle, Monto, SaldoCuenta, formatear_monto,
};
use tp2::two_phase_commit::{
    Compra, Coordinador, Decision, Enlace, Participante, ResultadoTransaccion, ServicioInventario,
    ServicioPagos,
};

const PRECIO: Monto = 15_000; // $150.00

fn main() {
    let (inventario, hilo_inventario) = InventarioEntradas::new(1..=3).iniciar();
    let (cuenta_1, hilo_1) = CuentaUsuario::new(40_000).iniciar();
    let (cuenta_2, hilo_2) = CuentaUsuario::new(10_000).iniciar();
    let (cuenta_3, hilo_3) = CuentaUsuario::new(30_000).iniciar();
    // Una cuenta cuyo actor se detiene antes de empezar, para simular un
    // servicio caído.
    let (cuenta_caida, hilo_caida) = CuentaUsuario::new(50_000).iniciar();
    cuenta_caida.detener();
    hilo_caida.join().unwrap();

    let mut coordinador = Coordinador::new();
    println!("Entradas a {} cada una", formatear_monto(PRECIO));

    // 1. Compra normal.
    let compra = Compra {
        tx: IdTransaccion(1),
        asiento: 1,
        precio: PRECIO,
    };
    let resultado = comprar(
        &mut coordinador,
        "la cuenta 1 ($400.00) compra el asiento 1",
        &cuenta_1,
        &inventario,
        compra,
    );
    verificar("se confirma", resultado == ResultadoTransaccion::Confirmada);
    verificar(
        "el asiento 1 queda vendido",
        inventario.consultar_asiento(1).unwrap() == Some(EstadoAsiento::Vendido(compra.tx)),
    );
    verificar(
        "se cobra el precio",
        cuenta_1.consultar_saldo().unwrap()
            == SaldoCuenta {
                disponible: 25_000,
                retenido: 0,
                debitado: PRECIO,
            },
    );

    // 2. Saldo insuficiente.
    let compra = Compra {
        tx: IdTransaccion(2),
        asiento: 2,
        precio: PRECIO,
    };
    let resultado = comprar(
        &mut coordinador,
        "la cuenta 2 ($100.00) intenta comprar el asiento 2",
        &cuenta_2,
        &inventario,
        compra,
    );
    verificar(
        "se aborta",
        matches!(resultado, ResultadoTransaccion::Abortada { .. }),
    );
    verificar(
        "el asiento 2 sigue libre",
        inventario.consultar_asiento(2).unwrap() == Some(EstadoAsiento::Libre),
    );
    verificar(
        "la cuenta 2 queda como estaba",
        cuenta_2.consultar_saldo().unwrap()
            == SaldoCuenta {
                disponible: 10_000,
                ..Default::default()
            },
    );

    // 3. Asiento ya vendido.
    let compra = Compra {
        tx: IdTransaccion(3),
        asiento: 1,
        precio: PRECIO,
    };
    let resultado = comprar(
        &mut coordinador,
        "la cuenta 1 intenta comprar el asiento 1, que ya está vendido",
        &cuenta_1,
        &inventario,
        compra,
    );
    verificar(
        "se aborta",
        matches!(resultado, ResultadoTransaccion::Abortada { .. }),
    );
    verificar(
        "el asiento 1 sigue vendido a la primera compra",
        inventario.consultar_asiento(1).unwrap() == Some(EstadoAsiento::Vendido(IdTransaccion(1))),
    );
    verificar(
        "a la cuenta 1 se le devuelve lo retenido",
        cuenta_1.consultar_saldo().unwrap()
            == SaldoCuenta {
                disponible: 25_000,
                retenido: 0,
                debitado: PRECIO,
            },
    );

    // 4. Servicio de pagos caído.
    let compra = Compra {
        tx: IdTransaccion(4),
        asiento: 3,
        precio: PRECIO,
    };
    let resultado = comprar(
        &mut coordinador,
        "una cuenta con el actor caído intenta comprar el asiento 3",
        &cuenta_caida,
        &inventario,
        compra,
    );
    verificar(
        "queda incompleta con decisión Rollback",
        resultado
            == ResultadoTransaccion::Incompleta {
                decision: Decision::Rollback,
                sin_respuesta: vec!["ServicioPagos".to_string()],
            },
    );
    verificar(
        "el asiento 3 se libera",
        inventario.consultar_asiento(3).unwrap() == Some(EstadoAsiento::Libre),
    );

    // 5. Partición de red justo después de que los dos votan YES.
    let compra = Compra {
        tx: IdTransaccion(5),
        asiento: 2,
        precio: PRECIO,
    };
    println!(
        "\n{}: la cuenta 1 compra el asiento 2 y la red se corta después de los votos",
        compra.tx
    );
    let pagos = ServicioPagos::new(cuenta_1.clone());
    let servicio_inventario = ServicioInventario::new(inventario.clone());
    let enlace_pagos = Enlace::new(&pagos);
    let enlace_inventario = Enlace::new(&servicio_inventario);
    enlace_pagos.cortar_tras_prepare();
    enlace_inventario.cortar_tras_prepare();
    let participantes: [&dyn Participante; 2] = [&enlace_pagos, &enlace_inventario];
    let resultado = coordinador
        .ejecutar(&compra, &participantes)
        .expect("cada compra usa una tx nueva");
    mostrar_log(&coordinador, compra.tx);
    println!("  resultado: {resultado:?}");
    verificar(
        "queda incompleta con decisión Commit",
        resultado
            == ResultadoTransaccion::Incompleta {
                decision: Decision::Commit,
                sin_respuesta: vec![
                    "ServicioPagos".to_string(),
                    "ServicioInventario".to_string(),
                ],
            },
    );
    verificar(
        "el asiento 2 queda bloqueado en Reservado",
        inventario.consultar_asiento(2).unwrap() == Some(EstadoAsiento::Reservado(compra.tx)),
    );
    verificar(
        "el precio queda retenido, sin cobrar",
        cuenta_1.consultar_saldo().unwrap()
            == SaldoCuenta {
                disponible: 10_000,
                retenido: PRECIO,
                debitado: PRECIO,
            },
    );

    // Mientras tanto, otro coordinador que sí tiene conexión con los servicios
    // intenta vender el mismo asiento.
    let mut otro_coordinador = Coordinador::new();
    let otra_compra = Compra {
        tx: IdTransaccion(6),
        asiento: 2,
        precio: PRECIO,
    };
    let resultado = comprar(
        &mut otro_coordinador,
        "la cuenta 3 ($300.00) intenta comprar el asiento 2 desde otro coordinador",
        &cuenta_3,
        &inventario,
        otra_compra,
    );
    verificar(
        "se aborta porque el asiento está bloqueado",
        matches!(
            &resultado,
            ResultadoTransaccion::Abortada { motivos }
                if motivos.iter().any(|m| m.starts_with("ServicioInventario"))
        ),
    );
    verificar(
        "la cuenta 3 queda como estaba",
        cuenta_3.consultar_saldo().unwrap()
            == SaldoCuenta {
                disponible: 30_000,
                ..Default::default()
            },
    );

    println!(
        "\n{}: vuelve la red y el coordinador reenvía la decisión del log",
        compra.tx
    );
    enlace_pagos.restablecer();
    enlace_inventario.restablecer();
    let sin_respuesta = coordinador
        .reenviar_decision(compra.tx, &participantes)
        .expect("la decisión está en el log");
    mostrar_log(&coordinador, compra.tx);
    verificar("los dos servicios confirman", sin_respuesta.is_empty());
    verificar(
        "el asiento 2 queda vendido",
        inventario.consultar_asiento(2).unwrap() == Some(EstadoAsiento::Vendido(compra.tx)),
    );
    verificar(
        "se cobra el precio",
        cuenta_1.consultar_saldo().unwrap()
            == SaldoCuenta {
                disponible: 10_000,
                retenido: 0,
                debitado: 2 * PRECIO,
            },
    );

    println!("\nEstado final del inventario:");
    for (asiento, estado) in inventario.listar_asientos().unwrap() {
        println!("  asiento {asiento}: {estado:?}");
    }
    println!("\nSaldos finales:");
    for (nombre, cuenta) in [
        ("cuenta 1", &cuenta_1),
        ("cuenta 2", &cuenta_2),
        ("cuenta 3", &cuenta_3),
    ] {
        let s = cuenta.consultar_saldo().unwrap();
        println!(
            "  {nombre}: disponible {:>8}  retenido {:>8}  cobrado {:>8}",
            formatear_monto(s.disponible),
            formatear_monto(s.retenido),
            formatear_monto(s.debitado)
        );
    }

    // Apagado ordenado: Detener a cada actor y esperar sus hilos.
    inventario.detener();
    cuenta_1.detener();
    cuenta_2.detener();
    cuenta_3.detener();
    hilo_inventario.join().unwrap();
    hilo_1.join().unwrap();
    hilo_2.join().unwrap();
    hilo_3.join().unwrap();
}

/// Arma los participantes de la compra, ejecuta el 2PC y muestra lo que quedó
/// en el log del coordinador.
fn comprar(
    coordinador: &mut Coordinador,
    descripcion: &str,
    cuenta: &CuentaHandle,
    inventario: &InventarioHandle,
    compra: Compra,
) -> ResultadoTransaccion {
    println!("\n{}: {descripcion}", compra.tx);
    let pagos = ServicioPagos::new(cuenta.clone());
    let servicio_inventario = ServicioInventario::new(inventario.clone());
    let resultado = coordinador
        .ejecutar(&compra, &[&pagos, &servicio_inventario])
        .expect("cada compra usa una tx nueva");
    mostrar_log(coordinador, compra.tx);
    println!("  resultado: {resultado:?}");
    resultado
}

/// Muestra las entradas del log del coordinador que corresponden a `tx`.
fn mostrar_log(coordinador: &Coordinador, tx: IdTransaccion) {
    for entrada in coordinador.log().iter().filter(|e| e.tx() == tx) {
        println!("  log: {entrada:?}");
    }
}

fn verificar(descripcion: &str, cumple: bool) {
    println!("  [{}] {descripcion}", if cumple { "OK" } else { "FALLA" });
    assert!(cumple, "invariante violado: {descripcion}");
}
