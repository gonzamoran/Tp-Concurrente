//! Tests de integración de la Parte 3: el 2PC con los actores reales de la
//! Parte 1 como participantes.
//!
//! Verifican la atomicidad de la compra: al final, o el asiento queda vendido
//! y el dinero cobrado, o el asiento queda libre y no se cobra nada.

use std::collections::HashMap;
use std::thread;
use tp2::actores::{
    CuentaUsuario, ErrorCuenta, EstadoAsiento, IdAsiento, IdTransaccion, InventarioEntradas, Monto,
    SaldoCuenta,
};
use tp2::two_phase_commit::{
    Compra, Coordinador, Decision, Enlace, Participante, ResultadoTransaccion, ServicioInventario,
    ServicioPagos,
};

const PRECIO: Monto = 15_000;

/// Los dos votan YES: el asiento queda vendido y el precio cobrado.
#[test]
fn compra_exitosa_vende_el_asiento_y_cobra() {
    let (inventario, hilo_inv) = InventarioEntradas::new([1, 2]).iniciar();
    let (cuenta, hilo_cuenta) = CuentaUsuario::new(20_000).iniciar();
    let pagos = ServicioPagos::new(cuenta.clone());
    let servicio_inventario = ServicioInventario::new(inventario.clone());
    let compra = Compra {
        tx: IdTransaccion(1),
        asiento: 1,
        precio: PRECIO,
    };

    let resultado = Coordinador::new().ejecutar(&compra, &[&pagos, &servicio_inventario]);

    assert_eq!(resultado, Ok(ResultadoTransaccion::Confirmada));
    assert_eq!(
        inventario.consultar_asiento(1).unwrap(),
        Some(EstadoAsiento::Vendido(compra.tx))
    );
    assert_eq!(
        inventario.consultar_asiento(2).unwrap(),
        Some(EstadoAsiento::Libre)
    );
    assert_eq!(
        cuenta.consultar_saldo().unwrap(),
        SaldoCuenta {
            disponible: 5_000,
            retenido: 0,
            debitado: PRECIO
        }
    );

    inventario.detener();
    cuenta.detener();
    hilo_inv.join().unwrap();
    hilo_cuenta.join().unwrap();
}

/// Pagos vota NO: el asiento que Inventario había reservado se libera.
#[test]
fn saldo_insuficiente_aborta_y_libera_el_asiento() {
    let (inventario, hilo_inv) = InventarioEntradas::new([1]).iniciar();
    let (cuenta, hilo_cuenta) = CuentaUsuario::new(10_000).iniciar();
    let pagos = ServicioPagos::new(cuenta.clone());
    let servicio_inventario = ServicioInventario::new(inventario.clone());
    let compra = Compra {
        tx: IdTransaccion(1),
        asiento: 1,
        precio: PRECIO,
    };

    let resultado = Coordinador::new().ejecutar(&compra, &[&pagos, &servicio_inventario]);

    match resultado {
        Ok(ResultadoTransaccion::Abortada { motivos }) => {
            assert_eq!(motivos.len(), 1);
            assert!(motivos[0].starts_with("ServicioPagos"));
        }
        otro => panic!("se esperaba Abortada, llegó {otro:?}"),
    }
    assert_eq!(
        inventario.consultar_asiento(1).unwrap(),
        Some(EstadoAsiento::Libre)
    );
    assert_eq!(
        cuenta.consultar_saldo().unwrap(),
        SaldoCuenta {
            disponible: 10_000,
            ..Default::default()
        }
    );

    inventario.detener();
    cuenta.detener();
    hilo_inv.join().unwrap();
    hilo_cuenta.join().unwrap();
}

/// Inventario vota NO porque el asiento ya se vendió: se devuelve lo retenido.
#[test]
fn asiento_vendido_aborta_y_devuelve_el_dinero() {
    let (inventario, hilo_inv) = InventarioEntradas::new([1]).iniciar();
    let (cuenta_a, hilo_a) = CuentaUsuario::new(20_000).iniciar();
    let (cuenta_b, hilo_b) = CuentaUsuario::new(20_000).iniciar();
    let servicio_inventario = ServicioInventario::new(inventario.clone());
    let mut coordinador = Coordinador::new();

    let primera = Compra {
        tx: IdTransaccion(1),
        asiento: 1,
        precio: PRECIO,
    };
    let pagos_a = ServicioPagos::new(cuenta_a.clone());
    assert_eq!(
        coordinador.ejecutar(&primera, &[&pagos_a, &servicio_inventario]),
        Ok(ResultadoTransaccion::Confirmada)
    );

    let segunda = Compra {
        tx: IdTransaccion(2),
        ..primera
    };
    let pagos_b = ServicioPagos::new(cuenta_b.clone());
    match coordinador.ejecutar(&segunda, &[&pagos_b, &servicio_inventario]) {
        Ok(ResultadoTransaccion::Abortada { motivos }) => {
            assert_eq!(motivos.len(), 1);
            assert!(motivos[0].starts_with("ServicioInventario"));
        }
        otro => panic!("se esperaba Abortada, llegó {otro:?}"),
    }

    assert_eq!(
        inventario.consultar_asiento(1).unwrap(),
        Some(EstadoAsiento::Vendido(primera.tx))
    );
    assert_eq!(
        cuenta_b.consultar_saldo().unwrap(),
        SaldoCuenta {
            disponible: 20_000,
            ..Default::default()
        }
    );

    inventario.detener();
    cuenta_a.detener();
    cuenta_b.detener();
    hilo_inv.join().unwrap();
    hilo_a.join().unwrap();
    hilo_b.join().unwrap();
}

/// Si el actor de la cuenta está caído, su voto cuenta como NO. El asiento se
/// libera, pero el rollback a Pagos no se puede confirmar y la transacción
/// queda incompleta (con la decisión anotada en el log).
#[test]
fn servicio_caido_cuenta_como_voto_no() {
    let (inventario, hilo_inv) = InventarioEntradas::new([1]).iniciar();
    let (cuenta, hilo_cuenta) = CuentaUsuario::new(20_000).iniciar();
    cuenta.detener();
    hilo_cuenta.join().unwrap();

    let pagos = ServicioPagos::new(cuenta);
    let servicio_inventario = ServicioInventario::new(inventario.clone());
    let compra = Compra {
        tx: IdTransaccion(1),
        asiento: 1,
        precio: PRECIO,
    };
    let mut coordinador = Coordinador::new();

    let resultado = coordinador.ejecutar(&compra, &[&pagos, &servicio_inventario]);

    assert_eq!(
        resultado,
        Ok(ResultadoTransaccion::Incompleta {
            decision: Decision::Rollback,
            sin_respuesta: vec!["ServicioPagos".to_string()]
        })
    );
    assert_eq!(coordinador.decision(compra.tx), Some(Decision::Rollback));
    assert_eq!(
        inventario.consultar_asiento(1).unwrap(),
        Some(EstadoAsiento::Libre)
    );

    inventario.detener();
    hilo_inv.join().unwrap();
}

/// Parte 3.2: los dos servicios votan YES y la red se corta antes de que les
/// llegue el Commit. Quedan bloqueados en PREPARED, con el asiento y el dinero
/// reservados, hasta que vuelve la red y el coordinador reenvía la decisión.
#[test]
fn particion_despues_del_yes_bloquea_hasta_que_vuelve_la_red() {
    let (inventario, hilo_inv) = InventarioEntradas::new([1]).iniciar();
    let (cuenta, hilo_cuenta) = CuentaUsuario::new(20_000).iniciar();
    let (otra_cuenta, hilo_otra) = CuentaUsuario::new(20_000).iniciar();
    let pagos = ServicioPagos::new(cuenta.clone());
    let servicio_inventario = ServicioInventario::new(inventario.clone());
    let enlace_pagos = Enlace::new(&pagos);
    let enlace_inventario = Enlace::new(&servicio_inventario);
    enlace_pagos.cortar_tras_prepare();
    enlace_inventario.cortar_tras_prepare();
    let participantes: [&dyn Participante; 2] = [&enlace_pagos, &enlace_inventario];
    let compra = Compra {
        tx: IdTransaccion(1),
        asiento: 1,
        precio: PRECIO,
    };
    let mut coordinador = Coordinador::new();

    let resultado = coordinador.ejecutar(&compra, &participantes);

    // El coordinador decidió Commit, pero no se lo pudo comunicar a nadie.
    assert_eq!(
        resultado,
        Ok(ResultadoTransaccion::Incompleta {
            decision: Decision::Commit,
            sin_respuesta: vec![
                "ServicioPagos".to_string(),
                "ServicioInventario".to_string()
            ]
        })
    );
    // Los participantes quedan en PREPARED: el asiento reservado y el precio
    // retenido, pero no vendido ni cobrado.
    assert_eq!(
        inventario.consultar_asiento(1).unwrap(),
        Some(EstadoAsiento::Reservado(compra.tx))
    );
    assert_eq!(
        cuenta.consultar_saldo().unwrap(),
        SaldoCuenta {
            disponible: 5_000,
            retenido: PRECIO,
            debitado: 0
        }
    );

    // El bloqueo afecta a otras compras: otro coordinador, que sí tiene
    // conexión con los servicios, no puede vender el mismo asiento.
    let otros_pagos = ServicioPagos::new(otra_cuenta.clone());
    let otra_compra = Compra {
        tx: IdTransaccion(2),
        ..compra
    };
    match Coordinador::new().ejecutar(&otra_compra, &[&otros_pagos, &servicio_inventario]) {
        Ok(ResultadoTransaccion::Abortada { motivos }) => {
            assert_eq!(motivos.len(), 1);
            assert!(motivos[0].starts_with("ServicioInventario"));
        }
        otro => panic!("se esperaba Abortada, llegó {otro:?}"),
    }
    assert_eq!(
        otra_cuenta.consultar_saldo().unwrap(),
        SaldoCuenta {
            disponible: 20_000,
            ..Default::default()
        }
    );

    // Al volver la red, el coordinador reenvía la decisión anotada en su log.
    enlace_pagos.restablecer();
    enlace_inventario.restablecer();
    assert_eq!(
        coordinador.reenviar_decision(compra.tx, &participantes),
        Ok(Vec::new())
    );
    assert_eq!(
        inventario.consultar_asiento(1).unwrap(),
        Some(EstadoAsiento::Vendido(compra.tx))
    );
    assert_eq!(
        cuenta.consultar_saldo().unwrap(),
        SaldoCuenta {
            disponible: 5_000,
            retenido: 0,
            debitado: PRECIO
        }
    );

    inventario.detener();
    cuenta.detener();
    otra_cuenta.detener();
    hilo_inv.join().unwrap();
    hilo_cuenta.join().unwrap();
    hilo_otra.join().unwrap();
}

/// Si la red se corta antes del voto, el coordinador aborta y no queda nada
/// bloqueado: el que no votó no reservó nada.
#[test]
fn particion_antes_del_voto_no_bloquea_recursos() {
    let (inventario, hilo_inv) = InventarioEntradas::new([1]).iniciar();
    let (cuenta, hilo_cuenta) = CuentaUsuario::new(20_000).iniciar();
    let pagos = ServicioPagos::new(cuenta.clone());
    let servicio_inventario = ServicioInventario::new(inventario.clone());
    let enlace_pagos = Enlace::new(&pagos);
    enlace_pagos.cortar();
    let participantes: [&dyn Participante; 2] = [&enlace_pagos, &servicio_inventario];
    let compra = Compra {
        tx: IdTransaccion(1),
        asiento: 1,
        precio: PRECIO,
    };
    let mut coordinador = Coordinador::new();

    let resultado = coordinador.ejecutar(&compra, &participantes);

    assert_eq!(
        resultado,
        Ok(ResultadoTransaccion::Incompleta {
            decision: Decision::Rollback,
            sin_respuesta: vec!["ServicioPagos".to_string()]
        })
    );
    assert_eq!(
        inventario.consultar_asiento(1).unwrap(),
        Some(EstadoAsiento::Libre)
    );
    assert_eq!(
        cuenta.consultar_saldo().unwrap(),
        SaldoCuenta {
            disponible: 20_000,
            ..Default::default()
        }
    );

    // Al volver la red llega el Rollback. Desde ahí, un Prepare atrasado de
    // esta tx ya no puede retener el dinero.
    enlace_pagos.restablecer();
    assert_eq!(
        coordinador.reenviar_decision(compra.tx, &participantes),
        Ok(Vec::new())
    );
    assert_eq!(
        cuenta.solicitar_debito(compra.tx, PRECIO),
        Err(ErrorCuenta::TransaccionFinalizada(compra.tx))
    );

    inventario.detener();
    cuenta.detener();
    hilo_inv.join().unwrap();
    hilo_cuenta.join().unwrap();
}

/// 200 clientes compran a la vez entre 20 asientos con 4 cuentas. Cada
/// cliente usa su propio coordinador y una tx nueva por intento.
#[test]
fn compras_concurrentes_con_2pc_conservan_invariantes() {
    const ASIENTOS: IdAsiento = 20;
    // Alcanzan para 5, 7, 2 y 0 entradas.
    let saldos: [Monto; 4] = [5 * PRECIO, 7 * PRECIO, 2 * PRECIO + PRECIO / 2, 0];

    let (inventario, hilo_inv) = InventarioEntradas::new(1..=ASIENTOS).iniciar();
    let (cuentas, hilos_cuentas): (Vec<_>, Vec<_>) = saldos
        .iter()
        .map(|&s| CuentaUsuario::new(s).iniciar())
        .unzip();

    let confirmadas: Vec<(usize, IdTransaccion)> = thread::scope(|s| {
        let clientes: Vec<_> = (0..200u64)
            .map(|i| {
                let id_cuenta = i as usize % cuentas.len();
                let pagos = ServicioPagos::new(cuentas[id_cuenta].clone());
                let servicio_inventario = ServicioInventario::new(inventario.clone());
                s.spawn(move || {
                    let mut coordinador = Coordinador::new();
                    for k in 0..ASIENTOS {
                        let compra = Compra {
                            tx: IdTransaccion(i * ASIENTOS as u64 + k as u64),
                            asiento: (i as IdAsiento + k) % ASIENTOS + 1,
                            precio: PRECIO,
                        };
                        match coordinador.ejecutar(&compra, &[&pagos, &servicio_inventario]) {
                            Ok(ResultadoTransaccion::Confirmada) => {
                                return Some((id_cuenta, compra.tx));
                            }
                            // Asiento ocupado o saldo insuficiente: se prueba el siguiente.
                            Ok(ResultadoTransaccion::Abortada { .. }) => {}
                            otro => panic!("resultado inesperado en {}: {otro:?}", compra.tx),
                        }
                    }
                    None
                })
            })
            .collect();
        clientes
            .into_iter()
            .filter_map(|h| h.join().unwrap())
            .collect()
    });

    // Las tx de los asientos vendidos son exactamente las confirmadas.
    let asientos = inventario.listar_asientos().unwrap();
    let mut vendidos: Vec<IdTransaccion> = asientos
        .iter()
        .filter_map(|(_, e)| match e {
            EstadoAsiento::Vendido(tx) => Some(*tx),
            _ => None,
        })
        .collect();
    let mut txs_confirmadas: Vec<IdTransaccion> = confirmadas.iter().map(|&(_, tx)| tx).collect();
    vendidos.sort();
    txs_confirmadas.sort();
    assert_eq!(vendidos, txs_confirmadas);
    assert!(
        !asientos
            .iter()
            .any(|(_, e)| matches!(e, EstadoAsiento::Reservado(_)))
    );

    // En cada cuenta: nada retenido, se cobró una vez por compra y el dinero
    // se conserva.
    let mut compras_por_cuenta: HashMap<usize, Monto> = HashMap::new();
    for &(id_cuenta, _) in &confirmadas {
        *compras_por_cuenta.entry(id_cuenta).or_default() += 1;
    }
    for (id_cuenta, cuenta) in cuentas.iter().enumerate() {
        let saldo = cuenta.consultar_saldo().unwrap();
        let compras = compras_por_cuenta.get(&id_cuenta).copied().unwrap_or(0);
        assert_eq!(saldo.retenido, 0);
        assert_eq!(saldo.debitado, compras * PRECIO);
        assert_eq!(saldo.disponible + saldo.debitado, saldos[id_cuenta]);
    }

    inventario.detener();
    cuentas.iter().for_each(|c| c.detener());
    hilo_inv.join().unwrap();
    hilos_cuentas.into_iter().for_each(|h| h.join().unwrap());
}
