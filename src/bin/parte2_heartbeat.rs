//! Simulador de la Parte 2: heartbeats, Monitor y caída de un nodo aleatorio.
//! Ejecutar con: cargo run --bin parte2_heartbeat
use std::{
    collections::HashMap,
    hash::{BuildHasher, RandomState},
    sync::mpsc::{self},
    thread::{self},
    time::Duration,
};
use tp2::heartbeat::{MensajeMonitor, MensajeNodo, Monitor, Nodo};

const LIDER_INICIAL: u64 = 3;

/// Número pseudoaleatorio en `0..n` usando solo la biblioteca estándar.
///
/// `RandomState` se inicializa con una semilla aleatoria distinta en cada
/// ejecución; hashear un valor fijo con ella da un número impredecible. Alcanza
/// para elegir qué nodo se cae, sin depender de crates externos (el proyecto
/// compila sin descargar nada).
fn aleatorio_hasta(n: usize) -> usize {
    (RandomState::new().hash_one(0u8) % n as u64) as usize
}

fn main() {
    let (tx, rx) = mpsc::channel::<MensajeMonitor>();
    let ids = vec![1, 2, 3];

    let mut senders = HashMap::new();
    let mut receivers = Vec::new();

    let mut hilos = vec![];

    for &i in &ids {
        let (caida_tx, caida_rx) = mpsc::channel::<MensajeNodo>();
        senders.insert(i, caida_tx);
        receivers.push((i, caida_rx));
    }

    for (i, r) in receivers {
        let otros: HashMap<_, _> = senders
            .iter()
            .filter(|(id, _)| **id != i)
            .map(|(id, s)| (*id, s.clone()))
            .collect();
        let nodo = Nodo::new(i, tx.clone(), r, otros, LIDER_INICIAL);
        hilos.push(thread::spawn(move || nodo.iniciar()));
    }
    drop(tx);

    let id_caido = ids[aleatorio_hasta(ids.len())];
    let tx_caido = senders[&id_caido].clone();

    hilos.push(thread::spawn(move || {
        thread::sleep(Duration::from_secs(3));
        println!("Simulando caida del nodo {id_caido}");
        let _ = tx_caido.send(MensajeNodo::Kill);
    }));

    let mut monitor = Monitor::new(ids, rx, LIDER_INICIAL, senders);
    monitor.monitorear();

    for hilo in hilos {
        let _ = hilo.join();
    }
}
