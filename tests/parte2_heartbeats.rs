use std::{
    collections::HashMap,
    sync::mpsc,
    thread::{self, JoinHandle},
    time::Duration,
};

use tp2::heartbeat::{EstadoNodo, MensajeMonitor, MensajeNodo, Monitor, Nodo};

const LIDER_INICIAL: u64 = 3;

fn levantar_cluster() -> (
    Monitor,
    Vec<JoinHandle<()>>,
    HashMap<u64, mpsc::Sender<MensajeNodo>>,
) {
    let (tx, rx) = mpsc::channel::<MensajeMonitor>();
    let ids = vec![1, 2, 3];

    let mut senders = HashMap::new();
    let mut recievers = Vec::new();

    let mut hilos = vec![];

    for &i in &ids {
        let (caida_tx, caida_rx) = mpsc::channel::<MensajeNodo>();
        senders.insert(i, caida_tx);
        recievers.push((i, caida_rx));
    }

    for (i, r) in recievers {
        let otros: HashMap<_, _> = senders
            .iter()
            .filter(|(id, _)| **id != i)
            .map(|(id, s)| (*id, s.clone()))
            .collect();
        let nodo = Nodo::new(i, tx.clone(), r, otros, LIDER_INICIAL);
        hilos.push(thread::spawn(move || nodo.iniciar()));
    }
    drop(tx);

    let monitor = Monitor::new(ids, rx, LIDER_INICIAL, senders.clone());
    (monitor, hilos, senders)
}

fn matar_en(senders: &HashMap<u64, mpsc::Sender<MensajeNodo>>, id: u64, ms: u64) -> JoinHandle<()> {
    let tx = senders[&id].clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(ms));
        let _ = tx.send(MensajeNodo::Kill);
    })
}

#[test]
fn e2e_caida_lider_elige_nuevo_coordinador() {
    let (mut monitor, hilos, senders) = levantar_cluster();
    let killer = matar_en(&senders, 3, 500);

    monitor.monitorear();

    killer.join().unwrap();
    for h in hilos {
        h.join().unwrap();
    }

    assert_eq!(monitor.lider, 2);
    assert_eq!(monitor.nodos[&3].1, EstadoNodo::Caido);
    assert_eq!(monitor.nodos[&1].1, EstadoNodo::Activo);
    assert_eq!(monitor.nodos[&2].1, EstadoNodo::Activo);
}

#[test]
fn e2e_caida_nodo_comun_mantiene_lider() {
    let (mut monitor, hilos, senders) = levantar_cluster();
    let killer = matar_en(&senders, 1, 500);

    monitor.monitorear();

    killer.join().unwrap();
    for h in hilos {
        h.join().unwrap();
    }

    assert_eq!(monitor.lider, 3);
    assert_eq!(monitor.nodos[&1].1, EstadoNodo::Caido);
    assert_eq!(monitor.nodos[&2].1, EstadoNodo::Activo);
    assert_eq!(monitor.nodos[&3].1, EstadoNodo::Activo);
}

#[test]
fn e2e_caidas_nodos_2_y_3() {
    let (mut monitor, hilos, senders) = levantar_cluster();
    let killer_lider = matar_en(&senders, 3, 500);
    let killer2 = matar_en(&senders, 2, 600);

    monitor.monitorear();

    killer_lider.join().unwrap();
    killer2.join().unwrap();

    for h in hilos {
        h.join().unwrap();
    }

    assert_eq!(monitor.lider, 1);
    assert_eq!(monitor.nodos[&1].1, EstadoNodo::Activo);
    assert_ne!(monitor.nodos[&2].1, EstadoNodo::Activo);
    assert_eq!(monitor.nodos[&3].1, EstadoNodo::Caido);
}
