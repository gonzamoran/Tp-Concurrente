use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::{thread, vec};

const BUFFER_SIZE: usize = 10;
const PAQUETES_POR_CAMION: usize = 10;

fn main() {
    let buffer = Arc::new((
        Mutex::new(VecDeque::<i32>::new()),
        Condvar::new(), // No lleno
        Condvar::new(), // No vacio
    ));

    let mut handles = vec![];

    for camion in 1..=2 {
        let buffer_productor = Arc::clone(&buffer);

        let proudctor = thread::spawn(move || {
            let (ref lock, ref no_lleno, ref no_vacio) = *buffer_productor;

            for i in 1..=PAQUETES_POR_CAMION {
                let id_paquete = (camion * 100 + i) as i32;
                let mut cinta = lock.lock().unwrap();

                while cinta.len() == BUFFER_SIZE {
                    cinta = no_lleno.wait(cinta).unwrap();
                }

                cinta.push_back(id_paquete);
                println!("Camion {camion}: dejo paquete {id_paquete}");

                no_vacio.notify_one();

                drop(cinta);
                thread::sleep(Duration::from_millis(100));
            }
        });
        handles.push(proudctor);
    }

    // Consumidores

    for robot in 1..=3 {
        let buffer_consumidor = Arc::clone(&buffer);

        thread::spawn(move || {
            let (ref lock, ref no_lleno, ref no_vacio) = *buffer_consumidor;

            loop {
                let mut cinta = lock.lock().unwrap();

                while cinta.is_empty() {
                    cinta = no_vacio.wait(cinta).unwrap();
                }

                let id_paquete = cinta.pop_front().unwrap();
                println!("Robot {robot}: tomo paquete: {id_paquete}");

                no_lleno.notify_one();

                drop(cinta);
                thread::sleep(Duration::from_millis(100));
            }
        });
    }

    for hilo in handles {
        hilo.join().unwrap();
    }

    thread::sleep(Duration::from_secs(2));
    println!("Trabajo terminado");
}
