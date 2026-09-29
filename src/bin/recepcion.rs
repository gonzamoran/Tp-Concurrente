use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::{thread, vec};

const BUFFER_SIZE: usize = 10;
const PAQUETES_POR_CAMION: usize = 10;
                                                                     
struct CintaTransportadora {
    paquetes: VecDeque<i32>,    // es una cola
    cerrada: bool,      // avisa  alos robots que no hay mas paquetes
    dejados: Vec<i32>,    // IDS productos en el orden en que los camiones los dejaron. Para tests
    tomados: Vec<i32>,   // IDS productos en el orden en que los robots los tomaron. Para tests
    max_ocupacion: usize,   // maximo de paqetes que hubo al mismo tiempo en la cinta.
}

// SE AGREGO SOLO PARA LOS TESTSs
struct Resultado {
    dejados: Vec<i32>,
    tomados: Vec<i32>,
    max_ocupacion: usize,
}

// Inicio del programa
fn ejecutar(camiones: usize, paquetes_por_camion: usize, robots: usize, capacidad: usize, pausa: Duration) -> Resultado {
    // Validacion de entrada
    assert!(capacidad > 0, "la capacidad de la cinta debe ser mayor que 0");
    // Validacion de entrada
    assert!(
        robots > 0 || camiones * paquetes_por_camion == 0,
        "hay paquetes pero no hay robots -> los camiones termirian bloqueados"
    );

    let buffer = Arc::new((
        Mutex::new(CintaTransportadora {
            paquetes: VecDeque::new(),
            cerrada: false,
            dejados: vec![],
            tomados: vec![],
            max_ocupacion: 0,
        }),
        Condvar::new(), // No lleno (esperan los camiones)
        Condvar::new(), // No vacio (esperan los robots)
    ));


    // LA PARTE DE LOS CAMIONES
    let mut handles_camiones = vec![];  // Aqui van los JoinHanlde de los threads.

    for camion in 1..=camiones {
        let buffer_productor = Arc::clone(&buffer); // Comparte la cinta con el camion.

        let handle_camion = thread::spawn(move || {  // Crea nuevo thread
            let (ref lock, ref no_lleno, ref no_vacio) = *buffer_productor;

            for i in 1..=paquetes_por_camion {
                let id_paquete = ((camion - 1) * paquetes_por_camion + i) as i32;
                let mut cinta = lock.lock().unwrap();   // intenta obterner el MUTEX

                // revisa si la cinta esta llena y si lo esta, el camion se queda esperando.
                while cinta.paquetes.len() >= capacidad {
                    cinta = no_lleno.wait(cinta).unwrap();
                }

                cinta.paquetes.push_back(id_paquete);   // agrega el paquete a la cinta.

                // Para el test
                cinta.dejados.push(id_paquete); // guarde el id en dejados
                let ocupacion = cinta.paquetes.len();   
                if ocupacion > cinta.max_ocupacion {  // actualizo el maximo
                    cinta.max_ocupacion = ocupacion;
                }

                println!("Camion {camion}: dejo paquete {id_paquete}");

                no_vacio.notify_one(); // despierta a algun robot que estuviese esperando.

                drop(cinta);    // libera el Mutex. Otro thread puede acceder a la cinta.
                thread::sleep(pausa);
            }
        });
        handles_camiones.push(handle_camion);
    }


    // LA PARTE DE LOS ROBOTS
    let mut handles_robots = vec![];  // Aqui van los JoinHanlde de los threads.

    for robot in 1..=robots {
        let buffer_consumidor = Arc::clone(&buffer); // Comparte la cinta con el robot.

        let handle_robot = thread::spawn(move || {  // Crea nuevo thread
            let (ref lock, ref no_lleno, ref no_vacio) = *buffer_consumidor;

            loop {
                let mut cinta = lock.lock().unwrap();

                while cinta.paquetes.is_empty() && !cinta.cerrada {
                    cinta = no_vacio.wait(cinta).unwrap();
                }

                let id_paquete = match cinta.paquetes.pop_front() {
                    Some(id) => id,
                    None => break, // cinta vaicia y cerrada: el robot termina su loop
                };
                cinta.tomados.push(id_paquete);     // se guarda el id del paquete tomado
                println!("Robot {robot}: tomo paquete: {id_paquete}");

                no_lleno.notify_one(); // despierta a algun camion que estuviese esperando.

                drop(cinta); // libera la cinta
                thread::sleep(pausa);
            }
        });
        handles_robots.push(handle_robot);
    }

    // Espera a que el thread de todos los caminiones termine.
    for hilo in handles_camiones {
        hilo.join().unwrap();
    }

    //
    {
        let (ref lock, ref no_lleno, ref no_vacio) = *buffer;
        lock.lock().unwrap().cerrada = true;  // ciera la cinta, osea no van a venir mas paquetes
        no_lleno.notify_all();
        no_vacio.notify_all(); // Despierta a todos los robots que estaban esperando
    }

    // Espera a que el thread de todos los robots termine.
    for hilo in handles_robots {
        hilo.join().unwrap();
    }

    // Construyo el resultado a partir del registro de la cinta.
    let cinta = buffer.0.lock().unwrap();
    let resultado = Resultado {
        dejados: cinta.dejados.clone(),
        tomados: cinta.tomados.clone(),
        max_ocupacion: cinta.max_ocupacion,
    };
    resultado
}

fn main() {
    ejecutar(2, PAQUETES_POR_CAMION, 3, BUFFER_SIZE, Duration::from_millis(100));
    println!("Trabajo terminado");
}

// TESTS
#[cfg(test)]    // Hace que los tests no se compilen con el resto del codigo.
mod tests {
    use super::*; // trae todo lo del archivo, lo que esta fuera del modulo tests
    use std::collections::HashSet; // guardar sin repetidos
    use std::sync::mpsc;

    // Ejecuta una funcion y le pone un tiempo maximo
    fn con_timeout<T, F>(limite: Duration, f: F) -> Option<T> where T: Send + 'static, F: FnOnce() -> T + Send + 'static {
        let (tx, rx) = mpsc::channel();  // crea un canal
        thread::spawn(move || {     // creo un hilo
            let _ = tx.send(f());   // ejecuta f()
        });
        rx.recv_timeout(limite).ok() // espera recibir algo hasta que alcance limite.
    }

    // Corre la simulación. Es un atajo.
    fn correr(camiones: usize, paquetes: usize, robots: usize, capacidad: usize) -> Resultado {
        con_timeout(Duration::from_secs(30), move || {
            ejecutar(camiones, paquetes, robots, capacidad, Duration::from_millis(1))
        })
        .expect("DEADLOCK: la simulación no terminó a tiempo")
    }


    #[test]
    fn se_dejan_todos_los_paquetes() {
        let r = correr(2, 10, 3, 10);
        assert_eq!(r.dejados.len(), 20);
    }

    #[test]
    fn se_toman_todos_los_paquetes() {
        let r = correr(2, 10, 3, 10);
        assert_eq!(r.tomados.len(), 20);
    }

    #[test]
    fn ningun_paquete_se_toma_dos_veces() {
        let r = correr(2, 10, 3, 10);
        let unicos: HashSet<_> = r.tomados.iter().collect();
        assert_eq!(unicos.len(), r.tomados.len(), "hay duplicados: {:?}", r.tomados);
    }

    #[test]
    fn se_toman_exactamente_los_que_se_dejaron() {
        let r = correr(2, 10, 3, 10);
        let mut a = r.dejados.clone();
        let mut b = r.tomados.clone();
        a.sort();
        b.sort();
        assert_eq!(a, b);
    }

    #[test]
    fn robots_pueden_esperar_hasta_que_lleguen_paquetes() {
        let r = correr(1, 5, 3, 10);

        assert_eq!(r.dejados.len(), 5);
        assert_eq!(r.tomados.len(), 5);
    }

    #[test]
    fn mas_robots_que_paquetes_terminan_correctamente() {
        let r = correr(1, 2, 20, 10);

        assert_eq!(r.dejados.len(), 2);
        assert_eq!(r.tomados.len(), 2);
    }

    #[test]
    fn procesa_gran_cantidad_de_paquetes() {
        let r = correr(10, 100, 5, 10);

        assert_eq!(r.dejados.len(), 1000);
        assert_eq!(r.tomados.len(), 1000);
        assert!(r.max_ocupacion <= 10);
    }

    #[test]
    fn la_cinta_respeta_el_orden_fifo() {
        // Los robots deben tomar los paquetes en el mismo orden
        // en que fueron colocados en la cinta.
        let r = correr(2, 10, 3, 10);
        assert_eq!(r.dejados, r.tomados);
    }

    #[test]
    fn la_cinta_nunca_supera_su_capacidad() {
        let r = correr(4, 25, 1, 3);
        assert!(r.max_ocupacion <= 3, "máximo observado: {}", r.max_ocupacion);
    }

    #[test]
    fn funciona_con_cinta_de_tamano_1() {
        let r = correr(2, 10, 3, 1);
        assert_eq!(r.tomados.len(), 20);
        assert_eq!(r.max_ocupacion, 1);
    }

    #[test]
    fn sin_camiones_termina_sin_hacer_nada() {
        let r = correr(0, 10, 3, 10);
        assert!(r.dejados.is_empty());
        assert!(r.tomados.is_empty());
    }

    #[test]
    fn productores_y_consumidores_funcionan_con_buffer_muy_pequeno() {
        let r = correr(4, 20, 1, 1);
        assert_eq!(r.dejados.len(), 80);
        assert_eq!(r.tomados.len(), 80);
        assert!(r.max_ocupacion <= 1);
    }

    #[test]
    fn estres_muchos_hilos() {
        // 8 camiones x 50 paquetes, 8 robots, cinta de 3, sin pausas.
        let r = con_timeout(Duration::from_secs(30), || {
            ejecutar(8, 50, 8, 3, Duration::ZERO)
        })
        .expect("DEADLOCK en la prueba de estrés");
        assert_eq!(r.dejados.len(), 400);
        assert_eq!(r.tomados.len(), 400);
        assert!(r.max_ocupacion <= 3);
    }

    #[test]
    fn repetir_30_veces_para_encontrar_deadlocks() {
        for i in 0..30 {
            let r = con_timeout(Duration::from_secs(10), || ejecutar(2, 10, 3, 2, Duration::from_millis(1)))
                .unwrap_or_else(|| panic!("DEADLOCK en la corrida {i}"));
            assert_eq!(r.tomados.len(), 20, "corrida {i}");
        }
    }

    // Estos tests deben tirar panic!
    #[test]
    #[should_panic(expected = "capacidad")]
    fn cinta_de_capacidad_cero_es_rechazada() {
        ejecutar(2, 10, 3, 0, Duration::from_millis(1));
    }

    #[test]
    #[should_panic(expected = "no hay robots")]
    fn sin_robots_es_rechazado() {
        ejecutar(2, 10, 0, 10, Duration::from_millis(1));
    }

    #[test]
    fn los_ids_de_paquete_son_unicos_con_muchos_paquetes() {
        let r = correr(2, 150, 3, 50);

        let unicos: HashSet<_> = r.dejados.iter().collect();

        assert_eq!(
            unicos.len(),
            r.dejados.len(),
            "hay ids de paquete repetidos"
        );
    }
}