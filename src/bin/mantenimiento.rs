use rand::Rng;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

const CANT_ROBOTS: i32 = 20;
const BAHIAS: usize = 4;

struct Mantenimiento {
    bahias: VecDeque<i32>,        // robots esperando, salen segun el orden de llegada
    mecanico_ocupado: bool,     
    robots_arreglados: Vec<i32>,    // el mecanico avisa aqui que termino de reparar un robot
    cerrado: bool,      // indica si van a llegar mas robots
    atendidos: Vec<i32>,    // ids en el orden en que el mecanico los atendio .Para tests
    rechazos: usize,    // cuantas veces se rechazó a un robot .Para tests
    max_bahias: usize,  // maximo de bahiuas ocupadas al mismo tiempo .Para tests
}

// PARA LOS TESTS
struct Resultado {
    atendidos: Vec<i32>,
    rechazos: usize,
    max_bahias: usize,
    sin_retirar: usize, // robots arreglados que nunca "salieron" (debe ser 0)
}


fn ejecutar(robots: i32, bahias: usize, duracion_reparacion: Duration, llegada: Duration, trabajo: Duration) -> Resultado {
    assert!(bahias > 0, "debe haber al menos una bahía de espera"); // Para tests

    let zona_mantenimiento = Arc::new((
        Mutex::new(Mantenimiento {
            bahias: VecDeque::<i32>::new(),
            mecanico_ocupado: false,
            robots_arreglados: Vec::new(),
            cerrado: false,
            atendidos: Vec::new(),
            rechazos: 0,
            max_bahias: 0,
        }),
        Condvar::new(), // hay robot: el mecanico duerme cuando no hay nadie
        Condvar::new(), // Robot arreglado
    ));

    let mantenimiento_clon = Arc::clone(&zona_mantenimiento);

    // Hilo del mecanico
    let mecanico = thread::spawn(move || {
        let (ref lock, ref hay_robot, ref robot_arreglado) = *mantenimiento_clon;

        loop {  // del mecanico
            let mut mantenimiento = lock.lock().unwrap();   // el mecanico obtiene el lock

            if mantenimiento.bahias.is_empty() && !mantenimiento.cerrado {
                println!("Mecánico duerme");
            }

            while mantenimiento.bahias.is_empty() && !mantenimiento.cerrado {
                mantenimiento = hay_robot.wait(mantenimiento).unwrap(); 
            }

            let id_robot = match mantenimiento.bahias.pop_front() {
                Some(id) => id,
                None => {
                    println!("Mecánico termina su jornada");
                    break;
                }
            };

            mantenimiento.mecanico_ocupado = true;
            mantenimiento.atendidos.push(id_robot);

            println!(
                "Mecánico arreglando robot: {id_robot} y hay {} bahias ocupadas",
                mantenimiento.bahias.len()
            );
            drop(mantenimiento); // reparo SIN el lock, para que puedan llegar otros robots

            thread::sleep(duracion_reparacion);

            let mut mantenimiento = lock.lock().unwrap();

            mantenimiento.mecanico_ocupado = false;
            mantenimiento.robots_arreglados.push(id_robot);

            robot_arreglado.notify_all();
        }
    });

    // Hilos de los robots
    let mut hilos_robots = vec![];

    for id_robot in 1..=robots {
        let mant_robot = Arc::clone(&zona_mantenimiento);

        let robot = thread::spawn(move || {
            let (ref lock, ref hay_robot, ref robot_arreglado) = *mant_robot;

            // Cada robot llega en un momento distinto pero siempre igual.
            thread::sleep(llegada * ((id_robot as u32 * 7) % 10));

            // loop para que el robot rechazado vuelva a intentar más tarde.
            loop {
                let mut mantenimiento = lock.lock().unwrap();
                println!("Robot {id_robot} llega para ser arreglado");

                if !mantenimiento.mecanico_ocupado && mantenimiento.bahias.is_empty() {
                    mantenimiento.bahias.push_back(id_robot);
                    println!("Robot {id_robot} despierta al mecánico");
                    hay_robot.notify_one();
                } else if mantenimiento.bahias.len() < bahias {
                    mantenimiento.bahias.push_back(id_robot);
                    println!("Robot {id_robot} entra a una bahia");
                } else {
                    println!("Robot {id_robot} rechazado, vuelve a trabajar");
                    mantenimiento.rechazos += 1;
                    drop(mantenimiento);
                    thread::sleep(trabajo); // simulacion de trabajo
                    continue; // el robot vuelve a intentar
                }

                // Esto es para los tests
                let ocupadas = mantenimiento.bahias.len();
                if ocupadas > mantenimiento.max_bahias {
                    mantenimiento.max_bahias = ocupadas;
                }

                while !mantenimiento.robots_arreglados.contains(&id_robot) {
                    mantenimiento = robot_arreglado.wait(mantenimiento).unwrap();
                }

                mantenimiento.robots_arreglados.retain(|&x| x != id_robot);
                println!("Robot {id_robot} sale arreglado");
                break; // ya lo arreglaron asi que termina su hilo
            }
        });
        hilos_robots.push(robot);
    }


    // Se espera a que todos los robots terminen (todos salgan reparados).
    for hilo in hilos_robots {
        hilo.join().unwrap();
    }

    // Aavisar al mecanico que no vendran mas robots y despertarlo.
    {
        let (ref lock, ref hay_robot, _) = *zona_mantenimiento;
        lock.lock().unwrap().cerrado = true;
        hay_robot.notify_all();
    }

    // Esperar a que el mecanico termine.
    mecanico.join().unwrap();

    // Genero el resultado a partir del registro.
    let m = zona_mantenimiento.0.lock().unwrap();
    let resultado = Resultado {
        atendidos: m.atendidos.clone(),
        rechazos: m.rechazos,
        max_bahias: m.max_bahias,
        sin_retirar: m.robots_arreglados.len(),
    };
    resultado
}

fn main() {
    ejecutar(CANT_ROBOTS, BAHIAS, Duration::from_millis(200), Duration::from_millis(60), Duration::from_millis(100));
    println!("Trabajo terminado");
}

// LOS TESTS
#[cfg(test)]
mod tests {
    use super::*; // trae todo lo del archivo 
    use std::collections::HashSet;
    use std::sync::mpsc;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }


    fn con_timeout<T, F>(limite: Duration, f: F) -> Option<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(limite).ok()
    }


    fn correr(robots: i32, bahias: usize) -> Resultado {
        con_timeout(Duration::from_secs(30), move || {
            ejecutar(robots, bahias, ms(5), ms(1), ms(5))
        })
        .expect("DEADLOCK: la simulación no terminó a tiempo")
    }


    #[test]
    fn todos_los_robots_son_reparados() {
        let r = correr(20, 4);
        assert_eq!(r.atendidos.len(), 20);
    }

    #[test]
    fn ningun_robot_se_repara_dos_veces() {
        let r = correr(20, 4);
        let unicos: HashSet<_> = r.atendidos.iter().collect();
        assert_eq!(unicos.len(), r.atendidos.len(), "duplicados: {:?}", r.atendidos);
    }

    #[test]
    fn se_atiende_a_cada_uno_de_los_robots() {
        let r = correr(20, 4);
        let mut ids = r.atendidos.clone();
        ids.sort();
        assert_eq!(ids, (1..=20).collect::<Vec<i32>>());
    }

    #[test]
    fn nunca_hay_mas_bahias_ocupadas_que_las_disponibles() {
        let r = correr(20, 4);
        assert!(r.max_bahias <= 4, "máximo observado: {}", r.max_bahias);
    }

    #[test]
    fn el_limite_de_bahias_se_respeta_con_otros_tamanos() {
        for bahias in [1, 2, 3] {
            let r = correr(15, bahias);
            assert!(r.max_bahias <= bahias, "bahías={bahias}, máximo: {}", r.max_bahias);
            assert_eq!(r.atendidos.len(), 15, "bahías={bahias}");
        }
    }

    #[test]
    fn ningun_robot_arreglado_queda_sin_retirar() {
        let r = correr(20, 4);
        assert_eq!(r.sin_retirar, 0);
    }

    #[test]
    fn un_solo_robot_es_atendido_sin_rechazos() {
        let r = correr(1, 4);
        assert_eq!(r.atendidos, vec![1]);
        assert_eq!(r.rechazos, 0);
    }

    #[test]
    fn con_pocos_robots_a_la_vez_no_hay_rechazos() {
        // 3 robots llegando exactamente juntos entran todos (1 + 2 en bahías <= 4).
        let r = con_timeout(Duration::from_secs(10), || {
            ejecutar(3, 4, ms(20), Duration::ZERO, ms(5))
        })
        .expect("DEADLOCK con pocos robots");
        assert_eq!(r.rechazos, 0);
        assert_eq!(r.atendidos.len(), 3);
    }

    #[test]
    fn sin_robots_el_mecanico_duerme_y_termina() {
        let r = correr(0, 4);
        assert!(r.atendidos.is_empty());
        assert_eq!(r.rechazos, 0);
    }

    #[test]
    fn muchos_robots_a_la_vez_provocan_rechazos_pero_todos_terminan() {
        // 10 robots llegan casi juntos, solo 2 bahías, reparación de 30 ms.
        let r = con_timeout(Duration::from_secs(30), || {
            ejecutar(10, 2, ms(30), Duration::ZERO, ms(10))
        })
        .expect("DEADLOCK con muchos robots");
        assert!(r.rechazos >= 1, "debía haber rechazos, hubo {}", r.rechazos);
        assert_eq!(r.atendidos.len(), 10, "un robot rechazado debe volver y ser reparado");
        assert!(r.max_bahias <= 2);
    }

    #[test]
    fn estres_50_robots_a_la_vez() {
        let r = con_timeout(Duration::from_secs(60), || {
            ejecutar(50, 4, ms(1), Duration::ZERO, ms(1))
        })
        .expect("DEADLOCK en la prueba de estrés");
        assert_eq!(r.atendidos.len(), 50);
        assert!(r.max_bahias <= 4);
        assert_eq!(r.sin_retirar, 0);
    }

    #[test]
    fn repetir_30_veces_para_cazar_deadlocks() {
        for i in 0..30 {
            let r = con_timeout(Duration::from_secs(10), || {
                ejecutar(10, 4, ms(2), ms(1), ms(2))
            })
            .unwrap_or_else(|| panic!("DEADLOCK en la corrida {i}"));
            assert_eq!(r.atendidos.len(), 10, "corrida {i}");
        }
    }

    #[test]
    #[should_panic(expected = "bahía")]
    fn cero_bahias_es_rechazado() {
        ejecutar(5, 0, ms(1), ms(1), ms(1));
    }
}