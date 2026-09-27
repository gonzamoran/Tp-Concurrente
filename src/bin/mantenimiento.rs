use rand::Rng;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

const CANT_ROBOTS: i32 = 20;
struct Mantenimiento {
    bahias: VecDeque<i32>,
    mecanico_ocupado: bool,
    robots_arreglados: Vec<i32>,
}

fn main() {
    let zona_mantenimiento = Arc::new((
        Mutex::new(Mantenimiento {
            bahias: VecDeque::<i32>::new(),
            mecanico_ocupado: false,
            robots_arreglados: Vec::new(),
        }),
        Condvar::new(), // hay robot: el mecanico duerme cuando no hay nadie
        Condvar::new(), // Robot arreglado
    ));

    let mantenimiento_clon = Arc::clone(&zona_mantenimiento);

    // Hilo del mecanico
    thread::spawn(move || {
        let (ref lock, ref hay_robot, _) = *mantenimiento_clon;

        loop {
            let mut mantenimiento = lock.lock().unwrap();

            while mantenimiento.bahias.is_empty() {
                println!("Mecanico duernme");
                mantenimiento = hay_robot.wait(mantenimiento).unwrap();
            }

            let id_robot = mantenimiento.bahias.pop_front().unwrap();
            mantenimiento.mecanico_ocupado = true;

            println!(
                "Mecanico arreglando robot: {id_robot} y hay {} bahias ocupadas",
                mantenimiento.bahias.len()
            );
            drop(mantenimiento);

            thread::sleep(Duration::from_millis(200));

            let mut mantenimiento = lock.lock().unwrap();

            mantenimiento.mecanico_ocupado = false;
            mantenimiento.robots_arreglados.push(id_robot);

            let (_, _, ref robot_arreglado) = *mantenimiento_clon;
            robot_arreglado.notify_all();
            drop(mantenimiento);
        }
    });

    let mut hilos_robots = vec![];

    for id_robot in 1..=CANT_ROBOTS {
        let mant_robot = Arc::clone(&zona_mantenimiento);

        let robot = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            thread::sleep(Duration::from_millis(rng.gen_range(50..600)));

            let (ref lock, ref hay_robot, ref robot_arreglado) = *mant_robot;

            let mut mantenimiento = lock.lock().unwrap();
            println!("Robot {id_robot} llega para ser arreglado");

            if !mantenimiento.mecanico_ocupado && mantenimiento.bahias.is_empty() {
                mantenimiento.bahias.push_back(id_robot);
                hay_robot.notify_one();
            } else if mantenimiento.bahias.len() < 4 {
                mantenimiento.bahias.push_back(id_robot);
                println!("Robot {id_robot} entra a una bahia");
            } else {
                println!("Robot {id_robot} rechazado, vuelve a trabajar");
                drop(mantenimiento);
                return;
            }

            while !mantenimiento.robots_arreglados.contains(&id_robot) {
                mantenimiento = robot_arreglado.wait(mantenimiento).unwrap();
            }

            mantenimiento.robots_arreglados.retain(|&x| x != id_robot);
            println!("Robot {id_robot} sale arreglado");
        });
        hilos_robots.push(robot);
    }

    for hilo in hilos_robots {
        hilo.join().unwrap();
    }
}
