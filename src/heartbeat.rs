//! Parte 2 - Coordinación y Tolerancia a Fallos.
//!
//! Detección de fallos: cada `Nodo` manda un `Heartbeat` cada 500 ms al
//! `Monitor` por un canal `mpsc` compartido. El Monitor usa `recv_timeout`
//! y declara a un nodo SOSPECHOSO si pasa 1 s sin latidos y CAIDO a los 2 s.
//!
//! Algoritmo Bully (cuando cae el líder):
//! - Un nodo P envía ELECTION a todos los nodos con id mayor.
//! - Si nadie responde, P gana la elección y es el nuevo coordinador.
//! - Si algún nodo mayor contesta OK, ese sigue con la elección y P espera
//!   el resultado.
//! - El nuevo coordinador se anuncia con un mensaje COORDINATOR.
use std::fmt::Display;
use std::{
    collections::HashMap,
    sync::mpsc::{Receiver, RecvTimeoutError, Sender},
    time::{Duration, Instant},
};

/// Posibles estados de un nodo.
/// El nodo empieza en estado Activo y puede pasar a Sospechoso o Caido.
#[derive(Debug, PartialEq)]
pub enum EstadoNodo {
    Activo,
    Sospechoso,
    Caido,
}

/// Mensajes que se mandan entre nodos durante el algoritmo Bully o control.
#[derive(Debug, PartialEq)]
pub enum MensajeNodo {
    /// Termina el hilo del nodo. Lo usa la simulación para provocar una caída
    /// y el Monitor para apagar el clúster al terminar.
    Kill,
    /// Notificacion del monitor indicando que tiene que empezar una eleccion
    IniciarEleccion,
    Election(u64),
    Ok(u64),
    Coordinator(u64),
}

/// Mensajes de los nodos hacia el Monitor.
#[derive(Debug, PartialEq)]
pub enum MensajeMonitor {
    Heartbeat(u64),
    NuevoLider(u64),
}

/// Struct para representar a un nodo. Tiene un id, un canal hacia el monitor, un canal receptor de mensajes de otros nodos y control.
/// Un hash en el que guarda un id de otro nodo como clave y un canal para comunicarse con ese nodo.
/// Id del lider actual
pub struct Nodo {
    pub id: u64,
    pub tx: Sender<MensajeMonitor>,
    pub mensajes: Receiver<MensajeNodo>,
    pub otros_nodos: HashMap<u64, Sender<MensajeNodo>>,
    pub lider: u64,
    espera_ok: Option<Instant>,
    espera_coord: Option<Instant>,
}

/// Es el orquestador. Tiene un hash en el que guarda el id como clave y el instante de la ultima escucha y el estado del nodo como valor.
/// rx: canal que recibe heartbeats y avisos de nuevo lider.
/// Campo lider en el que guarda el id del lider actual y un hash en el que guarda el id del nodo y un canal para comunicarse con ese nodo.
pub struct Monitor {
    pub nodos: HashMap<u64, (Instant, EstadoNodo)>,
    pub rx: Receiver<MensajeMonitor>,
    pub lider: u64,
    pub tx_nodos: HashMap<u64, Sender<MensajeNodo>>,
}

// Se implementa Display para el EstadoNodo para poder imprimir por pantalla los 3 posibles estados de un nodo.
impl Display for EstadoNodo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EstadoNodo::Activo => write!(f, "ACTIVO"),
            EstadoNodo::Sospechoso => write!(f, "SOSPECHOSO"),
            EstadoNodo::Caido => write!(f, "CAIDO"),
        }
    }
}

impl Nodo {
    /// Constructor de un nodo.
    pub fn new(
        id: u64,
        tx: Sender<MensajeMonitor>,
        mensajes: Receiver<MensajeNodo>,
        otros_nodos: HashMap<u64, Sender<MensajeNodo>>,
        lider: u64,
    ) -> Self {
        Nodo {
            id,
            tx,
            mensajes,
            otros_nodos,
            lider,
            espera_coord: None,
            espera_ok: None,
        }
    }

    /// Loop del nodo: manda un `Heartbeat` al Monitor cada 500 ms y, mientras
    /// tanto, atiende los mensajes del algoritmo Bully.
    ///
    /// Escucha su canal con `recv_timeout` de 50 ms para no quedarse bloqueado:
    /// así puede mandar el próximo latido a tiempo y revisar los timeouts de la
    /// elección aunque no le lleguen mensajes. Termina al recibir `Kill` (que
    /// simula su caída) o si se desconecta el canal.
    pub fn iniciar(mut self) {
        let mut proximo_heartbeat = Instant::now();
        loop {
            if Instant::now() >= proximo_heartbeat {
                if self.tx.send(MensajeMonitor::Heartbeat(self.id)).is_err() {
                    break;
                }
                proximo_heartbeat = Instant::now() + Duration::from_millis(500);
            }
            match self.mensajes.recv_timeout(Duration::from_millis(50)) {
                Ok(MensajeNodo::Kill) | Err(RecvTimeoutError::Disconnected) => break,
                Ok(msg) => self.manejar(msg),
                Err(RecvTimeoutError::Timeout) => {}
            }
            self.revisar_timeouts();
        }
    }

    /// Procesa cada tipo de MensajeNodo entrante segun las reglas del algoritmo Bully.
    fn manejar(&mut self, mensaje: MensajeNodo) {
        match mensaje {
            MensajeNodo::IniciarEleccion => {
                if self.espera_ok.is_none() && self.espera_coord.is_none() {
                    self.iniciar_eleccion();
                }
            }
            MensajeNodo::Election(de) => {
                println!(
                    "Nodo {}: recibi ELECTION de {de}, respondo OK y me postulo",
                    self.id
                );
                if let Some(tx) = self.otros_nodos.get(&de) {
                    let _ = tx.send(MensajeNodo::Ok(self.id));
                }
                if self.espera_ok.is_none() && self.espera_coord.is_none() {
                    self.iniciar_eleccion();
                }
            }
            MensajeNodo::Ok(de) => {
                println!("Nodo {}: recibi OK de {de}, espero COORDINATOR", self.id);
                if self.espera_ok.take().is_some() {
                    self.espera_coord = Some(Instant::now() + Duration::from_secs(1));
                }
            }
            MensajeNodo::Coordinator(id) => {
                self.lider = id;
                self.espera_ok = None;
                self.espera_coord = None;
                println!("Nodo {}: reconozco al nodo {id} como nuevo lider", self.id);
            }
            MensajeNodo::Kill => {}
        }
    }

    /// Inicia la postulación enviando `Election` a todos los nodos con id mayor
    /// al suyo. Si no hay ninguno, se convierte en coordinador.
    fn iniciar_eleccion(&mut self) {
        let mut hay_mayores = false;
        for (id, tx) in &self.otros_nodos {
            if *id > self.id {
                hay_mayores = true;
                println!("Nodo {}: envio ELECTION al nodo {id}", self.id);
                let _ = tx.send(MensajeNodo::Election(self.id));
            }
        }
        if hay_mayores {
            self.espera_ok = Some(Instant::now() + Duration::from_millis(300));
        } else {
            self.ser_coordinador();
        }
    }

    /// Revisa si vencieron los tiempos de espera:
    /// - `espera_ok`: ningún nodo mayor respondió OK, así que este nodo gana.
    /// - `espera_coord`: un nodo mayor respondió OK pero nunca anunció
    ///   COORDINATOR (por ejemplo, porque se cayó), así que se reinicia la
    ///   elección.
    fn revisar_timeouts(&mut self) {
        let ahora = Instant::now();
        if self.espera_ok.is_some_and(|t| ahora >= t) {
            self.espera_ok = None;
            self.ser_coordinador();
        }
        if self.espera_coord.is_some_and(|t| ahora >= t) {
            self.espera_coord = None;
            self.iniciar_eleccion();
        }
    }

    /// Se convierte en lider y avisa a los demas nodos y al monitor.
    fn ser_coordinador(&mut self) {
        self.lider = self.id;
        println!(
            "Nodo {}: gane la eleccion, soy el nuevo COORDINADOR",
            self.id
        );
        for tx in self.otros_nodos.values() {
            let _ = tx.send(MensajeNodo::Coordinator(self.id));
        }
        let _ = self.tx.send(MensajeMonitor::NuevoLider(self.id));
    }
}

impl Monitor {
    /// Constructor del Monitor.
    pub fn new(
        nodos: Vec<u64>,
        receive: Receiver<MensajeMonitor>,
        lider: u64,
        tx_nodos: HashMap<u64, Sender<MensajeNodo>>,
    ) -> Self {
        let mut hash_nodos = HashMap::new();
        for id in nodos {
            hash_nodos.insert(id, (Instant::now(), EstadoNodo::Activo));
        }
        Monitor {
            nodos: hash_nodos,
            rx: receive,
            lider,
            tx_nodos,
        }
    }

    /// Loop del monitor. Escucha el canal con `recv_timeout` de 100 ms:
    /// - Si llega un `Heartbeat`, actualiza el instante del último latido de
    ///   ese nodo y, si estaba SOSPECHOSO, lo vuelve a ACTIVO.
    /// - Si pasan 100 ms sin mensajes, sigue (es el "tic" del reloj).
    /// - Si el canal se desconecta, no quedan nodos vivos y termina.
    ///
    /// En cada vuelta llama a `verificar_timeouts()`.
    ///
    /// ¿Por qué 100 ms y no `recv_timeout(1s)`? Los 3 nodos mandan sus latidos
    /// por el mismo canal: con un timeout de 1 s, el latido de cualquier nodo
    /// lo reiniciaría aunque otro nodo esté caído. Por eso el timeout funciona
    /// como reloj y el tiempo sin latidos se mide por separado para cada nodo.
    pub fn monitorear(&mut self) {
        loop {
            match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(MensajeMonitor::Heartbeat(id)) => {
                    println!("Monitor: latido del nodo {id}");
                    if let Some((instante, estado)) = self.nodos.get_mut(&id) {
                        *instante = Instant::now();
                        if *estado != EstadoNodo::Activo {
                            *estado = EstadoNodo::Activo;
                            println!("El nodo {id} volvio a estado {estado}");
                        }
                    }
                }
                Ok(MensajeMonitor::NuevoLider(id)) => {
                    self.lider = id;
                    println!("Nuevo coordinador elegido por los nodos: nodo {id}");
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
            if self.verificar_timeouts() {
                break;
            }
        }
        for tx in self.tx_nodos.values() {
            let _ = tx.send(MensajeNodo::Kill);
        }
    }

    /// Recorre los nodos y actualiza su estado según el tiempo desde su último
    /// latido: 2 s o más → CAIDO; entre 1 s y 2 s → SOSPECHOSO.
    ///
    /// Si el nodo caído era el líder, dispara la elección entre los vivos y
    /// devuelve `false` (el monitoreo sigue hasta que haya nuevo líder).
    /// Si cayó un nodo que no era líder, devuelve `true` para terminar el
    /// monitoreo (la demo termina ahí, porque el líder sigue activo).
    pub fn verificar_timeouts(&mut self) -> bool {
        let mut lider_caido = false;
        let mut otro_caido = false;

        for (nodo, (instant, estado)) in &mut self.nodos {
            let tiempo_transcurrido = instant.elapsed();
            if tiempo_transcurrido >= Duration::from_secs(2) {
                if *estado != EstadoNodo::Caido {
                    *estado = EstadoNodo::Caido;
                    println!("El nodo {nodo} paso a estado {estado}");
                    if *nodo == self.lider {
                        lider_caido = true;
                    } else {
                        otro_caido = true;
                    }
                }
            } else if tiempo_transcurrido >= Duration::from_secs(1)
                && *estado != EstadoNodo::Sospechoso
            {
                *estado = EstadoNodo::Sospechoso;
                println!("El nodo {nodo} paso a estado {estado}");
            }
        }
        if lider_caido {
            self.empezar_eleccion();
            return false;
        }
        // El monitor no la corta si hay una eleccion en curso.
        otro_caido && self.nodos[&self.lider].1 != EstadoNodo::Caido
    }

    /// Envía `IniciarEleccion` a todos los nodos que no están CAIDOS.
    fn empezar_eleccion(&self) {
        for (id, (_, estado)) in &self.nodos {
            if *estado != EstadoNodo::Caido
                && let Some(tx) = self.tx_nodos.get(id)
            {
                let _ = tx.send(MensajeNodo::IniciarEleccion);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::mpsc;

    fn hace(ms: u64) -> Instant {
        Instant::now() - Duration::from_millis(ms)
    }

    fn monitor_de_prueba() -> (Monitor, HashMap<u64, Receiver<MensajeNodo>>) {
        let (_tx, rx) = mpsc::channel::<MensajeMonitor>();
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();
        for id in [1, 2, 3] {
            let (s, r) = mpsc::channel();
            senders.insert(id, s);
            receivers.insert(id, r);
        }
        (Monitor::new(vec![1, 2, 3], rx, 3, senders), receivers)
    }

    fn nodo_prueba(
        id: u64,
        otros: &[u64],
    ) -> (
        Nodo,
        HashMap<u64, Receiver<MensajeNodo>>,
        Receiver<MensajeMonitor>,
    ) {
        let (tx_monitor, rx_monitor) = mpsc::channel();
        let (_tx_nodo, rx_nodo) = mpsc::channel();
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();

        for &otro_nodo in otros {
            let (s, r) = mpsc::channel();
            senders.insert(otro_nodo, s);
            receivers.insert(otro_nodo, r);
        }
        let nodo = Nodo::new(id, tx_monitor, rx_nodo, senders, 3);
        (nodo, receivers, rx_monitor)
    }

    #[test]
    fn nodo_pasa_a_sospechoso_despues_1s() {
        let (mut monitor, _rx) = monitor_de_prueba();
        monitor.nodos.get_mut(&1).unwrap().0 = hace(1200);

        assert!(!monitor.verificar_timeouts());
        assert_eq!(monitor.nodos[&1].1, EstadoNodo::Sospechoso);
    }

    #[test]
    fn nodo_pasa_a_caido_despues_2s() {
        let (mut monitor, _rx) = monitor_de_prueba();
        monitor.nodos.get_mut(&1).unwrap().0 = hace(2100);

        assert!(monitor.verificar_timeouts());
        assert_eq!(monitor.nodos[&1].1, EstadoNodo::Caido);
    }
    #[test]
    fn nodo_caido_no_es_lider_y_lider_se_mantiene() {
        let (mut monitor, _rx) = monitor_de_prueba();
        monitor.nodos.get_mut(&1).unwrap().0 = hace(2100);

        assert!(monitor.verificar_timeouts());
        assert_eq!(monitor.nodos[&1].1, EstadoNodo::Caido);
        assert_eq!(monitor.lider, 3);
    }

    #[test]
    fn caida_del_lider_empieza_eleccion() {
        let (mut monitor, rx) = monitor_de_prueba();
        monitor.nodos.get_mut(&3).unwrap().0 = hace(2100);

        assert!(!monitor.verificar_timeouts());
        assert_eq!(monitor.nodos[&3].1, EstadoNodo::Caido);
        assert_eq!(rx[&1].try_recv(), Ok(MensajeNodo::IniciarEleccion));
        assert_eq!(rx[&2].try_recv(), Ok(MensajeNodo::IniciarEleccion));

        assert!(rx[&3].try_recv().is_err());
    }

    #[test]
    fn bully_responde_ok_y_se_postula() {
        let (mut nodo, rx, _rx_mon) = nodo_prueba(2, &[1, 3]);

        nodo.manejar(MensajeNodo::Election(1));

        assert_eq!(rx[&1].try_recv(), Ok(MensajeNodo::Ok(2)));
        assert_eq!(rx[&3].try_recv(), Ok(MensajeNodo::Election(2)));

        assert!(nodo.espera_ok.is_some());
    }

    #[test]
    fn bully_nodo_mayor_coordinador() {
        let (mut nodo, rx, rx_mon) = nodo_prueba(3, &[1, 2]);

        nodo.manejar(MensajeNodo::IniciarEleccion);
        assert_eq!(nodo.lider, 3);
        assert_eq!(rx[&1].try_recv(), Ok(MensajeNodo::Coordinator(3)));
        assert_eq!(rx[&2].try_recv(), Ok(MensajeNodo::Coordinator(3)));
        assert_eq!(rx_mon.try_recv(), Ok(MensajeMonitor::NuevoLider(3)));
    }
    #[test]
    fn bully_sin_rta_ok_gana_por_timeout() {
        let (mut nodo, _rx, rx_mon) = nodo_prueba(2, &[1, 3]);

        nodo.manejar(MensajeNodo::IniciarEleccion);
        nodo.espera_ok = Some(hace(10));
        nodo.revisar_timeouts();

        assert_eq!(nodo.lider, 2);
        assert_eq!(rx_mon.try_recv(), Ok(MensajeMonitor::NuevoLider(2)));
    }
}
