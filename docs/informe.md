# TP2 - Motor Distribuido de Reservas y Pagos

Borrador del informe (después se exporta a PDF/Word para la entrega).

## Parte 1: Modelos de Concurrencia y Actores

### 1.1 Diseño del modelo (actores y mensajes)

Código: `src/actores/` (`cuenta_usuario.rs`, `inventario_entradas.rs`, `mod.rs`).

En el TP1 el estado compartido se protegía con primitivas de sincronización
sobre memoria compartida. Acá cambiamos el enfoque, cada entidad es un
actor, un hilo que es dueño exclusivo de su estado y que solo
se comunica recibiendo y enviando mensajes. Implementamos los actores con la
biblioteca estándar (`std::thread` y `std::sync::mpsc`), sin dependencias
externas.

**Anatomía de un actor**

| Pieza | Implementación | Rol |
|-------|----------------|-----|
| Estado | `struct CuentaUsuario` / `struct InventarioEntradas` (campos privados) | Se mueve al hilo del actor en `iniciar()`. Nadie más tiene acceso. |
| Buzón (mailbox) | `mpsc::Receiver<Mensaje...>` | Cola FIFO de mensajes pendientes. |
| Loop | `for msg in rx { self.manejar(msg) }` | Procesa **un mensaje por vez**, hasta recibir `Detener` o hasta que se cierren todos los `Sender`. |
| Dirección | `CuentaHandle` / `InventarioHandle` (envuelven el `Sender`, son `Clone`) | Lo único que comparten los demás hilos. |
| Respuesta | Cada mensaje trae un `responder: Sender<...>` propio | Patrón *request-reply* sobre canales. |

**Actor `CuentaUsuario`**

- Estado: `saldo_disponible`, `debitado` (total cobrado), `retenciones: tx → monto`
  y `finalizadas: tx → Confirmada | Cancelada`.
- Mensajes:

```rust
pub enum MensajeCuenta {
    ConsultarSaldo  { responder: Sender<SaldoCuenta> },
    Acreditar       { monto: Monto, responder: Sender<Result<(), ErrorCuenta>> },
    SolicitarDebito { tx: IdTransaccion, monto: Monto, responder: Sender<Result<(), ErrorCuenta>> }, // retiene (prepare)
    ConfirmarDebito { tx: IdTransaccion, responder: Sender<Result<(), ErrorCuenta>> },               // cobra (commit)
    CancelarDebito  { tx: IdTransaccion, responder: Sender<Result<(), ErrorCuenta>> },               // devuelve (rollback)
    Detener,
}
```

**Actor `InventarioEntradas`**

- Estado: `asientos: id → Libre | Reservado(tx) | Vendido(tx)`, `reservas: tx → asiento`
  y `finalizadas`.
- Mensajes:

```rust
pub enum MensajeInventario {
    ConsultarAsiento { asiento: IdAsiento, responder: Sender<Option<EstadoAsiento>> },
    ListarAsientos   { responder: Sender<Vec<(IdAsiento, EstadoAsiento)>> },
    SolicitarReserva { tx: IdTransaccion, asiento: IdAsiento, responder: Sender<Result<(), ErrorInventario>> }, // bloquea (prepare)
    ConfirmarReserva { tx: IdTransaccion, responder: Sender<Result<(), ErrorInventario>> },                     // vende (commit)
    LiberarReserva   { tx: IdTransaccion, responder: Sender<Result<(), ErrorInventario>> },                     // libera (rollback)
    Detener,
}
```

Máquinas de estado de una transacción en cada actor:

```text
CuentaUsuario:       SolicitarDebito ──► retenido ──ConfirmarDebito──► confirmado
                                            └───────CancelarDebito───► cancelado

InventarioEntradas:  Libre ──SolicitarReserva(tx)──► Reservado(tx) ──ConfirmarReserva──► Vendido(tx)
                       ▲                                  │
                       └──────────LiberarReserva──────────┘
```

Un asiento `Vendido` no vuelve nunca a `Libre`. Eso garantiza localmente que
una entrada no se vende dos veces.

**Decisiones de diseño**

1. **Operaciones en dos fases.** Toda operación que modifica estado tiene una
   fase provisoria (retener fondos o reservar el asiento) y otra definitiva
   (confirmar o deshacer). Lo diseñamos así pensando en la Parte 3: en el
   Two-Phase Commit, estos mismos mensajes son el `Prepare`, el `Commit` y el
   `Rollback` de cada participante.
2. **Idempotencia por `IdTransaccion`.** Si un mensaje se repite con la misma
   `tx`, su efecto no se duplica: confirmar dos veces cobra una sola vez. Esto
   es necesario porque en un sistema distribuido un coordinador que no recibió
   la respuesta tiene que poder reintentar sin miedo.
3. **Aborto presunto.** Cancelar o liberar una `tx` que el actor no conoce
   responde `Ok` y la registra como cancelada. Así, un `SolicitarDebito`
   que llega tarde (por ejemplo, demorado en la red) no puede revivir una
   transacción que ya se abortó.
4. **Transiciones inválidas rechazadas.** Confirmar algo cancelado o liberar
   algo vendido devuelve un error y no cambia el estado.
5. **Montos en centavos (`u64`).** Nunca se usa `f64`: los flotantes no
   representan exactamente valores como 0,10 y acumulan error, y el enunciado
   exige cobros exactos. Además, `Acreditar` usa `checked_add` para detectar
   desbordamientos.
6. **Lógica separada del loop.** Cada actor expone su lógica como métodos
   puros (`solicitar_debito`, `confirmar_reserva`, ...). El loop solo
   despacha mensajes. Esto permite testear la lógica sin hilos y la
   concurrencia por separado.

**Ejemplo: una compra.** La orquesta `comprar_entrada` en `src/actores/mod.rs`.
Es una orquestación local y simple que la Parte 3 reemplaza por el
coordinador 2PC.

```text
Cliente                InventarioEntradas              CuentaUsuario
   │ SolicitarReserva(tx, 3) ──►│                              │
   │◄──────────── Ok ───────────│ asiento 3: Reservado(tx)     │
   │ SolicitarDebito(tx, $150) ─┼─────────────────────────────►│
   │◄────────────────────── Ok ─┼──────────────────────────────│ disponible -= 150; retenido += 150
   │ ConfirmarReserva(tx) ─────►│ asiento 3: Vendido(tx)       │
   │ ConfirmarDebito(tx) ───────┼─────────────────────────────►│ retenido -= 150; debitado += 150
   │                            │                              │
   │ (si el débito falla: LiberarReserva(tx) + CancelarDebito(tx))
```

### 1.2 Por qué el Modelo de Actores elimina las data races sin locks

**Qué es una data race.** Hay una *data race* cuando dos hilos acceden a la
misma posición de memoria al mismo tiempo, al menos uno de los accesos es una
escritura y no hay ninguna sincronización que ordene esos accesos. El
resultado depende del intercalado del scheduler. El ejemplo clásico es la
*lost update*: dos hilos leen `saldo = 100`, cada uno resta 80 y escribe 20.
Se cobró dos veces y el saldo quedó positivo.

Para que exista una data race se necesitan las tres condiciones a la vez:
**(1) estado compartido**, **(2) mutable** y **(3) acceso concurrente**. El modelo
de memoria compartida ataca la condición 3 con locks: los accesos siguen
siendo concurrentes, pero el mutex los ordena. El modelo de actores, en
cambio, elimina **la condición 1 por diseño**.

**1. El estado no se comparte: tiene un único dueño.** El estado de la cuenta
vive dentro del hilo del actor y ningún otro hilo tiene una referencia a él.
En nuestro código, `CuentaUsuario::iniciar(self)` toma el estado **por valor**
y lo mueve al closure de `thread::spawn`:

```rust
pub fn iniciar(self) -> (CuentaHandle, JoinHandle<()>) {
    let (buzon, rx) = mpsc::channel();
    let hilo = thread::spawn(move || self.ejecutar(rx)); // `self` se mueve al hilo
    (CuentaHandle { buzon }, hilo)                        // afuera solo queda el Sender
}
```

Después del `move`, el hilo que creó la cuenta ya no puede usarla. Si lo
intenta, el código no compila (*use of moved value*). En Rust esto no es una
convención que haya que respetar: el **sistema de ownership** lo garantiza en
tiempo de compilación. Lo único que sale del hilo es un `Sender`, que no da
acceso al saldo. Como no hay `Arc<Mutex<CuentaUsuario>>` ni ningún otro
alias, no existe memoria compartida y mutable que proteger.

**2. La comunicación es por mensajes que transfieren ownership.** Los datos que
viajan en un mensaje (`tx`, `monto`, el `responder`) se **mueven** por el canal:
el emisor deja de tenerlos y el actor pasa a ser su dueño. El canal exige que
el tipo sea `Send`, y el compilador rechaza tipos que no se pueden transferir
de forma segura entre hilos, como `Rc` o `RefCell`. Por lo tanto, ningún dato
queda accesible desde dos hilos a la vez.

**3. El buzón serializa las operaciones.** Mil hilos pueden enviar mensajes a la
misma cuenta al mismo tiempo, pero el actor los saca del buzón y los procesa
**de a uno**, hasta terminar cada handler antes de empezar el siguiente. Todo
handler es atómico respecto de los demás, sin necesidad de que el
programador marque una sección crítica. Por eso este *check-and-act* es
correcto:

```rust
if self.saldo_disponible < monto { return Err(SaldoInsuficiente{..}); }
self.saldo_disponible -= monto;           // nadie pudo modificar el saldo entre el if y la resta
self.retenciones.insert(tx, monto);
```

Con memoria compartida y sin lock, entre el `if` y la resta otro hilo podría
gastar el mismo saldo. Es una condición de carrera de tipo TOCTOU
(*time-of-check to time-of-use*). En el actor eso no puede pasar, porque no
existe otro hilo que toque `saldo_disponible`.

la sincronización no desaparece: está encapsulada dentro
del canal. `mpsc` internamente usa operaciones atómicas para que muchos
productores encolen de forma segura. La diferencia es que esa sincronización
es una sola, genérica y probada, y protege la cola, no el estado de negocio.
Sobre el estado de la cuenta no hay ningún lock, y el programador no puede
olvidarse de tomarlo ni tomarlos en el orden equivocado.

**Verificación empírica** (`tests/parte1_actores.rs`): 1000 hilos piden el
mismo asiento a la vez y exactamente uno lo obtiene. 100 hilos intentan
debitar de una cuenta con saldo para 10 débitos y exactamente 10 lo logran, con
saldo final exacto. 200 hilos mezclan acreditaciones, débitos y cancelaciones
sin que se pierda ninguna actualización.

**Límites del modelo.**

- **Condiciones de carrera lógicas entre actores.** Cada actor es consistente
  por separado, pero una compra toca *dos* actores y entre el mensaje a uno y
  el mensaje al otro el mundo puede cambiar (o el orquestador puede caerse).
  La atomicidad entre actores no la da el modelo: requiere un protocolo. Ese es justamente el motivo de que
  los mensajes ya estén divididos en prepare, commit y rollback.
- **Deadlocks.** Si el actor A espera sincrónicamente una respuesta de B
  mientras B espera una de A, ambos quedan bloqueados. Por eso nuestros actores
  nunca esperan a otro actor: la coordinación queda afuera, en el
  orquestador o coordinador.
- **Cuello de botella.** Un actor procesa de a un mensaje, así que un actor puede limitar
  el throughput. Se mitiga particionando el estado: un actor por evento o por
  sector, ya que asientos de distintos sectores no compiten entre sí.

### 1.3 Async/Await (Tokio) vs. thread-per-request con 50.000 conexiones

**El punto de partida: casi todo el tiempo se espera.** Cuando un usuario
compra una entrada, su conexión pasa la mayor parte del tiempo esperando: que
llegue el pedido completo por la red, que responda otro servicio o que el
usuario reciba la respuesta. El trabajo real del procesador es muy poco. Por lo
tanto, con 50.000 conexiones la pregunta no es cuánto cuesta trabajar, sino
**cuánto cuesta esperar**.

**Una analogía: el restaurante.**

- *Thread-per-request* es un restaurante que asigna **un mozo por mesa**. El
  mozo toma el pedido y se queda parado al lado de la mesa hasta que llega la
  comida, sin atender a nadie más. Con 50.000 mesas hacen falta 50.000 mozos, y
  casi todos están quietos esperando.
- *Async/Await* es un restaurante con **pocos mozos que atienden todas las
  mesas**. El mozo toma un pedido, lo pasa a la cocina y, mientras se cocina,
  atiende otra mesa. Cuando un plato está listo, la cocina avisa y algún mozo
  libre lo lleva. Con unos pocos mozos se atienden miles de mesas.

**¿Por qué un hilo por conexión escala mal?**

1. **Cada hilo es caro.** El sistema operativo le reserva a cada hilo su propia
   memoria (su *stack*) y estructuras internas para administrarlo. Uno solo
   no pesa, pero 50.000 juntos consumen muchísima memoria, aunque estén
   quietos.
2. **El sistema operativo tiene un límite de hilos.** Muchas configuraciones no
   permiten crear tantos, y el servidor directamente deja de aceptar
   conexiones.
3. **Cambiar de hilo cuesta.** Con miles de hilos, el sistema operativo
   pasa mucho tiempo alternando entre ellos: guarda el estado de uno, carga el
   del otro, y cada cambio implica intervención del kernel. Ese tiempo se
   pierde administrando en lugar de atender pedidos.
4. **Se paga por no hacer nada.** La mayoría de esos hilos están bloqueados
   esperando datos, pero igual ocupan memoria y lugar en el planificador.

**¿Por qué Async/Await escala mejor?**

1. **Tareas livianas en lugar de hilos.** Cada conexión es una *tarea*, no un
   hilo. Una tarea solo guarda lo mínimo para retomar donde quedó, así que
   ocupa una fracción mínima de lo que ocupa un hilo. 50.000 tareas entran
   cómodamente en memoria.
2. **Pocos hilos hacen todo el trabajo.** Tokio usa aproximadamente un hilo por
   núcleo del procesador y reparte las tareas entre ellos. La cantidad de hilos
   no crece aunque crezcan las conexiones.
3. **Esperar no ocupa a nadie.** Cuando una tarea llega a un `.await` y los
   datos todavía no están, la tarea se "estaciona" y el hilo queda libre para
   otra. Cuando el sistema operativo avisa que llegaron datos para esa
   conexión, la tarea se retoma. Ese aviso llega para muchas conexiones a la
   vez, así que nadie tiene que estar preguntando una por una.
4. **Cambiar de tarea es barato.** Pasar de una tarea a otra lo hace Tokio
   dentro del programa, sin pedirle nada al sistema operativo. Es mucho más
   rápido que cambiar de hilo.

**Resumen**

| | Un hilo por conexión | Async/Await (Tokio) |
|---|---|---|
| Qué atiende cada conexión | un hilo del sistema operativo | una tarea liviana |
| Hilos para 50.000 conexiones | 50.000 | unos pocos (≈ cantidad de núcleos) |
| Mientras espera datos | el hilo queda bloqueado | la tarea se estaciona y el hilo atiende otra |
| Memoria | mucha, y puede chocar con límites del sistema | poca |
| Cambiar entre conexiones | lento (lo hace el sistema operativo) | rápido (lo hace el programa) |

**La condición: no bloquear.** El modelo funciona porque las tareas ceden el
hilo cada vez que esperan (en cada `.await`). Si una tarea hace algo que
bloquea sin ceder, como un cálculo muy largo o una espera que no es async,
ocupa el hilo y frena a todas las tareas que dependen de él. Siguiendo con la
analogía, es como un mozo que se pone a cocinar: deja de atender sus mesas.
Por eso ese tipo de trabajo se manda a hilos aparte (en Tokio,
`spawn_blocking`). Además, async rinde cuando el programa pasa la mayor parte
del tiempo esperando. Si el problema fuera puro cálculo, no ganaría nada.

**Relación con nuestro diseño.** Las dos ideas se complementan. El servidor web
async sostiene las 50.000 conexiones con pocos hilos y convierte cada pedido
en un **mensaje** a los actores (`SolicitarReserva`, `SolicitarDebito`, ...).
Los actores, a su vez, procesan esos mensajes de a uno y protegen su estado sin
locks. Con Tokio, cada actor sería una tarea en lugar de un hilo, pero los
mensajes serían exactamente los mismos que diseñamos.

## Parte 2: Coordinación y Tolerancia a Fallos
### 2.1 Detección de fallos (diseño del simulador)
Para modelar el cluster de 3 nodos coordinadores se diseño una arquitectura usando hilos y pasaje de mensajes por canales MPSC.
- Componentes:
   - Nodos(`Nodo`): Se instancian 3 nodos con ids unicos, donde el nodo es el nodo de mayor ID. Cada nodo tiene su propio hilo y envia un mensaje (`MensajeMonitor::Heartbeat(id)`) cada 500ms hacia el Monitor.
   - Monitor (`Monitor`): Detecta fallos. Mantiene un Hash en el que registra el tiempo del ultimo ladito recibido y estado actual de cada nodo.
- Deteccion basada en timeouts:
   - El monitor escucha en su canal de entrada mediante rec_timeout(100ms). Al recivir un latido, actualiza el Instant del nodo correspondiente, y si se encontraba en estado Sospechoso lo vuelve a poner como Activo.
   - En cada iteracion ejecuta `verificar_timeouts()`, evaluando el tiempo transcurrido desde el ultimo latido de cada nodo.
| Tiempo sin rta | Estado | Accion del Monitor |
| --- | --- | --- |
| < 1s | `ACTIVO` | operacion normal | 
| >= 1s y < 2s | `SOSPECHOSO` | Registra que esta demorando (Lo puede reveritr a ACTIVO si le llega un latido antes de los 2 seg)|
| >= 2s | `CAIDO` | Si es un nodo comun, el lider actual se mantiene. Si es el lider, dispara la eleccion | 

- Simulacion de caida aleatoria: Desde main, un hilo auxiliar espera 3 segundos, elige un nodo al azar y le envia `MensajeNodo::Kill`, haciendo que ese nodo salga de su bucle y deje de enviar heartbeats.

### 2.2 Algoritmo Bully para elegir el nuevo líder

El Algoritmo Bully asume que todos los procesos tienen un id unico, que cada nodo conoce a sus pares y que el coordinador siempmre debe ser el nodo activo con el ID mas alto.

Escenario en el cluster de 3 nodos. (Lider inicial Nodo 3)
- Caso A - Cae un nodo no lider (Nodo 1 o Nodo 2)
El monitor detecta el fallo tras 2 segundos sin latidos y marca al nodo como CAIDO. Como lider sigue enviando hearbeats, el cluster continua operando sin necesidad de iniciar una eleccion. (En el codigo, se hizo para demostracion, que una vez que cae un nodo, que el programa corte. Si es necesaria una eleccion, se elige y luego corta).
- Caso B - Cae el nodo lider (Nodo 3):
1. Despues de 2 segundos sin recibir heartbeats del Nodo 3, el Monitor lo declara como `CAIDO` y notifica a los nodos sobrevivientes (1 y 2), mediante `IniciarEleccion`.
2. Envio de `ELECTION`: Cada nodo inicia la eleccion envia el mensaje `ELECTION` a todos lo snodos con ID mayor al suyo y activa un temporizador. 
   - El Nodo 1 envia ELECTION a los nodos `2` y `3`.
   - El Nodo 2 envia ELECTION solo al nodo `3`.
3. Respuesta `OK`: Al recibir `ELECTION(1)`, el Nodo 2 (activo y que tiene mayor prioridad), responde `OK(2)` al Nodo 1 y continua con su propia posutlaicon. Al recibir el `OK` el Nodo 1 corta su candidatura y espera a que se le anuncie el ganador bajo un timeout (`espera_coord`).
4. Proclamacion(`COORDINATOR`): Como el Nodo 3 esta caido, nunca responde `OK` al Nodo 2. Al vencer el temporizador `espera_ok` (300ms) sin respuestas, el Nodo 2 concluye que es el nodo vivo de mayor jerarquia, se autoproclama nuevo coordinador, envia `COORDINATOR(2)` a sus pares y notifica `NuevoLider(2)` al Monitor.  
5. Tolerancia a fallos: Si el Nodo 2 tambien cayera luego de haber enviado `OK`, se venceria el temporizador `espera_coord` en el Nodo 1, el cual reinciaria la elección y, al no oobtener respuesta de nodos mayores, se proclamaria lider.

### 2.3 Plan de pruebas parte 2.

- **Pruebas unitarias ** (`src/heartbeat.rs`): Evalúan la lógica interna de `Monitor` y `Nodo` con marcas de tiempo pasadas (`Instant::now() - Duration`) y leyendo mensajes directamente desde los canales `mpsc` con `try_recv()`, sin hilos ni `thread::sleep`.
- **Pruebas End-2-End** (`tests/parte2_heartbeats.rs`): Levantan el clúster real de 3 nodos concurrentes más el Monitor con fallos inducidos (`MensajeNodo::Kill`), validando tiempos reales, intercambio de mensajes Bully y cierre limpio de hilos (`join`).


## 1. Pruebas Unitarias (`src/heartbeat.rs`)

| Caso de prueba | Escenario / Entrada | Resultado esperado | Aspecto validado |
|---|---|---|---|
| `nodo_pasa_a_sospechoso_despues_1s` | Último latido del nodo 1 hace 1200 ms. Se invoca `verificar_timeouts()`. | Devuelve `false`. El estado del nodo 1 pasa a `Sospechoso`. | Detección de demora ($\ge 1\text{ s}$ y $< 2\text{ s}$) sin corte prematuro del monitor. |
| `nodo_pasa_a_caido_despues_2s` | Último latido del nodo 1 hace 2100 ms. Se invoca `verificar_timeouts()`. | Devuelve `true`. El estado del nodo 1 pasa a `Caido`. | Detección de fallo confirmado ($\ge 2\text{ s}$). |
| `nodo_caido_no_es_lider_y_lider_se_mantiene` | Clúster `[1, 2, 3]` con líder 3. Nodo 1 sin latido hace 2100 ms. | `verificar_timeouts()` devuelve `true`, nodo 1 queda `Caido` y `monitor.lider` permanece en 3. | Estabilidad del liderazgo ante caída de un nodo común. |
| `caida_del_lider_empieza_eleccion` | Clúster `[1, 2, 3]` con líder 3. Nodo 3 sin latido hace 2100 ms. | `verificar_timeouts()` devuelve `false`, nodo 3 pasa a `Caido`. Los canales de 1 y 2 reciben `IniciarEleccion`; el de 3 no recibe nada. | Disparo selectivo de Bully solo hacia nodos vivos al caer el líder. |
| `bully_responde_ok_y_se_postula` | Nodo 2 (pares `[1, 3]`) procesa `MensajeNodo::Election(1)`. | Envía `Ok(2)` al nodo 1, envía `Election(2)` al nodo 3 y activa `espera_ok`. | Respuesta de prioridad y propagación ascendente de Bully. |
| `bully_nodo_mayor_coordinador` | Nodo 3 (pares `[1, 2]`) procesa `IniciarEleccion`. | Actualiza `lider = 3`, envía `Coordinator(3)` a 1 y 2, y envía `NuevoLider(3)` al Monitor. | Proclamación inmediata cuando no existen pares con ID mayor. |
| `bully_sin_rta_ok_gana_por_timeout` | Nodo 2 inicia elección hacia 3; se simula el vencimiento de `espera_ok` y se invoca `revisar_timeouts()`. | Actualiza `lider = 2` y envía `NuevoLider(2)` al Monitor. | Victoria por timeout cuando los nodos superiores están caídos. |

## 2. Pruebas de Integración / E2E (`tests/parte2_heartbeats.rs`)

Todos los casos instancian 3 nodos en hilos separados con líder inicial 3, inducen fallos mediante `Kill` y verifican el `join()` sin deadlocks ni panics.

| Caso de prueba | Estímulo / Fallo inducido | Resultado esperado | Aspecto validado |
|---|---|---|---|
| `e2e_caida_lider_elige_nuevo_coordinador` | Se envía `Kill` al nodo 3 (líder) a los 500 ms. | El Monitor detecta la caída tras 2 s, dispara Bully y finaliza con `monitor.lider == 2`, nodo 3 en `Caido`, nodos 1 y 2 en `Activo`. | Flujo completo de detección + elección `ELECTION → OK → COORDINATOR` en concurrencia real. |
| `e2e_caida_nodo_comun_mantiene_lider` | Se envía `Kill` al nodo 1 a los 500 ms. | El Monitor marca al nodo 1 como `Caido`, corta el monitoreo y mantiene `monitor.lider == 3`, con nodos 2 y 3 en `Activo`. | Continuidad del líder sin elecciones innecesarias ante fallos no críticos. |
| `e2e_caidas_nodos_2_y_3` | Se envía `Kill` al nodo 3 y al nodo 2 de forma consecutiva. | El nodo 1 inicia elección, vence `espera_ok` sin respuestas y resulta elegido (`monitor.lider == 1`), con nodo 1 en `Activo`, nodo 3 en `Caido` y nodo 2 fuera de `Activo`. | Fallo múltiple simultáneo y promoción del único sobreviviente. |


## Parte 3: Transacciones Distribuidas
### 3.1 Coordinador 2PC (Prepare y Commit/Rollback)

Código: `src/two_phase_commit.rs`.

Una compra tiene que debitar el dinero en `ServicioPagos` y asignar el asiento
en `ServicioInventario`. Las dos cosas pasan o no pasa ninguna. Para lograrlo,
el `Coordinador` ejecuta un Two-Phase Commit entre los dos servicios.

**Participantes.** Cada servicio implementa el trait `Participante`, que tiene
las tres operaciones que el coordinador necesita:

```rust
pub trait Participante {
    fn nombre(&self) -> &str;
    fn prepare(&self, compra: &Compra) -> Voto;                  // Fase 1
    fn commit(&self, tx: IdTransaccion) -> Result<(), String>;   // Fase 2
    fn rollback(&self, tx: IdTransaccion) -> Result<(), String>; // Fase 2
}

pub enum Voto { Yes, No(String) }
pub enum Decision { Commit, Rollback }
```

Los servicios no tienen lógica propia: traducen cada operación del 2PC a los
mensajes de los actores de la Parte 1, que ya estaban divididos en dos fases.

| Participante | Prepare | Commit | Rollback |
|---|---|---|---|
| `ServicioPagos` (sobre `CuentaUsuario`) | `SolicitarDebito`: retiene el precio | `ConfirmarDebito`: lo cobra | `CancelarDebito`: lo devuelve |
| `ServicioInventario` (sobre `InventarioEntradas`) | `SolicitarReserva`: reserva el asiento | `ConfirmarReserva`: lo vende | `LiberarReserva`: lo libera |

Si la operación del actor devuelve `Ok`, el participante vota `Yes`. Cualquier
error, incluido `ActorDetenido`, es un voto `No` con el motivo. Votar `Yes`
significa que el recurso quedó reservado: nadie más puede usar ese saldo ni
ese asiento hasta que llegue la decisión.

**Coordinador.** El método `Coordinador::ejecutar` tiene las dos fases
explícitas:

```rust
// Fase 1 (Prepare): se le pide el voto a todos los participantes.
self.log.push(EntradaLog::EsperandoVotos(tx));
let mut motivos = Vec::new();
for participante in participantes {
    if let Voto::No(motivo) = participante.prepare(compra) {
        motivos.push(format!("{}: {motivo}", participante.nombre()));
    }
}

// Commit solo si todos votaron YES. La decisión se anota antes de comunicarla.
let decision = if motivos.is_empty() { Decision::Commit } else { Decision::Rollback };
self.log.push(EntradaLog::Decision(tx, decision));

// Fase 2 (Commit/Rollback): la decisión se manda a todos, con reintentos.
let sin_respuesta = self.fase2(tx, decision, participantes);
// Si alguno no respondió: Incompleta (la decisión queda en el log).
// Si todos respondieron: Confirmada o Abortada.
```

```rust
fn fase2(&mut self, tx: IdTransaccion, decision: Decision,
         participantes: &[&dyn Participante]) -> Vec<String> {
    let mut sin_respuesta = Vec::new();
    for participante in participantes {
        if !enviar_decision(*participante, tx, decision) {
            sin_respuesta.push(participante.nombre().to_string());
        }
    }
    if sin_respuesta.is_empty() {
        self.log.push(EntradaLog::Fin(tx));
    }
    sin_respuesta
}
```

```text
Coordinador              ServicioPagos            ServicioInventario
    │ log: EsperandoVotos(tx)  │                          │
    │── Prepare(tx, $150) ────►│ retiene $150             │
    │◄──────────── Yes ────────│                          │
    │── Prepare(tx, asiento 3) ┼─────────────────────────►│ reserva el asiento 3
    │◄──────────── Yes ────────┼──────────────────────────│
    │ log: Decision(tx, Commit)│                          │
    │── Commit(tx) ───────────►│ cobra $150               │
    │── Commit(tx) ────────────┼─────────────────────────►│ asiento 3: Vendido(tx)
    │ log: Fin(tx)             │                          │
```

El resultado es un `ResultadoTransaccion`:

- `Confirmada`: todos votaron `Yes` y todos aplicaron el commit.
- `Abortada { motivos }`: alguno votó `No` y todos deshicieron lo reservado.
- `Incompleta { decision, sin_respuesta }`: la decisión está tomada, pero
  algún participante no la confirmó después de todos los intentos. Cuando
  vuelve a estar accesible, `Coordinador::reenviar_decision(tx, ...)` le
  manda la decisión anotada en el log y cierra la transacción (ver 3.2).

**Decisiones de diseño**

1. **Log del coordinador.** El coordinador anota `EsperandoVotos`, después la
   `Decision` y al final `Fin`. La decisión se escribe *antes* de mandar la
   fase 2: ese es el punto de commit. A partir de ahí la decisión no cambia, y
   si el coordinador se cayera, al volver sabría qué reenviar. En un sistema
   real el log está en disco. Acá es un `Vec` en memoria.
2. **El rollback va a todos.** También a los participantes que votaron `No` o
   que no respondieron. Como los actores implementan aborto presunto (Parte 1),
   deshacer una `tx` que no conocen responde `Ok` y la marca como cancelada. Así,
   si a un participante el `Prepare` le llega tarde, lo rechaza.
3. **Reintentos en la fase 2.** `enviar_decision` manda la decisión hasta
   `INTENTOS_FASE2` (3) veces, con una pausa entre intentos. Reintentar no
   tiene riesgo porque commit y rollback son idempotentes (Parte 1): confirmar
   dos veces cobra una sola vez. Si un participante no responde, el
   coordinador igual le manda la decisión al resto.
4. **Nunca se vuelve a votar una tx decidida.** Si se llama a `ejecutar` con
   una `tx` que ya está en el log, devuelve `ErrorCoordinador::TransaccionRepetida`
   sin contactar a nadie. Volver a pedir votos podría terminar en una decisión
   distinta de la anotada.
5. **Participantes como trait.** El coordinador no sabe qué hace cada
   participante, solo le pide votos y le manda decisiones. Esto permite
   agregar más servicios sin tocar el coordinador y probarlo con
   participantes de prueba que fallan a propósito.
6. **Los `Prepare` se mandan de a uno.** El coordinador pide los votos en
   orden y no corta en el primer NO: siempre los junta todos antes de
   decidir. Se podrían pedir en paralelo para bajar la latencia, pero el
   protocolo sería el mismo, porque la decisión se toma recién cuando están
   todos los votos.
7. **El coordinador no es un actor.** Lo ejecuta el hilo que atiende la
   compra (en el sistema completo, uno de los nodos coordinadores de la
   Parte 2). No comparte estado con nadie: se comunica con los servicios solo
   por mensajes, a través de los handles de la Parte 1. Varias compras pueden
   correr a la vez, cada una con su coordinador, y la exclusión mutua sobre
   asientos y saldos la siguen dando los actores. Lo verifica el test
   `compras_concurrentes_con_2pc_conservan_invariantes`.

**Propiedades ACID.** El objetivo de esta parte es mantener ACID aunque la
compra involucre a dos servicios:

- **Atomicidad:** es lo que garantiza el 2PC. O los dos servicios confirman o
  los dos deshacen.
- **Consistencia:** al terminar cada transacción se cumple el invariante del
  negocio: un asiento está vendido si y solo si se cobró su precio, y el
  dinero se conserva.
- **Aislamiento:** mientras la compra está en curso, el asiento `Reservado` y
  el saldo `retenido` funcionan como locks: ninguna otra transacción los
  puede usar.
- **Durabilidad:** depende de que el log del coordinador y el estado de los
  participantes sobrevivan a una caída. En un sistema real estarían en disco.
  En nuestra simulación están en memoria, así que esta propiedad no la
  cubrimos.

### 3.2 Partición de red después de que ambos votan YES

**El escenario.** Los dos servicios recibieron el `Prepare`, reservaron sus
recursos y votaron YES. El coordinador recibió los dos votos, decidió
`Commit` y lo anotó en su log. Justo ahí se produce una partición de red y los
mensajes de `Commit` no llegan.

```text
Coordinador                  ServicioPagos              ServicioInventario
    │── Prepare(tx, $150) ──────►│ retiene $150               │
    │◄─────────────── Yes ───────│                            │
    │── Prepare(tx, asiento 2) ──┼───────────────────────────►│ reserva el asiento 2
    │◄─────────────── Yes ───────┼────────────────────────────│
    │ log: Decision(tx, Commit)  │                            │
════╪═══════════ partición ══════╪════════════════════════════╪════
    │── Commit ──────X           │ PREPARED: no sabe          │ PREPARED: no sabe
    │── Commit ──────X           │ qué se decidió             │ qué se decidió
```

**Respuesta corta.** El sistema no queda inconsistente, pero se bloquea. Los
dos servicios se quedan esperando la decisión con el asiento y el dinero
reservados, y la compra no termina hasta que se restablece la comunicación
con el coordinador.

**1. Los participantes quedan en duda.** Al votar YES, cada participante se
comprometió a hacer lo que decida el coordinador y perdió el derecho a decidir
solo. Ahora está en el estado PREPARED (también llamado *incierto*) y no sabe
cuál fue la decisión. Desde su punto de vista hay dos situaciones posibles, y
no las puede distinguir:

- El coordinador decidió `Commit`, y quizás al otro participante sí le llegó.
- El coordinador decidió `Rollback`, por ejemplo porque el voto del otro
  participante no le llegó y lo contó como NO.

**2. No puede decidir solo.** Cualquier regla local, como "si pasan 30
segundos sin noticias, abortar", rompe la atomicidad en alguno de los dos casos:

| El participante decide solo... | ...pero el coordinador decidió | Resultado |
|---|---|---|
| Pagos hace rollback (devuelve el dinero) | Commit, e Inventario vendió el asiento | asiento vendido sin cobrar |
| Inventario hace rollback (libera el asiento) | Commit, y Pagos cobró | cobro sin entrada, y el asiento se puede vender a otro |
| Pagos hace commit (cobra) | Rollback | cobro sin entrada |
| Inventario hace commit (vende el asiento) | Rollback | asiento vendido sin cobrar |

Preguntarle al otro participante tampoco alcanza: si el otro también está en
PREPARED, sabe lo mismo que él. Si el otro ya hubiera recibido la decisión, o
hubiera votado NO, sí podría resolverle la duda, pero en este escenario a
ninguno le llegó la decisión y los dos votaron YES. La única opción segura es
**esperar** a que llegue.

**3. Los recursos quedan bloqueados.** Mientras espera, cada participante
tiene que mantener lo que reservó en el `Prepare`, porque es la garantía de
que va a poder cumplir con su voto:

- El asiento queda `Reservado(tx)`: no se vende ni se libera. Cualquier otro
  comprador que lo pida recibe "no disponible".
- El precio queda `retenido`: el usuario no lo puede usar para otra compra,
  pero tampoco se le cobró.

El bloqueo no afecta solo a esta compra, sino a todas las que necesitan esos
recursos. En una base de datos con locks, esas otras transacciones quedarían
esperando el lock. En nuestro diseño, se rechazan.

**4. El coordinador tampoco puede cerrar la transacción.** Su decisión ya es
definitiva, porque está en el log. Hasta puede avisarle al cliente que la
compra está confirmada: el commit es inevitable, porque los dos participantes
reservaron todo lo necesario y ya no pueden echarse atrás. Pero no puede
anotar `Fin` ni olvidar la `tx` hasta que los dos confirmen, así que sigue
reintentando.

**5. Cuando vuelve la red.** El coordinador reenvía la decisión anotada en el
log. También podría ser el participante el que, al reconectarse, le pregunta
qué se decidió. Como commit es idempotente, reenviarla no tiene riesgo aunque
a alguno ya le hubiera llegado. Los participantes la aplican y liberan los
recursos: el asiento queda vendido y el dinero cobrado. La partición demoró la
compra, pero no la dejó inconsistente.

**Si la partición es parcial.** Si a Inventario le llega el commit y a Pagos
no, durante un tiempo el asiento figura vendido y el dinero solo retenido. No
es una inconsistencia: Pagos ya no puede terminar en otra cosa que commit, así
que es un estado intermedio que se resuelve cuando le llega el mensaje.

**En nuestro código.** Para reproducir el escenario agregamos `Enlace`, que
simula la red entre el coordinador y un participante. Su método
`cortar_tras_prepare()` deja pasar el `Prepare` y el voto, y corta la red
justo después. Con los dos enlaces cortados así, `ejecutar` devuelve
`Incompleta { decision: Commit, .. }`.

En la simulación no hay timeouts reales: un mensaje que no llega se
representa con un error inmediato del `Enlace`. En un sistema distribuido
real, el coordinador esperaría cada respuesta con un timeout (por ejemplo, con
`recv_timeout`). Al vencer, haría lo mismo que hace acá: contar el voto como NO
en la fase 1, o reintentar la decisión en la fase 2.

El escenario 5 de la demo (`cargo run --bin parte3_2pc`) muestra todo lo
descrito arriba. La compra rival se hace desde otro coordinador, que sí tiene
conexión con los servicios:

```text
tx#5: la cuenta 1 compra el asiento 2 y la red se corta después de los votos
  log: EsperandoVotos(IdTransaccion(5))
  log: Decision(IdTransaccion(5), Commit)
  resultado: Incompleta { decision: Commit, sin_respuesta: ["ServicioPagos", "ServicioInventario"] }
  [OK] queda incompleta con decisión Commit
  [OK] el asiento 2 queda bloqueado en Reservado
  [OK] el precio queda retenido, sin cobrar

tx#6: la cuenta 3 ($300.00) intenta comprar el asiento 2 desde otro coordinador
  log: EsperandoVotos(IdTransaccion(6))
  log: Decision(IdTransaccion(6), Rollback)
  log: Fin(IdTransaccion(6))
  resultado: Abortada { motivos: ["ServicioInventario: el asiento 2 no está disponible"] }
  [OK] se aborta porque el asiento está bloqueado
  [OK] la cuenta 3 queda como estaba

tx#5: vuelve la red y el coordinador reenvía la decisión del log
  log: EsperandoVotos(IdTransaccion(5))
  log: Decision(IdTransaccion(5), Commit)
  log: Fin(IdTransaccion(5))
  [OK] los dos servicios confirman
  [OK] el asiento 2 queda vendido
  [OK] se cobra el precio
```

Los actores de la Parte 1 no liberan solos una reserva después de un tiempo, y
no deben hacerlo. Si el inventario liberara el asiento por vencimiento después
de votar YES, estaría tomando una decisión unilateral: es la segunda fila de
la tabla.

**Relación con el Teorema CAP.** CAP dice que, cuando hay una partición de red
(P), un sistema distribuido tiene que elegir entre consistencia (C) y
disponibilidad (A). Estrictamente, CAP habla de datos replicados, pero acá
aparece la misma disyuntiva. 2PC elige C: prefiere dejar bloqueados el asiento
y el dinero antes que arriesgarse a vender sin cobrar o a cobrar sin vender. Es
un comportamiento CP, y la disponibilidad que se pierde es concreta: mientras
dure la partición, la transacción no termina, el asiento no se le puede vender
a nadie y el saldo retenido no se puede usar.

Un diseño AP dejaría que cada servicio decidiera solo para seguir
respondiendo. Aceptaría inconsistencias que después habría que corregir con
operaciones compensatorias, como en el patrón Saga. El enunciado exige que
nunca se venda la misma entrada dos veces y que los cobros sean exactos, así
que para este sistema lo correcto es elegir consistencia.

### 3.3 ¿Qué se compromete, Safety o Liveness? (blocking)

**Las dos propiedades.**

- **Safety** ("nunca pasa nada malo"). En 2PC significa tres cosas: todos los
  participantes que deciden, deciden lo mismo; solo se decide Commit si todos
  votaron YES; y una decisión tomada no se revierte. Cuando se viola, se viola
  en un momento concreto y no tiene arreglo, como un asiento vendido sin
  cobrar.
- **Liveness** ("algo bueno termina pasando"). Que todo participante termine
  decidiendo. Se viola cuando alguien espera para siempre.

**La que se compromete es Liveness.** En el escenario de 3.2 ningún
participante decide algo distinto de lo que decidió el coordinador,
simplemente porque ninguno decide. Cuando el estado final llega, es el
correcto: asiento vendido y precio cobrado. Lo que no se puede garantizar es
que llegue. Mientras dure la partición los participantes no terminan, y si la
partición no se resolviera nunca, no terminarían nunca. Safety se mantiene;
liveness no.

**Bloqueo (blocking).** Un protocolo de commit es *bloqueante* si existen
fallas ante las cuales un participante que no falló no puede terminar hasta
que la falla se repare. 2PC es bloqueante, y la causa es la *ventana de
incertidumbre*: el tramo entre que un participante vota YES y recibe la
decisión. Fuera de esa ventana siempre hay una salida segura:

| Momento en que se corta la red | Qué puede hacer el participante | ¿Se bloquea? |
|---|---|---|
| Antes de recibir el `Prepare` | Abortar solo: nunca votó, así que no se pudo haber decidido Commit | No |
| Después de votar NO | Abortar solo: sabe que la decisión va a ser Rollback | No |
| **Después de votar YES y antes de recibir la decisión** | **Nada: esperar con los recursos reservados** | **Sí** |
| Después de recibir la decisión | Ya terminó | No |

El coordinador, en cambio, nunca se bloquea esperando votos: si un voto no
llega, lo cuenta como NO y aborta, porque todavía nadie confirmó nada. El test
`particion_antes_del_voto_no_bloquea_recursos` muestra ese caso. La red se
corta antes del voto, se decide Rollback y no queda ningún recurso tomado.

**El bloqueo es el precio de la safety.** Un participante dentro de la ventana
de incertidumbre tiene dos opciones: decidir por timeout o esperar. Decidir
por timeout recupera liveness, pero rompe safety en alguno de los casos de la
tabla de 3.2. 2PC elige esperar: sacrifica liveness para conservar safety.

No es un defecto de 2PC en particular. Está demostrado que, si puede haber
fallas de comunicación, todo protocolo de commit atómico puede dejar
participantes bloqueados (Bernstein, Hadzilacos y Goodman, *Concurrency
Control and Recovery in Database Systems*, cap. 7).

**Cómo se mitiga.** El bloqueo no se puede eliminar, pero sí acortar:

- **Reenviar la decisión desde el log.** Es lo que hace
  `Coordinador::reenviar_decision`: apenas vuelve la red, la transacción
  termina. El participante también puede preguntar qué se decidió con
  `Coordinador::decision(tx)`.
- **Replicar el log del coordinador.** Si la decisión está replicada en los 3
  nodos coordinadores de la Parte 2, la caída del líder no bloquea: el nuevo
  líder la lee y la reenvía. Para que dos coordinadores aislados no tomen
  decisiones distintas, la decisión tiene que quedar confirmada en una mayoría
  de nodos (es la idea de Paxos Commit). Aun así, un participante aislado de
  todos los coordinadores sigue bloqueado.
- **Three-Phase Commit (3PC).** Agrega una fase intermedia para que los
  participantes no queden en duda si se cae el coordinador. Pero supone una
  red sincrónica: con particiones puede terminar con decisiones distintas en
  cada lado. Cambia bloqueo por pérdida de safety.
- **Decisiones heurísticas.** En sistemas reales (por ejemplo, XA) un operador
  puede forzar el commit o el rollback de una transacción en duda. Recupera
  liveness a costa de safety: si se equivoca, hay que conciliar a mano.

**Resumen**

| | Durante la partición | Cuando vuelve la red |
|---|---|---|
| Safety | se mantiene: nadie decide distinto | se mantiene |
| Liveness | se pierde: los participantes quedan bloqueados en PREPARED | se recupera: el coordinador reenvía la decisión |
| Recursos | asiento `Reservado` y dinero retenido, nadie los puede usar | asiento vendido y dinero cobrado |

## Parte 4: Modelado Formal con Redes de Petri
### 4.1 Red de Petri Coordinador + 1 Participante
Lugares obligatorios: `Coordinador_Esperando_Votos`, `Participante_Prepared`, `Estado_Commit`.
### 4.2 Invariante de lugar (Commit y Rollback nunca a la vez)
### 4.3 Boundedness: qué significa que la red sea 1-acotada (safe)

## Plan de Pruebas

### Parte 1: Actores

Comandos: `cargo test` (todos), `cargo test --test parte1_actores` (concurrencia)
y `cargo run --bin parte1_actores` (demo con verificación de invariantes).

**Tests unitarios** (módulos `tests` dentro de `src/actores/*.rs`). Prueban la
lógica pura de cada actor, sin hilos, para aislar errores de lógica de errores
de concurrencia.

| Actor | Caso | Resultado esperado |
|-------|------|--------------------|
| Cuenta | Retener | baja `disponible` y sube `retenido`, sin cobrar |
| Cuenta | Saldo insuficiente | error `SaldoInsuficiente` y estado sin cambios |
| Cuenta | Retener → confirmar | lo retenido pasa a `debitado` |
| Cuenta | Retener → cancelar | el saldo vuelve completo |
| Cuenta | Mensajes repetidos | idempotentes, no se cobra dos veces |
| Cuenta | Misma tx con otro monto | `ConflictoTransaccion` |
| Cuenta | Transiciones inválidas | confirmar algo desconocido o cancelado, cancelar algo confirmado, reabrir una tx terminada: todo se rechaza |
| Cuenta | Cancelar tx desconocida | `Ok`, y un `SolicitarDebito` posterior se rechaza (aborto presunto) |
| Cuenta | Monto 0 / desbordamiento | `MontoInvalido` / `Desbordamiento` |
| Inventario | Reservar → confirmar | `Reservado(tx)` y después `Vendido(tx)` |
| Inventario | Asiento ocupado | reservar uno reservado o vendido da `AsientoNoDisponible` |
| Inventario | Reservar → liberar | vuelve a `Libre` y otra tx lo puede reservar |
| Inventario | Asiento inexistente | `AsientoInexistente` |
| Inventario | Idempotencia y transiciones inválidas | igual que en la cuenta; un asiento vendido nunca vuelve a `Libre` |

**Tests de integración** (`tests/parte1_actores.rs`). Usan actores reales en
hilos y muchos clientes concurrentes. El resultado tiene que coincidir con el
de una ejecución serial.

| Test | Escenario | Verifica |
|------|-----------|----------|
| `mil_hilos_un_solo_asiento` | 1000 hilos reservan el mismo asiento | exactamente 1 `Ok` y 999 `AsientoNoDisponible` |
| `debitos_concurrentes_no_sobregiran` | 100 hilos debitan de una cuenta con saldo para 10 | exactamente 10 éxitos, saldo final 0 y debitado exacto |
| `no_se_pierden_actualizaciones` | 200 hilos acreditan, retienen y confirman o cancelan | saldo final exacto, sin *lost updates* |
| `compras_concurrentes_conservan_invariantes` | 200 clientes compran entre 20 asientos con 4 cuentas | sin doble venta, sin reservas ni retenciones colgadas, conservación del dinero, cobrado = vendidos × precio |
| `apagado_del_actor` | `Detener` y soltar todos los handles | el hilo termina (join con timeout) y los pedidos posteriores devuelven `ActorDetenido` en lugar de colgarse |

Como un test concurrente puede pasar "de casualidad", los tests de integración
se corrieron 30 veces seguidas sin fallos:
`for i in $(seq 30); do cargo test -q --test parte1_actores || break; done`.

**Demo** (`src/bin/parte1_actores.rs`). 50 clientes compiten por 5 asientos con
cuentas de saldos distintos. Al final verifica con `assert!` los mismos
invariantes del negocio y los imprime como `[OK]`.

### Parte 3: Two-Phase Commit

Comandos: `cargo test` (todos), `cargo test --test parte3_2pc` (con actores
reales) y `cargo run --bin parte3_2pc` (demo).

**Tests unitarios** (módulo `tests` dentro de `src/two_phase_commit.rs`). Usan
un `ParticipanteDePrueba` que vota lo que se le indica, falla la fase 2 las
veces que se le pida y anota cada mensaje que recibe. Así se prueba el
protocolo por separado de los actores.

| Caso | Resultado esperado |
|------|--------------------|
| Todos votan `Yes` | `Confirmada`; cada participante recibe `prepare` y `commit`; log `EsperandoVotos → Decision(Commit) → Fin` |
| Uno vota `No` | `Abortada` con el motivo; todos reciben `rollback`, incluido el que votó `No`; nadie recibe `commit` |
| Un participante falla la fase 2 y después responde | se reintenta hasta que responde y termina `Confirmada` |
| Un participante nunca responde la fase 2 | `Incompleta`; el otro igual recibe el `commit`; la decisión queda en el log, pero no hay `Fin` |
| `tx` repetida | `TransaccionRepetida` y no se contacta a ningún participante |
| Partición después del voto (`Enlace::cortar_tras_prepare`) | `Incompleta` con decisión `Commit` y el commit no llega; reenviar con la red cortada no sirve; al restablecerla, `reenviar_decision` entrega el commit y anota `Fin` |
| Partición antes del voto (`Enlace::cortar`) | el voto no llega y cuenta como NO, así que se decide `Rollback`; el otro participante recibe `rollback` |
| `reenviar_decision` de una tx terminada | `Ok` sin contactar a nadie |
| `reenviar_decision` de una tx desconocida | `TransaccionDesconocida` |

**Tests de integración** (`tests/parte3_2pc.rs`). Usan `ServicioPagos` y
`ServicioInventario` sobre los actores reales y verifican que, al final, la
compra se haya hecho completa o no se haya hecho.

| Test | Escenario | Verifica |
|------|-----------|----------|
| `compra_exitosa_vende_el_asiento_y_cobra` | saldo suficiente y asiento libre | asiento `Vendido(tx)` y precio cobrado, nada retenido |
| `saldo_insuficiente_aborta_y_libera_el_asiento` | Pagos vota `No` | `Abortada`, el asiento vuelve a `Libre` y la cuenta queda igual |
| `asiento_vendido_aborta_y_devuelve_el_dinero` | Inventario vota `No` | `Abortada`, el asiento sigue vendido a la primera compra y se devuelve lo retenido |
| `servicio_caido_cuenta_como_voto_no` | el actor de la cuenta está detenido | `Incompleta` con decisión `Rollback` y el asiento liberado |
| `particion_despues_del_yes_bloquea_hasta_que_vuelve_la_red` | los dos votan YES y la red se corta antes del Commit (Parte 3.2) | `Incompleta` con decisión `Commit`; asiento `Reservado` y precio retenido sin cobrar; otra compra del mismo asiento se aborta; al volver la red y reenviar la decisión, el asiento queda `Vendido` y el precio cobrado |
| `particion_antes_del_voto_no_bloquea_recursos` | la red con Pagos se corta antes del Prepare | se decide `Rollback`; el asiento queda libre y la cuenta intacta; después del rollback reenviado, un Prepare atrasado de esa tx se rechaza |
| `compras_concurrentes_con_2pc_conservan_invariantes` | 200 clientes, cada uno con su coordinador, compran entre 20 asientos con 4 cuentas | las tx de los asientos vendidos son exactamente las confirmadas; no quedan reservas ni retenciones; en cada cuenta lo cobrado es compras × precio y el dinero se conserva |

Los tests de integración se corrieron 30 veces seguidas sin fallos:
`for i in $(seq 30); do cargo test -q --test parte3_2pc || break; done`.

**Demo** (`src/bin/parte3_2pc.rs`). Ejecuta cinco compras: una normal, una con
saldo insuficiente, una de un asiento ya vendido, una con el actor de la
cuenta caído y una con partición de red después de los votos. La última
muestra el bloqueo, el rechazo de otra compra del mismo asiento hecha desde
otro coordinador y la recuperación al volver la red. Para cada una muestra el log del coordinador y
el resultado, y verifica con `assert!` el estado del asiento y de la cuenta.

## Cómo la Red de Petri captura los escenarios de fallo del código
