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
### 2.2 Algoritmo Bully para elegir el nuevo líder

## Parte 3: Transacciones Distribuidas
### 3.1 Coordinador 2PC (Prepare y Commit/Rollback)
### 3.2 Partición de red después de que ambos votan YES
### 3.3 ¿Qué se compromete, Safety o Liveness? (blocking)

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

## Cómo la Red de Petri captura los escenarios de fallo del código
