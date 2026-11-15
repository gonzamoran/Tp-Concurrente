# TP2 - Motor Distribuido de Reservas y Pagos

Borrador del informe (después se exporta a PDF/Word para la entrega).

## Parte 1: Modelos de Concurrencia y Actores
### 1.1 Diseño del modelo (actores y mensajes)
### 1.2 Por qué el Modelo de Actores elimina las data races sin locks
### 1.3 Async/Await (Tokio) vs. thread-per-request con 50.000 conexiones

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

## Cómo la Red de Petri captura los escenarios de fallo del código
