## 📋 Ejercicio: ETL de Procesamiento de Números

 **Objetivo** : Extraer números, transformarlos (calcular si son primos y su raíz cuadrada), y cargarlos en una "base de datos" (lista).

#### **SequentialETL**

---

- Hace un proceso sequencial, devolviendo las variables y usandolas de step en step

#### **QueuedETL**

---

- Encola los resultados de cada paso de la ETL.

* No hay mejoras en los tiempos, de hecho empeora porque encolar/desencolar tiene coste computacional

#### **ThreadedETL**

---

Los threads necesitan colas porque:

* **Thread-safety** : Sincronización automática entre threads
* **Desacoplamiento** : Cada thread trabaja a su ritmo
* **Buffer** : Si un stage es más lento, no bloquea a los demás inmediatamente

**Sin colas** tendrías que usar locks manualmente, lo cual es más propenso a errores (deadlocks, race conditions)

**Opcional (pero recomendable)**: Las funciones de la ETL pasan a ser unitarias, y el el threas quien gestiona y usa la cola

- Cuando la funcion de `extract` es unitaria, debemos usar `yield` para devolver cada batch.

Podemos usar 3 threads o usar 2 threads + el hilo principal

#### TransformParallelizationETL

---

Conceptos clave:

1. **Data Parallelism** : Procesar múltiples batches simultáneamente
2. **ThreadPoolExecutor** : Pool de workers que procesan tareas en paralelo
3. **Mantener orden** (opcional): Usar `submit()` + `as_completed()` o `map()`

Tenemos dos opciones:

- Paralilizar en el metodo `transform`, para transformer un único batch mas rápido. Esto es ideal si en batch hay muchas operaciones I/O.
- Paralilizar en el metodo `transformer_thread`, para transformar el conjunto de batches mas rápido

| Objetivo                                                  | Mejor opción                                   | Explicación                                                 |
| --------------------------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| ⚡ Acelerar el tiempo de transformación de*cada batch* | **Opción 1 (dentro de `transform`)**   | Divide el trabajo de un batch entre varios hilos o procesos. |
| 🚀 Aumentar el*throughput global*de la pipeline         | **Opción 2 (en `transformer_thread`)** | Permite transformar varios batches al mismo tiempo.          |

##### ThreadPoolExecutor

`ThreadPoolExecutor` es una clase en Python (del módulo `concurrent.futures`) que permite **ejecutar tareas concurrentemente usando un grupo de hilos (threads)**

| Método                          | Qué hace                                                                      | Cuándo lo usas típicamente                                                                           |
| -------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `executor.map(func, iterable)` | Lanza todas las tareas y te devuelve un*iterador ordenado*de resultados.     | Cuando ya tienes**todas las tareas por adelantado**y quieres los resultados **en orden** . |
| `executor.submit(func, arg)`   | Lanza**una sola tarea**y te devuelve un `Future`(resultado pendiente). | Cuando las tareas**van llegando dinámicamente**(por una cola, stream, etc).                     |

#### LoaderParallelizationETL

---

**Se puede paralelizar el loader**, pero **puede ser peligroso** dependiendo de *qué hace exactamente la carga* (y hacia dónde cargas).

| **Opción**                    | **Explicación**                                                                                                                                | **Ejemplos**                                                                                                                                                                        |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ✅**Paralelizar el loader**    | Seguro cuando cada carga es independiente y no comparten destino ni recurso.<br />Ideal para operaciones I/O o bases de datos con buena concurrencia. | - Guardar cada batch en un archivo distinto (`batch_1.csv`,`batch_2.csv`)<br />- Enviar datos a diferentes APIs o endpoints• Insertar en distintas tablas o bases de datos separadas |
| 🚫**No paralelizar el loader** | Peligroso cuando varios hilos escriben en el mismo recurso, ya que<br /> puede causar race conditions, bloqueos o corrupción de datos.               | - Escribir en una misma lista compartida `self.database`<br />- Insertar en una única tabla sin control de transacciones<br />- Escribir simultáneamente en el mismo archivo          |

Aunque en nuestro ejemplo NO debería paralelizarse el loader, vamos a hacerlo para comprovar la performance, puesto que la base de datos es ficticia

## Resultados

| Etapa | Descripción                        | Tiempo |
| ----- | ----------------------------------- | ------ |
| 1     | Sequential                          | 3.30s  |
| 2     | Colas sin threads                   | 3.34s  |
| 3.1   | Pipeline (3 threads + colas)        | 2.15s  |
| 3.2   | Pipeline (main +2 threads + colas) | 2.13s  |
| 4.1   | Transform parallelization per batch | 2.13s  |
| 4.2   | Transform thread parallelization    | 2.16s  |
| 4.3   | Transform parallelization all       | 2.12s  |
| 5     | Load thread parallelization         | 2.12   |

Razones por las que paralelizar el `transformer_thread` puede ser más lento

* En CPython,  **solo un hilo ejecuta código Python puro a la vez** . Si `transform()` es CPU-bound (como `is_prime()`, `sqrt()`, loops grandes),  **los hilos no corren realmente en paralelo** , sino que se turnan.
