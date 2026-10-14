# Medir si Spark escala de verdad

Este documento responde a una sola pregunta: **acabo de subir workers, ¿por qué
el job tarda lo mismo?**

No explica cómo añadir workers — eso está en
[ESCALAR_SPARK_WORKERS.md](ESCALAR_SPARK_WORKERS.md), y conviene leerlo primero
si todavía no tiene el segundo worker levantado. Este empieza donde aquel
termina: el master ya anuncia los cores, y aun así nada cambió.

La respuesta casi siempre es la misma. Añadir workers sube **un** techo de
cinco, y el tiempo lo marca el más bajo de los cinco. Si no baja el resto, subir
workers no se nota, y la medición dice «no escala» cuando lo que pasa es que el
cuello está en otro sitio.

---

## El comando que los lista

```powershell
docker compose -f docker-compose.windows.yml exec -T airflow-worker `
    python /opt/airflow/scripts/medir_escalado_spark.py --estado
```

Pregunta al master cuánto hay, lee las Variables de Airflow para ver cuánto se
está pidiendo, y compara. Salida real de un cluster recién escalado a dos
workers:

```
  workers vivos : 2
  cores         : 4  (en uso ahora: 0)
  memoria       : 4096 MB

  ok  Cluster: 2 worker(s), 4 core(s)
  !!  RECURSOS pide 1 executor(s) x 1 core(s) = 1, con CORES_MAX 2
  !!  Particion de lectura: 0 de 1 tabla(s) activa(s) la declaran
  !!  archivos_por_tabla = 1
  ok  Pool de Airflow: default_pool

  VEREDICTO: 3 tope(s) activos.
```

El cluster está bien y los tres topes que quedan son de configuración. **Mientras
el veredicto no diga 0, medir es perder el tiempo**: el número que salga no va a
hablar de los workers.

---

## Los cinco topes, en el orden en que muerden

### 1. El cluster

`SPARK_WORKER_REPLICAS x SPARK_WORKER_CORES` cores y
`SPARK_WORKER_REPLICAS x SPARK_WORKER_MEMORY` de memoria, todo en `.env`. Es el
techo absoluto: ninguno de los otros cuatro puede pasarlo.

Es también el único que no es configuración sino máquina. Los otros cuatro se
cambian con un JSON y un `sync_variables.py`.

Y recuerde que `--scale` **no es persistente**: el compose declara
`deploy.replicas: ${SPARK_WORKER_REPLICAS:-1}`, así que el siguiente `up -d` sin
el flag vuelve a un worker. Lo que escalaste en caliente hay que fijarlo en
`.env`.

### 2. `RECURSOS`, en la Variable `CONFIGURACION_SPARK`

Cuatro claves:

```json
"RECURSOS": {
  "NUM_EXECUTORS":   2,
  "EXECUTOR_CORES":  2,
  "CORES_MAX":       4,
  "EXECUTOR_MEMORY": "1g",
  "DRIVER_MEMORY":   "1g"
}
```

`CORES_MAX` es un techo **de la aplicación**, no del cluster. Con `CORES_MAX: 2`
da igual que haya cuatro cores libres: el job toma dos y los otros dos workers
se quedan sanos, visibles en la interfaz del master, y sin hacer nada. Es el
tope más fácil de pasar por alto precisamente porque todo parece correcto.

Al revés también duele: si `CORES_MAX` es **mayor** que los cores del cluster,
Spark espera a que aparezcan y la aplicación se queda en `WAITING` para siempre,
sin fallar y sin mensaje de error.

El DAG avisa de esto solo en el log cuando detecta la diferencia, así que si
nadie lee el log no se entera.

### 3. `particion` — el paralelismo DENTRO de una tabla

En la Variable `BT2SQL_SPARK`, por tabla:

```json
"particion": {"columna": "ID", "desde": 1, "hasta": 48213907, "particiones": 4}
```

Sin esto, Spark lee JDBC por **una sola conexión**. Una conexión es una tarea, y
una tarea ocupa un core. Subir executors no cambia nada en la lectura porque no
hay nada que repartir.

`particiones` debe cuadrar con `CORES_MAX`. Cuatro particiones con `CORES_MAX: 2`
significa que dos esperan turno.

#### `desde` y `hasta` NO son un filtro

Esto es lo que hay que leer dos veces, porque cuesta una corrida entera
entenderlo solo.

`desde` y `hasta` van tal cual a `lowerBound` y `upperBound` de Spark, y lo
único que hacen es **definir dónde cortar los rangos**. No descartan ninguna
fila. Con `desde: 1`, `hasta: 1000000` y 4 particiones, Spark genera estos
cuatro `WHERE`:

```sql
-- particion 0
WHERE ID <  250000 OR ID IS NULL
-- particion 1
WHERE ID >= 250000 AND ID < 500000
-- particion 2
WHERE ID >= 500000 AND ID < 750000
-- particion 3
WHERE ID >= 750000
```

Mire la primera y la última. **La primera se lleva además todo lo que esté por
debajo de `desde`, y todos los NULL. La última se lleva todo lo que esté por
encima de `hasta`, sin techo.**

Así que si los `ID` reales llegan a 48 millones y usted puso `hasta: 1000000`,
la partición 3 trae **47 millones de filas** y las otras tres unas pocas. Un
archivo `part-*` cien veces más grande que sus hermanos es exactamente este
error, y es el más frecuente de todos. No falla, no avisa: produce el dato
correcto, en un solo core, con el tiempo de la lectura sin particionar y encima
el coste de haberla intentado.

Los valores reales se sacan del origen antes de declarar nada:

```sql
SELECT MIN(ID) AS minimo,
       MAX(ID) AS maximo,
       COUNT(*) AS filas,
       COUNT(*) - COUNT(ID) AS nulos
  FROM GPPPBTDB.FSH005;
```

Si `nulos` no es cero, esas filas van **todas** a la partición 0 haga lo que
haga con los límites. Con muchos NULL, esa columna no sirve como clave de
partición.

#### Compruebe el reparto antes de confiar en él

Límites correctos no garantizan reparto parejo: Spark divide el **rango** en
tramos iguales, no los **datos**. Una columna de fechas concentrada en cierres
de mes, o un número de cuenta asignado por bloques, sigue saliendo torcida. La
consulta que lo dice en segundos, con los mismos cortes que va a usar Spark:

```sql
SELECT FLOOR( (CAST(ID AS BIGINT) - 1) * 4 / (48213907 - 1) ) AS particion,
       COUNT(*) AS filas
  FROM GPPPBTDB.FSH005
 GROUP BY FLOOR( (CAST(ID AS BIGINT) - 1) * 4 / (48213907 - 1) )
 ORDER BY 1;
```

(Sustituya el `1`, el `48213907` y el `4` por su `desde`, `hasta` y
`particiones`.) Si las cuatro cuentas están dentro de un factor de 2 o 3, sirve.
Si una se lleva el 90%, cambie de columna.

Cuando ninguna columna del negocio reparte bien, la salida es una clave
sintética y uniforme por construcción. En DB2 for i, `RRN()` devuelve el número
relativo de registro, que es uniforme por definición; `partitionColumn` exige
una columna, así que se expone con una subconsulta como origen:

```sql
(SELECT RRN(T) AS RRN_, T.* FROM GPPPBTDB.FSH005 T) X
```

Verifíquelo en su versión del AS/400 antes de montarlo en el catálogo.

#### Cuando el reparto es desigual por naturaleza: `modo`

Todo lo anterior supone que, con los límites correctos, el rango reparte
razonablemente. Con una columna de fecha sobre un core bancario eso casi nunca
es cierto: el mes de la migración inicial trae mil veces más que un mes normal,
y **los límites buenos para un bloque de fechas son malos para otro**. Un JSON
estático no puede seguir al dato.

Por eso `particion` acepta ahora tres modos.

##### `modo: "rango"` — el de siempre, y el valor por defecto

```json
"particion": {"columna": "ID", "desde": 1, "hasta": 48213907, "particiones": 4}
```

Parte el **rango** en tramos iguales. Sirve con una clave secuencial y densa.
Es lo que había antes de este cambio y sigue siendo el comportamiento si no
declara `modo`, así que **ninguna tabla del catálogo cambia de comportamiento
por sí sola**.

##### `modo: "cuantiles"` — parte por conteo, y se ajusta solo

```json
"particion": {
  "modo": "cuantiles",
  "columna": "PGFCIE",
  "bucket": "PGFCIE / 100",
  "particiones": 8
}
```

Antes de leer, el job lanza un `COUNT(*) GROUP BY bucket` contra el origen —
respetando el `FILTRO` de la tabla, así que mide la distribución **del conjunto
que de verdad va a extraer** — y reparte los buckets contiguos en grupos de peso
parecido. Los cortes salen de los datos de esa corrida, no de un número escrito
a mano hace tres meses.

`bucket` es opcional y por omisión es la propia columna. Agrúpelo solo cuando la
columna tenga demasiados valores distintos: con `PGFCIE` en formato `AAAAMMDD`,
veinte años son unos 7 300 valores y agrupar no hace falta; con una marca de
tiempo al microsegundo, sí. `PGFCIE / 100` agrupa por mes.

El coste es una consulta de agregación. Si la columna está indexada, DB2 la
resuelve con un recorrido de índice y tarda segundos incluso sobre miles de
millones de filas. **Si no está indexada, es un recorrido completo de la tabla
antes de la extracción** — mire el tiempo que reporta el log (`histograma: N
bucket(s) en X.Xs`) y si duele, pase a `predicados`.

##### `modo: "predicados"` — usted escribe los cortes

```json
"particion": {
  "modo": "predicados",
  "predicados": [
    "PGFCIE <  20190201",
    "PGFCIE >= 20190201 AND PGFCIE < 20200101",
    "PGFCIE >= 20200101 AND PGFCIE < 20220101",
    "PGFCIE >= 20220101",
    "PGFCIE IS NULL"
  ]
}
```

Control total y cero consultas extra. El precio es que los cortes envejecen: hay
que revisarlos cuando el volumen cambie. Y aquí **los NULL son suyos** — siga
leyendo.

#### El agujero de `predicados`: los NULL se pierden en silencio

Esto no es un detalle menor y conviene tenerlo claro antes de cambiar de modo.

En `modo: "rango"`, Spark agrega **él solo** un `OR col IS NULL` a la primera
partición. Nunca pierde una fila.

Con `predicates` **no agrega nada**. Una fila cuyo valor es `NULL` no cumple
`col <= x` ni `col > x` — no cumple *ningún* predicado — y simplemente no se lee.
Sin error, sin aviso, sin diferencia visible salvo un conteo que no cuadra.

En `modo: "cuantiles"` el job agrega esa partición por usted; se desactiva con
`"nulos": false` si sabe que la columna es `NOT NULL`. En `modo: "predicados"`
**la escribe usted**, como en el ejemplo de arriba. Si la columna admite nulos y
no pone esa línea, pierde esas filas.

#### El límite que ningún modo supera

Spark no parte **dentro** de un valor. Si un solo valor de la columna concentra
más filas que el objetivo por partición — un `20190131` con el 30% del total
porque ahí entró la migración — no hay corte posible que lo equilibre, en
ninguno de los tres modos.

El job lo detecta y lo dice en el log antes de leer:

```
[FSH005] reparto estimado por particion: 30.000.000.000, 23.500.000.000, ...
[FSH005] la particion mas grande se lleva el 30% de 100.000.000.000 filas
         (lo parejo seria el 12%). El reparto sigue torcido.
[FSH005] el valor 201901 de PGFCIE concentra 30.000.000.000 filas (30% del
         total): NINGUN corte por esta columna puede equilibrarlo
```

Las salidas, en orden de esfuerzo:

Primero, **un bucket más fino**. Si agrupó por mes y el mes es el problema,
quite el `bucket` y agrupe por día; si el día sigue concentrando, el problema no
es la granularidad.

Segundo, **otra columna**. La fecha es la columna del `FILTRO`, pero **no tiene
por qué ser la columna de partición**: son cosas independientes. Si el `FILTRO`
ya recorta a un rango de fechas, puede particionar por cuenta, por oficina o por
cualquier columna numérica que reparta mejor dentro de ese recorte.

Tercero, **una clave sintética uniforme**. En DB2 for i, `RRN()` es el número
relativo de registro y es uniforme por definición:

```json
"particion": {"modo": "cuantiles", "columna": "RRN_",
              "bucket": "MOD(RRN_, 16)", "particiones": 16, "nulos": false}
```

con el origen expuesto como subconsulta en `COLUMNAS`/`FILTRO` para que `RRN_`
exista. Advertencia: el predicado de partición se aplica en la consulta exterior,
y si DB2 no lo empuja hacia dentro de la subconsulta, cada partición recorre la
tabla entera — dieciséis particiones serían dieciséis recorridos completos.
**Mire el plan antes de montarlo en el catálogo**, con una sola tabla y pocas
particiones.

#### `archivos_por_tabla` no arregla el sesgo

Conviene saberlo porque es la reacción natural. `archivos_por_tabla` es un
`coalesce(N)`, y `coalesce` **junta particiones sin redistribuir**: la grande
sigue grande, y si le toca compañía queda todavía más grande. Lo que redistribuye
es `repartition`, que cuesta un shuffle completo. El sesgo se arregla en la
lectura, eligiendo bien la columna y los límites, no en la escritura.

### 4. `archivos_por_tabla`

En `BT2SQL_SPARK`, bajo `extraccion`. Es un `coalesce(N)` justo antes de
escribir el parquet. Con `1`, **toda** la escritura ocurre en una tarea y en un
core, por muchos executors que haya leyendo.

Es el tope que más engaña, porque la lectura sí se paraleliza y en la interfaz
se ve actividad en todos los workers — y luego el job se queda minutos en la
última etapa con un solo core al 100%.

Debe acompañar al paralelismo, no quedarse en 1.

### 5. El pool de Airflow — el paralelismo ENTRE tablas

`AIRFLOW_POOL_SPARK` decide cuántos `spark-submit` corren a la vez. Un slot
significa que dos tablas nunca se extraen en paralelo aunque el cluster tenga
sitio de sobra.

**En este proyecto ese tope no está activo, y el reporte lo dice mal.** La
variable no está declarada en `.env`, así que cae a `default_pool`, que en
Airflow trae **128 slots**. No limita nada. El texto que el script imprime
debajo de esa línea («con un solo slot, dos tablas nunca…») es la explicación
genérica del tope, no un hallazgo sobre su cluster: el script debería imprimir
cuántos slots tiene el pool y no lo hace. Confírmelo usted mismo:

```powershell
docker compose -f docker-compose.windows.yml exec -T airflow-scheduler `
    airflow pools list
```

---

## Las dos clases de paralelismo

Esta distinción es la que decide qué configuración le sirve, y no se parece a lo
que uno supone.

**Dentro de una tabla.** Varias particiones de la misma tabla leyéndose a la vez.
Necesita `particion` declarada, `archivos_por_tabla` acorde, y `CORES_MAX` alto.
Sirve cuando tiene pocas tablas y son grandes.

**Entre tablas.** Varias tablas extrayéndose a la vez, cada una con su propio
`spark-submit`. **No necesita `particion` para nada.** Necesita slots en el pool
(los hay), varias tablas activas, y que la suma de los executors de todos los
jobs quepa en el cluster. Sirve cuando tiene muchas tablas, que es el caso real
de Bantotal.

Son independientes. Puede tener una sin la otra, y el `--estado` las mide por
separado: el tope 3 y el 4 son de la primera, el 5 es de la segunda.

---

## La aritmética que decide todo: la memoria

Casi nadie la hace, y es la que manda. Un executor no cuesta lo que pide:

```
coste real = EXECUTOR_MEMORY + max(384 MB, 10% de EXECUTOR_MEMORY)
```

Ese segundo término es `spark.executor.memoryOverhead`, y no se puede ignorar
porque el master reserva el total, no el heap.

Con un cluster de 2 workers × 2 cores × 2048 MB — **4 cores y 4096 MB**:

| `EXECUTOR_MEMORY` | coste real | caben en 4096 MB | ¿entra en un worker de 2048? |
|---|---|---|---|
| `512m` | 896 MB | 4 | sí |
| `1g` | 1408 MB | **2** | sí |
| `1536m` | 1920 MB | 2 | sí, justo |
| `2g` | 2432 MB | 1 | **no** |

Dos cosas que salen de esa tabla y que conviene tener presentes.

**Con executors de 1g, el cluster solo admite dos, aunque tenga cuatro cores
libres.** La memoria es el tope, no la CPU. Si quiere los cuatro cores tiene que
darle dos cores a cada executor, no pedir cuatro executors.

**Con workers de 2 GB, `EXECUTOR_MEMORY` no puede pasar de `1536m`.** Un executor
tiene que caber **entero en un worker**, no repartido entre dos. Pedir `2g`
cuesta 2432 MB y ningún worker anuncia tanto, así que el master no lo coloca en
ninguna parte: la aplicación queda aceptada, sin executors, en `WAITING`, sin
error. En la interfaz del master se ve como una app viva que no avanza.

Y un detalle que juega a favor: **el driver corre en client mode, dentro de
`airflow-worker`**, así que `DRIVER_MEMORY` no consume memoria del cluster. Sí
consume RAM de la VM de WSL2, que es otra contabilidad — la de
[ESCALAR_SPARK_WORKERS.md](ESCALAR_SPARK_WORKERS.md).

---

## Tres configuraciones que caben en 4 cores y 4096 MB

### A — una tabla usando el cluster entero

```json
"NUM_EXECUTORS": 2, "EXECUTOR_CORES": 2, "CORES_MAX": 4, "EXECUTOR_MEMORY": "1g"
```

4 de 4 cores, 2816 de 4096 MB. Requiere `particion` con al menos 4 particiones y
`archivos_por_tabla: 4`.

### B — dos tablas a la vez, también con los cuatro cores

```json
"NUM_EXECUTORS": 1, "EXECUTOR_CORES": 2, "CORES_MAX": 2, "EXECUTOR_MEMORY": "1g"
```

Cada job toma 1 executor de 2 cores y 1408 MB. Dos jobs: 4 cores y 2816 MB.
Requiere dos tablas activas, `particion` con 2 particiones y
`archivos_por_tabla: 2`.

**Esta es la que sirve para el caso real**, que son decenas de tablas. La A es
para medir, la B es para producir.

### C — conservador, una tabla

```json
"NUM_EXECUTORS": 1, "EXECUTOR_CORES": 2, "CORES_MAX": 2, "EXECUTOR_MEMORY": "1g"
```

2 de 4 cores, 1408 de 4096 MB. Deja la mitad libre. Es la configuración de
partida razonable mientras valida que el pipeline funciona.

Después de cambiar cualquiera de las tres hay que publicar la Variable:

```powershell
docker compose -f docker-compose.windows.yml exec -T airflow-scheduler `
    python /opt/airflow/scripts/sync_variables.py --solo CONFIGURACION_SPARK
```

Y volver a correr `--estado` hasta que diga **0 topes**.

---

## El orden correcto, que no es el intuitivo

**Primero volumen, después configuración, al final workers.**

Si mide con las tablas de Bantotal — `FSH005` tiene del orden de cinco filas —
va a medir ruido, y peor: va a medir **más lento** con dos workers que con uno.
Abrir cuatro conexiones JDBC y arrancar dos JVMs para cinco filas cuesta más que
hacerlo en una. No es un error del cluster ni de la medida; es el coste fijo del
paralelismo, que con datasets pequeños supera al trabajo.

Spark empieza a rendir con volúmenes donde una sola máquina sufre. Por debajo de
eso, cualquier curva de escalado va a salir plana o al revés.

Para generar ese volumen está `BTGEN`, en
`sql/bds/00_banco_pruebas_stg_ods_bds.sqlserver.sql`, que ya trae la columna `ID`
secuencial que `particion` necesita. Con un millón de filas la curva empieza a
decir algo; con cinco, no.

Y para gestionar qué tablas están activas:

```powershell
docker compose -f docker-compose.windows.yml exec -T airflow-worker `
    python /opt/airflow/scripts/tablas_prueba_carga.py --listar
```

Acepta `--anadir`, `--quitar` y `--activar`.

---

## Medir

Con el veredicto en 0 topes y volumen suficiente:

```powershell
# 1. etiquetar la configuracion actual
docker compose -f docker-compose.windows.yml exec -T airflow-worker `
    python /opt/airflow/scripts/medir_escalado_spark.py --marcar "A_2w_4c"

# 2. lanzar el DAG y esperar a que termine

# 3. ver la curva
docker compose -f docker-compose.windows.yml exec -T airflow-worker `
    python /opt/airflow/scripts/medir_escalado_spark.py --curva
```

`--marcar` guarda la configuración junto a la etiqueta, así que la curva compara
corridas sabiendo qué cambió entre una y otra. Si marca dos corridas con la
misma etiqueta y configuración distinta, la curva miente y no hay forma de
detectarlo después.

Use nombres que describan el cluster y la configuración, no el intento:
`A_2w_4c` dice algo dentro de tres semanas, `prueba3` no.

---

## Qué esperar de la curva

Un escalado perfecto no existe. Duplicar cores no divide el tiempo por dos
porque hay una parte que no se paraleliza: abrir la conexión, planificar, el
commit del parquet al final. Con una mejora del 60-70% al duplicar ya puede
estar satisfecho.

Si la curva sale **plana**, queda un tope activo que el `--estado` no vio. Los
dos candidatos son que la lectura va por una conexión (`particion` mal declarada
o con una columna mal repartida) y que la escritura va por un core
(`archivos_por_tabla`).

Si sale **al revés** — más workers, más tiempo — el dataset es demasiado pequeño
para lo que cuesta repartirlo, o el origen es el cuello. Bantotal con cuatro
conexiones concurrentes puede ser más lento que con una si el AS/400 está
limitando; eso no se arregla en Spark.

---

## Resumen

```
1. --estado           ->  tiene que decir 0 topes
2. volumen            ->  BTGEN con un millon de filas
3. --marcar           ->  etiqueta que describa el cluster
4. lanzar el DAG
5. --curva
```

Y la pregunta que ahorra el día: **¿tengo particiones suficientes para llenar
los cores que acabo de añadir, y cabe en memoria lo que voy a pedir?** Si
cualquiera de las dos es no, el tiempo no va a cambiar.
