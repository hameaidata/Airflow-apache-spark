# El pipeline Spark: una sola Variable, un solo destino

## Que quedo y que se borro

Habia dos pipelines Spark y se confundian entre si. Quedo uno:

```
Bantotal (IBM i)  --Spark-->  parquet  --Spark-->  STG en SQL Server
```

| Pieza | Archivo |
|---|---|
| DAG | `airflow/dags/production/dag_stg_bt2sql_carga_spark.py` |
| `dag_id` | `STG_BT2SQL_CARGA_SPARK` |
| Job de extraccion | `spark/jobs/etl/bt2sql_extraccion_spark.py` |
| Job de carga | `spark/jobs/etl/bt2sql_carga_spark.py` |
| Variable | `BT2SQL_SPARK` (una sola) |
| JSON puente | `/opt/spark-data/runtime/bt2sql_spark.json` |

Se borro el pipeline Spark que cargaba a **SingleStore**
(`dag_bt_parquet_singlestore_spark.py` y sus dos jobs). No habia dos destinos
que mantener: el destino es SQL Server.

El gemelo en pandas, `dag_stg_bt2sql_carga.py`, sigue en pie a proposito: la
forma de dar por bueno el pipeline Spark es correr los dos sobre el mismo dia y
comparar fila a fila.

## Las conexiones son Generic, y eso hay que declararlo

`CONEXION_BANTOTAL` y `CONEXION_SQLSERVER` son de tipo **Generic** en Airflow.
Generic es el tipo que se elige cuando no hay un proveedor instalado para ese
sistema, y **no dice nada del motor**. Eso tiene una consecuencia que no se ve
hasta que falla: con dos conexiones Generic, cualquier regla que traduzca
`generic` a un motor fijo acierta en una y se equivoca en la otra **en
silencio** — arma una URL `jdbc:as400://` apuntando a SQL Server y carga el
driver de IBM i para escribir en una tabla de SQL Server. El error que sale de
ahi no menciona el tipo de conexion por ningun lado.

Asi que cada conexion Generic declara su motor en el campo **Extra**:

| Connection | Extra |
|---|---|
| `CONEXION_BANTOTAL` | `{"motor": "as400", "libraries": "GPPPBTDB", "naming": "system"}` |
| `CONEXION_SQLSERVER` | `{"motor": "mssql"}` |

Si no esta declarado, `motor_de_conexion()` lo deduce del puerto cuando el
puerto solo puede pertenecer a un motor (1433 → mssql, 8471/9471/446 → as400,
5432 → postgres, 1521 → oracle, 50000 → db2). El 3306 queda fuera a proposito:
lo usan MySQL y SingleStore por igual. Si no hay ninguna pista, falla al
arrancar diciendo que agregar y donde.

## Todo lo configurable esta en la Variable

`BT2SQL_SPARK` tiene tres partes:

**`tablas`** — la lista que hay que editar para agregar o quitar una tabla. Cada
entrada sirve a las dos mitades del pipeline:

| Campo | Para que | Lo usa |
|---|---|---|
| `esquema`, `tabla` | de donde se lee en el core | extraccion |
| `columnas` | vacio = todas | extraccion |
| `filtro` | vacio = sin WHERE | extraccion |
| `tipos` | `COLUMNA:TIPO|COLUMNA:TIPO`, manda sobre el esquema del parquet | extraccion |
| `particion` | lectura en paralelo; `null` = una sola conexion | extraccion |
| `tabla_destino` | a que tabla STG sube | carga |
| `batch_size`, `commit_every` | tamanos del INSERT | carga |
| `activo` | `S`/`N` | las dos |

Una tabla se agrega **en un solo sitio**. El motivo de que el destino este aqui
y no en el catalogo de la base: agregar una tabla pasa a ser editar un JSON
versionado, revisable en un diff, en vez de un UPDATE a mano contra una tabla
de produccion que nadie ve pasar.

**`extraccion.procesos`** — el calendario. Una tabla entra en una corrida solo
si esta en los **dos** sitios: en `tablas` con `activo = "S"`, y en `procesos`
con `estado = 1` y el flag del dia (`estado_diario` / `semanal` / `mensual`) en
1. El cruce es por `nombre_proceso` + esquema.

**`origen_tablas`** — `"variable"` (lo normal) o `"catalogo"`. Con `"catalogo"`
se ignora `tablas` y se lee `CTL_PARAMETROS_PARQUET` desde la base, para el caso
en que el catalogo lo mantenga otra area.

### Lo unico que NO esta en la Variable

Las **credenciales** (van en `Admin > Connections`) y el **estado de cada
corrida** (va en `ctl_proceso_parquet` y `ctl_carga_stg`). Esa segunda
separacion importa: que archivos parquet produjo el batch de hoy es estado de la
corrida y solo la base lo sabe; a donde va cada tabla es configuracion.

Antes las dos salian de la misma consulta, con un `INNER JOIN` contra
`CTL_PARAMETROS_PARQUET`. Ese join tenia un efecto que no se ve leyendolo: una
tabla extraida sin fila en el catalogo **desaparecia del resultado** y nadie se
enteraba — el parquet quedaba en disco y la tabla destino con los datos del dia
anterior. Ahora una tabla extraida que no este declarada es un error con nombre.

## El JSON puente y el control de publicacion

Un job de Spark **no puede leer Variables de Airflow**: corre en otro
contenedor, sin la base de metadatos y sin la libreria `airflow`. El puente es
un JSON en el volumen `spark_data`:

```
Admin > Variables        tarea preparar_config         spark-submit
  (BT2SQL_SPARK)   --->   escribe el JSON       --->   lee --config
                          en /opt/spark-data/runtime/
```

**Lo que esta en la Variable es irrelevante para el job hasta que alguien lo
copia a ese archivo.** El parametro `publicar_config` controla ese paso, y por
defecto lo hace solo:

```
publicar_config = "auto"   (POR DEFECTO)
    Compara el hash de la Variable con el del archivo publicado.
    Iguales   -> no toca nada y lo dice en el log.
    Distintos -> respalda el anterior y publica, con los dos hashes.
    Editar la Variable y lanzar el DAG basta.

publicar_config = "no"
    No mira la Variable. Usa el archivo tal como esta en el volumen.
    Para relanzar una corrida vieja con EXACTAMENTE su configuracion.

publicar_config = "si"
    Publica aunque los hashes coincidan. Para cuando alguien edito el
    archivo del volumen a mano y hay que devolverlo a lo que dice la Variable.
```

### Por que `auto` y no `no`

La primera version de esto exigia `publicar_config='si'` a mano, con `no` por
defecto, para que la configuracion de produccion no cambiara sin que alguien lo
decidiera. Era peor, y el motivo vale escribirlo: **olvidarse del parametro
-que es lo normal- produce el sintoma mas desconcertante de todos.** Se edita la
Variable, se sincroniza, se lanza el DAG, y no pasa nada. Sin error. El pipeline
corre con la configuracion anterior y nada en la interfaz lo dice. Es
exactamente el problema que este documento existe para evitar, reintroducido por
el mecanismo que pretendia evitarlo.

`auto` no pierde nada de lo que `no` protegia:

- **Una edicion a medias no entra sola**, porque la compuerta real esta mas
  arriba: la Variable no se edita a mano en la interfaz, se sincroniza desde un
  JSON versionado con `scripts/sync_variables.py`. Ese es el acto deliberado.
- **El rastro queda igual.** El hash de la configuracion usada se escribe en el
  log de toda corrida, publique o no.
- **La vuelta atras sigue existiendo**, por el respaldo fechado.

Y lo que `no` hace bien -fijar la configuracion- sigue disponible cuando de
verdad hace falta: relanzar una corrida de la semana pasada sin que una edicion
posterior se cuele.

Cada publicacion deja el archivo anterior al lado
(`bt2sql_spark.20261006143052.json`). **Nadie limpia esos respaldos**; hay que
borrarlos a mano cada tanto.

## Cambiar la configuracion, paso a paso

1. Editar `airflow/config/json/BT2SQL_SPARK.json`.
2. `python scripts/sync_variables.py --solo BT2SQL_SPARK`
3. Lanzar el DAG. Sin parametros.
4. En el log de `preparar_config`, confirmar `CONFIGURACION PUBLICADA` con el
   hash nuevo. Si dice `SIN CAMBIOS`, el paso 2 no llego a la Variable.


---

## Las ocho tareas del DAG

```
verificar_origen_destino
  └─ preparar_config
      └─ inventario_tablas
          └─ validar_catalogo
              └─ extraccion/
                   ├─ prioridad_1/
                   │    ├─ stg_fsh005
                   │    └─ stg_msfd008
                   └─ prioridad_2/          (espera a prioridad_1 entero)
                        ├─ stg_fsh012
                        └─ stg_fsh031
                  └─ extraccion_completa
                      └─ verificar_parquet   (compuerta)
                          └─ cargar_stg_spark
                              └─ limpiar_parquet
```

### Una tarea por proceso, agrupada por prioridad

El grafo se dibuja desde la configuración, igual que hace
`BDS_DATAHUB_PROCESOS` con sus Variables, y por el mismo motivo: con una sola
tarea de extracción la interfaz no dice qué tabla tardó, cuál falló ni cuál se
saltó, y reintentar una obliga a reintentarlas todas.

Los grupos de prioridad van **encadenados**, no en paralelo — prioridad 1 entera
antes que prioridad 2, que es lo que significa el campo y lo que hace el
pipeline de pandas. Dentro de un grupo las tablas sí corren en paralelo.

**El coste, dicho claro:** cada tarea es un `spark-submit`, y arrancar la JVM
cuesta entre 15 y 30 segundos. Con una sola tarea ese arranque se pagaba una vez
y las tablas se recorrían en serie dentro del job; ahora se paga por tabla, pero
en paralelo, así que en la práctica se gana tiempo en cuanto hay más de dos
tablas. Lo que hay que vigilar es no lanzar más drivers de los que el cluster
aguanta: para eso está el pool `AIRFLOW_POOL_SPARK`, que se declara en
`Admin > Pools`. Sin pool declarado Airflow no limita nada.

**Las tablas que hoy no entran se dibujan igual** y se saltan en ejecución, en
rosa, con el motivo en su log. Un grafo que cambia de forma según el día es
mucho más difícil de leer que uno estable con tareas saltadas.

`extraccion_completa` es el punto de unión, como `ods_completo` en
`BDS_DATAHUB_PROCESOS`. Lleva `trigger_rule="all_done"` porque saltarse tablas
por calendario es el funcionamiento normal y con la regla por defecto una sola
saltada dejaría la carga sin ejecutar; lo que decide si hay algo que cargar es
`verificar_parquet`, que mira el disco.

Si la configuración no se puede leer al dibujar el grafo, el DAG **no
desaparece**: aparece con una sola tarea `configuracion_no_disponible` que falla
diciendo qué revisar. Un DAG ausente no se diagnostica; uno en rojo con un
mensaje, sí.

Las cuatro primeras corren en Python dentro de Airflow y cuestan segundos. Las
dos de Spark son las que levantan el cluster. Ese orden es deliberado: **todo
lo que puede fallar por configuración falla antes de que se encienda un
executor**, porque un error de configuración descubierto dentro de un job de
Spark llega envuelto en una traza de la JVM y en la interfaz se lee como un
`spark-submit` con código de salida 1.

### `verificar_origen_destino` — la compuerta cero

Ejecuta el `sql_fecha` contra Bantotal, que es la consulta más barata que el
job necesita de todas formas: si responde, el core está arriba, la credencial
sirve y la biblioteca del Extra resuelve. Después comprueba que SQL Server
tiene el catálogo y las dos bitácoras. Falla en rojo, no se salta: que el core
no responda no es «hoy no había nada que hacer».

### `inventario_tablas` — las dos listas, enteras

Imprime **`procesos`** y **`tablas`** completas, porque los dos huecos posibles
son silenciosos: una tabla sin proceso no se extrae y no da error, y un proceso
sin tabla no hace nada y tampoco avisa.

```
PROCESOS DECLARADOS (extraccion.procesos): 5
  NOMBRE_PROCESO   ESQUEMA    ESTADO PRIOR DIARIO SEMANAL MENSUAL  ENTRA HOY / OBSERVACION
  ----------------------------------------------------------------------------------------
  STG_FSH005       GPPPBTDB        1     1      1       0       0  SI
  STG_FST017       GPPPBTDB        1     1      1       0       0  no  -- SIN entrada en 'tablas'
  STG_MSFD008      GPPPBTDB        0     1      1       0       0  no  -- estado=0
  STG_FSH012       GPPPBTDB        1     2      1       0       0  SI
  STG_FSH031       GPPPBTDB        1     2      0       0       0  no  -- estado_diario=0

TABLAS DECLARADAS: 4  |  se extraeran hoy: 2  |  no: 2  (tipo_ejecucion=diario)
  PRIOR PROCESO          ORIGEN               DESTINO       ESTADO    PARTICION  TIPOS   MOTIVO
  ---------------------------------------------------------------------------------------------
      1 STG_FSH005       GPPPBTDB.FSH005      STG_FSH005    ACTIVA    PGCOD      si
      2 STG_FSH012       GPPPBTDB.FSH012      STG_FSH012    ACTIVA    -          destino
      1 STG_MSFD008      GPPPBTDB.MSFD008     STG_MSFD008   inactiva  -          destino estado=0 en procesos
      2 STG_FSH031       GPPPBTDB.FSH031      STG_FSH031    inactiva  -          destino estado_diario=0 en procesos
```

La columna `TIPOS` dice de dónde saldrán: `si` cuando la tabla los declara en
la Variable, `destino` cuando se leerán de la tabla destino de SQL Server.

La tarea corta la corrida si una tabla activa no declara `tabla_destino`, y si
**ninguna** pasa los dos filtros — no tiene sentido levantar un cluster para no
extraer nada.

### La doble llave, y `prioridad`

Una tabla entra en una corrida solo si pasa **los dos** filtros, que están en
sitios distintos de la misma configuración:

| Dónde | Condición |
|---|---|
| `tablas[]` | `activo == "S"` |
| `extraccion.procesos[]` | mismo `nombre_proceso` + `nombre_esquema`, `estado == 1`, y el flag del día en 1 (`estado_diario` / `estado_semanal` / `estado_mensual`, según `tipo_ejecucion`) |

Y `prioridad`, del proceso, decide el **orden** de extracción: primero todas las
de prioridad 1, luego las de 2, y dentro de cada nivel por nombre de tabla. Sin
`prioridad` declarada vale 99, o sea al final.

> **Esto faltaba en el job, y era un fallo de verdad.** Hasta el 2026-10-07 el
> job de Spark extraía todas las tablas con `activo="S"` en el orden en que
> estuvieran escritas en la Variable: **ignoraba `procesos` por completo.** Una
> tabla con `estado: 0`, o con el flag del día en 0, se extraía igual — el DAG la
> reportaba como `inactiva` en el inventario y Spark la extraía a continuación.
> Y eso no es solo un día de datos de más: rompe la única validación que dice
> que la traducción a Spark salió bien, que es correr los dos pipelines sobre el
> mismo día y comparar fila a fila, porque el de pandas sí filtra. La semántica
> ahora se copia de `filtrar_procesos()` de `etl_bt2sql/bt2sql_extraccion.py`
> para que las dos mitades decidan igual.

### `validar_catalogo` — la deriva entre las dos listas

La Variable decide qué se extrae; eso no cambia. Lo que esta tarea busca es la
deriva contra `CTL_PARAMETROS_PARQUET`, porque las dos listas existen y las
mantiene gente distinta: la Variable va en el repositorio y se revisa en un
diff, el catálogo vive en SQL Server y lo edita quien opere la base. Mientras
el pipeline de pandas lea el catálogo y el de Spark la Variable, una tabla
puede entrar en uno y no en el otro sin que nada falle — y entonces comparar
las dos corridas sobre el mismo día deja de significar nada.

La comparación va **en una sola dirección: de la Variable al catálogo**. Del
catálogo solo sale un conteo, sin nombres. Eso es deliberado: el catálogo puede
tener cien tablas de otros pipelines, y listarlas convertiría el log del DAG en
un volcado de `CTL_PARAMETROS_PARQUET` que taparía lo único que importa — si lo
declarado está registrado. **En el DAG solo se ve lo que declara la Variable.**

| Situación | Qué hace | Por qué |
|---|---|---|
| Declarada y **no** registrada en el catálogo | **error** | El catálogo es el registro de lo configurado; extraer algo que no figura ahí es trabajar fuera de registro |
| `tabla_destino` distinto entre las dos | **error** | Dos pipelines escribiendo la misma tabla de origen en destinos distintos es ambigüedad real |
| En el catálogo y no declarada | **no se reporta** | No es asunto de este DAG: lo que no está en la Variable no se extrae, y punto |

Se relaja con `"validacion_catalogo": "aviso"` en la Variable, o se apaga con
`"ninguna"` — y entonces la deriva deja de vigilarse, que es precisamente lo
que esta tarea existe para evitar.

### `verificar_parquet` — por qué mira carpetas

Spark escribe un **directorio** con varios `part-*.parquet` dentro, no un
archivo suelto. Un `os.path.isfile` copiado del pipeline de pandas daría
«falta» para todas las tablas, con los datos perfectamente escritos en disco.


---

## Los tipos de dato: de dónde salen y en qué orden

El casteo no es estético. Si el parquet no trae los tipos que la tabla destino
espera, el `INSERT` por JDBC falla — o, peor, **no falla y trunca**. Y quien
sabe esos tipos con certeza no es la Variable ni el catálogo: es la propia
tabla destino.

De ahí el orden de preferencia, que el job aplica tabla por tabla:

| | Fuente | Cuándo se usa | Mantenimiento |
|---|---|---|---|
| 1 | `tablas[].tipos` de la Variable | si está declarado, gana siempre | a mano; es el override explícito, para cuando el destino tiene un tipo más ancho a propósito |
| 2 | `INFORMATION_SCHEMA.COLUMNS` de `tabla_destino` | si `tipos` está vacío y `tipos_desde_destino` es `true` | **ninguno**: no se desincroniza nunca, porque *es* el destino |
| 3 | Lo que infiera el driver JDBC del core | último recurso | — y es justo lo que suele romper la escritura |

El paso 2 es el que conviene dejar trabajar. `FECHA_PROCESO` y `BATCH_ID` se
excluyen porque las inyecta la extracción, no vienen del core. Un tipo de SQL
Server que no se sepa traducir (`geography`, `xml`, `varbinary`) se deja pasar
con un aviso y sin inventar nada, en vez de forzarlo a texto a ciegas. Si la
tabla destino no existe, se avisa y se cae al paso 3 — el error de «tabla
destino inexistente» tiene que salir de la carga, que lo puede explicar bien.

El formato de `tipos` es `COLUMNA:TIPO|COLUMNA:TIPO`:

```
PGCOD:DECIMAL(3,0)|FSH005TCV:DECIMAL(17,8)|NOMBRE:VARCHAR(50)|N:BIGINT|F:DATE|TS:DATETIME2|IND:BIT
```

`BIT` se trata como entero y no como booleano, a propósito: en el core un
indicador viene como 0/1 numérico, y un `BooleanType` en el parquet obliga a
convertir otra vez al escribir en un `BIT` de SQL Server.

### La cadena vacía pasa a NULL antes del cast

`''` casteado a decimal da `NULL` en Spark, pero a entero da `0` en algunas
versiones. Un cero inventado en un importe es peor que un nulo, así que el
blanqueo va primero y explícito, también para fechas.

### Por qué el problema del parquet de pandas no existe aquí

En el pipeline de pandas, una columna que venía toda en `NULL` en el primer
bloque se inferia como tipo `null` de Arrow, y el bloque siguiente ya no podía
castear contra ese tipo: la escritura moría a mitad con
`ArrowNotImplementedError`. Spark no tiene ese fallo porque infiere el esquema
una sola vez, de los metadatos del `ResultSet`, no bloque a bloque. El casteo
aquí no es para evitar *ese* error, sino para que el parquet coincida con el
destino.
