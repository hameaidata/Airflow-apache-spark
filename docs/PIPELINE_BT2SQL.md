# BT2SQL — Bantotal (IBM i) → Parquet → STG en SQL Server 2022

Extrae tablas del core Bantotal por JDBC, las deja en parquet organizadas por
día y por tabla, y las sube a las tablas STG de `GNBPE_DATAHUB` en SQL Server.

Es la primera mitad del flujo. De STG a ODS y de ODS a BDS se encargan los
stored procedures del DataHub, orquestados aparte, igual que `BT_DATAHUB` hace
hoy con SingleStore.

```
   Bantotal                  disco                     SQL Server
  (GPPPBTDB)          /data/bt2sql/<día>/             (GNBPE_DATAHUB)

   FSH005    ──────►  20260926/FSH005/FSH005.parquet  ──────►  STG_FSH005
   MSFD008   ──────►  20260926/MSFD008/…parquet       ──────►  STG_MSFD008
   FSH031    ──────►  20260926/FSH031/…parquet        ──────►  STG_FSH031
   FSH012    ──────►  20260926/FSH012/…parquet        ──────►  STG_FSH012

               extraer_parquet          cargar_stg
```

## Qué archivos son

| Archivo | Qué hace |
|---|---|
| `airflow/dags/production/dag_bt2sql_stg.py` | El DAG. Cuatro tareas, nada de lógica. |
| `airflow/dags/production/etl_bt2sql/bt2sql_comun.py` | Conexiones, JVM, rutas, validaciones. |
| `airflow/dags/production/etl_bt2sql/bt2sql_extraccion.py` | Core → parquet. |
| `airflow/dags/production/etl_bt2sql/bt2sql_carga.py` | Parquet → STG. |
| `airflow/config/json/BT2SQL_EXTRACCION.json` | Variable: qué tablas y cuándo. |
| `airflow/config/json/BT2SQL_CARGA.json` | Variable: cómo se carga. |
| `sql/bt2sql/20_bt2sql_control_sqlserver.sql` | Las tres tablas de control. |
| `sql/bt2sql/21_bt2sql_catalogo_ejemplo.sql` | Catálogo de ejemplo y plantillas STG. |
| `airflow/tests/unit/test_bt2sql.py` | 58 tests, sin necesitar ninguna base. |

## Por qué Variables nuevas y no las del pipeline de SingleStore

Los nombres de las claves son **exactamente** los mismos que en
`EXTRACCION_BT_STG` y `CARGAR_PARQUET_CONFIG` (hay dos tests que lo comprueban
y fallan si alguien agrega o quita una). Lo que cambia son los *valores*,
porque el SQL que contienen se ejecuta contra motores distintos:

```
EXTRACCION_BT_STG   →  SELECT … LIMIT 1        dialecto MySQL/SingleStore
BT2SQL_EXTRACCION   →  SELECT … FETCH FIRST    dialecto DB2 for i
```

Compartir una sola Variable entre los dos pipelines obligaría a que el mismo
texto SQL funcionara en tres motores a la vez. No hay forma de escribirlo.

Lo único que se agregó son `conn_id_origen` y `conn_id_destino`, que en el
pipeline de SingleStore estaban escritos dentro del código.

## Puesta en marcha

### 1. Las tablas de control

```bash
sqlcmd -S <servidor> -d GNBPE_DATAHUB -U <usuario> -P <clave> \
       -i sql/bt2sql/20_bt2sql_control_sqlserver.sql
sqlcmd -S <servidor> -d GNBPE_DATAHUB -U <usuario> -P <clave> \
       -i sql/bt2sql/21_bt2sql_catalogo_ejemplo.sql
```

Los dos son idempotentes: se pueden correr las veces que haga falta.

### 2. Las tablas STG destino

Cada una necesita `FECHA_PROCESO` como **primera** columna y `BATCH_ID` como
**última**. La carga arma el INSERT leyendo el esquema del propio parquet, así
que si faltan, SQL Server responde `Invalid column name`.

La plantilla está comentada en `21_bt2sql_catalogo_ejemplo.sql`.

### 3. Las dos Connections

En Admin → Connections, o con la CLI. Las credenciales quedan cifradas con el
Fernet key; no van en ningún archivo del repositorio.

**`CONEXION_BANTOTAL`** — tipo Generic

| Campo | Valor |
|---|---|
| Host | la IP o nombre del AS/400 |
| Port | `8471` |
| Login / Password | el usuario del core |
| Extra | `{"libraries": "GPPPBTDB"}` |

**`CONEXION_SQLSERVER`** — tipo Generic

| Campo | Valor |
|---|---|
| Host | el servidor SQL |
| Port | `1433` |
| Schema | `GNBPE_DATAHUB` |
| Extra | `{"encrypt": "true", "trustServerCertificate": "true"}` |

Con instancia nombrada, poner `instanceName` en el Extra en vez de `Port`.

### 4. Las Variables

```bash
python scripts/sync_variables.py
```

o subir a mano los dos JSON de `airflow/config/json/`.

### 5. El .env

```dotenv
# Carpeta de los parquet de BT2SQL. La de dentro del contenedor no cambia.
BT2SQL_PARQUET_HOST_DIR=./data/bt2sql
BT2SQL_PARQUET_CONTAINER_DIR=/data/bt2sql

# En Red Hat:
# BT2SQL_PARQUET_HOST_DIR=/datos/datahub/bt2sql
```

### 6. Primera corrida

Lanzar `BT2SQL_STG` a mano, con `tipo_ejecucion = diario`.

## La fecha de proceso sale del core, no de SQL Server

Es la única diferencia de fondo con el pipeline de SingleStore, y tiene un
motivo concreto. Allí `sql_fecha` lee `STG_FST017` del propio DataHub, que ya
existe porque otro proceso la cargó antes. Aquí esa tabla la llena **este**
pipeline: el primer día estaría vacía y la extracción moriría con «no devolvió
ninguna fecha» sin haber extraído nada.

Leyéndola de `GPPPBTDB.FST017` el problema desaparece: el core siempre está y
siempre tiene la fecha de cierre al día.

La fecha es de **negocio**, no la del reloj. Por eso una corrida lanzada a las
2 de la madrugada se archiva bajo el día contable correcto y no bajo el
siguiente.

## Las carpetas

```
/data/bt2sql/
    20260926/                        ← un día
        FSH005/
            FSH005.parquet           ← una tabla
        MSFD008/
            MSFD008.parquet
    20260925/
        …
```

La carpeta por tabla deja sitio para partir un parquet en varios archivos el
día que una tabla no quepa cómoda en uno solo, sin cambiar rutas ni tocar lo ya
guardado.

`limpiar_parquet` borra las carpetas de día más viejas que `retencion_dias`.
Solo toca carpetas cuyo nombre son 8 dígitos que forman una fecha: cualquier
otra cosa dentro del directorio se deja intacta, porque la carpeta está fuera
del contenedor y puede tener vecinos que no son de este pipeline.

## El rastro de auditoría

Tres tablas, con los mismos nombres que usa el pipeline de SingleStore, para
que las consultas de operación sirvan para los dos.

| Tabla | Qué guarda |
|---|---|
| `CTL_PARAMETROS_PARQUET` | Qué se extrae, con qué columnas y filtro, y a qué STG va. |
| `ctl_proceso_parquet` | Una fila **por tabla y por corrida**, con la ruta completa. |
| `ctl_carga_stg` | Una fila por archivo cargado. |

`ctl_proceso_parquet` es además el punto de encuentro entre las dos mitades: la
extracción deja ahí la ruta y la carga la lee. Ninguna de las dos tiene una
ruta escrita en el código, que es lo que permite que la carpeta cambie cada día
sin que nadie edite nada.

Cómo fue la última corrida, de punta a punta:

```sql
SELECT  p.batch_id, p.fecha_proceso, p.tabla_origen,
        p.estado AS extraccion, p.filas_procesadas,
        c.estado AS carga,      c.filas_cargadas
FROM    ctl_proceso_parquet p
LEFT    JOIN ctl_carga_stg c
        ON c.batch_id = p.batch_id AND c.archivo_parquet = p.archivo_parquet
WHERE   p.batch_id = (SELECT MAX(batch_id) FROM ctl_proceso_parquet)
ORDER   BY p.tabla_origen;
```

Hay más consultas de operación al final de `21_bt2sql_catalogo_ejemplo.sql`.

## Qué tabla entra en una corrida

Tiene que estar en los dos sitios:

- en `CTL_PARAMETROS_PARQUET` con `ACTIVO = 'S'`
- en la Variable, en `procesos`, con `estado = 1` y el flag del calendario en 1

El cruce se hace por `(NOMBRE_PARQUET, ESQUEMA)` contra
`(nombre_proceso, nombre_esquema)`.

Parece redundante y no lo es: permite desactivar una tabla para todos los
calendarios de golpe con un UPDATE, sin tocar la Variable, y sacarla solo del
diario sin tocar la base.

Si no coinciden, la tabla se extrae cero veces. Es el error de configuración
más común, y el DAG falla diciéndolo con esos nombres exactos.

## Los estados

**Extracción** (`ctl_proceso_parquet.estado`)

| Estado | Significa |
|---|---|
| `EJECUTANDO` | Empezó y no ha cerrado. Si se queda así, la tarea murió de golpe. |
| `TERMINADO` | Hay archivo en disco con filas dentro. |
| `SIN_DATOS` | Se consultó bien y no había filas. **No es un error.** |
| `ERROR` | Falló. El motivo está en `msg_error`. |

`SIN_DATOS` se separa de `TERMINADO` a propósito: la carga no debe tocar una
tabla destino que no recibió datos, porque truncarla la dejaría vacía sin
motivo. Una tabla que aparece días seguidos como `SIN_DATOS` casi siempre
significa que el `FILTRO` quedó mal escrito.

**Carga** (`ctl_carga_stg.estado`): `INICIADO` → `FINALIZADO` o `ERROR`. Los
cuatro valores salen de la Variable, no están escritos en el código.

## Cómo se carga cada tabla

```
1. CREATE   STG_FSH005_STG   con SELECT TOP 0 * INTO desde la destino
2. INSERT   todo el parquet en la staging
3. TRUNCATE destino + INSERT desde staging   ← EN UNA SOLA TRANSACCIÓN
4. DROP     la staging
```

El paso 3 es el que importa. Si se truncara la destino **antes** de leer el
parquet y el archivo estuviera corrupto, la tabla quedaría vacía y sin datos
que reponer hasta la corrida siguiente. Haciéndolo al final, la destino solo se
vacía cuando los datos nuevos ya están dentro de la base, y si algo falla el
ROLLBACK la deja exactamente como estaba.

Esto funciona porque en SQL Server `TRUNCATE TABLE` **sí** es transaccional. En
MySQL y SingleStore hace un commit implícito y no hay vuelta atrás, que es por
lo que el otro pipeline tuvo que resolverlo de otra forma.

La staging se crea con `SELECT TOP 0 … INTO` y no con un `CREATE TABLE` escrito
a mano, para que los tipos, longitudes y precisiones los copie el motor de la
tabla real. Declarándolos, un `VARCHAR(50)` en destino contra un
`VARCHAR(MAX)` en staging pasa desapercibido hasta que una fila larga revienta
el INSERT final, ya dentro de la transacción.

## Una tabla que falla no arrastra a las demás

Cada tabla se extrae y se carga por separado. Si una falla, las otras siguen.
Al final la tarea falla igual, para que la corrida no salga verde con tablas
sin cargar, pero lo que sí se extrajo está en disco y se puede subir relanzando
solo la carga.

## Relanzar solo la carga

Un *clear* de la tarea `cargar_stg` funciona: sin `batch_id` por XCom, la carga
toma la última extracción `TERMINADO` de cada tabla y lo avisa en el log. Puede
subir datos de un día anterior, así que conviene mirar el aviso.

## Tres cosas de JDBC que cuesta caro descubrir

**Una sola JVM por proceso.** JPype la levanta una vez y no se le puede ampliar
el classpath después. Por eso `_arrancar_jvm` la arranca con los dos jars de
golpe: si arrancara solo con el de Bantotal, la carga a SQL Server se quedaría
sin driver y sin forma de arreglarlo en caliente.

**Los hilos hay que atarlos.** La extracción usa `ThreadPoolExecutor`, y JPype
solo conoce los hilos que se le presentan. Si un hilo llama a Java sin estar
atado, no salta una excepción que se pueda capturar: la JVM **aborta el
proceso** del worker. La tarea queda zombie y en el log no hay traza, solo un
corte a media frase. Lo resuelve `atar_hilo_a_jvm()`, que es la primera línea
de cada hilo.

**numpy no cruza a Java.** JPype no sabe convertir un `numpy.int64` y falla con
un «No matching overloads» que no menciona numpy por ninguna parte. Y un `NaN`
tiene que llegar como `NULL`, no como el texto `nan`. De eso se encarga
`filas_nativas`.

Y una cuarta, de configuración: `naming=sql` en las propiedades de la conexión
va de la mano con escribir `GPPPBTDB.FSH005` con punto. Con `naming=system`
sería `GPPPBTDB/FSH005` con barra. Si no cuadran, el driver responde un `-204`
«objeto no encontrado» que parece un problema de permisos y manda a buscar
donde no es. Hay un test que impide cambiar una de las dos cosas sin la otra.

## Los tests

```bash
docker compose exec airflow-scheduler pytest /opt/airflow/tests/unit/test_bt2sql.py -v
```

58 tests, ninguno necesita Bantotal, SQL Server ni la JVM: las conexiones se
sustituyen por un doble que registra el SQL que se le manda. El parquet sí es
real, porque los problemas de tipos solo aparecen escribiendo el archivo.

Las Variables se leen de los JSON versionados y no de un diccionario copiado en
el test: si alguien cambia una clave en el JSON y se olvida del código, los
tests fallan.

## Lo que este DAG no hace

No toca ODS ni BDS. Llega hasta STG y ahí termina.

El paso de STG a ODS y de ODS a BDS lo hacen los stored procedures del DataHub.
Los que hay hoy en `sql/sql_sp/` están escritos en dialecto SingleStore y no
corren en SQL Server tal cual, así que esa segunda mitad está pendiente de
decidir si se portan, si ya existen versiones T-SQL, o si ODS y BDS se quedan
en SingleStore.
