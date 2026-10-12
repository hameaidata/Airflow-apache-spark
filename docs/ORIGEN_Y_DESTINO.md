# Origen y destino: dónde está definido cada uno

**Origen: Bantotal (IBM i / AS400). Destino: SQL Server.**

El pipeline `STG_BT2SQL_CARGA_SPARK` lee del core y deja las tablas STG en
`GNBPE_DATAHUB`. Esta página lista los **seis sitios** donde eso está
declarado, en el orden en que el pipeline los consulta. Si hay que cambiar un
origen o un destino, están todos aquí y no hay un séptimo.

> **Nota sobre el modo de prueba.** Mientras no se tiene el IBM i a mano, el
> origen se apunta a SQL Server para poder probar de punta a punta. Eso NO es
> la arquitectura: es una Connection distinta en el sitio 2. La clave
> `_MODO_PRUEBA` de la Variable lleva la lista exacta de ediciones para volver,
> y está repetida al final de esta página.

---

## 1. Las Connections de Airflow

**Admin → Connections.** El único lugar del proyecto con credenciales. No hay
usuarios ni claves en los DAG, ni en las Variables, ni en los JSON del
repositorio.

| Connection | Papel | Qué la define |
|---|---|---|
| `CONEXION_BANTOTAL` | **origen** | host, puerto `8471`, usuario, clave, *Schema* = biblioteca por defecto, Extra `{"motor": "as400", "libraries": "GPPPBTDB"}` |
| `CONEXION_SQLSERVER` | **destino** | host, puerto `1433`, usuario, clave, *Schema* = **la base de datos**, Extra `{"motor": "mssql"}` |

Dos trampas en esta tabla, las dos vistas en producción:

**`Schema` en SQL Server es la BASE DE DATOS, no el esquema.** Airflow lo
traduce a `databaseName` en la URL JDBC. Si queda vacío o dice `master`, el
pipeline crea las tablas de control y escribe las STG dentro de la base de
sistema, y no falla al hacerlo. Tiene que decir `GNBPE_DATAHUB`.

**El Extra con `motor` no es opcional en una Connection de tipo Generic.** El
tipo `Generic` de Airflow no dice nada del motor, y es el tipo que se usa
cuando no hay un proveedor instalado para ese sistema. Sin el `motor`
declarado, cualquier regla que traduzca `generic` a un motor fijo acierta en
una de las dos conexiones y se equivoca en la otra **en silencio**: arma una
URL `jdbc:as400://` contra SQL Server o al revés. El error que sale de ahí no
menciona ni el motor ni el driver.

---

## 2. La Variable `BT2SQL_SPARK`

Quién juega cada papel. Vive en `airflow/config/json/BT2SQL_SPARK.json` y se
publica con `scripts/sync_variables.py`.

```
extraccion.conn_id_origen     ->  CONEXION_BANTOTAL
extraccion.conn_id_destino    ->  CONEXION_SQLSERVER   (las tablas de control)
carga.conn_id_destino         ->  CONEXION_SQLSERVER

extraccion.sql_fecha          ->  consulta de la fecha de proceso, en el
                                  DIALECTO DEL ORIGEN

tablas[].esquema + .tabla     ->  la tabla en el ORIGEN
tablas[].tabla_destino        ->  la tabla STG en el DESTINO
```

`sql_fecha` es dialecto y por eso hay dos valores guardados:

```
sql_fecha                 =  "SELECT TOP 1 PGFCIE FROM GPPPBTDB.FST017"
_sql_fecha_PRODUCCION     =  "SELECT PGFCIE FROM GPPPBTDB.FST017 FETCH FIRST 1 ROW ONLY"
```

`FETCH FIRST n ROWS ONLY` es DB2 for i; SQL Server no lo acepta sin `ORDER BY`
y `OFFSET`. `SELECT TOP 1` es SQL Server; DB2 for i no lo acepta. Al cambiar el
origen hay que cambiar esta línea o la primera tarea falla.

La fecha se lee del **origen** y no del destino a propósito: la STG la llena
este mismo proceso, así que leerla de ahí sería preguntarle a la corrida
anterior.

---

## 3. Los nombres por defecto, en el DAG

`airflow/dags/production/dag_stg_bt2sql_carga_spark.py`:

```
línea  96   CONN_ORIGEN_DEFECTO  = "CONEXION_BANTOTAL"
línea  97   CONN_DESTINO_DEFECTO = "CONEXION_SQLSERVER"
línea 170   CONN_ORIGEN, CONN_DESTINO = _conn_ids()
```

`_conn_ids()` resuelve en **tres pasos**, y el orden importa:

1. el JSON publicado en el volumen (`RUNTIME_JSON`) — es gratis de leer, y esto
   corre en cada pasada del parser, cada 30 segundos;
2. si ese archivo no existe todavía, la **Variable**;
3. y solo si tampoco, los nombres por defecto de arriba.

El paso 2 existe porque sin él, en una instalación nueva, el DAG usaba
`CONEXION_BANTOTAL` por mucho que la Variable dijera otra cosa, y la primera
tarea fallaba con `The conn_id 'CONEXION_BANTOTAL' isn't defined` mientras el
aviso del log recomendaba justo lo que ya estaba hecho.

---

## 4. El catálogo, en SQL Server

`dbo.CTL_PARAMETROS_PARQUET`, una fila por tabla:

| Columna | Papel |
|---|---|
| `ESQUEMA` | biblioteca del **origen** (`GPPPBTDB`) |
| `TABLA` | nombre físico en el **origen** (`FSH005`) |
| `NOMBRE_PARQUET` | nombre **lógico** del proceso (`STG_FSH005`), la llave que cruza con `nombre_proceso` de la Variable |
| `TABLA_DESTINO` | tabla STG en el **destino** (`STG_FSH005`) |
| `FILTRO` | el `WHERE` de esa tabla; ver `docs/PIPELINE_BT2SQL.md` para las marcas de fecha |

**De cuál de los dos sitios sale la lista depende de `origen_tablas`:**

```
origen_tablas = "variable"   (lo normal)  ->  manda tablas[] de la Variable;
                                              CTL_PARAMETROS_PARQUET NO se
                                              consulta para la lista
origen_tablas = "catalogo"               ->  se ignora tablas[] y la lista sale
                                              del catálogo
```

Con `"variable"`, el `FILTRO` que el job usa es el de `tablas[].filtro`, **no**
el de la columna `FILTRO` del catálogo. Es la confusión más fácil de tener:
se edita el catálogo, no pasa nada, y parece que el filtro no funciona.

Aun así el catálogo no es opcional: `validacion_catalogo: "estricto"` compara
las dos listas y falla si una tabla declarada en la Variable no está
registrada aquí, o si el destino difiere entre las dos.

---

## 5. De Connection a driver y URL

`airflow/plugins/utils/spark_config.py`. Tres funciones, y son las que
convierten una Connection en algo que la JVM pueda usar:

| Función | Qué hace |
|---|---|
| `motor_de_conexion(conn)` | resuelve el motor real: primero el Extra (`motor`, `engine`, `driver`, `tipo`), después el puerto si identifica al motor sin ambigüedad, y si no, falla diciendo qué poner y dónde |
| `driver_for_conn(conn)` | elige la clase del driver y su jar |
| `jdbc_url(conn)` | arma la cadena de conexión |

Las URL que salen de aquí para este pipeline:

```
origen    jdbc:as400://<host>/<biblioteca>;<propiedades de proteccion del core>
destino   jdbc:sqlserver://<host>:1433;databaseName=<Schema>;...
```

El catálogo `JDBC_DRIVERS` de ese archivo conoce **as400, mssql, postgres,
mysql, singlestore, db2 (`jdbc`) y oracle**. Oracle está declarado pero su jar
no se descarga: habilitarlo exige añadir la coordenada Maven al `ARG
JDBC_DRIVERS` de **los dos** Dockerfile y reconstruir.

Los dos Dockerfile tienen que llevar la misma lista. Hay un test
(`airflow/tests/unit/test_drivers_jdbc.py`) que las compara y falla si
divergen, porque un driver que está en la imagen de Airflow y no en la de Spark
produce un `No suitable driver` **dentro del job**, no al arrancar, así que el
stack parece estar bien.

---

## 6. Cómo llegan los dos al job de Spark

`ConDestino.execute`, en el DAG, líneas 523-542. Una tarea de extracción
necesita las **dos** conexiones a la vez: lee del origen y escribe su bitácora
en el destino.

```
ORIGEN   -->  --jdbc-url <url>          (argumento de spark-submit)
              ORIGEN_USUARIO            (variable de entorno)
              ORIGEN_CLAVE              (variable de entorno)

DESTINO  -->  DESTINO_JDBC_URL          (variable de entorno)
              DESTINO_DRIVER_CLASS
              DESTINO_USUARIO
              DESTINO_CLAVE
```

**Las credenciales viajan por variables de entorno y nunca por `--conf` ni como
argumentos de línea de comandos.** No es una preferencia de estilo: todo lo que
va en `--conf` aparece en la pestaña Environment de la UI de Spark, en la salida
de `ps` dentro del contenedor, y —con `spark.eventLog.enabled`— escrito en
disco en el event log.

El jar de cada motor se añade a `--jars` **y** a
`spark.driver.extraClassPath`. Los dos, porque en client mode `--jars` llega a
los executors pero no al classpath del sistema del driver, y estos jobs abren
conexiones de control con `jvm.Class.forName()`, que va por ese classpath. Sin
el segundo, el fallo es
`ClassNotFoundException: com.microsoft.sqlserver.jdbc.SQLServerDriver` con el
log diciendo `Added JAR ... mssql-jdbc.jar` tres líneas antes.

---

## Si cambia algo, qué toca

| Cambia | Sitios a tocar |
|---|---|
| Host, puerto, usuario o clave | solo el **1** |
| La base de datos del destino | el **1** (campo *Schema*) |
| La biblioteca del origen | el **1** (*Schema* y `libraries`) y el **2** (`tablas[].esquema`) y el **4** (`ESQUEMA`) |
| Qué Connection es el origen | el **2** (`conn_id_origen`); el **3** solo si se quiere cambiar el respaldo |
| El motor del origen o del destino | el **1** (Extra `motor`) y el **2** (`sql_fecha`, por el dialecto) |
| Un motor que no está en la lista | el **5** (`JDBC_DRIVERS` y `jdbc_url`) y los dos Dockerfile, y reconstruir |
| Añadir una tabla | el **2** (`procesos` y `tablas`) y el **4** (una fila), más la DDL de la STG |

---

## Volver del modo de prueba a Bantotal

Las cuatro ediciones, tal como las lista `_MODO_PRUEBA` en la Variable:

1. `extraccion.conn_id_origen` = el valor de `extraccion._conn_id_origen_PRODUCCION`
2. `extraccion.sql_fecha` = el valor de `extraccion._sql_fecha_PRODUCCION`
3. `tablas[].activo` = `"S"` en MSFD008, FSH031 y FSH012
4. registrar esas tres en `CTL_PARAMETROS_PARQUET`, que `validacion_catalogo: "estricto"` exige

Y dos que conviene no olvidar:

- la Connection `CONEXION_BANTOTAL` tiene que existir, con su Extra;
- las `tablas[].particion` están en `null` en el modo de prueba porque con
  pocas filas 8 particiones dejarían 1 llena y 7 vacías. Al volver a Bantotal
  hay que reponer el valor que está guardado en `_particion_PRODUCCION`, o
  Spark leerá cada tabla con **una sola conexión** y el cluster no aportará
  nada en la lectura.

Después de cualquier edición de la Variable hay que **publicar**, o el DAG
seguirá viendo lo de antes:

```powershell
docker compose -f docker-compose.windows.yml exec -T airflow-scheduler `
    python /opt/airflow/scripts/sync_variables.py --solo BT2SQL_SPARK
```

---

## Una limitación conocida

El pipeline de **Spark** admite cambiar el origen; el de **pandas** no.

`dag_stg_bt2sql_carga.py` (pandas) llama a `ejecutar_extraccion`, que llama a
`conexion_bantotal()` en `airflow/dags/production/etl_bt2sql/bt2sql_comun.py`,
y esa función tiene la URL fija en el código:

```python
url = f"jdbc:as400://{conn.host}:{int(conn.port or 8471)};" + ";".join(propiedades)
```

Usa además `DRIVER_BANTOTAL` y `JAR_BANTOTAL` fijos, e ignora lo que diga la
Connection. Está **clavado a IBM i**: con el origen apuntando a SQL Server
armaría `jdbc:as400://<host>:1433` y cargaría el driver de JTOpen contra SQL
Server.

`dag_stg_bt2sql_carga_spark.py` no tiene el problema, por dos razones: su
compuerta `verificar_origen_destino` elige el conector por el motor resuelto, y
el job recibe la URL ya construida por `jdbc_url(conn)`.

El arreglo pendiente es que `conexion_bantotal` delegue en `motor_de_conexion`
y `jdbc_url` como ya hace el lado de Spark.

---

## Comprobar qué está usando de verdad

La primera tarea del DAG, `verificar_origen_destino`, abre las dos conexiones y
deja en su log el motor **resuelto** de cada una, no el `conn_type`:

```
ORIGEN   CONEXION_BANTOTAL      motor as400, se abre con conexion_bantotal
ORIGEN   CONEXION_BANTOTAL      OK   fecha de proceso: 2026-10-10
DESTINO  CONEXION_SQLSERVER     OK   base GNBPE_DATAHUB
```

Registra el motor resuelto y no `conn.conn_type` porque con las Connections de
tipo Generic de este proyecto `conn_type` es el literal `"generic"`, así que el
log decía `motor=generic` mientras el driver que de verdad cargaba era el de
SQL Server. Un log que contradice lo que hace el código cuesta más que no
tenerlo.

Y en el log de cada tarea de extracción, desde el 2026-10-12, sale la cabecera
con el origen y el destino de esa tabla:

```
EXTRAYENDO [1/4]  GPPPBTDB.FSH005
   destino parquet : /data/bt2sql/spark/20261010/FSH005
   destino STG     : STG_FSH005
   SQL : SELECT ... FROM GPPPBTDB.FSH005 WHERE FSH005FEC = 20261010
```

---

## Ver también

- `docs/GUIA_CONEXIONES_DAGS.md` — qué Connection usa cada uno de los cinco pipelines
- `docs/PIPELINE_BT2SQL.md` — el pipeline completo, y las marcas del `FILTRO`
- `docs/CONEXION_BANTOTAL_AS400_JDBC.md` — las propiedades de protección del core
- `docs/MODELO_CONTROL_UNIFICADO.md` — las tablas de control
