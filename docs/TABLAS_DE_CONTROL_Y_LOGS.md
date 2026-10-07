# Tablas de control y de log del DataHub

Inventario de **todas** las tablas que los DAGs y los stored procedures usan
para saber qué procesar y para dejar constancia de lo que procesaron.

Está armado leyendo el repositorio, no de memoria: cada tabla lleva los
archivos donde se la nombra, y al final hay una sección con las que se usan
pero **no tienen DDL en ningún sitio**.

---

## El mapa de un vistazo

```
   BANTOTAL (AS/400)                  SINGLESTORE                SQL SERVER 2022
        │                              DATAHUB                   GNBPE_DATAHUB
        │
        ├── STG_BT_PARQUET ──► STG ──┐
        │     CTL_PARAMETROS_PARQUET             │
        │     ctl_proceso_parquet                │
        │     ctl_carga_stg                      │
        │     ETL_CONFIG                         │
        │                                        ▼
        │                              BDS_DATAHUB_PROCESOS (ODS → BDS)
        │                                CTL_CFG_PROCESOS
        │                                CONTROL_EJECUCIONES_SP
        │                                ctl_log_proceso
        │                                        │
        │                                        ├──► EXP_S2SQL_CARGA ──► SQL Server
        │                                        │      CTL.CTL_S2SQL_CATALOGO
        │                                        │      CTL.CTL_S2SQL_LOTE
        │                                        │      CTL.CTL_S2SQL_LOG_CARGA
        │                                        │
        └── STG_BT2SQL_CARGA ─────────────────────────────────────────► STG
              CTL_PARAMETROS_PARQUET
              ctl_proceso_parquet
              ctl_carga_stg
```

**Cuidado con esto:** el pipeline de Bantotal→SingleStore y el de
Bantotal→SQL Server usan **los mismos tres nombres de tabla**
(`CTL_PARAMETROS_PARQUET`, `ctl_proceso_parquet`, `ctl_carga_stg`) en **bases
distintas**. Fue una decisión deliberada, para que las consultas de operación
sirvan para los dos, pero significa que al escribir una consulta hay que tener
claro contra qué motor se está ejecutando. No son la misma tabla.

---

## Resumen

| Tabla | Base | Tipo | Quién escribe | DDL |
|---|---|---|---|---|
| `CTL_PARAMETROS_PARQUET` | SingleStore **y** SQL Server | catálogo | equipo de datos, a mano | ✅ |
| `ETL_CONFIG` | SingleStore | catálogo | equipo de datos, a mano | ✅ |
| `ctl_proceso_parquet` | SingleStore **y** SQL Server | bitácora | extracción | ✅ |
| `ctl_carga_stg` | SingleStore **y** SQL Server | bitácora | carga a STG | ✅ |
| `CTL.CTL_S2SQL_CATALOGO` | SQL Server | catálogo | equipo de datos, a mano | ✅ |
| `CTL.CTL_S2SQL_LOTE` | SQL Server | bitácora | S2SQL, una fila por corrida | ✅ |
| `CTL.CTL_S2SQL_LOG_CARGA` | SQL Server | bitácora | S2SQL, una por tabla | ✅ |
| `CTL_CFG_PROCESOS` | SingleStore | catálogo | equipo de datos, a mano | ❌ |
| `CONTROL_EJECUCIONES_SP` | SingleStore | bitácora | orquestador ODS/BDS | ❌ |
| `ctl_log_proceso` | SingleStore | bitácora | los propios SP | ❌ |
| `MON_EJECUCIONES` | SingleStore | bitácora | — (nombre huérfano) | ❌ |

---

## 1. Extracción y carga a STG

### `CTL_PARAMETROS_PARQUET` — catálogo

Qué tablas se extraen del origen, con qué columnas y con qué filtro. Lo
administra el equipo de datos; ningún proceso escribe aquí.

Es la mitad de una **doble llave**: una tabla entra en una corrida solo si
está aquí con `ACTIVO = 'S'` **y** en la Variable de Airflow
(`EXTRACCION_BT_STG` o `BT2SQL_EXTRACCION`) con `estado = 1` y el flag del
calendario correspondiente. El cruce va por `(NOMBRE_PARQUET, ESQUEMA)` contra
`(nombre_proceso, nombre_esquema)`.

Parece redundante y no lo es: permite apagar una tabla para todos los
calendarios con un `UPDATE`, sin tocar la Variable, y sacarla solo del diario
sin tocar la base. Cuando los nombres no coinciden, la tabla se extrae **cero
veces** y el DAG falla diciéndolo con esos nombres exactos.

**Columnas:** `ESQUEMA`, `TABLA`, `COLUMNAS`, `FILTRO`, `ACTIVO`,
`NOMBRE_PARQUET`, `TIPOS`, `TABLA_DESTINO`, `BATCH_SIZE`

> **`FILTRO` es SQL libre que se concatena en la consulta contra el origen.**
> No se puede validar sin un parser. Quien pueda editar esa columna puede
> consultar cualquier cosa del core con los permisos del pipeline. Dé permiso
> de escritura solo a quien ya tendría ese acceso de todos modos.

- DDL: `sql/bt2sql/20_bt2sql_control_sqlserver.sql`
- Lo leen: `etl/extraccion_parquet.py`, `etl_bt2sql/bt2sql_extraccion.py`
- Variables: `EXTRACCION_BT_STG.sql_parametros_parquet`, `BT2SQL_EXTRACCION.sql_parametros_parquet`

### `ETL_CONFIG` — mapeo origen → destino (solo SingleStore)

A qué tabla STG va cada archivo parquet, con su `batch_size` y su
`commit_every`.

**Se solapa con `CTL_PARAMETROS_PARQUET`.** El pipeline de SingleStore reparte
la misma información entre las dos: el catálogo dice qué extraer y `ETL_CONFIG`
dice a dónde va. El pipeline de BT2SQL lo unificó metiendo `TABLA_DESTINO` y
`BATCH_SIZE` dentro del catálogo, que es por lo que no tiene `ETL_CONFIG`.
Vale la pena unificar también el de SingleStore, pero eso cambia el `sql_jobs`
de la Variable y hay que hacerlo con cuidado.

**Columnas:** `id_config`, `tabla_origen`, `tabla_destino`, `batch_size`,
`commit_every`, `activo`, `orden`, `descripcion`, `fec_creacion`

- DDL: `sql/05_pipeline_parquet.sql`
- Lo lee: `etl/carga_parquet.py`, vía `CARGAR_PARQUET_CONFIG.sql_jobs`

### `ctl_proceso_parquet` — bitácora de extracción

**Una fila por tabla y por corrida.** No una por corrida: si se extraen veinte
tablas y falla una, se ve exactamente cuál, con su error, su hora y su
duración, sin abrir el log de Airflow.

Es además **el punto de encuentro entre las dos mitades del pipeline**:
`archivo_parquet` guarda la ruta completa, y de ahí la lee la carga. Ninguna de
las dos tiene una ruta escrita en el código, que es lo que permite que la
carpeta cambie cada día sin que nadie edite nada.

**Columnas:** `id_log`, `nom_proceso`, `esquema`, `tabla_origen`,
`archivo_parquet`, `batch_id`, `fecha_proceso`, `commit_every`, `fec_inicio`,
`fec_termino`, `estado`, `host_name`, `msg_error`, `filas_procesadas`,
`duracion_segundos`

**Estados:**

| Estado | Significa |
|---|---|
| `EJECUTANDO` | Empezó y no cerró. Si se queda así, la tarea murió de golpe. |
| `TERMINADO` | Hay archivo en disco con filas dentro. |
| `SIN_DATOS` | Se consultó bien y no había filas. **No es un error.** |
| `ERROR` | Falló. El motivo está en `msg_error`. |

`SIN_DATOS` se separa de `TERMINADO` a propósito: la carga no debe tocar una
tabla destino que no recibió datos, porque truncarla la dejaría vacía sin
motivo. Una tabla que aparece días seguidos como `SIN_DATOS` casi siempre
significa que el `FILTRO` del catálogo quedó mal escrito.

- DDL: `sql/05_pipeline_parquet.sql` (SingleStore), `sql/bt2sql/20_bt2sql_control_sqlserver.sql` (SQL Server)

### `ctl_carga_stg` — bitácora de carga

Una fila por **archivo** cargado. Es la contraparte de `ctl_proceso_parquet`:
uniendo las dos por `batch_id` se sigue una tabla de punta a punta, desde que
sale del origen hasta que está en STG.

**Columnas:** `id_log`, `nom_proceso`, `batch_id`, `archivo_parquet`,
`tabla_destino`, `fec_inicio`, `fec_termino`, `estado`, `host_name`,
`msg_error`, `filas_cargadas`, `duracion_segundos`

**Estados:** `INICIADO` → `EJECUTADO` → `FINALIZADO`, o `ERROR`. Los cuatro
valores salen de la Variable (`CARGAR_PARQUET_CONFIG` o `BT2SQL_CARGA`), no
están escritos en el código.

---

## 2. SingleStore → SQL Server (S2SQL)

Estas tres viven en el esquema `CTL` de SQL Server y siguen otra convención de
nombres. Es el pipeline más reciente y el mejor estructurado de los tres.

### `CTL.CTL_S2SQL_CATALOGO` — catálogo

Qué tablas se exportan, con qué modo de carga y con qué claves.

**Columnas:** `id_catalogo`, `modo_carga`, `columnas`, `filtro_where`,
`claves_merge`, más las de esquema y tabla de origen y destino.

`modo_carga` admite `REEMPLAZO`, `INCREMENTAL` y `MERGE`. Los tres pasan por
una tabla staging; la diferencia está en cómo se aplica al destino.

### `CTL.CTL_S2SQL_LOTE` — una fila por corrida

Es la única bitácora de las tres pipelines que tiene nivel de **lote**, no de
tabla. Sirve para responder «¿cómo fue la corrida de anoche?» de un vistazo,
sin agregar.

**Columnas:** `batch_id`, `fecha_proceso`, `fec_inicio`, `fec_fin`, `estado`,
`tablas_total`, `tablas_ok`, `tablas_error`, `host_name`

### `CTL.CTL_S2SQL_LOG_CARGA` — una fila por tabla

La más detallada de todas. Distingue filas leídas de escritas, e insertadas de
actualizadas, que es lo que hace falta para auditar un `MERGE`.

**Columnas:** `id_log`, `batch_id`, `modo_carga`, `archivo_parquet`,
`fec_inicio`, `fec_fin`, `estado`, `filas_leidas`, `filas_escritas`,
`filas_insertadas`, `filas_actualizadas`, `marca_desde`, `marca_hasta`,
`duracion_segundos`, `msg_error`, `host_name`

- DDL de las tres: `sql/s2sql/10_s2sql_control_sqlserver.sql`
- Ejemplos: `sql/s2sql/11_s2sql_catalogo_ejemplo.sql`

---

## 3. STG → ODS → BDS

Aquí están las cuatro tablas problemáticas: **ninguna tiene DDL en el
repositorio**. Existen en la base porque alguien las creó a mano en algún
momento, y su definición no está versionada en ningún sitio.

### `CTL_CFG_PROCESOS` — catálogo de procesos ODS y BDS

Qué stored procedure corresponde a cada proceso, de qué capa es y a qué grupo
pertenece. El orquestador la cruza con la Variable (`DAG_ODS_TABLAS` o
`DAG_BDS_TABLAS`) igual que el pipeline de parquet cruza su catálogo con la
suya: doble llave otra vez.

**Columnas que el código necesita** (declaradas en `COLUMNAS_CFG_PROCESOS`):
`ID_PROCESO`, `CAPA`, `GRUPO_PROCESO`, `NOMBRE_PROCESO`, `TIPO_PROCESO`,
`STORED_PROCEDURE`, `SCHEMA_ORIGEN`, `TABLA_ORIGEN`, `SCHEMA_DESTINO`,
`TABLA_DESTINO`, `ACTIVO`

El código las lista explícitamente en vez de hacer `SELECT *`, y con razón:
con `SELECT *`, cualquier columna nueva de la tabla entra al diccionario del
proceso y puede pisar una clave del JSON en el merge `{**proceso_db, **proceso_json}`.

- La leen: `etl-datahub/table_ods.py`, `etl-datahub/table_bds.py`

### `CONTROL_EJECUCIONES_SP` — bitácora del orquestador

Una fila por ejecución de stored procedure, con el contexto de Airflow.

**Columnas:** `ID_PROCESO`, `DAG_ID`, `RUN_ID`, `TASK_ID`, `CAPA`,
`GRUPO_PROCESO`, `NOMBRE_PROCESO`, `FECHA_INICIO`, `FECHA_FIN`,
`DURACION_SEGUNDOS`, `ESTADO`, `MENSAJE_ERROR`

El nombre es configurable desde la Variable (`AUDITORIA.TABLA_LOG`);
`CONTROL_EJECUCIONES_SP` es el valor por defecto.

> Este nombre por defecto se unificó en su día porque había **dos** valores
> distintos en el mismo archivo: `CONTROL_EJECUCIONES_SP` en `DEFAULT_CONFIG` y
> `MON_EJECUCIONES` como respaldo. Según si la Variable traía `AUDITORIA.TABLA_LOG`
> o no, los logs se repartían entre dos tablas y ninguna tenía la historia
> completa. Si en su base existe `MON_EJECUCIONES` con filas, son de esa época.

### `ctl_log_proceso` — bitácora de los propios stored procedures

Distinta de la anterior: esta la escribe **el SP desde dentro**, no el
orquestador. Un proceso queda registrado dos veces, desde los dos lados, y eso
es útil: si `CONTROL_EJECUCIONES_SP` dice que el SP terminó bien y
`ctl_log_proceso` dice `ERROR`, el fallo está en el manejo de excepciones del
propio SP.

**Columnas:** `id_log`, `nom_proceso`, `tabla_origen`, `tabla_destino`,
`fec_inicio`, `fec_termino`, `estado`, `filas_insertadas`, `msg_error`

**Estados:** `INICIADO` / `EJECUTANDO` → `TERMINADO`, `WARNING` o `ERROR`.
`WARNING` es específico de estos SP: significa que el proceso corrió sin error
pero no insertó ninguna fila.

La usan: `SP_BDS_MAESTRO_TABLAS`, `SP_BDS_OPERACIONES`, `SP_BDS_SALDOS_CIERRE`,
`SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS`, `SP_UPDATE_BDS_SALDOS_CIERRE`,
`SP_CU_REPORTE_DETALLE_PRODUCTOS`.

> **`filas_insertadas` no es de fiar en la versión actual de los SP.** Se lee
> con `ROW_COUNT()` después de varios `DROP TABLE`, así que registra lo que
> devolvió un `DROP`, nunca el `INSERT`. Corregido en las versiones reescritas
> de `sql/sp_reescritos/`, donde se lee pegado al `INSERT`.

---

## Consultas de operación

**Cómo fue la última corrida de extracción y carga, de punta a punta:**

```sql
SELECT  p.batch_id, p.fecha_proceso, p.tabla_origen,
        p.estado AS extraccion, p.filas_procesadas,
        c.tabla_destino, c.estado AS carga, c.filas_cargadas
FROM    ctl_proceso_parquet p
LEFT    JOIN ctl_carga_stg c
        ON c.batch_id = p.batch_id AND c.archivo_parquet = p.archivo_parquet
WHERE   p.batch_id = (SELECT MAX(batch_id) FROM ctl_proceso_parquet)
ORDER   BY p.tabla_origen;
```

**Tablas que se extrajeron pero no se cargaron** — se pierden en silencio. Casi
siempre falta `TABLA_DESTINO` en el catálogo, o la fila está con `ACTIVO = 'N'`:

```sql
SELECT  p.batch_id, p.tabla_origen, p.esquema, p.archivo_parquet
FROM    ctl_proceso_parquet p
LEFT    JOIN CTL_PARAMETROS_PARQUET e
        ON e.TABLA = p.tabla_origen AND e.ESQUEMA = p.esquema AND e.ACTIVO = 'S'
WHERE   p.estado = 'TERMINADO' AND e.ID_PARAMETRO IS NULL;
```

**Tablas que llevan días sin datos** — normalmente el `FILTRO` quedó mal:

```sql
SELECT  tabla_origen, COUNT(*) AS corridas_sin_datos, MAX(fecha_proceso) AS ultima
FROM    ctl_proceso_parquet
WHERE   estado = 'SIN_DATOS'
  AND   fec_inicio >= DATEADD(day, -15, SYSDATETIME())
GROUP   BY tabla_origen
HAVING  COUNT(*) >= 3;
```

**Procesos ODS/BDS que el orquestador dio por buenos y el SP registró mal:**

```sql
SELECT  o.NOMBRE_PROCESO, o.FECHA_INICIO, o.ESTADO AS estado_orquestador,
        l.estado AS estado_sp, l.msg_error
FROM    CONTROL_EJECUCIONES_SP o
JOIN    ctl_log_proceso l ON l.nom_proceso = o.NOMBRE_PROCESO
WHERE   o.ESTADO = 'TERMINADO' AND l.estado <> 'TERMINADO'
  AND   o.FECHA_INICIO >= DATEADD(day, -7, SYSDATETIME());
```

**Corridas que se quedaron colgadas** — estado de ejecución sin cierre:

```sql
SELECT 'extraccion' AS fase, tabla_origen AS objeto, fec_inicio
FROM   ctl_proceso_parquet WHERE estado = 'EJECUTANDO'
UNION ALL
SELECT 'carga', tabla_destino, fec_inicio
FROM   ctl_carga_stg WHERE estado IN ('INICIADO', 'EJECUTADO')
ORDER  BY fec_inicio;
```

---

## Lo que falta

**Cuatro tablas sin DDL versionado:** `CTL_CFG_PROCESOS`,
`CONTROL_EJECUCIONES_SP`, `ctl_log_proceso` y `MON_EJECUCIONES`.

Las tres primeras son las que sostienen todo el tramo STG → ODS → BDS, que es
la mitad del DataHub. Existen en la base porque alguien las creó a mano, y su
definición no está en ningún archivo. Las consecuencias son concretas: montar
un entorno nuevo desde cero es imposible sin volcarlas primero de producción a
mano; nadie puede revisar en un pull request si una columna cambió; y si
alguien altera una en producción, no queda rastro.

Las columnas listadas arriba salen de leer el código que las consulta, así que
son las que el código **necesita**, no necesariamente todas las que la tabla
tiene ni con los tipos correctos.

**Tres convenciones de nombres conviviendo:** `CTL_PARAMETROS_PARQUET` en
mayúsculas, `ctl_proceso_parquet` en minúsculas, `CTL.CTL_S2SQL_CATALOGO` con
esquema propio. En SQL Server da igual con la intercalación habitual; en
SingleStore también. Es cosmético, pero hace que las consultas escritas para un
pipeline no se puedan copiar a otro sin revisarlas.

**`ETL_CONFIG` duplica parte de `CTL_PARAMETROS_PARQUET`.** El pipeline de
BT2SQL ya lo unificó; el de SingleStore no.

**Ningún proceso limpia estas tablas.** Crecen indefinidamente. Los parquet sí
tienen retención (`retencion_dias` en las Variables), pero sus bitácoras no.
