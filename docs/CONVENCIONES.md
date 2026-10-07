# Convenciones del DataHub

Reglas de nomenclatura y estructura para Connections, DAGs y archivos.

No son preferencias de estilo. Cada una resuelve un problema que ya ocurrió en
este proyecto, y la mayoría están respaldadas por un test que falla si alguien
se las salta.

---

## 1. Connections: Airflow y Spark no son lo mismo

### Por qué hay que separarlas

Parece burocracia tener dos entradas para la misma base. No lo es: **las
credenciales corren riesgos distintos según quién abra la conexión.**

**Cuando la abre Airflow**, el worker lee la Connection del metastore, la
descifra con el Fernet key en memoria, y la usa. La contraseña no sale de ese
proceso. Si alguien mira la interfaz de Airflow, ve el nombre de la Connection,
no su contenido.

**Cuando la consume Spark**, la cosa cambia. Si las credenciales viajan como
configuración del job (`spark.*` o el diccionario `properties` de
`spark.read.jdbc`), acaban en tres sitios que nadie audita:

- la pestaña **Environment** de la interfaz de Spark, visible para cualquiera
  que alcance el puerto 8082
- la salida de `ps` dentro de los contenedores
- **los event logs**, que con `spark.eventLog.enabled true` se escriben en
  `/opt/spark-events` y **quedan en disco**, para que el History Server los lea
  después

Ese último es el que duele: no es una exposición momentánea, es un archivo con
la contraseña dentro que sobrevive al job.

### Lo que ya hace bien y lo que no

Su `BsgSparkJdbcOperator` **lo hace bien**: pasa usuario y clave por variables
de entorno (`ORIGEN_USUARIO`, `ORIGEN_CLAVE`), que no aparecen en la pestaña
Environment ni en los event logs.

```python
env_vars = {
    self.user_env: conn.login,
    self.password_env: conn.password,
}
```

`test_sqlserver.py` **lo hace mal**:

```python
properties = {"user": USER, "password": PASSWORD, "driver": "..."}
df = spark.read.jdbc(url=jdbc_url, properties=properties)
```

Eso mete la contraseña en la configuración del job. Con `eventLog` activado
—y lo está— queda escrita en disco.

### La regla

**Prefijo por quién abre la conexión:**

```
AF_<MOTOR>_<AMBIENTE>      la abre el worker de Airflow
SPK_<MOTOR>_<AMBIENTE>     la consume un job de Spark
```

| Nombre | Quién | Driver tiene que estar en |
|---|---|---|
| `AF_BANTOTAL_PRD` | worker de Airflow | `airflow-bsg` |
| `AF_SQLSERVER_PRD` | worker de Airflow | `airflow-bsg` |
| `AF_SINGLESTORE_PRD` | worker de Airflow | `airflow-bsg` |
| `SPK_SQLSERVER_PRD` | job de Spark | `spark-bsg` |
| `SPK_SINGLESTORE_PRD` | job de Spark | `spark-bsg` |

El prefijo va delante y no detrás **porque la interfaz de Airflow ordena
alfabéticamente**. Con `AF_` y `SPK_` al principio, las conexiones quedan
agrupadas por ejecutor, y la pregunta de auditoría —«¿qué credenciales pueden
acabar en un log de Spark?»— se contesta mirando un bloque contiguo.

### Las dos entradas llevan credenciales distintas

Esta es la parte que hace que la separación valga la pena. La cuenta que usa
Spark debería tener **menos permisos**: solo lectura sobre las tablas que
necesita, nada de escritura, nada de DDL. Así, si esa credencial se filtra por
un event log, el daño está acotado.

Con una sola entrada compartida eso es imposible: o le da a Spark los permisos
de Airflow, o le quita a Airflow los que necesita.

### Cómo migrar sin tocar código

Los nombres actuales (`CONEXION_BANTOTAL`, `CONEXION_SQLSERVER`) están como
**valor por defecto** en el código, pero se pueden sobrescribir desde la
Variable:

```json
{
  "conn_id_origen":  "AF_BANTOTAL_PRD",
  "conn_id_destino": "AF_SQLSERVER_PRD"
}
```

Así que la migración es editar Variables, no código. Se pueden crear las nuevas,
apuntar las Variables, verificar, y recién entonces borrar las viejas.

### Lo que hay hoy

```
CONEXION_BANTOTAL          -> AF_BANTOTAL_PRD
CONEXION_SQLSERVER         -> AF_SQLSERVER_PRD
CONEXION_SINGLESTORE       -> AF_SINGLESTORE_PRD
BT_CONEXION_PREPRODUCTION  -> AF_BANTOTAL_PRE
BT_PREPRODUCTION           -> duplicada de la anterior; eliminar
conexion_bitacora          -> AF_SQLSERVER_PRD  (es la misma base)
mi_base                    -> eliminar
```

`BT_CONEXION_PREPRODUCTION` y `BT_PREPRODUCTION` son dos nombres para lo mismo,
y `mi_base` no dice nada sobre a qué apunta. Son exactamente el tipo de cosa que
un esquema de nombres impide.

---

## 2. DAGs: procesos continuos y todo lo demás

### El problema real

Hoy hay **once `dag_id` en tres estilos distintos**:

```
BT2SQL_STG  ODS_PROCESOS_DATAHUB  VALIDAR_CONEXIONES     mayúsculas
STG_BT_PARQUET  carga_parquet_singlestore    minúsculas
Extraer_datos_bt                                          mezclado
```

Y **siete de ocho tienen `schedule=None`**. Nada indica si eso es deliberado —
«este DAG se dispara desde otro»— o si alguien se olvidó de ponerle horario.
Esa ambigüedad es la que hay que eliminar.

### La regla de nombres

```
<CAPA>_<PIPELINE>_<ACCION>
```

| Parte | Valores |
|---|---|
| `CAPA` | `STG`, `ODS`, `BDS`, `CU`, `EXP` (exportación), `UTIL`, `LAB` |
| `PIPELINE` | `BT2SQL`, `BT`, `S2SQL`, `DATAHUB` |
| `ACCION` | `EXTRACCION`, `CARGA`, `PROCESOS`, `EXPORT`, ... |

Todo en mayúsculas con guion bajo. La capa va primero porque es lo que agrupa
en la lista ordenada de la interfaz: todos los `STG_` juntos, todos los `ODS_`
juntos, y las utilidades al final sin mezclarse con producción.

```
STG_BT2SQL_CARGA          Bantotal -> parquet -> STG en SQL Server
STG_BT_PARQUET            Bantotal -> parquet -> STG en SingleStore
STG_BT_PARQUET_SPARK      lo mismo, con Spark
ODS_DATAHUB_PROCESOS      STG -> ODS
BDS_DATAHUB_PROCESOS      ODS -> BDS
EXP_S2SQL_SQLSERVER       SingleStore -> SQL Server
UTIL_VALIDAR_CONEXIONES   diagnóstico
UTIL_AUDITORIA_ROLES      seguridad
LAB_BT_PREPRODUCCION      pruebas, NUNCA se programa
```

`LAB_` es importante: marca lo que no es producción. Los tres `test_*.py` que
hoy están en `dags/production/` deberían ser `LAB_` y vivir en otra carpeta
—ahora mismo abren conexiones al core cada 30 segundos, al parsearse.

### La regla de los procesos continuos

**Un DAG de producción o tiene `schedule`, o declara por qué no lo tiene.**

```python
# Continuo: se ejecuta solo
schedule="0 3 * * *"
tags=["produccion", "continuo", "stg"]

# Bajo demanda: alguien o algo lo dispara
schedule=None
tags=["produccion", "manual", "stg"]

# Disparado por otro DAG
schedule=None
tags=["produccion", "disparado", "ods"]
```

Las tres etiquetas `continuo`, `manual` y `disparado` son excluyentes y
**obligatorias**. Con eso, el filtro por etiqueta de la interfaz contesta de
inmediato «qué corre solo» — y un `schedule=None` deja de ser ambiguo: si no
lleva `manual` ni `disparado`, es un olvido y el test lo marca.

### Etiquetas obligatorias

Cada DAG lleva al menos tres:

1. **Ambiente**: `produccion` o `laboratorio`
2. **Cadencia**: `continuo`, `manual` o `disparado`
3. **Capa**: `stg`, `ods`, `bds`, `cu`, `exp`, `utilidad`

Y las que quiera después: `bantotal`, `sqlserver`, `spark`...

---

## 3. Archivos

```
airflow/dags/production/
    dag_stg_bt2sql_carga.py          un DAG por archivo
    etl_bt2sql/                      sus módulos, en un paquete al lado
        __init__.py
        bt2sql_comun.py
        bt2sql_extraccion.py
        bt2sql_carga.py
```

**El nombre del archivo es `dag_` + el `dag_id` en minúsculas.** Suena trivial
y no lo es: cuando la interfaz muestra `STG_BT2SQL_CARGA` roto, uno quiere
saber qué archivo abrir sin buscar. Hoy `Extraer_datos_bt` está en
`dag_util_orquestador_json.py` y no hay forma de adivinarlo.

**Un DAG por archivo.** Dos DAGs en un archivo significa que un error de
sintaxis los rompe los dos.

**Los módulos van en un paquete al lado, y ese paquete va en `.airflowignore`.**
Sin eso, el scheduler los importa como si fueran DAGs cada 30 segundos — con
`jaydebeapi` dentro, eso es levantar una JVM por cada pasada del parser.

---

## 4. Reglas de implementación

Estas nacen de fallos concretos de este proyecto, no de un manual.

### Obligatorio en todo DAG

```python
with DAG(
    dag_id="STG_BT2SQL_CARGA",
    description="...",                    # sale en la lista de la interfaz
    start_date=datetime(2026, 1, 1),      # fijo, NUNCA datetime.now()
    schedule=None,
    catchup=False,                        # o se dispara una corrida por día perdido
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=10),   # ver abajo
    render_template_as_native_obj=True,   # ver abajo
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "on_failure_callback": alertar_fallo,
    },
    doc_md=__doc__,
    tags=["produccion", "manual", "stg"],
) as dag:
```

**`dagrun_timeout` no es opcional.** Con `max_active_runs=1`, una corrida
atascada bloquea **todas** las siguientes. `execution_timeout` protege cada
tarea, pero no cubre el tiempo en `queued` esperando un slot del pool ni en
`up_for_retry`.

**`render_template_as_native_obj=True` en cuanto se pase un XCom entre tareas.**
Sin esto, `"{{ ti.xcom_pull(...) }}"` entrega la **cadena** `'None'` cuando no
hay XCom, y un `if batch_id:` la da por buena. La consulta busca
`batch_id = 'None'`, no encuentra nada, el respaldo nunca se ejecuta, y la
tarea sale **verde** sin haber cargado una fila. Ya pasó una vez.

**`start_date` fijo.** Con `datetime.now()` el DAG cambia de fecha de inicio en
cada parseo y el scheduler se comporta de forma impredecible.

### Obligatorio en toda tarea

```python
PythonOperator(
    task_id="cargar_stg",
    python_callable=cargar_stg,
    execution_timeout=timedelta(hours=4),   # obligatorio
    pool=POOL_SQLSERVER,
    retries=1,
)
```

**`execution_timeout` en todas.** Sin él, una consulta trabada contra el core
deja la tarea en `running` indefinidamente y, con `max_active_runs=1`, el DAG
entero parado.

**Reintentos según el tipo de fallo.** Un reintento solo sirve si el fallo puede
ser pasajero. Una biblioteca que falta o una columna renombrada no se arreglan
reintentando, y reintentar tres veces una extracción larga es molestar al core
tres veces para llegar al mismo error. Extracciones: `retries=1`. Compuertas de
verificación: `retries=0`.

**Limpiezas con `trigger_rule="all_done"`.** Si no, las carpetas viejas se
acumulan justo los días con problemas, que es cuando menos falta hace quedarse
sin disco.

### Imports

**Nada pesado a nivel de módulo en un archivo de DAG.** `jaydebeapi`, `pyodbc`,
`pyarrow`, `pandas` van **dentro** de la función.

El proceso que parsea los DAGs importa cada archivo cada 30 segundos. Importar
`jaydebeapi` arriba levanta una JVM en cada pasada.

Y hay un efecto lateral que importa más: con el import dentro de la función, un
driver que falte en un worker hace fallar **esa tarea**, con su traza en su log.
Con el import arriba, tumba el archivo entero con un *Broken DAG* y desaparecen
todos sus DAGs de la interfaz.

### Credenciales

**Nunca en un archivo.** Siempre en una Connection.

Hoy `test_bt_dev.py`, `test_bt_preproduccion.py` y `test_sqlserver.py` tienen
usuario y contraseña en texto plano, están en `dags/production/`, y **no están
en `.airflowignore`** — o sea que el scheduler los ejecuta cada 30 segundos,
abriendo sesiones de Spark y conexiones al core.

Eso hay que moverlo y rotar esas tres contraseñas. Es lo más urgente de este
documento.

---

## 5. Migración sugerida

No de golpe. Por orden de riesgo:

1. **Sacar los `test_*.py` de `dags/production/`** y rotar sus contraseñas.
   Es lo único verdaderamente urgente.
2. **Agregar las etiquetas de cadencia** a los ocho DAGs. Es cosmético y no
   rompe nada.
3. **Crear las Connections nuevas** junto a las viejas, apuntar las Variables,
   verificar, borrar las viejas.
4. ~~**Renombrar los `dag_id`.**~~ **Hecho el 2026-10-07.** Ver la sección 9.
   Era lo más disruptivo, y el motivo sigue en pie para la próxima vez: cambiar
   un `dag_id` **pierde el historial** de ese DAG, porque Airflow lo trata como
   uno nuevo.

---

## 6. Qué está automatizado

`airflow/tests/unit/test_convenciones.py` verifica lo que se puede verificar
leyendo el código:

- toda tarea tiene `execution_timeout`
- todo DAG tiene `dagrun_timeout`, `catchup=False`, `tags` y `owner`
- todo DAG declara su cadencia (`continuo`, `manual` o `disparado`)
- ningún archivo de DAG importa `jaydebeapi`, `pyodbc`, `pyarrow` o `pandas`
  a nivel de módulo
- ningún archivo bajo `dags/` contiene una contraseña en texto plano
- los `dag_id` nuevos siguen `<CAPA>_<PIPELINE>_<ACCION>`
- las Connections nuevas siguen `AF_` o `SPK_`

Los DAGs actuales están en una lista de excepciones **explícita**, con la fecha
en que se agregaron. La lista está para encogerse: cada vez que se migre uno, se
quita de ahí. Lo que no se admite es que crezca.

---

## 8. Lo que se aplicó el 2026-09-30

No es una lista de intenciones: son los cambios que ya están en el repositorio,
con la comprobación que los vigila.

### Lo que el scheduler registra ahora

De **doce** `dag_id` a **ocho**. Los cuatro que desaparecieron no se
renombraron: nunca debieron existir.

| Antes salía en la interfaz | Venía de | Qué pasó |
|---|---|---|
| `carga_parquet_singlestore` | `examples/cargar_parquet_config.py` | `.airflowignore` |
| `Extraer_datos_bt` | `examples/extraccion_parquet.py` | `.airflowignore` |
| `VALIDAR_CONEXIONES` | `templates/plantilla_conexion.py` | `.airflowignore` |
| `PLANTILLA_cambiar_este_nombre` | `templates/plantilla_dag.py` | `.airflowignore` |

`examples/` era una copia vieja de `production/etl/`. Esa carpeta sí estaba
ignorada; la copia no, así que el scheduler importaba `pyodbc`, `ibm_db`,
`singlestoredb`, `pyarrow` y `pandas` **cada 30 segundos** para registrar dos
`dag_id` duplicados.

Y `templates/plantilla_conexion.py` **no compila** — le falta una coma en la
línea 44. Mientras estuvo en la carpeta, el scheduler dejaba un *Broken DAG*
permanente en la interfaz, del que nadie se hacía cargo porque el archivo no
era de nadie.

Comprobarlo después de reiniciar el scheduler:

```bash
docker compose exec airflow-webserver airflow dags list
```

Deben salir **ocho**, y ninguno de la tabla de arriba.

### Los ocho, con su cadencia declarada

| `dag_id` | Archivo | Cadencia |
|---|---|---|
| `STG_BT2SQL_CARGA` | `dag_stg_bt2sql_carga.py` | `manual` |
| `BDS_DATAHUB_PROCESOS` | `dag_bds_datahub_procesos.py` | `manual` |
| `EXP_S2SQL_CARGA` | `dag_exp_s2sql_carga.py` | `manual` |
| `STG_BT_PARQUET` | `dag_stg_bt_parquet.py` | `manual` |
| `STG_BT2SQL_CARGA_SPARK` | `dag_stg_bt2sql_carga_spark.py` | `manual` |
| `UTIL_AUDITORIA_ROLES` | `dag_util_auditoria_roles.py` | `continuo` |
| `UTIL_ORQUESTADOR_JSON` | `dag_util_orquestador_json.py` | `disparado` |
| `LAB_BT_ODBC` | `dag_lab_bt_odbc.py` | `manual` |

`BT2SQL_STG` se renombró a `STG_BT2SQL_CARGA` y su archivo a
`dag_stg_bt2sql_carga.py`. Se pudo renombrar sin pensarlo dos veces porque
**todavía no ha corrido en producción**: no hay historial que perder. Los otros
siete sí lo tienen, y por eso siguen en la lista de excepciones con su nombre
propuesto al lado, esperando una ventana en que valga la pena perderlo.

### La credencial de `dag_lab_bt_odbc.py`

Tenía el usuario y la contraseña de **BT PREPRODUCCIÓN** escritos en el
archivo. Estaban en tres sitios a la vez: en git con todo su historial, dentro
de la imagen de Docker, y en el traceback que Airflow muestra en la interfaz
cuando la tarea falla.

Ahora usa la Connection `AF_BANTOTAL_PRE`. **Hay que crearla antes de volver a
lanzar ese DAG**, o falla:

```bash
airflow connections add AF_BANTOTAL_PRE \
    --conn-type odbc --conn-host "BT PREPRODUCCION" \
    --conn-login USUARIO --conn-password CLAVE
```

**Esa contraseña hay que rotarla igual.** Sacarla del archivo no la borra del
historial de git: sigue en cada commit anterior, y ahí la ve cualquiera que
clone el repositorio. Lo mismo vale para las de `test_bt_dev.py`,
`test_bt_preproduccion.py` y `test_sqlserver.py`, que ahora están en
`.airflowignore` pero siguen en disco.

### `dagrun_timeout` donde faltaba

`execution_timeout` protege cada tarea, pero no cuenta el tiempo en `queued`
esperando un slot ni el de `up_for_retry`. Con `max_active_runs=1`, una corrida
atascada ahí bloquea **todas** las siguientes, sin error y sin aviso.

| DAG | Tope puesto | Por qué ese |
|---|---|---|
| `STG_BT2SQL_CARGA_SPARK` | 10 h | extracción y carga de 4 h más las tres tareas cortas |
| `UTIL_ORQUESTADOR_JSON` | 12 h | el manifiesto decide cuántas tareas, el peor caso no está acotado por el código |
| `UTIL_AUDITORIA_ROLES` | 1 h | tres tareas de 15 min; margen para reintentos sin colgarse hasta el lunes siguiente |

### Tres errores que tenía la propia comprobación

Vale la pena dejarlos escritos, porque los tres hacían que el test **mintiera
en la dirección cómoda**:

1. **Miraba sólo `production/*.py`.** Por eso `examples/` y `templates/` —que
   el scheduler sí importaba— quedaban fuera. El test pasaba por no mirar.
   Ahora recorre `airflow/dags/` entero y aplica los patrones de
   `.airflowignore`, que es exactamente lo que hace el DagBag.
2. **No contaba `execution_timeout` en `default_args`**, donde lo hereda cada
   tarea. Denunciaba a `dag_util_auditoria_roles.py` y `dag_util_orquestador_json.py`, que sí
   estaban protegidos, y exigía un plazo a los `EmptyOperator`, que terminan en
   el mismo instante en que arrancan.
3. **No resolvía `dag_id=DAG_ID`.** `dag_bds_datahub_procesos.py` y `dag_exp_s2sql_carga.py`
   declaran el nombre arriba como constante, así que eran invisibles para todos
   los tests de nombre — justo donde un nombre mal puesto se esconde.

Un test que se equivoca hacia el «todo bien» es peor que no tenerlo: da una
garantía que no existe.


---

## 9. Lo que se aplicó el 2026-10-07

### Los seis renombres

`LEGADO_NOMBRE` quedó **vacía**, y el tope del test que la vigila bajó de 10 a
cero: a partir de ahora un `dag_id` que no siga la convención se arregla antes
de entrar, no se anota como deuda.

| Antes | Ahora | Archivo |
|---|---|---|
| `etl_bt_parquet_singlestore` | `STG_BT_PARQUET` | `dag_stg_bt_parquet.py` |
| `BT_DATAHUB` | `BDS_DATAHUB_PROCESOS` | `dag_bds_datahub_procesos.py` |
| `S2SQL_EXPORT` | `EXP_S2SQL_CARGA` | `dag_exp_s2sql_carga.py` |
| `orquestador_json` | `UTIL_ORQUESTADOR_JSON` | `dag_util_orquestador_json.py` |
| `auditoria_roles` | `UTIL_AUDITORIA_ROLES` | `dag_util_auditoria_roles.py` |
| `bt_odbc_test` | `LAB_BT_ODBC` | `dag_lab_bt_odbc.py` |

Los dos que ya cumplían no se tocaron: `STG_BT2SQL_CARGA` y
`STG_BT2SQL_CARGA_SPARK`, que son el pipeline Bantotal → SQL Server y el eje
del proyecto.

### El precio, dicho antes de que se note

Airflow identifica un DAG por su `dag_id`, no por su archivo. Al cambiar el
`dag_id`:

- **El historial de corridas del nombre viejo no se migra.** Sigue en la base
  de metadatos, pero colgando de un `dag_id` que ya no existe en ningún
  archivo.
- **Los seis nombres viejos aparecen en la interfaz como DAGs sin archivo**
  («removed»). No se ejecutan y no estorban más que visualmente, pero hay que
  borrarlos a mano: en la lista de DAGs, el botón de la papelera en cada uno.
  Borrar ahí elimina el historial de ese `dag_id`; si hace falta conservarlo,
  expórtelo antes.
- **Las pausas se reinician.** Un DAG que estaba en pausa con el nombre viejo
  aparece con el estado por defecto en el nombre nuevo. Reviselo antes de
  dejarlo correr.
- **Lo que no se rompe:** ningún DAG dispara a otro por `dag_id` en este
  proyecto, así que no hay `TriggerDagRunOperator` ni `ExternalTaskSensor` que
  quedara apuntando al vacío. Se verificó archivo por archivo.

### Por qué `BDS_DATAHUB_PROCESOS` y no `ODS_...`

Ese DAG orquesta la capa ODS y después la BDS **en un solo DAG**, así que
ninguna capa sola lo describe. Se nombró por el producto final —la capa BDS— y
lleva las dos etiquetas, `bds` y `ods`, para que el filtro de la interfaz lo
encuentre por cualquiera de las dos.

Lo correcto según la convención sería partirlo en dos DAGs, `ODS_DATAHUB_PROCESOS`
y `BDS_DATAHUB_PROCESOS`, con el segundo esperando al primero. Eso no es un
renombre: es un cambio de grafo, con su propia validación. Queda pendiente y
anotado aquí para que no se pierda.

### Etiquetas completadas

De paso se normalizaron las etiquetas de los seis, que es lo que hace útil el
filtro de la interfaz. Faltaba el ambiente en `LAB_BT_ODBC` (ahora
`laboratorio`), la capa en cuatro, y `BDS_DATAHUB_PROCESOS` las traía en
MAYÚSCULAS, que para el filtro son etiquetas distintas de las minúsculas que
usa el resto. Se agregó `exp` a la lista de capas válidas: `EXP` ya era una
capa legítima en el `dag_id` y no tenía etiqueta.
