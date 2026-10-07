# Validar el pipeline Spark, en orden

Todos los comandos asumen Windows y `docker-compose.windows.yml`. En Red Hat,
cambie el archivo por `docker-compose.rhel.yml` y `docker` por `podman` si
corresponde.

Para no repetir la ruta larga:

```powershell
$C = "docker compose -f docker-compose.windows.yml"
```

```bash
C="docker compose -f docker-compose.windows.yml"
```

---

## 0. Antes de levantar: que el compose sea válido

```bash
docker compose -f docker-compose.windows.yml config --quiet && echo "compose OK"
```

Sin salida y código 0 significa que el YAML y todas las variables del `.env`
resuelven. Si falta una variable, lo dice aquí y no a mitad del arranque.

---

## 1. Recrear los contenedores

`restart` **no** sirve: el volumen `spark_events` es nuevo y un contenedor ya
creado no adopta volúmenes nuevos.

```bash
docker compose -f docker-compose.windows.yml up -d
docker compose -f docker-compose.windows.yml ps
```

Los nueve servicios en `running`, y `spark-master` en `(healthy)` a los ~30 s.

---

## 2. Que el volumen que causaba el error del worker esté montado

```bash
docker compose -f docker-compose.windows.yml exec airflow-worker ls -ld /opt/spark-events
```

Tiene que listar el directorio. Antes daba `No such file or directory`, y eso
era exactamente lo que mataba al worker nuevo.

---

## 3. Que los plugins se hayan recargado

```bash
docker compose -f docker-compose.windows.yml exec airflow-worker \
  python -c "import airflow, sys; sys.path.append('/opt/airflow/plugins'); \
from utils.spark_config import driver_for_conn, motor_de_conexion, jdbc_url; \
print('plugins recargados OK')"
```

Si sale `ImportError: cannot import name 'driver_for_conn'`, el proceso sigue
con el módulo viejo en memoria: ver `DIAGNOSTICO_SPARK.md`.

> **`append`, no `insert(0, ...)`, y `import airflow` primero.** Ese detalle no
> es estilo. `sys.path.insert(0, '/opt/airflow/plugins')` pone la carpeta de
> plugins **delante** de la biblioteca estándar, así que un paquete ahí que se
> llame igual que un módulo estándar lo tapa. Con `plugins/logging/` existiendo,
> el `import logging` de `airflow/configuration.py` resolvía al paquete del
> proyecto y salía esto:
>
> ```
> AttributeError: module 'logging' has no attribute 'getLogger'
> ```
>
> que no menciona `plugins/` por ningún lado. Airflow en operación normal no
> sufre esto porque **añade** `plugins/` al final de `sys.path`. Las dos
> carpetas vacías culpables se borraron el 2026-10-07 y hay un test que impide
> que vuelvan, pero la forma correcta del comando sigue siendo esta.

---

## 4. Los drivers JDBC, en los dos lados

```bash
docker compose -f docker-compose.windows.yml exec airflow-worker \
  python /opt/airflow/verificar_drivers.py

docker compose -f docker-compose.windows.yml exec airflow-worker ls -l /opt/airflow/jars
docker compose -f docker-compose.windows.yml exec spark-worker  ls -l /opt/spark/jars | grep -Ei "jt400|mssql"
```

El jar tiene que estar en **las dos** imágenes. Si solo está en Airflow, el job
arranca y falla en el executor con un `ClassNotFoundException` que no nombra el
archivo.

---

## 5a. Cómo se llaman sus Connections

Antes de comprobar el motor hay que saber el nombre. El repositorio espera
`CONEXION_BANTOTAL` y `CONEXION_SQLSERVER`, pero eso es solo el **valor por
defecto**: los nombres de verdad salen de la Variable.

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler \
  airflow connections list -o table
```

Si las suyas se llaman de otra forma, **no renombre las Connections**:
decláreles el nombre en la Variable, que es donde vive la configuración.

```json
"extraccion": {
  "conn_id_origen": "EL_NOMBRE_DE_SU_CONEXION_A_BANTOTAL",
  "conn_id_destino": "EL_NOMBRE_DE_SU_CONEXION_A_SQLSERVER"
}
```

Luego `sync_variables.py --solo BT2SQL_SPARK` y lanzar el DAG. El DAG los lee del JSON publicado en cada pasada del
parser; si el archivo no existe todavía, cae a los nombres por defecto con un
aviso en el log del scheduler en vez de romper el parseo — un DAG que no
aparece en la interfaz es mucho peor de diagnosticar que uno que aparece y
falla en su primera tarea.

---

## 5b. Las conexiones Generic: que resuelvan a un motor

Esta es la comprobación que importa con conexiones de tipo `Generic`, porque
`Generic` no dice nada del motor.

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler python -c "
import airflow, sys; sys.path.append('/opt/airflow/plugins')
from airflow.hooks.base import BaseHook
from utils.spark_config import motor_de_conexion, driver_for_conn, jdbc_url
for cid in ('CONEXION_BANTOTAL','CONEXION_SQLSERVER'):
    c = BaseHook.get_connection(cid)
    print(f'{cid:22} tipo={c.conn_type:10} motor={motor_de_conexion(c):9} '
          f'driver={driver_for_conn(c).driver_class.rsplit(chr(46),1)[-1]}')
    print(f'{chr(32)*22} url={jdbc_url(c).split(chr(59))[0]}')
"
```

Salida esperada: `motor=as400` para Bantotal y `motor=mssql` para SQL Server.
Si falla, el mensaje dice qué poner en el campo **Extra** de la Connection:
`{"motor": "as400"}` o `{"motor": "mssql"}`.

---

## 6. Los tests del repositorio

```bash
docker compose -f docker-compose.windows.yml exec airflow-webserver \
  pytest /opt/airflow/tests/unit -q
```

Si `pytest` no está en la imagen:

```bash
docker compose -f docker-compose.windows.yml exec airflow-webserver \
  pip install --quiet pytest && \
docker compose -f docker-compose.windows.yml exec airflow-webserver \
  pytest /opt/airflow/tests/unit -q
```

Los que vigilan este trabajo son `test_convenciones.py` (nombres, archivos,
`execution_timeout`, cadencia) y `test_drivers_jdbc.py`.

---

## 7. Que el scheduler registre los ocho DAGs y ninguno roto

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler \
  airflow dags list-import-errors

docker compose -f docker-compose.windows.yml exec airflow-scheduler \
  airflow dags list
```

`list-import-errors` tiene que salir **vacío**. `dags list` tiene que mostrar
exactamente ocho, todos con prefijo de capa:

```
BDS_DATAHUB_PROCESOS  EXP_S2SQL_CARGA  LAB_BT_ODBC  STG_BT2SQL_CARGA
STG_BT2SQL_CARGA_SPARK  STG_BT_PARQUET  UTIL_AUDITORIA_ROLES  UTIL_ORQUESTADOR_JSON
```

Los seis nombres viejos aparecerán en la interfaz como DAGs sin archivo. Se
borran con la papelera de cada uno; eso elimina su historial.

---

## 8. Las ocho tareas del DAG Spark

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler \
  airflow tasks list STG_BT2SQL_CARGA_SPARK --tree
```

---

## 9. Publicar la Variable

```bash
docker compose -f docker-compose.windows.yml exec airflow-webserver \
  python /opt/airflow/scripts/sync_variables.py --solo BT2SQL_SPARK --dry-run

docker compose -f docker-compose.windows.yml exec airflow-webserver \
  python /opt/airflow/scripts/sync_variables.py --solo BT2SQL_SPARK
```

Eso deja la Variable en Airflow. **Todavía no está en el volumen**, que es lo
que lee Spark.

---

## 10. Probar las cuatro tareas baratas sin levantar Spark

Las cuatro primeras tareas leen el JSON del volumen, así que hay que publicarlo
una vez. Esto hace a mano exactamente lo que hace la publicacion automatica, y
sirve para poder probar por partes sin gastar una corrida de Spark:

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler bash -lc '
mkdir -p /opt/spark-data/runtime && python -c "
from airflow.models import Variable
import json
json.dump(Variable.get(\"BT2SQL_SPARK\", deserialize_json=True),
          open(\"/opt/spark-data/runtime/bt2sql_spark.json\", \"w\"),
          ensure_ascii=False, indent=2)
print(\"publicado a mano, solo para validar\")"'
```

Y ahora cada tarea por separado, en este orden:

```bash
D=$(date +%Y-%m-%d)
for T in verificar_origen_destino preparar_config inventario_tablas validar_catalogo; do
  echo "================ $T"
  docker compose -f docker-compose.windows.yml exec airflow-scheduler \
    airflow tasks test STG_BT2SQL_CARGA_SPARK $T $D
done
```

Qué mirar en cada una:

| Tarea | Qué debe decir el log |
|---|---|
| `verificar_origen_destino` | `ORIGEN ... OK fecha de proceso del core: …`, `DESTINO ... OK` para las tres tablas de control, y `SPARK ... motor=as400` / `motor=mssql` |
| `preparar_config` | `CONFIGURACION SIN CAMBIOS` o `CONFIGURACION PUBLICADA`, con el hash en los dos casos |
| `inventario_tablas` | la tabla de tablas, con `ACTIVA` / `inactiva` y el motivo de las que no entran |
| `validar_catalogo` | `Tablas declaradas en la Variable: N | registradas en el catálogo: N` y una línea `registrada` por tabla |

---

## 11. La corrida real

Sin parámetros. `publicar_config` vale `auto`, así que publica la Variable si
cambió y no toca nada si no.

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler \
  airflow dags trigger STG_BT2SQL_CARGA_SPARK
```

Los parámetros solo hacen falta para salirse de lo normal:

```bash
# relanzar con la configuración EXACTA que ya está en el volumen
--conf '{"publicar_config":"no"}'
# forzar la publicación aunque el hash no haya cambiado
--conf '{"publicar_config":"si"}'
# recargar un día concreto
--conf '{"tipo_ejecucion":"reproceso"}'
```

---

## 12. Que Spark de verdad usó el cluster

```bash
docker compose -f docker-compose.windows.yml exec airflow-worker \
  ls -l /opt/spark-events
```

Un archivo por aplicación. Si está vacío después de una corrida, el job no
llegó a crear el `SparkContext`.

| Dónde | Qué ver |
|---|---|
| `http://localhost:8082` (master) | la aplicación con sus executors mientras corre |
| `http://localhost:18080` (history) | la aplicación ya terminada, con sus etapas |

En el History Server, abra la aplicación y mire las **etapas de la lectura**:
si una tabla tiene una sola tarea, se leyó con una sola conexión y Spark no
aportó paralelismo ahí. El job lo avisa en el log, y se arregla declarando
`particion` para esa tabla en la Variable.

---

## 13. Comparar contra el pipeline de pandas

Es la única validación que dice que la traducción a Spark salió bien. Los dos
DAGs escriben en carpetas distintas y se distinguen en la bitácora por
`nom_proceso`, así que pueden correr el mismo día:

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler \
  airflow dags trigger STG_BT2SQL_CARGA
```

Y después, contra SQL Server, fila a fila por tabla:

```sql
SELECT nom_proceso, tabla_origen, estado, filas_procesadas, duracion_segundos
FROM   dbo.ctl_proceso_parquet
WHERE  fecha_proceso = CAST(GETDATE() AS DATE)
ORDER  BY tabla_origen, nom_proceso;
```

Mismo `filas_procesadas` por tabla entre `SPARK_EXTRACCION_BANTOTAL` y el
proceso de pandas. Si no coinciden, no siga: la diferencia es el hallazgo.

---

## 14. La suite de tests del pipeline Spark

80 tests en `airflow/tests/unit/test_bt2sql_spark.py`. No necesitan ni Bantotal,
ni SQL Server, ni un cluster, ni la JVM.

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler \
  pytest /opt/airflow/tests/unit/test_bt2sql_spark.py -v
```

| Sección | Qué vigila |
|---|---|
| 1. Conexiones Generic | que `Generic` no se traduzca a un motor fijo, que el motor salga del Extra o del puerto, que el 3306 **no** se adivine, y que la URL del core lleve `prompt=false` |
| 2. La Variable | que traiga `tablas`, que la doble llave cuadre **en los dos sentidos**, que las particiones estén bien formadas, y que los dos pipelines no se pisen |
| 3. Calendario y prioridad | que `estado`, el flag del día y `prioridad` se apliquen de verdad, por las dos ramas (`variable` y `catalogo`) |
| 4. Tipos de dato | los ocho tipos del parser, que `BIT` sea entero, y que los tipos se deduzcan de `INFORMATION_SCHEMA` de la tabla destino |
| 5. La carga | el cruce lote ↔ destino, y que una tabla extraída sin declarar sea **error y no silencio** |
| 6. Tareas de Python | el inventario, los cortes, y que los `conn_id` salgan de la configuración |
| 7. Publicación | que `auto` no reescriba un archivo idéntico, que publique y respalde cuando cambió, y que `no` ignore la Variable |
| 8. El grafo | una tarea por proceso, grupos por prioridad encadenados, `--proceso` correcto en cada una, `all_done` en el punto de unión, y que sin configuración el DAG **no desaparezca** |
| 9. El compose | que `spark_events` esté montado en los servicios de Airflow — lo que tumbaba a los workers nuevos |

### Se comprobó que los tests tienen dientes

Un test que no falla cuando el código se rompe es peor que no tenerlo: da
confianza sin darla. Así que se rompió el código a propósito, seis veces, y se
verificó que la suite lo cazaba:

| Se rompió | Lo caza |
|---|---|
| Devolver `"generic"` a los `conn_types` de as400 | `test_generic_no_se_traduce_a_as400_por_omision` |
| Que el job deje de aplicar el calendario (rama `variable`) | 4 tests |
| Que lo deje de aplicar por la rama `catalogo` | `test_el_calendario_tambien_se_aplica_leyendo_del_catalogo` |
| Quitar el orden por prioridad | `test_el_job_ordena_por_prioridad` |
| Dejar de excluir `FECHA_PROCESO`/`BATCH_ID` de los tipos | `test_los_tipos_salen_de_la_tabla_destino...` |
| Desmontar `spark_events` del worker | `test_spark_events_esta_montado...` |
| Quitar `--proceso` de las tareas | `test_cada_tarea_manda_su_propio_proceso_al_job` |

La segunda fila de esa tabla **no existía al principio**: la rama `catalogo` no
tenía un solo test, y romperla dejaba la suite en verde. Salió justamente de
hacer esta comprobación.

### Dos trampas de `sys.path` que dan el mismo error

Las dos producen esto, que no menciona su causa por ningún lado:

```
ImportError: cannot import name 'DagBag' from 'airflow.models' (unknown location)
```

La primera: la raíz del repositorio tiene una carpeta `airflow/` **sin
`__init__.py`**, así que lanzar `pytest` desde ahí la importa como *namespace
package* y tapa el paquete instalado. El archivo de tests quita la raíz de
`sys.path` para evitarlo.

La segunda la provocó el propio test: una versión reemplazaba
`sys.modules["airflow.models"]` por un doble y no lo devolvía, de modo que
cualquier test posterior recibía el falso. Se arregló con
`monkeypatch.setitem`, que lo deshace solo. Es la razón de la regla: un test no
ensucia estado global.
