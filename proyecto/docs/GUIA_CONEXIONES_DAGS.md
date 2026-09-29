# Guía: credenciales, bases de datos, SPs, Parquet y auditoría

Cinco DAGs de ejemplo en `airflow/dags/examples/`, todos con sintaxis validada.
Están pensados para leerse en orden.

| Archivo | Qué resuelve |
|---|---|
| `20_credenciales_variables_conexiones.py` | Variables vs Connections, y el error que degrada el scheduler |
| `21_sqlserver_stored_procedures.py` | SQL Server: SPs con y sin resultado, OUTPUT, transacciones |
| `22_db2_ibm_jdbc.py` | DB2 vía JDBC, carga incremental con marca de agua |
| `23_spark_jdbc_parquet.py` | Extracción masiva en paralelo → Parquet |
| `24_auditoria_logs_a_base_datos.py` | Auditoría de negocio en base de datos |

---

## Antes de nada: la imagen

La imagen oficial de Airflow **no trae** `spark-submit`, ni drivers JDBC, ni
`pymssql`. Sin eso, los ejemplos 21 a 23 no arrancan. El `Dockerfile` incluido
añade las tres cosas:

```powershell
docker build -t airflow-bsg:2.11.2 .
```

Luego descomenta en tu `.env`:

```ini
AIRFLOW_IMAGE=airflow-bsg:2.11.2
```

y `docker compose ... up -d`.

Subí la versión base a **Airflow 2.11.2** (la última 2.x) porque los providers
actuales de SQL Server, JDBC y Spark exigen `apache-airflow>=2.11.0`. Lo
verifiqué leyendo los metadatos de los paquetes en PyPI. Con 2.10.5 habría que
quedarse en providers de hace más de un año.

Dos avisos sobre el build, que no pude ejecutar desde aquí:

- Las URLs de los drivers JDBC en Maven Central no las pude comprobar (sin
  salida a ese host). Si alguna da 404, la versión correcta está en
  `central.sonatype.com`, y los `ARG` del Dockerfile la dejan cambiar sin tocar
  el resto.
- `ibm-db` descarga el driver nativo de IBM al instalarse, y esa descarga suele
  estar bloqueada en redes corporativas. El build **tolera ese fallo** a
  propósito: DB2 por JDBC no lo necesita.

---

## 1. Credenciales: la corrección más importante

Preguntaste cómo instanciar las credenciales que configuras "del lado de
variables". Ahí hay que cambiar el enfoque:

> **Variables = configuración. Connections = credenciales.**

Ambas se cifran con la Fernet key. Pero las Variables:

- se ven en texto plano en `Admin > Variables` para cualquiera con permiso de lectura
- aparecen en los logs cuando Jinja las renderiza
- no tienen host/puerto/esquema, así que acabas parseando strings a mano

Airflow enmascara el valor de una Variable en los logs **solo** si su nombre
contiene `password`, `secret`, `passwd`, `authorization`, `api_key`, `apikey` o
`token`. Una Variable llamada `usuario_bd_produccion` no se enmascara nunca.

Para banca: credenciales en Connections. Y en producción, en un secrets backend
(Vault), donde ni siquiera tocan la base de datos de Airflow.

### El error que degrada el scheduler

```python
# NUNCA así — nivel superior del archivo
RUTA = Variable.get("ruta_datos")

dag = DAG(...)
```

El scheduler re-parsea cada archivo de `dags/` cada 30 segundos. Esa línea se
ejecuta en **cada parseo**. Con 50 DAGs son 100 consultas por minuto a la base
de datos solo para leer configuración. Y si la Variable no existe, el archivo
entero falla al importarse y el DAG desaparece de la UI sin mensaje visible.

Las tres formas correctas están en el ejemplo 20: dentro de la tarea, con
plantilla Jinja (`{{ var.value.x }}`, la más barata), o desde una Connection.

### Crear una Connection

```bash
docker compose exec airflow-scheduler airflow connections add sqlserver_core \
  --conn-type mssql --conn-host 10.20.30.40 --conn-port 1433 \
  --conn-login usuario_etl --conn-password 'LaClave' \
  --conn-schema BD_NEGOCIO \
  --conn-extra '{"encrypt":"yes","trustServerCertificate":"yes"}'

# probarla sin escribir un DAG:
docker compose exec airflow-scheduler airflow connections test sqlserver_core
```

También por variable de entorno (`AIRFLOW_CONN_SQLSERVER_CORE=mssql://...`),
que no toca la base de datos y va bien para CI.

---

## 2. SQL Server y procedimientos almacenados

El ejemplo 21 cubre los cuatro casos. Dos detalles que ahorran horas:

**`SET NOCOUNT ON` al principio de cada SP.** Sin eso, los mensajes "N rows
affected" viajan como conjuntos de resultados vacíos y `pymssql` te devuelve
`None` antes del resultado real. Es la causa de la mayoría de los "mi SP
funciona en SSMS pero no en Airflow".

**Los parámetros OUTPUT necesitan bajar al cursor.** `hook.run()` no los expone.
Se declaran variables T-SQL, se llama al SP y se hace `SELECT` de ellas — más
portable que `callproc()`.

Para transacciones de varias sentencias, `conn.autocommit(False)` y
`commit`/`rollback` explícitos. Sin eso cada sentencia se confirma sola y un
fallo a mitad deja la base inconsistente.

---

## 3. DB2 de IBM

Dos caminos: JDBC (recomendado) o `ibm_db` nativo. JDBC no depende de librerías
de IBM y es el mismo driver que usará Spark.

**El detalle que hace fallar a todo el mundo:** desde el provider JDBC 4.0.0,
poner `driver_path` o `driver_class` en el extra de la Connection está
deshabilitado por defecto (fue un vector de ejecución remota de código). Hay que
activarlo:

```yaml
AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA: "true"
AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA: "true"
```

Ya las añadí a los dos compose. Sin ellas el hook ignora el driver en silencio y
falla con un "driver not found" que no dice el motivo real.

**Segunda confusión frecuente:** en una Connection de tipo JDBC, el campo *host*
es la **URL JDBC completa** (`jdbc:db2://servidor:50000/BD`), no el nombre del
servidor.

Y la sintaxis de SP en DB2 es `CALL`, no `EXEC`.

---

## 4. Spark y el mito del SP

Esto responde directamente a tu pregunta de "cómo paso esos SP a Spark":

> **Spark no puede ejecutar un procedimiento almacenado.**

El lector JDBC toma la opción `dbtable` y la mete literalmente en
`SELECT * FROM <dbtable>`. Además, antes de leer necesita inferir el esquema vía
`prepareStatement().getMetaData()`, y un SP no devuelve metadatos fiables por esa
vía.

Circulan trucos tipo `.option("dbtable", "(SET NOCOUNT ON; EXEC sp) AS t")`.
Funcionan a veces, con ciertos drivers y ciertos SPs, y se rompen en producción
sin aviso. No los uses.

**El patrón correcto son tres pasos**, y es lo que implementa el ejemplo 23:

1. Airflow ejecuta el SP (pymssql) y lo materializa en una tabla de staging
2. Airflow calcula los límites reales de partición (`MIN(id)`, `MAX(id)`)
3. Spark lee esa tabla en paralelo y escribe Parquet

Más código, pero cada paso es observable y reintentable por separado.

### Lo que decide el rendimiento

Sin `partitionColumn`, Spark abre **una sola conexión** y trae toda la tabla por
un hilo. Da igual cuántos workers tengas. Es el error número uno con Spark +
JDBC.

```python
.option("partitionColumn", "id")
.option("lowerBound", minimo)      # el MIN real, no inventado
.option("upperBound", maximo + 1)
.option("numPartitions", 8)
.option("fetchsize", 10000)        # el default de muchos drivers es 10
```

Los límites tienen que ser **reales**. Si pones 0 a 1.000.000 cuando los ids van
de 900.000 a 950.000, casi todas las particiones quedan vacías y una sola hace
el trabajo — sin que ningún error lo indique.

### Dimensionar el executor

`executor_memory` debe ser **menor** que `SPARK_WORKER_MEMORY`, dejando ~1 GB de
margen para la JVM. Si pides más de lo que el worker anuncia, el master nunca
coloca el executor y la aplicación se queda en `WAITING` para siempre, sin
error visible.

Y fija `spark.cores.max`: sin eso, la primera aplicación que llega toma todos
los cores del cluster y las siguientes esperan.

---

## 5. Parquet

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(df.write
   .mode("overwrite")
   .partitionBy("anio", "mes")
   .option("compression", "snappy")
   .parquet(ruta))
```

Genera `ruta/anio=2026/mes=8/parte-0000.snappy.parquet`. Al filtrar por
`anio`/`mes` al leer, Spark salta las carpetas que no aplican (*partition
pruning*). Junto con el *column pruning* del formato columnar, es la diferencia
entre leer 2 GB y 2 TB.

**Cuidado con la granularidad.** Particionar por día genera miles de carpetas
con archivos diminutos y degrada más de lo que ayuda. Apunta a archivos de
128 MB a 1 GB.

`partitionOverwriteMode=dynamic` hace que `overwrite` reescriba solo las
particiones presentes en el DataFrame, en vez de borrar el directorio entero.
Sin eso, reprocesar un día borra el histórico completo.

---

## 6. Logs y auditoría — otra corrección

Preguntaste cómo mandar los logs a una base de datos. Hay que separar tres cosas:

| | Qué es | Estado |
|---|---|---|
| **Logs de tarea** | la salida de tu código | Airflow los guarda como **archivos** |
| **Auditoría de Airflow** | quién disparó qué DAG, quién editó una Connection | ya existe: tabla `log`, en `Browse > Audit Logs` |
| **Auditoría de negocio** | "el proceso X cargó N filas a las Z" | **no existe, la construyes tú** |

**Airflow no sabe escribir los logs de tarea en una base de datos.** El remote
logging soporta S3, GCS, Azure Blob, Alibaba OSS y Elasticsearch. No hay handler
de base de datos, y escribir uno propio es mala idea: cada línea sería un
INSERT, y un DAG hablador tumbaría la base.

Lo que sí se hace es el tercer punto: una tabla de auditoría con **eventos de
negocio**, no líneas de log. El ejemplo 24 trae el DDL, la función de registro y
los `on_failure_callback` / `on_success_callback` que la alimentan solos.

Dos decisiones de diseño ahí:

- **Base separada de la metastore de Airflow.** `airflow db migrate` puede tocar
  objetos en una actualización, compites por conexiones con el scheduler, y un
  auditor querrá una base cuyo retention controles tú.
- **El fallo al auditar no tumba el proceso** (try/except). La decisión
  contraria también es defendible en banca; si esa es tu política, quita el
  try/except. Está marcado en el código.

### Algo que morderá a este stack

Los workers de Celery son efímeros. Si escalas a la baja, **el contenedor
desaparece y sus logs con él**. La UI mostrará "log file not found" para tareas
que sí se ejecutaron — un hallazgo de auditoría en un entorno regulado.

La solución es remote logging:

```ini
AIRFLOW__LOGGING__REMOTE_LOGGING=True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://mi-bucket/airflow-logs
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=s3_logs
```

No lo dejé configurado porque necesita un bucket real. Es lo primero que
añadiría antes de que esto vea datos de verdad.

---

## Lo que falta para producción

Por orden de urgencia:

1. **Remote logging** — sin esto pierdes logs al escalar (arriba)
2. **TLS** — todo va en claro ahora mismo: la UI, Postgres, el RPC de Spark
3. **Secrets backend (Vault)** — para que las credenciales dejen de vivir en la
   metastore; también evita el paso por XCom del ejemplo 23
4. **Retention** — `airflow db clean` con archivado previo; en banca hay que
   conservar el rastro N años
5. **Los módulos de Python** (security, resilience, governance) que aún no
   existen en disco

Los ejemplos usan `PythonOperator` en vez de operadores propios a propósito:
funcionan sin ningún módulo adicional. Cuando reconstruyamos los módulos, se
refactorizan encima.
