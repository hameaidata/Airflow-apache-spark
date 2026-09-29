# Plugin Spark para DAGs reutilizables

Este proyecto usa Airflow como orquestador y Spark como ejecutor de trabajos
pesados. La regla de diseño es simple:

- Airflow decide que corre, cuando corre y con que dependencias.
- Spark ejecuta lectura, transformacion y escritura distribuida.
- Las credenciales viven en Connections de Airflow, no en los DAGs.
- Los DAGs no repiten rutas de jars, scripts ni configuracion base de Spark.

## Piezas creadas

`airflow/plugins/utils/spark_config.py`

Centraliza:

- `spark_default`
- `/opt/spark-apps`
- `/opt/airflow/jars`
- drivers JDBC por `conn_type`
- construccion de URL JDBC desde una Connection
- configuracion base de Spark

`airflow/plugins/operators/spark_operator.py`

Expone dos operadores:

- `BsgSparkSubmitOperator`: wrapper general de `SparkSubmitOperator`.
- `BsgSparkJdbcOperator`: wrapper para jobs Spark que necesitan conectarse por
  JDBC usando una Connection de Airflow.

Ambos quedan registrados en `Admin -> Plugins` a traves de
`airflow/plugins/bsg_plugin.py`.

## Uso recomendado en un DAG de produccion

```python
from operators.spark_operator import BsgSparkJdbcOperator

t_spark = BsgSparkJdbcOperator(
    task_id="spark_a_parquet",
    jdbc_conn_id="sqlserver_core",
    application="etl/jdbc_a_parquet.py",
    name="jdbc_a_parquet_{{ ds_nodash }}",
    application_args=[
        "--tabla", "dbo.stg_movimientos_diarios",
        "--ruta-salida", "/opt/spark-data/parquet/movimientos",
        "--fecha", "{{ ds }}",
        "--columna-particion", "id",
        "--limite-inferior", "{{ ti.xcom_pull(task_ids='limites', key='lo') }}",
        "--limite-superior", "{{ ti.xcom_pull(task_ids='limites', key='hi') }}",
        "--particiones", "8",
    ],
    executor_memory="1500m",
    executor_cores=2,
    num_executors=2,
    cores_max=4,
)
```

El operador agrega automaticamente:

- `--jdbc-url`
- `--driver-class`
- `--jars` con el driver correcto
- `ORIGEN_USUARIO`
- `ORIGEN_CLAVE`
- configuracion base de Spark

## Motores soportados inicialmente

| Airflow `conn_type` | Driver JDBC | Jar |
|---|---|---|
| `postgres`, `postgresql` | `org.postgresql.Driver` | `postgresql-jdbc.jar` |
| `mssql`, `odbc` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `mssql-jdbc.jar` |
| `jdbc` | `com.ibm.db2.jcc.DB2Driver` | `db2-jcc.jar` |
| `oracle` | `oracle.jdbc.OracleDriver` | `ojdbc.jar` |
| `singlestore` | `com.singlestore.jdbc.Driver` | `singlestore-jdbc.jar` |
| `mysql` | `com.mysql.cj.jdbc.Driver` | `mysql-jdbc.jar` |

Para anadir otro motor, agregue una entrada en `JDBC_DRIVERS` y una rama en
`jdbc_url()`.

## DAGs de produccion ya conectados al plugin

- `airflow/dags/production/orquestador_json.py`

El cambio importante en `orquestador_json` es que cada paso Spark ahora usa su
propia clave `"conexion"` del manifiesto, en vez de compartir una credencial
global por XCom.

## Verificacion despues de reiniciar Airflow

Los plugins se cargan al arrancar. Despues de cambiar estos archivos:

```powershell
docker compose -f docker-compose.windows.yml restart airflow-webserver airflow-scheduler
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow dags list-import-errors
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow dags test orquestador_json 2026-09-08
```
