"""
23 — Spark: extraccion masiva a Parquet, y el mito del SP en Spark

=============================================================================
LO PRIMERO, PORQUE AHORRA DIAS DE PELEA:

    SPARK NO PUEDE EJECUTAR UN PROCEDIMIENTO ALMACENADO.

El lector JDBC de Spark toma la opcion `dbtable` y la mete literalmente en:

    SELECT * FROM <dbtable>

Ademas, antes de leer nada, Spark necesita inferir el esquema, y para eso llama
a prepareStatement().getMetaData(). Un SP no devuelve metadatos fiables por esa
via. Por eso fallan todos los intentos de poner "EXEC dbo.mi_sp" en dbtable.

Circulan trucos tipo:
    .option("dbtable", "(SET NOCOUNT ON; EXEC dbo.mi_sp) AS t")
No lo hagas. Funciona a veces, con ciertos drivers y ciertos SPs, y se rompe en
produccion sin aviso al cambiar cualquier cosa.

EL PATRON CORRECTO son tres pasos, y es el que implementa este DAG:

    1. Airflow ejecuta el SP contra SQL Server  (pymssql, ejemplo 21)
    2. El SP deja su resultado en una tabla fisica o de staging
    3. Spark lee ESA TABLA en paralelo y la escribe a Parquet

Es mas codigo, pero cada paso es observable y reintentable por separado.
=============================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

log = logging.getLogger(__name__)

CONN_ORIGEN = "sqlserver_core"
TABLA_STAGING = "dbo.stg_movimientos_diarios"
RUTA_PARQUET = "/opt/spark-data/parquet/movimientos"


# =============================================================================
# PASO 1 — El SP se ejecuta desde Airflow y materializa a una tabla
# =============================================================================

def paso1_ejecutar_sp_a_staging(**context):
    """El SP escribe su resultado en una tabla que Spark si puede leer."""
    hook = MsSqlHook(mssql_conn_id=CONN_ORIGEN)
    fecha = context["ds"]

    sql = f"""
        SET NOCOUNT ON;

        -- Idempotencia: borrar lo de esta fecha antes de reinsertar.
        -- Sin esto, un reintento duplica los datos.
        DELETE FROM {TABLA_STAGING} WHERE fecha_proceso = %s;

        INSERT INTO {TABLA_STAGING}
        EXEC dbo.sp_movimientos_consolidados @fecha = %s;
    """
    hook.run(sql, parameters=(fecha, fecha), autocommit=True)

    # INSERT INTO ... EXEC funciona solo si la tabla destino tiene EXACTAMENTE
    # las mismas columnas, en el mismo orden y con tipos compatibles, que las
    # que devuelve el SP. Si no coinciden, SQL Server da un error poco claro
    # sobre "column name or number of supplied values does not match".

    total = hook.get_first(
        f"SELECT COUNT(*) FROM {TABLA_STAGING} WHERE fecha_proceso = %s",
        parameters=(fecha,),
    )[0]

    if total == 0:
        raise ValueError(f"El SP no dejo filas para {fecha}. Se aborta.")

    log.info("Staging poblado con %d filas", total)
    return {"filas": total}


# =============================================================================
# PASO 2 — Calcular los limites de particion para la lectura paralela
# =============================================================================

def paso2_calcular_limites(**context):
    """Spark necesita lowerBound y upperBound reales para repartir bien.

    Si los inventas (p.ej. 0 a 1.000.000 cuando los ids van de 900.000 a
    950.000), casi todas las particiones quedan vacias y una sola hace el
    trabajo. El paralelismo se pierde sin que ningun error lo indique.
    """
    hook = MsSqlHook(mssql_conn_id=CONN_ORIGEN)
    fecha = context["ds"]

    minimo, maximo, total = hook.get_first(
        f"SELECT MIN(id), MAX(id), COUNT(*) FROM {TABLA_STAGING} "
        f"WHERE fecha_proceso = %s",
        parameters=(fecha,),
    )

    # Una particion por cada ~250k filas, con techo de 32 para no saturar la
    # base de origen: cada particion es una conexion concurrente, y a un SQL
    # Server de produccion no le hace gracia recibir 200 de golpe.
    particiones = max(1, min(32, (total // 250_000) + 1))

    log.info("id entre %s y %s, %s filas -> %s particiones",
             minimo, maximo, total, particiones)

    ti = context["task_instance"]
    ti.xcom_push(key="limite_inferior", value=int(minimo))
    ti.xcom_push(key="limite_superior", value=int(maximo) + 1)  # upperBound es exclusivo
    ti.xcom_push(key="particiones", value=particiones)

    return {"min": minimo, "max": maximo, "particiones": particiones}


# =============================================================================
# PASO 3 — Preparar el entorno del spark-submit
# =============================================================================

def paso3_preparar_entorno(**context):
    """Arma la URL JDBC desde la Connection y deja las credenciales listas.

    Las credenciales van por variables de entorno hacia spark-submit, nunca
    como argumentos (serian visibles en `ps` y en la UI de Spark).
    """
    conn = BaseHook.get_connection(CONN_ORIGEN)

    jdbc_url = (
        f"jdbc:sqlserver://{conn.host}:{conn.port or 1433};"
        f"databaseName={conn.schema};"
        f"encrypt=true;trustServerCertificate=true;loginTimeout=30"
    )

    ti = context["task_instance"]
    ti.xcom_push(key="jdbc_url", value=jdbc_url)
    # La contrasena tambien va por XCom, que esta cifrado con la Fernet key en
    # la base de datos. No es ideal; lo limpio seria un secrets backend y que
    # el job de Spark lo consulte directamente. Para eso sirve Vault.
    ti.xcom_push(key="usuario", value=conn.login)
    ti.xcom_push(key="clave", value=conn.password)

    log.info("JDBC preparado para %s", conn.host)


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="ej23_spark_jdbc_parquet",
    description="SP -> staging -> Spark en paralelo -> Parquet",
    default_args=default_args,
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ejemplo", "spark", "parquet"],
) as dag:

    sp_a_staging = PythonOperator(
        task_id="paso1_sp_a_staging",
        python_callable=paso1_ejecutar_sp_a_staging,
    )

    limites = PythonOperator(
        task_id="paso2_calcular_limites",
        python_callable=paso2_calcular_limites,
    )

    entorno = PythonOperator(
        task_id="paso3_preparar_entorno",
        python_callable=paso3_preparar_entorno,
    )

    # -------------------------------------------------------------------------
    # El spark-submit propiamente dicho.
    #
    # Requiere:
    #   - spark-submit en el worker de Airflow -> lo instala el Dockerfile
    #   - Connection "spark_default" de tipo Spark, host spark://spark-master,
    #     puerto 7077
    #   - el jar del driver JDBC, que --jars sube a los executors
    # -------------------------------------------------------------------------
    extraer_a_parquet = SparkSubmitOperator(
        task_id="paso4_spark_a_parquet",
        conn_id="spark_default",
        application="/opt/spark-apps/etl/jdbc_a_parquet.py",
        name="jdbc_a_parquet_{{ ds_nodash }}",

        jars="/opt/airflow/jars/mssql-jdbc.jar",

        # Dimensionamiento. Regla que evita el 90% de los problemas:
        # executor_memory debe ser MENOR que SPARK_WORKER_MEMORY del .env,
        # dejando ~1 GB de margen para overhead de la JVM. Si pides mas de lo
        # que el worker anuncia, el master nunca coloca el executor y la
        # aplicacion se queda en WAITING para siempre, sin error.
        executor_memory="1500m",
        executor_cores=2,
        num_executors=2,
        driver_memory="1g",

        conf={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.shuffle.partitions": "16",
            # Techo de cores para esta aplicacion. Sin esto, la primera app que
            # llega toma TODOS los cores del cluster y las siguientes esperan.
            # En un cluster compartido siempre hay que fijarlo.
            "spark.cores.max": "4",
        },

        env_vars={
            "ORIGEN_USUARIO": "{{ ti.xcom_pull(task_ids='paso3_preparar_entorno', key='usuario') }}",
            "ORIGEN_CLAVE": "{{ ti.xcom_pull(task_ids='paso3_preparar_entorno', key='clave') }}",
        },

        application_args=[
            "--jdbc-url", "{{ ti.xcom_pull(task_ids='paso3_preparar_entorno', key='jdbc_url') }}",
            "--driver-class", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "--tabla", TABLA_STAGING,
            "--ruta-salida", RUTA_PARQUET,
            "--fecha", "{{ ds }}",
            "--columna-particion", "id",
            "--limite-inferior", "{{ ti.xcom_pull(task_ids='paso2_calcular_limites', key='limite_inferior') }}",
            "--limite-superior", "{{ ti.xcom_pull(task_ids='paso2_calcular_limites', key='limite_superior') }}",
            "--particiones", "{{ ti.xcom_pull(task_ids='paso2_calcular_limites', key='particiones') }}",
        ],
        verbose=False,
    )

    sp_a_staging >> limites >> entorno >> extraer_a_parquet


# =============================================================================
# LEER EL PARQUET DESPUES
# =============================================================================
#
#   df = spark.read.parquet("/opt/spark-data/parquet/movimientos")
#
#   # Partition pruning: Spark solo abre la carpeta anio=2026/mes=8
#   df = df.filter((F.col("anio") == 2026) & (F.col("mes") == 8))
#
#   # Column pruning: Parquet es columnar, solo lee las columnas pedidas
#   df.select("cuenta_id", "importe")
#
# Esas dos cosas juntas son la razon de usar Parquet y no CSV. Un CSV obliga a
# leer y parsear el archivo entero para sacar dos columnas de un mes.
#
# Desde pandas, sin Spark, para volumenes que quepan en memoria:
#   import pandas as pd
#   df = pd.read_parquet(ruta, columns=["cuenta_id", "importe"],
#                        filters=[("anio", "=", 2026)])
