from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from operators.spark_operator import BsgSparkJdbcOperator


SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"
CONFIG_RUNTIME_PATH = "/opt/spark-data/runtime/bt_parquet_singlestore_spark.json"
VARIABLE_SPARK = "CONFIGURACION_SPARK"

# ============================================================================
# RECURSOS
# ============================================================================
# Salen de la Variable CONFIGURACION_SPARK para no tener numeros de
# infraestructura incrustados en el DAG. Los defaults de abajo son los que
# caben en el worker por defecto de docker-compose (2 cores / 2G):
#
#     1 executor x (1024m + 384m de overhead) = 1408m <= 2048m   OK
#     cores_max = 2                                   <= 2       OK
#
# La explicacion completa y la tabla de escalado estan en el propio JSON.
# Si se piden mas recursos de los que hay, Spark NO falla: deja el job en
# WAITING hasta que lo mata execution_timeout.
# ============================================================================
RECURSOS_DEFAULT = {
    "EXECUTOR_MEMORY": "1g",
    "EXECUTOR_CORES": 1,
    "NUM_EXECUTORS": 1,
    "CORES_MAX": 2,
    "DRIVER_MEMORY": "1g",
    "NUM_PARTITIONS": 2,
}


def cargar_configuracion_spark() -> tuple[dict, dict]:
    """Lee CONFIGURACION_SPARK en tiempo de parseo, con defaults seguros."""
    try:
        config = Variable.get(VARIABLE_SPARK, deserialize_json=True) or {}
    except Exception:
        # Sin la Variable el DAG igual se construye con los defaults, para que
        # una Variable faltante no tumbe el archivo entero.
        return dict(RECURSOS_DEFAULT), {}

    recursos = {**RECURSOS_DEFAULT, **(config.get("RECURSOS") or {})}
    # Las claves que empiezan con _ son documentacion dentro del JSON.
    conf = {k: str(v) for k, v in (config.get("CONF") or {}).items() if not k.startswith("_")}
    return recursos, conf


RECURSOS, SPARK_CONF = cargar_configuracion_spark()


def preparar_config_spark() -> str:
    """Materializa las Variables de Airflow en un JSON que lee Spark.

    Airflow y Spark corren en contenedores distintos: Spark no puede leer las
    Variables porque no tiene la metadata database. El puente es el volumen
    compartido spark_data, montado como /opt/spark-data en ambos lados.

    Falla explicitamente si falta una Variable. Sin esto, el job arranca y
    muere con un KeyError dentro del executor, que es mucho mas dificil de
    diagnosticar desde la UI de Airflow.
    """
    config = {}
    faltantes = []
    for clave, variable in (("extraccion", "EXTRACCION_BT_STG"),
                            ("carga", "CARGAR_PARQUET_CONFIG")):
        try:
            config[clave] = Variable.get(variable, deserialize_json=True)
        except Exception as exc:
            faltantes.append(f"{variable} ({exc})")

    if faltantes:
        raise AirflowException(
            "Faltan Variables de Airflow requeridas por este DAG: "
            + "; ".join(faltantes)
            + ". Cargalas con scripts/sync_variables.py o en Admin > Variables."
        )

    os.makedirs(os.path.dirname(CONFIG_RUNTIME_PATH), exist_ok=True)
    with open(CONFIG_RUNTIME_PATH, "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)

    # El contenedor de Spark corre con otro usuario que el de Airflow: sin
    # esto, spark-submit falla con PermissionError al leer el JSON.
    os.chmod(CONFIG_RUNTIME_PATH, 0o644)
    return CONFIG_RUNTIME_PATH


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def argumentos_comunes() -> list[str]:
    return [
        "--config-path", CONFIG_RUNTIME_PATH,
        "--num-partitions", str(RECURSOS["NUM_PARTITIONS"]),
        "--host-name", "airflow_spark_{{ ds_nodash }}",
    ]


with DAG(
    dag_id="etl_bt_parquet_singlestore_spark",
    description="Version Spark de extraccion Parquet y carga hacia SingleStore",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "parquet", "singlestore", "spark", "produccion"],
) as dag:

    preparar_config = PythonOperator(
        task_id="preparar_config_spark",
        python_callable=preparar_config_spark,
        execution_timeout=timedelta(minutes=5),
    )

    extraer_parquet_spark = BsgSparkJdbcOperator(
        task_id="extraer_parquet_spark",
        jdbc_conn_id=SINGLESTORE_CONN_ID,
        application="etl/bt_extraccion_parquet_spark.py",
        name="bt_extraccion_parquet_{{ ds_nodash }}",
        application_args=["--tipo-ejecucion", "diario", *argumentos_comunes()],
        conf=SPARK_CONF,
        executor_memory=RECURSOS["EXECUTOR_MEMORY"],
        executor_cores=int(RECURSOS["EXECUTOR_CORES"]),
        num_executors=int(RECURSOS["NUM_EXECUTORS"]),
        cores_max=int(RECURSOS["CORES_MAX"]),
        execution_timeout=timedelta(hours=4),
        verbose=False,
    )

    cargar_singlestore_spark = BsgSparkJdbcOperator(
        task_id="cargar_singlestore_spark",
        jdbc_conn_id=SINGLESTORE_CONN_ID,
        application="etl/bt_carga_parquet_spark.py",
        name="bt_carga_singlestore_{{ ds_nodash }}",
        application_args=argumentos_comunes(),
        conf=SPARK_CONF,
        executor_memory=RECURSOS["EXECUTOR_MEMORY"],
        executor_cores=int(RECURSOS["EXECUTOR_CORES"]),
        num_executors=int(RECURSOS["NUM_EXECUTORS"]),
        cores_max=int(RECURSOS["CORES_MAX"]),
        execution_timeout=timedelta(hours=4),
        verbose=False,
    )

    preparar_config >> extraer_parquet_spark >> cargar_singlestore_spark
