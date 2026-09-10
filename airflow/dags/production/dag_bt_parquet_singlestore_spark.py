from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from operators.spark_operator import BsgSparkJdbcOperator


SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"
CONFIG_RUNTIME_PATH = "/opt/spark-data/runtime/bt_parquet_singlestore_spark.json"


def preparar_config_spark() -> str:
    """Materializa las Variables de Airflow para que las lea Spark."""
    config = {
        "extraccion": Variable.get("EXTRACCION_BT_STG", deserialize_json=True),
        "carga": Variable.get("CARGAR_PARQUET_CONFIG", deserialize_json=True),
    }

    os.makedirs(os.path.dirname(CONFIG_RUNTIME_PATH), exist_ok=True)
    with open(CONFIG_RUNTIME_PATH, "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)
    return CONFIG_RUNTIME_PATH


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


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
        application_args=[
            "--config-path",
            CONFIG_RUNTIME_PATH,
            "--tipo-ejecucion",
            "diario",
            "--host-name",
            "airflow_spark_{{ ds_nodash }}",
        ],
        executor_memory="1500m",
        executor_cores=1,
        num_executors=2,
        cores_max=4,
        execution_timeout=timedelta(hours=4),
        verbose=False,
    )

    cargar_singlestore_spark = BsgSparkJdbcOperator(
        task_id="cargar_singlestore_spark",
        jdbc_conn_id=SINGLESTORE_CONN_ID,
        application="etl/bt_carga_parquet_spark.py",
        name="bt_carga_singlestore_{{ ds_nodash }}",
        application_args=[
            "--config-path",
            CONFIG_RUNTIME_PATH,
            "--host-name",
            "airflow_spark_{{ ds_nodash }}",
        ],
        executor_memory="1500m",
        executor_cores=1,
        num_executors=2,
        cores_max=4,
        execution_timeout=timedelta(hours=4),
        verbose=False,
    )

    preparar_config >> extraer_parquet_spark >> cargar_singlestore_spark
