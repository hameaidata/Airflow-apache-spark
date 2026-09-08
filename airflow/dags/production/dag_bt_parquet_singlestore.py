from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator



CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, CURRENT_DIR)

from etl.carga_parquet import ejecutar_carga
from etl.extraccion_parquet import ejecutar_extraccion

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="etl_bt_parquet_singlestore",
    description=(
            "Carga masiva de archivos parquet "
            "hacia SingleStore"
        ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "etl",
        "parquet",
        "singlestore",
        "data-engineering",
        "produccion"
    ]
) as dag:

    t1 = PythonOperator(
        task_id="extraer_parquet",
        python_callable=ejecutar_extraccion
    )

    t2 = PythonOperator(
        task_id="cargar_singlestore",
        python_callable=ejecutar_carga
    )

    t1 >> t2