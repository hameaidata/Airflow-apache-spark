from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

from operators.spark_operator import BsgSparkJdbcOperator


SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"
CONFIG_RUNTIME_PATH = "/opt/spark-data/runtime/bt_parquet_singlestore_spark.json"


def preparar_config_spark() -> str:
    """
    Materializa las Variables de Airflow para que las lea Spark.

    MEJORADO: Incluye validaciones robustas de:
    - Existencia de variables en Airflow
    - Estructura JSON esperada
    - Campos requeridos en cada sección
    - Creación segura de directorios
    """
    try:
        # Cargar variables de Airflow
        extraccion_config = Variable.get("EXTRACCION_BT_STG", deserialize_json=True)
        carga_config = Variable.get("CARGAR_PARQUET_CONFIG", deserialize_json=True)
    except Exception as exc:
        raise AirflowException(
            f"Fallo cargar variables de Airflow (EXTRACCION_BT_STG, CARGAR_PARQUET_CONFIG): {exc}"
        ) from exc

    # Validación: no pueden estar vacios
    if not extraccion_config:
        raise AirflowException(
            "Variable EXTRACCION_BT_STG esta vacia o no es deserializable como JSON"
        )

    if not carga_config:
        raise AirflowException(
            "Variable CARGAR_PARQUET_CONFIG esta vacia o no es deserializable como JSON"
        )

    # Validación: estructura esperada
    if not isinstance(extraccion_config, dict):
        raise AirflowException(
            f"EXTRACCION_BT_STG debe ser objeto JSON, recibido: {type(extraccion_config).__name__}"
        )

    if not isinstance(carga_config, dict):
        raise AirflowException(
            f"CARGAR_PARQUET_CONFIG debe ser objeto JSON, recibido: {type(carga_config).__name__}"
        )

    # Validación: campos requeridos en carga
    campos_requeridos_carga = ["tabla_control", "sql_jobs", "estado_iniciado",
                                "estado_ejecutado", "estado_finalizado", "estado_error"]
    for campo in campos_requeridos_carga:
        if campo not in carga_config or not carga_config[campo]:
            raise AirflowException(
                f"CARGAR_PARQUET_CONFIG falta o tiene vacio el campo requerido: '{campo}'"
            )

    # Log de configuración (sin exponer secretos)
    print("[config] Validacion OK:")
    print(f"  extraccion: {list(extraccion_config.keys())}")
    print(f"  carga: {list(carga_config.keys())}")

    # Construir configuración final
    config = {
        "extraccion": extraccion_config,
        "carga": carga_config,
    }

    # Crear directorio de runtime (seguro)
    config_dir = os.path.dirname(CONFIG_RUNTIME_PATH)
    try:
        os.makedirs(config_dir, exist_ok=True)
        os.chmod(config_dir, 0o777)  # Permisos para que spark pueda leer
    except Exception as exc:
        raise AirflowException(
            f"Fallo crear directorio de config {config_dir}: {exc}"
        ) from exc

    # Escribir archivo de configuración
    try:
        with open(CONFIG_RUNTIME_PATH, "w", encoding="utf-8") as fh:
            json.dump(config, fh, ensure_ascii=False, indent=2)

        # Hacer archivo legible por todos (spark corre como usuario distinto)
        os.chmod(CONFIG_RUNTIME_PATH, 0o644)

    except Exception as exc:
        raise AirflowException(
            f"Fallo escribir archivo de config {CONFIG_RUNTIME_PATH}: {exc}"
        ) from exc

    print(f"[config] Archivo de configuracion creado: {CONFIG_RUNTIME_PATH}")
    return CONFIG_RUNTIME_PATH


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="etl_bt_parquet_singlestore_spark",
    description="Version Spark de extraccion Parquet y carga hacia SingleStore. Incluye validaciones robustas.",
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
        doc="""
        Prepara la configuración runtime para los jobs de Spark.

        Lee las Variables de Airflow:
        - EXTRACCION_BT_STG (JSON con config de extracción)
        - CARGAR_PARQUET_CONFIG (JSON con config de carga)

        Valida estructura y campos requeridos.
        Crea archivo /opt/spark-data/runtime/bt_parquet_singlestore_spark.json

        **Falla si:**
        - Las variables no existen o están vacías
        - No son JSON válido
        - Falta algún campo requerido
        - No se puede crear el directorio o escribir el archivo
        """,
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
            "--num-partitions",
            "4",
            "--host-name",
            "airflow_spark_{{ ds_nodash }}",
        ],
        executor_memory="1500m",
        executor_cores=1,
        num_executors=2,
        cores_max=4,
        execution_timeout=timedelta(hours=4),
        verbose=False,
        doc="""
        Extrae datos desde SingleStore hacia Parquet.

        Lee la configuración en EXTRACCION_BT_STG (generada por preparar_config_spark).
        Ejecuta el job Spark en spark/jobs/etl/bt_extraccion_parquet_spark.py

        Parametros:
        - config-path: Ruta del archivo JSON de configuración
        - tipo-ejecucion: "diario" (versión manual)
        - num-partitions: Numero de particiones de Spark
        - host-name: Identificador de ejecución (para logging)
        """,
    )

    cargar_singlestore_spark = BsgSparkJdbcOperator(
        task_id="cargar_singlestore_spark",
        jdbc_conn_id=SINGLESTORE_CONN_ID,
        application="etl/bt_carga_parquet_spark.py",
        name="bt_carga_singlestore_{{ ds_nodash }}",
        application_args=[
            "--config-path",
            CONFIG_RUNTIME_PATH,
            "--num-partitions",
            "4",
            "--host-name",
            "airflow_spark_{{ ds_nodash }}",
        ],
        executor_memory="1500m",
        executor_cores=1,
        num_executors=2,
        cores_max=4,
        execution_timeout=timedelta(hours=4),
        verbose=False,
        doc="""
        Carga datos desde Parquet hacia SingleStore.

        Lee la configuración en CARGAR_PARQUET_CONFIG (generada por preparar_config_spark).
        Ejecuta el job Spark en spark/jobs/etl/bt_carga_parquet_spark.py

        El job trunca la tabla destino y carga nuevos datos.
        Registra inicio, fin y errores en tabla de control.

        Parametros:
        - config-path: Ruta del archivo JSON de configuración
        - num-partitions: Numero de particiones de Spark
        - host-name: Identificador de ejecución (para logging)
        """,
    )

    # Definir el flujo: secuencial
    # 1. Preparar config
    # 2. Extraer datos a Parquet
    # 3. Cargar desde Parquet a SingleStore
    preparar_config >> extraer_parquet_spark >> cargar_singlestore_spark
