"""
etl_bt_parquet_singlestore_spark - Version Spark del pipeline Parquet.

    preparar_config_spark -> extraer_parquet_spark -> cargar_singlestore_spark

Hace lo mismo que etl_bt_parquet_singlestore pero delegando el trabajo pesado
a un cluster Spark en vez de a pandas dentro del worker de Airflow.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime, timedelta

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


# ============================================================================
# IMPORTACION DEL OPERADOR PROPIO
# ============================================================================
# Sintoma que corrige este bloque:
#
#     Broken DAG: [/opt/airflow/dags/production/dag_bt_parquet_singlestore_spark.py]
#     ModuleNotFoundError: No module named 'operators.spark_operator'
#
# El operador vive en airflow/plugins/operators/spark_operator.py, y el archivo
# esta ahi: el modulo se importa sin problemas en cuanto la carpeta plugins/
# esta en sys.path. Airflow normalmente la anade el solo
# (settings.prepare_syspath), y por eso el "from operators.spark_operator ..."
# pelado funcionaba. Depender de eso es fragil por tres motivos:
#
#   1. "operators" es un nombre de primer nivel demasiado generico. Cualquier
#      paquete instalado que se llame igual lo tapa, y como la carpeta plugins/
#      se ANADE al final de sys.path, el intruso gana. El mensaje en ese caso
#      es exactamente el de arriba: "operators" se encuentra, pero
#      spark_operator no esta dentro. Ese matiz es la pista: si la carpeta
#      simplemente no estuviera en sys.path, el error diria
#      "No module named 'operators'", sin el sufijo.
#
#   2. prepare_syspath() solo corre en procesos que inicializan Airflow por
#      completo. Un proceso que parsea el archivo sin esa inicializacion no ve
#      la carpeta.
#
#   3. Si el bind mount ./airflow/plugins no llega a algun contenedor, ahi la
#      carpeta esta vacia y el import falla solo en ese servicio, de forma
#      intermitente segun quien parsee el archivo.
#
# El DAG hermano (dag_bt_parquet_singlestore.py) nunca se rompio porque resuelve
# su ruta el mismo con sys.path.insert. Aqui se hace lo mismo y, ademas, si el
# nombre "operators" estuviera ocupado por otro paquete, se carga el archivo
# por RUTA ABSOLUTA, que no puede ser tapado por nadie.
# ============================================================================

PLUGINS_FOLDER = conf.get("core", "plugins_folder", fallback="/opt/airflow/plugins")

if PLUGINS_FOLDER and PLUGINS_FOLDER not in sys.path:
    sys.path.append(PLUGINS_FOLDER)


def _cargar_operador_spark():
    """Devuelve BsgSparkJdbcOperator, venga de donde venga.

    Primero el import normal. Si falla, carga el archivo directamente de
    plugins/operators/spark_operator.py. Si tampoco esta, lanza un error que
    dice QUE se busco y DONDE, en vez del ModuleNotFoundError pelado que
    obliga a adivinar.
    """
    try:
        from operators.spark_operator import BsgSparkJdbcOperator
        return BsgSparkJdbcOperator
    except ImportError as exc_import:
        ruta = os.path.join(PLUGINS_FOLDER, "operators", "spark_operator.py")
        if not os.path.isfile(ruta):
            raise AirflowException(
                f"No se encuentra el operador Spark del proyecto.\n"
                f"  Import fallido : from operators.spark_operator import BsgSparkJdbcOperator\n"
                f"  Motivo         : {exc_import}\n"
                f"  Archivo buscado: {ruta} (NO EXISTE)\n"
                f"  plugins_folder : {PLUGINS_FOLDER}\n"
                f"Revisa que el docker-compose monte ./airflow/plugins en "
                f"{PLUGINS_FOLDER} para ESTE servicio."
            ) from exc_import

        # El archivo existe pero el nombre "operators" apunta a otro sitio.
        # Se carga por ruta, registrandolo en sys.modules con un nombre propio
        # para no pelearse con el paquete que ocupa "operators".
        spec = importlib.util.spec_from_file_location("bsg_spark_operator", ruta)
        modulo = importlib.util.module_from_spec(spec)
        # Registrar ANTES de ejecutar: sin esto, las dataclasses y cualquier
        # referencia al propio modulo durante la ejecucion fallan.
        sys.modules[spec.name] = modulo
        spec.loader.exec_module(modulo)
        return modulo.BsgSparkJdbcOperator


BsgSparkJdbcOperator = _cargar_operador_spark()


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
    doc_md=__doc__,
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
