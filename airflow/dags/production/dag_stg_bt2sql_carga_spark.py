"""
STG_BT2SQL_CARGA_SPARK - Bantotal (IBM i) -> Parquet -> STG en SQL Server, con Spark.

GEMELO DE STG_BT2SQL_CARGA. NO LO REEMPLAZA.
--------------------------------------------
Mismas cuatro etapas, mismo contrato, mismas tablas de control. Lo unico que
cambia es quien lee y quien escribe: aqui el core se lee con varias conexiones
en paralelo y la staging se escribe con varios executores.

Los dos DAGs pueden correr el mismo dia sin pisarse:

    STG_BT2SQL_CARGA        /data/bt2sql/<fecha>/<tabla>/<tabla>.parquet
    STG_BT2SQL_CARGA_SPARK  /data/bt2sql/spark/<fecha>/<tabla>/     (carpeta)

y se distinguen en la bitacora por nom_proceso. Esa convivencia es a proposito:
la forma de dar por bueno el pipeline Spark es correr los dos sobre el mismo
dia y comparar fila a fila, no confiar en que la traduccion salio bien.

DONDE GANA SPARK, DICHO SIN ADORNOS
-----------------------------------
Gana en dos sitios y en ninguno mas:

  - la LECTURA del core, si la tabla declara columna de particion. Sin eso,
    Spark lee con UNA conexion y hace el mismo trabajo que pandas arrastrando
    un cluster. El job lo avisa en el log.
  - la ESCRITURA a la staging, que va en paralelo.

El catalogo, la bitacora y el TRUNCATE+INSERT final siguen siendo sentencias
sueltas ejecutadas desde el driver. Mandarlas por el cluster solo agregaria
coordinacion.

QUE HACE FALTA ANTES DE LA PRIMERA CORRIDA
------------------------------------------
  1. La Variable BT2SQL_SPARK, con scripts/sync_variables.py
  2. Las Connections CONEXION_BANTOTAL y CONEXION_SQLSERVER
  3. Los dos jobs en /opt/spark-apps/etl/ (los monta el compose desde spark/jobs)
  4. El volumen spark_data montado, que es el puente para el JSON de runtime
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator

logger = logging.getLogger(__name__)

NOMBRE = "STG_BT2SQL_CARGA_SPARK"
VARIABLE = "BT2SQL_SPARK"

# Puente entre Airflow y Spark. El job no puede leer Variables -no tiene la
# metadata database-, asi que el DAG las materializa aqui y le pasa la ruta.
# El volumen spark_data lo montan los dos lados.
RUNTIME_JSON = "/opt/spark-data/runtime/bt2sql_spark.json"
APPS = "/opt/spark-apps/etl"

POOL_SQLSERVER = os.environ.get("AIRFLOW_POOL_SQLSERVER", "default_pool")

CONN_ORIGEN = "CONEXION_BANTOTAL"
CONN_DESTINO = "CONEXION_SQLSERVER"

VARIABLE_SPARK = "CONFIGURACION_SPARK"


# ============================================================================
# RECURSOS DEL CLUSTER
# ----------------------------------------------------------------------------
# SIN ESTO EL JOB CORRE CON UN SOLO EXECUTOR.
#
# BsgSparkSubmitOperator trae de fabrica executor_memory=1g, executor_cores=1,
# num_executors=1 y cores_max=2. Son defaults prudentes para que algo arranque
# recien instalado, no para procesar el core. Si el operador no recibe estos
# cuatro valores, Spark sale al cluster y usa UN slot: el paralelismo de la
# lectura particionada no se nota porque no hay donde repartirlo.
#
# DRIVER_MEMORY importa aqui mas que en otros jobs: en client mode el driver
# corre dentro del worker de Airflow, y es quien hace el .count() y mantiene
# las conexiones de catalogo y bitacora.
# ============================================================================
RECURSOS_DEFAULT = {
    "EXECUTOR_MEMORY": "1g",
    "EXECUTOR_CORES": 1,
    "NUM_EXECUTORS": 1,
    "CORES_MAX": 2,
    "DRIVER_MEMORY": "1g",
}


def cargar_recursos() -> dict:
    """Lee CONFIGURACION_SPARK en tiempo de parseo.

    Si falta o esta mal, NO tumba el archivo: cae a los defaults y deja un
    WARNING. Ese aviso es importante -la version de este patron en el otro DAG
    se traga la excepcion en silencio, y un recurso que no se aplica se parece
    mucho a un cluster lento.
    """
    from airflow.models import Variable

    try:
        config = Variable.get(VARIABLE_SPARK, deserialize_json=True) or {}
    except Exception as exc:                                    # noqa: BLE001
        logger.warning(
            "No se pudo leer la Variable %s (%s). Se usan los recursos por "
            "defecto: %s. El job correra con UN executor.",
            VARIABLE_SPARK, exc, RECURSOS_DEFAULT)
        return dict(RECURSOS_DEFAULT)

    recursos = {**RECURSOS_DEFAULT, **{k: v for k, v in config.items()
                                       if k in RECURSOS_DEFAULT}}
    if recursos == RECURSOS_DEFAULT:
        logger.warning(
            "%s no define ninguno de %s. El job correra con UN executor.",
            VARIABLE_SPARK, ", ".join(RECURSOS_DEFAULT))
    return recursos


RECURSOS = cargar_recursos()

# ----------------------------------------------------------------------------
# spark.eventLog.enabled DESACTIVADO A PROPOSITO, y es un parche, no un diseno.
#
# spark/config/spark-defaults.conf lo activa apuntando a /opt/spark-events,
# pero el volumen spark_events NO esta montado en ningun servicio del compose
# -ni en los de Airflow ni en los de Spark-, aunque si esta declarado. En
# client mode el driver vive en el worker de Airflow: al construir el
# SparkContext busca ese directorio, no lo encuentra, y el job muere en el
# segundo 1 con un FileNotFoundException que no menciona ni a Airflow ni al
# eventLog.
#
# Desactivarlo aqui deja correr estos dos jobs sin tocar el compose ni el
# spark-defaults.conf, que es lo que se pidio. EL ARREGLO DE VERDAD es montar
# el volumen spark_events en los contenedores que ejecutan el driver y en
# spark-history; cuando eso este hecho, quite estas dos lineas y recupera la
# interfaz del History Server.
# ----------------------------------------------------------------------------
SPARK_CONF = {
    "spark.eventLog.enabled": "false",
    "spark.sql.adaptive.enabled": "true",
}


# ============================================================================
# OPERADOR LOCAL: dos conexiones en un mismo job
# ----------------------------------------------------------------------------
# BsgSparkJdbcOperator inyecta UNA Connection como ORIGEN_USUARIO/ORIGEN_CLAVE.
# La extraccion necesita dos: Bantotal para los datos y SQL Server para el
# catalogo y la bitacora. Esta subclase vive aqui y no toca el plugin, que ya
# esta en produccion sirviendo a otro DAG.
#
# Las credenciales del segundo destino van por VARIABLES DE ENTORNO y no por
# --conf ni por argumento: lo que entra en spark.* aparece en la pestana
# Environment de la interfaz de Spark, en la salida de ps dentro del
# contenedor, y -con spark.eventLog.enabled activo- queda escrito en disco
# para que lo lea el History Server.
# ============================================================================
def _operador_spark(**kwargs):
    """Importa el plugin DENTRO de la funcion.

    A nivel de modulo obligaria al scheduler a resolver el plugin en cada
    pasada del parser, y si el plugin falla se cae el archivo entero con un
    Broken DAG en vez de fallar solo la tarea.
    """
    from operators.spark_operator import BsgSparkJdbcOperator
    return BsgSparkJdbcOperator(**kwargs)


class _SparkDosConexiones:
    """Fabrica del operador de extraccion, que necesita las dos Connections."""

    @staticmethod
    def crear(**kwargs):
        from airflow.hooks.base import BaseHook
        from operators.spark_operator import BsgSparkJdbcOperator
        from utils.spark_config import driver_for_conn_type, jdbc_url

        class ConDestino(BsgSparkJdbcOperator):
            def execute(self, context):
                conn = BaseHook.get_connection(CONN_DESTINO)
                self.env_vars = {
                    **dict(self.env_vars or {}),
                    "DESTINO_JDBC_URL": jdbc_url(conn),
                    "DESTINO_DRIVER_CLASS": driver_for_conn_type(conn.conn_type).driver_class,
                    "DESTINO_USUARIO": conn.login,
                    "DESTINO_CLAVE": conn.password,
                }
                return super().execute(context)

        return ConDestino(**kwargs)


# ============================================================================
# TAREAS DE PYTHON
# ============================================================================
def preparar_config(tipo_ejecucion: str = "diario") -> str:
    """Materializa la Variable en el volumen compartido y devuelve el batch_id.

    Falla aqui, en un segundo, si la Variable no esta: es mucho mas barato que
    descubrirlo cuando el job ya pidio recursos al cluster y murio con un
    FileNotFoundError que no menciona a Airflow.
    """
    import json

    from airflow.models import Variable

    try:
        config = Variable.get(VARIABLE, deserialize_json=True)
    except Exception as exc:                                    # noqa: BLE001
        raise AirflowException(
            f"Falta la Variable {VARIABLE} o no es JSON valido ({exc}). "
            f"Cargala con  python scripts/sync_variables.py --solo {VARIABLE}"
        ) from exc

    for seccion in ("extraccion", "carga"):
        if seccion not in config:
            raise AirflowException(
                f"La Variable {VARIABLE} no tiene la seccion {seccion!r}. "
                f"Debe traer 'extraccion' y 'carga'.")

    config["extraccion"]["tipo_ejecucion"] = tipo_ejecucion

    destino = Path(RUNTIME_JSON)
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("configuracion materializada en %s", RUNTIME_JSON)

    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    logger.info("batch_id de esta corrida: %s", batch_id)
    return batch_id


def verificar_parquet(batch_id: str | None = None) -> bool:
    """Compuerta: hay salida del lote EN DISCO?

    Comprueba CARPETAS y no archivos: Spark escribe un directorio con varios
    part-*.parquet dentro, no un archivo suelto. Esa es la unica diferencia
    real con la compuerta del pipeline de pandas, y es la que haria fallar un
    copiar y pegar de aquella.
    """
    import json

    from airflow.hooks.base import BaseHook

    config = json.loads(Path(RUNTIME_JSON).read_text(encoding="utf-8"))["carga"]
    batch = str(batch_id or "").strip()
    if not batch or batch.lower() in ("none", "null"):
        raise AirflowException(
            "verificar_parquet no recibio batch_id. Sin el no se sabe que "
            "lote mirar.")

    import jaydebeapi  # noqa: F401  -- se usa a traves del helper comun

    from etl_bt2sql.bt2sql_comun import conexion_sqlserver

    conn = conexion_sqlserver({"conn_id_destino": CONN_DESTINO})
    try:
        cur = conn.cursor()
        cur.execute(config["sql_jobs"].replace("{BATCH_ID}", "?"), (batch,))
        jobs = cur.fetchall()
    finally:
        conn.close()

    if not jobs:
        logger.info("No hay archivos registrados para el batch %s. La carga se "
                    "salta: no es un error, es que no habia nada que extraer.", batch)
        return False

    existen, faltan = [], []
    for ruta, destino, *_ in jobs:
        # isdir, no isfile: la salida de Spark es una carpeta.
        (existen if os.path.isdir(str(ruta)) else faltan).append((str(ruta), destino))

    for ruta, destino in existen:
        partes = len([f for f in os.listdir(ruta) if f.endswith(".parquet")])
        logger.info("  OK     %-34s -> %-28s  %s archivo(s)", ruta, destino, partes)
    for ruta, destino in faltan:
        logger.error("  FALTA  %-34s -> %-28s", ruta, destino)

    if not existen:
        raise AirflowException(
            f"La bitacora registra {len(jobs)} salida(s) del batch {batch} pero "
            f"NINGUNA esta en disco. Lo mas habitual es que la carpeta de parquet "
            f"no este montada en los contenedores de Spark, solo en los de Airflow.")
    if faltan:
        raise AirflowException(
            f"Faltan {len(faltan)} de {len(jobs)} salidas del batch {batch}. "
            f"Cargar solo una parte dejaria el dia incompleto sin que nada lo "
            f"advirtiera.")

    logger.info("Las %s salidas del batch estan en disco. Se procede a cargar.", len(existen))
    return True


def limpiar_parquet() -> dict:
    """Borra las carpetas de dias anteriores a la retencion.

    Solo toca carpetas cuyo nombre son ocho digitos y parsean como fecha: asi
    una carpeta puesta a mano no se borra por accidente. Con retencion_dias en
    0 no borra nada.
    """
    import json
    import shutil

    config = json.loads(Path(RUNTIME_JSON).read_text(encoding="utf-8"))["extraccion"]
    raiz = Path(config["output_dir"])
    dias = int(config.get("retencion_dias", 0))
    if dias <= 0 or not raiz.exists():
        logger.info("retencion_dias=%s: no se borra nada", dias)
        return {"borradas": 0}

    corte = datetime.now().date() - timedelta(days=dias)
    borradas = []
    for carpeta in sorted(raiz.iterdir()):
        if not carpeta.is_dir() or not (len(carpeta.name) == 8 and carpeta.name.isdigit()):
            continue
        try:
            fecha = datetime.strptime(carpeta.name, "%Y%m%d").date()
        except ValueError:
            continue
        if fecha < corte:
            shutil.rmtree(carpeta, ignore_errors=True)
            borradas.append(carpeta.name)

    logger.info("retencion %s dias: %s carpeta(s) borradas %s",
                dias, len(borradas), borradas or "")
    return {"borradas": len(borradas), "carpetas": borradas}


def alertar_fallo(context) -> None:
    ti = context.get("task_instance")
    logger.error("FALLO_%s tarea=%s intento=%s run=%s",
                 NOMBRE, getattr(ti, "task_id", "?"),
                 getattr(ti, "try_number", "?"), context.get("run_id"))


# ============================================================================
# EL DAG
# ============================================================================
default_args = {
    "owner": "datahub",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alertar_fallo,
}

with DAG(
    dag_id=NOMBRE,
    description="Gemelo Spark de STG_BT2SQL_CARGA: Bantotal a Parquet y carga a STG",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Con max_active_runs=1 una corrida atascada bloquea todas las siguientes, y
    # execution_timeout no cubre el tiempo en queued esperando un slot del
    # cluster ni el de up_for_retry.
    dagrun_timeout=timedelta(hours=10),
    # Sin esto, "{{ ti.xcom_pull(...) }}" entrega la CADENA 'None' cuando no hay
    # XCom, y un 'if batch_id:' la da por buena.
    render_template_as_native_obj=True,
    default_args=default_args,
    doc_md=__doc__,
    params={
        "tipo_ejecucion": Param(
            default="diario",
            type="string",
            enum=["diario", "reproceso"],
            description="diario usa la fecha del core; reproceso recarga el dia indicado.",
        ),
    },
    tags=["manual", "spark", "bt2sql", "bantotal", "sqlserver", "stg", "produccion"],
) as dag:

    tarea_config = PythonOperator(
        task_id="preparar_config",
        python_callable=preparar_config,
        op_kwargs={"tipo_ejecucion": "{{ params.tipo_ejecucion }}"},
        execution_timeout=timedelta(minutes=5),
        doc_md="Materializa la Variable en el volumen compartido y genera el batch_id.",
    )

    tarea_extraer = _SparkDosConexiones.crear(
        task_id="extraer_parquet_spark",
        jdbc_conn_id=CONN_ORIGEN,
        application=f"{APPS}/bt2sql_extraccion_spark.py",
        application_args=[
            "--config", RUNTIME_JSON,
            "--batch-id", "{{ ti.xcom_pull(task_ids='preparar_config') }}",
            "--tipo-ejecucion", "{{ params.tipo_ejecucion }}",
        ],
        conf=SPARK_CONF,
        executor_memory=RECURSOS["EXECUTOR_MEMORY"],
        executor_cores=int(RECURSOS["EXECUTOR_CORES"]),
        num_executors=int(RECURSOS["NUM_EXECUTORS"]),
        cores_max=int(RECURSOS["CORES_MAX"]),
        driver_memory=RECURSOS["DRIVER_MEMORY"],
        pool=POOL_SQLSERVER,
        execution_timeout=timedelta(hours=4),
        verbose=False,
        doc_md=(
            "Lee el catalogo y la fecha del core, consulta Bantotal en paralelo "
            "cuando la tabla declara particion, y escribe un parquet por tabla."
        ),
    )

    tarea_verificar = ShortCircuitOperator(
        task_id="verificar_parquet",
        python_callable=verificar_parquet,
        op_kwargs={"batch_id": "{{ ti.xcom_pull(task_ids='preparar_config') }}"},
        # Sin esto el corto-circuito salta tambien la limpieza, que lleva
        # trigger_rule="all_done" justamente para correr siempre.
        ignore_downstream_trigger_rules=False,
        pool=POOL_SQLSERVER,
        execution_timeout=timedelta(minutes=15),
        doc_md="Compuerta: si no hay salida en disco, la carga queda en SKIPPED, no en failed.",
    )

    tarea_cargar = _operador_spark(
        task_id="cargar_stg_spark",
        jdbc_conn_id=CONN_DESTINO,
        application=f"{APPS}/bt2sql_carga_spark.py",
        application_args=[
            "--config", RUNTIME_JSON,
            "--batch-id", "{{ ti.xcom_pull(task_ids='preparar_config') }}",
        ],
        conf=SPARK_CONF,
        executor_memory=RECURSOS["EXECUTOR_MEMORY"],
        executor_cores=int(RECURSOS["EXECUTOR_CORES"]),
        num_executors=int(RECURSOS["NUM_EXECUTORS"]),
        cores_max=int(RECURSOS["CORES_MAX"]),
        driver_memory=RECURSOS["DRIVER_MEMORY"],
        pool=POOL_SQLSERVER,
        execution_timeout=timedelta(hours=4),
        verbose=False,
        doc_md=(
            "Staging con los tipos de la destino, escritura en paralelo, y "
            "TRUNCATE+INSERT en una transaccion desde el driver."
        ),
    )

    tarea_limpiar = PythonOperator(
        task_id="limpiar_parquet",
        python_callable=limpiar_parquet,
        # all_done: la limpieza corre aunque la carga falle. Si no, los parquet
        # de un dia fallido se quedan para siempre y el disco crece sin tope.
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=30),
        doc_md="Borra las carpetas anteriores a retencion_dias. Corre siempre.",
    )

    tarea_config >> tarea_extraer >> tarea_verificar >> tarea_cargar >> tarea_limpiar
