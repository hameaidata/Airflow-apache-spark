"""
BT2SQL_STG - Bantotal (IBM i) -> Parquet -> STG en SQL Server.

    extraer_parquet  ->  verificar_parquet  ->  cargar_stg  ->  limpiar_parquet

Misma estructura que etl_bt_parquet_singlestore: las dos mitades estan
separadas y se comunican por la bitacora, no por rutas fijas en el codigo.

verificar_parquet es una compuerta: mira los archivos del lote EN DISCO y, si
no hay ninguno, deja cargar_stg en SKIPPED en vez de fallar. La razon concreta
queda escrita en el log de esa tarea.

DONDE VIVEN LOS ARCHIVOS
------------------------
Una carpeta por DIA y, dentro, una por TABLA:

    /data/bt2sql/20260926/FSR011/FSR011.parquet
                 ^^^^^^^^ ^^^^^^
                 dia      tabla

La carpeta del dia se borra entera cuando vence la retencion. La carpeta por
tabla deja sitio para partir un parquet en varios archivos el dia que una
tabla no quepa comoda en uno solo, sin cambiar rutas ni tocar lo ya guardado.

La ruta del host se define en .env y la de dentro del contenedor no cambia:

    BT2SQL_PARQUET_HOST_DIR=./data/bt2sql
    BT2SQL_PARQUET_CONTAINER_DIR=/data/bt2sql

EL RASTRO DE AUDITORIA
----------------------
Tres tablas en SQL Server (GNBPE_DATAHUB), con los mismos nombres que ya usa
el pipeline de SingleStore, asi que las consultas de operacion sirven para los
dos:

    CTL_PARAMETROS_PARQUET   que tablas se extraen, con que columnas y filtro
    ctl_proceso_parquet      una fila POR TABLA Y POR CORRIDA: cuando empezo,
                             cuando termino, cuantas filas, que archivo, que
                             error
    ctl_carga_stg            lo mismo para la carga

De ctl_proceso_parquet sale ademas la ruta completa que la carga abre despues.

EL ORIGEN
---------
Bantotal preproduccion, por JDBC con JTOpen, con las mismas propiedades que el
script test_bt_preproduccion.py que ya esta probado. El servidor y las
credenciales NO estan aqui: salen de la Connection CONEXION_BANTOTAL, cifradas
con el Fernet key.

DESPUES DE ESTE DAG
-------------------
Este DAG llega hasta STG. El paso de STG a ODS y de ODS a BDS lo hacen los
stored procedures del DataHub, orquestados aparte, igual que BT_DATAHUB hace
hoy con SingleStore.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ShortCircuitOperator


logger = logging.getLogger(__name__)

# etl_bt2sql/ vive junto a este archivo. Airflow pone dags/ en sys.path, pero
# no dags/production/. Se usa append y NO insert(0): insertar al principio pone
# esta carpeta por delante de la biblioteca estandar, y el dia que alguien cree
# aqui un modulo llamado json.py o logging.py se rompe el interprete entero.
DIR_ACTUAL = os.path.dirname(os.path.abspath(__file__))
if DIR_ACTUAL not in sys.path:
    sys.path.append(DIR_ACTUAL)

# Limita cuantas tareas golpean SQL Server a la vez. Por defecto default_pool
# para que el DAG funcione tal cual; cuando cree el Pool en Admin > Pools, pon
# en .env:  AIRFLOW_POOL_SQLSERVER=sqlserver
POOL_SQLSERVER = os.environ.get("AIRFLOW_POOL_SQLSERVER", "default_pool")


# ============================================================================
# CALLABLES
# ============================================================================
# jaydebeapi, pyarrow y pandas se importan DENTRO de cada funcion. Si se
# importaran arriba, el proceso que parsea los DAGs cargaria las tres cada 30
# segundos para dibujar un grafo de cuatro tareas. Con jaydebeapi la razon pesa
# el doble: arrastra JPype, que levanta una JVM.
#
# Efecto lateral util: un driver que falte en un worker hace fallar esa tarea
# con su traza en su log, en vez de tumbar el archivo entero con un Broken DAG.
# ============================================================================
def extraer_parquet(tipo_ejecucion: str = "diario"):
    from etl_bt2sql.bt2sql_extraccion import ejecutar_extraccion

    return ejecutar_extraccion(tipo_ejecucion=tipo_ejecucion)


def verificar_parquet(batch_id: str | None = None) -> bool:
    from etl_bt2sql.bt2sql_carga import verificar_parquet as _verificar

    return _verificar(batch_id=batch_id)


def cargar_stg(batch_id: str | None = None):
    from etl_bt2sql.bt2sql_carga import ejecutar_carga

    return ejecutar_carga(batch_id=batch_id)


def limpiar_parquet():
    from etl_bt2sql.bt2sql_extraccion import limpiar_parquet as _limpiar

    return _limpiar()


def alertar_fallo(context) -> None:
    """Una linea grepeable por cada tarea que falla.

    Airflow ya escribe la traza dentro del log de ESA tarea. Esta linea sale en
    el log del scheduler con un prefijo fijo, asi que un 'grep FALLO_BT2SQL'
    contesta "que se rompio anoche" sin abrir la interfaz. Es tambien el punto
    unico donde enganchar correo o Teams mas adelante.
    """
    ti = context.get("task_instance")
    logger.error(
        "FALLO_BT2SQL dag=%s tarea=%s intento=%s/%s run=%s error=%s",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        getattr(ti, "try_number", "?"),
        getattr(ti, "max_tries", "?"),
        context.get("run_id", "?"),
        context.get("exception"),
    )


# ============================================================================
# REINTENTOS
# ============================================================================
# Un reintento solo sirve si el fallo puede ser pasajero: un corte de red, el
# core reiniciandose. Los de este pipeline casi nunca lo son (una biblioteca
# que no existe, una columna renombrada, un permiso que falta), y reintentar
# tres veces una extraccion larga contra el core es molestarlo tres veces para
# llegar al mismo error.
# ============================================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alertar_fallo,
}

with DAG(
    dag_id="BT2SQL_STG",
    description="Bantotal (IBM i) a Parquet y carga a STG en SQL Server",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Con max_active_runs=1, una corrida atascada bloquea TODAS las siguientes.
    # execution_timeout protege cada tarea, pero no el tiempo que pasa en
    # queued esperando un slot del pool ni en up_for_retry.
    dagrun_timeout=timedelta(hours=10),
    # Sin esto, "{{ ti.xcom_pull(...) }}" entrega la CADENA 'None' cuando no hay
    # XCom, y un 'if batch_id:' la da por buena: la carga consultaria
    # WHERE batch_id = 'None', no encontraria nada, y el respaldo de "toma la
    # ultima extraccion TERMINADA" nunca se ejecutaria. Ya costo un fallo
    # silencioso en el pipeline de SingleStore.
    render_template_as_native_obj=True,
    default_args=default_args,
    doc_md=__doc__,
    params={
        "tipo_ejecucion": Param(
            "diario",
            type="string",
            enum=["diario", "semanal", "mensual"],
            title="Tipo de ejecucion",
            description=(
                "Que flag de la Variable BT2SQL_EXTRACCION decide las tablas de "
                "la corrida: estado_diario, estado_semanal o estado_mensual."
            ),
        ),
    },
    tags=["bt2sql", "bantotal", "sqlserver", "stg", "produccion"],
) as dag:

    # execution_timeout es obligatorio: sin el, una consulta trabada contra el
    # core o contra SQL Server deja la tarea en running indefinidamente y
    # bloquea el DAG entero, porque max_active_runs=1.
    tarea_extraer = PythonOperator(
        task_id="extraer_parquet",
        python_callable=extraer_parquet,
        op_kwargs={"tipo_ejecucion": "{{ params.tipo_ejecucion }}"},
        pool=POOL_SQLSERVER,
        retries=1,
        execution_timeout=timedelta(hours=4),
        doc_md=(
            "Lee `CTL_PARAMETROS_PARQUET`, extrae de Bantotal las tablas activas "
            "segun `params.tipo_ejecucion`, y escribe un parquet por tabla en "
            "`/data/bt2sql/<fecha>/<TABLA>/`.\n\n"
            "Registra **una fila por tabla y por corrida** en "
            "`ctl_proceso_parquet`, con la ruta completa del archivo.\n\n"
            "Devuelve el `batch_id`, que las siguientes tareas usan para saber "
            "que archivos tocar."
        ),
    )

    # Compuerta: mira los archivos del lote en disco y decide si hay algo que
    # cargar.
    #
    #   hay archivos  -> True  -> cargar_stg se EJECUTA
    #   no hay        -> False -> cargar_stg queda en SKIPPED (rosa), no failed
    #
    # ignore_downstream_trigger_rules=False es importante: sin eso el corto
    # circuito saltaria TODO lo que viene despues, incluida la limpieza, que
    # tiene trigger_rule="all_done" justamente para correr siempre.
    #
    # retries=0: esto es un ls sobre una carpeta montada y un SELECT. Si falla,
    # es porque el montaje no esta o la base no responde, y ninguna de las dos
    # se arregla sola en cinco minutos.
    tarea_verificar = ShortCircuitOperator(
        task_id="verificar_parquet",
        python_callable=verificar_parquet,
        op_kwargs={"batch_id": "{{ ti.xcom_pull(task_ids='extraer_parquet') }}"},
        ignore_downstream_trigger_rules=False,
        pool=POOL_SQLSERVER,
        retries=0,
        execution_timeout=timedelta(minutes=15),
        doc_md=(
            "Comprueba que los parquet del lote existan **en disco**, no solo en "
            "`ctl_proceso_parquet`.\n\n"
            "| Situacion | Resultado |\n"
            "|---|---|\n"
            "| Estan todos | la carga se ejecuta |\n"
            "| No hay ninguno registrado | la carga queda en **skipped**, no falla |\n"
            "| Faltan algunos en disco | se cancela: cargar solo una parte dejaria "
            "el dia incompleto sin que nada lo advirtiera |\n\n"
            "Mirar el disco y no solo la bitacora es lo que detecta que la carpeta "
            "externa no este montada en el worker que tomo la tarea."
        ),
    )

    # El batch_id sale del XCom de la extraccion. Si esta tarea se ejecuta sola
    # (clear de una sola tarea), llega None -de verdad, ver
    # render_template_as_native_obj arriba- y la carga toma la ultima
    # extraccion TERMINADA de cada tabla, avisandolo en el log.
    tarea_cargar = PythonOperator(
        task_id="cargar_stg",
        python_callable=cargar_stg,
        op_kwargs={"batch_id": "{{ ti.xcom_pull(task_ids='extraer_parquet') }}"},
        pool=POOL_SQLSERVER,
        retries=1,
        execution_timeout=timedelta(hours=4),
        doc_md=(
            "Lee de `ctl_proceso_parquet` la ruta de los parquet del lote, la cruza "
            "con `CTL_PARAMETROS_PARQUET` para saber la tabla STG destino, y carga.\n\n"
            "Cada tabla pasa por una **staging** y se aplica con TRUNCATE + INSERT "
            "en una sola transaccion: la tabla destino nunca se ve a medias, y el "
            "TRUNCATE ocurre cuando los datos nuevos ya estan en la base.\n\n"
            "Una tabla que falla no impide que las demas se carguen; al final la "
            "tarea falla si hubo errores, para que la corrida no salga verde con "
            "tablas sin cargar."
        ),
    )

    # Corre aunque falle la carga: si no, las carpetas viejas se acumulan justo
    # los dias con problemas, que es cuando menos falta hace quedarse sin disco.
    #
    # No lleva pool: solo toca disco, no abre conexiones, y no tiene por que
    # competir por los slots con las tareas que si las abren.
    tarea_limpiar = PythonOperator(
        task_id="limpiar_parquet",
        python_callable=limpiar_parquet,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "Borra las carpetas `/data/bt2sql/<yyyyMMdd>/` mas viejas que "
            "`retencion_dias` de la Variable `BT2SQL_EXTRACCION`.\n\n"
            "Solo toca carpetas cuyo nombre son 8 digitos que forman una fecha: "
            "cualquier otra cosa dentro del directorio se deja intacta. Con "
            "`retencion_dias: 0` no borra nada."
        ),
    )

    tarea_extraer >> tarea_verificar >> tarea_cargar >> tarea_limpiar
