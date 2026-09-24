"""
etl_bt_parquet_singlestore - Extraccion a Parquet y carga a STG.

    extraer_parquet  ->  verificar_parquet  ->  cargar_singlestore  ->  limpiar_parquet

verificar_parquet es una compuerta: mira la carpeta del dia y, si no hay
archivos, deja cargar_singlestore en SKIPPED en vez de fallar. La razon
concreta queda en el log de esa tarea.

DONDE VIVEN LOS ARCHIVOS
------------------------
Los parquet se escriben en una carpeta FUERA del contenedor, montada como
bind mount. La ruta del host se define en .env, con una linea para Windows y
otra para Red Hat (se activa comentando/descomentando):

    PARQUET_HOST_DIR=D:/datahub/parquet        <- Windows
    #PARQUET_HOST_DIR=/datos/datahub/parquet   <- Red Hat

    PARQUET_CONTAINER_DIR=/data/parquet        <- igual en ambos

Dentro del contenedor la ruta siempre es Linux (/data/parquet), y es esa la
que se guarda en la base y la que abren los procesos. Cambiar de plataforma
no afecta ni al codigo ni a los datos.

Estructura:

    /data/parquet/20260923/FSH005.parquet
                  ^^^^^^^^  carpeta por fecha de proceso (no por reloj)

COMO SE ENLAZAN LAS DOS MITADES
-------------------------------
La extraccion guarda la RUTA COMPLETA de cada archivo en
CTL_PROCESO_PARQUET.archivo_parquet, junto con el batch_id de la corrida.
La carga lee esa tabla (unida a ETL_CONFIG, que dice a que tabla STG va cada
origen) y sube desde ahi. La tabla es el punto de encuentro: no hay rutas
fijas en ningun lado.

El batch_id viaja por XCom de la primera tarea a la segunda, para que la
carga suba exactamente lo que se acaba de extraer. Sin eso, si la extraccion
de hoy falla, la carga podria recargar los archivos de ayer.

MODOS DE EJECUCION
------------------
El parametro tipo_ejecucion (diario / semanal / mensual) decide que flag de
CTL_PARAMETROS_PARQUET se mira para saber que tablas entran en la corrida.
Se elige al lanzar el DAG a mano, en "Trigger DAG w/ config".
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


# ============================================================================
# RESOLUCION DE LOS MODULOS DEL PIPELINE
# ============================================================================
# extraccion_parquet.py y carga_parquet.py viven en etl/, al lado de este
# archivo. Airflow pone dags/ en sys.path, pero no dags/production/, asi que
# hay que anadirlo.
#
# Se usa append y NO insert(0) a proposito. Insertar al principio pone esta
# carpeta por DELANTE de la biblioteca estandar: el dia que alguien cree aqui
# un modulo llamado json.py, logging.py o types.py, se rompe el interprete
# entero y el error no se parecera en nada a la causa. Con append, los
# nombres del proyecto solo se resuelven cuando nadie mas los reclama.
# ============================================================================
DIR_ACTUAL = os.path.dirname(os.path.abspath(__file__))
if DIR_ACTUAL not in sys.path:
    sys.path.append(DIR_ACTUAL)


# ============================================================================
# POOL
# ============================================================================
# Limita cuantas tareas golpean SingleStore a la vez, en este DAG y en el
# resto. Sin pool, un backfill de BT_DATAHUB y este DAG pueden abrir todas
# las conexiones que quieran y tumbar la base.
#
# Por defecto queda en default_pool para que el DAG funcione tal cual. Cuando
# crees el Pool en Admin > Pools (4 slots es un punto de partida razonable),
# pon en .env:
#
#     AIRFLOW_POOL_SINGLESTORE=singlestore
#
# Se lee de una variable de ENTORNO y no de una Variable de Airflow a
# proposito: esto se evalua en cada parseo del archivo (cada 30 s) y una
# Variable de Airflow seria una consulta a la metadata database cada vez.
# ============================================================================
POOL_SINGLESTORE = os.environ.get("AIRFLOW_POOL_SINGLESTORE", "default_pool")


# ============================================================================
# IMPORTS PEREZOSOS
# ============================================================================
# etl/extraccion_parquet.py y etl/carga_parquet.py importan pandas, pyarrow y
# singlestoredb en su primer nivel. Si este DAG los importara arriba, el
# proceso que parsea los DAGs cargaria esas tres librerias CADA 30 SEGUNDOS,
# solo para construir un grafo de cuatro tareas. Es la causa numero uno de
# schedulers lentos.
#
# Importando dentro de la funcion, el coste se paga una vez, en el worker,
# cuando la tarea de verdad se ejecuta.
#
# Efecto lateral util: un ImportError del pipeline (falta ibm_db, por ejemplo)
# ya no rompe el archivo entero con un Broken DAG. El DAG se sigue viendo en
# la interfaz y falla la tarea concreta, con su traza en el log de la tarea,
# que es donde se busca.
# ============================================================================
def extraer_parquet(tipo_ejecucion: str = "diario"):
    from etl.extraccion_parquet import ejecutar_extraccion

    return ejecutar_extraccion(tipo_ejecucion=tipo_ejecucion)


def verificar_parquet(batch_id: str | None = None) -> bool:
    from etl.carga_parquet import verificar_parquet as _verificar

    return _verificar(batch_id=batch_id)


def cargar_singlestore(batch_id: str | None = None):
    from etl.carga_parquet import ejecutar_carga

    return ejecutar_carga(batch_id=batch_id)


def limpiar_parquet():
    from etl.extraccion_parquet import limpiar_parquet as _limpiar

    return _limpiar()


def alertar_fallo(context) -> None:
    """Deja una linea unica y grepeable por cada tarea que falla.

    Airflow ya escribe la traza, pero dentro del log de ESA tarea: para
    enterarte tienes que ir a buscarla. Esta linea sale en el log del
    scheduler con un prefijo fijo, asi que un 'grep FALLO_ETL' sobre los logs
    del stack contesta "que se rompio anoche" sin abrir la interfaz.

    Es ademas el punto de enganche: si manana se quiere avisar por correo o
    por Teams, se hace aqui y lo heredan las cuatro tareas.
    """
    ti = context.get("task_instance")
    logger.error(
        "FALLO_ETL dag=%s tarea=%s intento=%s/%s run=%s error=%s",
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
# El valor de default_args aplica a las tareas baratas. Las caras lo bajan.
#
# Un reintento solo tiene sentido si el fallo puede ser pasajero: un corte de
# red, la base reiniciandose. Los de este pipeline casi nunca lo son (tabla
# que no existe, credencial vencida, disco lleno), y reintentar una extraccion
# de 4 horas tres veces son 12 horas de cluster ocupado para llegar al mismo
# error, con el agravante de que cada intento abre un batch_id nuevo y deja
# otra fila en ERROR en CTL_PROCESO_PARQUET.
# ============================================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alertar_fallo,
}

with DAG(
    dag_id="etl_bt_parquet_singlestore",
    description="Extraccion a Parquet en carpeta externa y carga a STG",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Con max_active_runs=1, una corrida atascada bloquea TODAS las siguientes.
    # execution_timeout protege cada tarea por separado, pero no cubre el
    # tiempo que una tarea pasa en queued o up_for_retry. dagrun_timeout es el
    # tope duro de la corrida completa: 4 h + 4 h + margen.
    dagrun_timeout=timedelta(hours=10),
    # ------------------------------------------------------------------
    # CORRIGE UN BUG REAL, no es cosmetico.
    #
    # op_kwargs se renderiza con Jinja, y Jinja convierte todo a texto. Cuando
    # "{{ ti.xcom_pull(task_ids='extraer_parquet') }}" no encuentra XCom,
    # devuelve None, que Jinja entrega como la CADENA 'None'.
    #
    # Esa cadena es truthy. El "if batch_id:" de obtener_jobs() la daba por
    # buena y consultaba  WHERE batch_id = 'None', que no devuelve nada, en
    # lugar de caer al respaldo de "toma la ultima extraccion TERMINADA".
    # Resultado: al limpiar solo cargar_singlestore para reintentar una carga,
    # no cargaba nada y el log no decia por que.
    #
    # Con render_template_as_native_obj el None llega como None de Python y el
    # respaldo funciona. Comprobado con las dos variantes en Airflow real.
    # ------------------------------------------------------------------
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
                "Que flag de CTL_PARAMETROS_PARQUET decide las tablas de la "
                "corrida: estado_diario, estado_semanal o estado_mensual."
            ),
        ),
    },
    tags=["etl", "parquet", "singlestore", "data-engineering", "produccion"],
) as dag:

    # execution_timeout es obligatorio: sin el, una consulta trabada contra BT
    # o SingleStore deja la tarea en running indefinidamente y bloquea el DAG
    # entero, porque max_active_runs=1.
    tarea_extraer = PythonOperator(
        task_id="extraer_parquet",
        python_callable=extraer_parquet,
        op_kwargs={"tipo_ejecucion": "{{ params.tipo_ejecucion }}"},
        pool=POOL_SINGLESTORE,
        retries=1,
        execution_timeout=timedelta(hours=4),
        doc_md=(
            "Lee las tablas activas de CTL_PARAMETROS_PARQUET segun "
            "`params.tipo_ejecucion`, escribe un parquet por tabla en "
            "`/data/parquet/<fecha>/` y registra la **ruta completa** de cada "
            "archivo en `CTL_PROCESO_PARQUET`.\n\n"
            "Devuelve el `batch_id` de la corrida, que las siguientes tareas "
            "usan para saber que archivos tocar."
        ),
    )

    # Compuerta: mira la carpeta del dia y decide si hay algo que cargar.
    #
    #   hay archivos  -> devuelve True  -> cargar_singlestore se EJECUTA
    #   no hay        -> devuelve False -> cargar_singlestore queda en SKIPPED
    #                                      (rosa en la UI), no en failed
    #
    # Se ve como una tarea propia en el grafo, asi que de un vistazo se sabe
    # si la corrida cargo algo o no. El motivo queda escrito en su log.
    #
    # ignore_downstream_trigger_rules=False es importante: sin eso el corto
    # circuito saltaria TODO lo que viene despues, incluida la limpieza, que
    # tiene trigger_rule="all_done" justamente para correr siempre.
    #
    # retries=0: esto es un ls sobre una carpeta montada y un SELECT. Si falla,
    # es porque el montaje no esta o la base no responde, y ninguna de las dos
    # se arregla sola en cinco minutos. Reintentar solo retrasa el diagnostico.
    tarea_verificar = ShortCircuitOperator(
        task_id="verificar_parquet",
        python_callable=verificar_parquet,
        op_kwargs={"batch_id": "{{ ti.xcom_pull(task_ids='extraer_parquet') }}"},
        ignore_downstream_trigger_rules=False,
        pool=POOL_SINGLESTORE,
        retries=0,
        execution_timeout=timedelta(minutes=15),
        doc_md=(
            "Comprueba que los parquet del batch existan **en disco**, no solo "
            "en `CTL_PROCESO_PARQUET`.\n\n"
            "| Situacion | Resultado |\n"
            "|---|---|\n"
            "| Hay archivos | la carga se ejecuta |\n"
            "| No hay ninguno | la carga queda en **skipped**, no falla |\n"
            "| Faltan algunos | se cancela, salvo `cargar_parcial: true` |\n\n"
            "Tambien detecta que la carpeta externa no este montada en el worker."
        ),
    )

    # El batch_id sale del XCom de la tarea anterior. Si esta tarea se ejecuta
    # sola (clear de una sola tarea), llega None -de verdad, ver
    # render_template_as_native_obj arriba- y la carga toma la ultima
    # extraccion TERMINADA de cada tabla, avisandolo en el log.
    tarea_cargar = PythonOperator(
        task_id="cargar_singlestore",
        python_callable=cargar_singlestore,
        op_kwargs={"batch_id": "{{ ti.xcom_pull(task_ids='extraer_parquet') }}"},
        pool=POOL_SINGLESTORE,
        retries=1,
        execution_timeout=timedelta(hours=4),
        doc_md=(
            "Lee de `CTL_PROCESO_PARQUET` la ruta de los parquet del batch, la "
            "cruza con `ETL_CONFIG` para saber la tabla destino, y carga a STG.\n\n"
            "Valida que el archivo exista **antes** de truncar la tabla destino."
        ),
    )

    # Corre aunque falle la carga: si no, las carpetas viejas se acumulan
    # justamente los dias con problemas, que es cuando menos falta hace
    # quedarse sin disco.
    #
    # No lleva pool: solo toca disco, no abre conexiones a SingleStore, y no
    # tiene por que competir por los slots con las tareas que si las abren.
    tarea_limpiar = PythonOperator(
        task_id="limpiar_parquet",
        python_callable=limpiar_parquet,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=30),
        doc_md=(
            "Borra las carpetas `/data/parquet/<fecha>/` mas viejas que "
            "`retencion_dias` (Variable `EXTRACCION_BT_STG`).\n\n"
            "Solo toca carpetas con nombre `yyyyMMdd`: cualquier otra cosa dentro "
            "de la carpeta se deja intacta. Con `retencion_dias: 0` no borra nada."
        ),
    )

    tarea_extraer >> tarea_verificar >> tarea_cargar >> tarea_limpiar
