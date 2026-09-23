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
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, CURRENT_DIR)

from etl.carga_parquet import ejecutar_carga, verificar_parquet
from etl.extraccion_parquet import ejecutar_extraccion, limpiar_parquet

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_bt_parquet_singlestore",
    description="Extraccion a Parquet en carpeta externa y carga a STG",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=__doc__,
    tags=["etl", "parquet", "singlestore", "data-engineering", "produccion"],
) as dag:

    # execution_timeout es obligatorio: sin el, una consulta trabada contra BT
    # o SingleStore deja la tarea en running indefinidamente y bloquea el DAG
    # entero, porque max_active_runs=1.
    extraer_parquet = PythonOperator(
        task_id="extraer_parquet",
        python_callable=ejecutar_extraccion,
        execution_timeout=timedelta(hours=4),
        doc_md=(
            "Lee las tablas activas de CTL_PARAMETROS_PARQUET, escribe un parquet "
            "por tabla en `/data/parquet/<fecha>/` y registra la **ruta completa** "
            "de cada archivo en `CTL_PROCESO_PARQUET`.\n\n"
            "Devuelve el `batch_id` de la corrida, que la siguiente tarea usa "
            "para saber que archivos cargar."
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
    verificar = ShortCircuitOperator(
        task_id="verificar_parquet",
        python_callable=verificar_parquet,
        op_kwargs={"batch_id": "{{ ti.xcom_pull(task_ids='extraer_parquet') }}"},
        ignore_downstream_trigger_rules=False,
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
    # sola (clear de una sola tarea), llega None y la carga toma la ultima
    # extraccion TERMINADA de cada tabla, avisandolo en el log.
    cargar_singlestore = PythonOperator(
        task_id="cargar_singlestore",
        python_callable=ejecutar_carga,
        op_kwargs={
            "batch_id": "{{ ti.xcom_pull(task_ids='extraer_parquet') }}",
        },
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
    limpiar = PythonOperator(
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

    extraer_parquet >> verificar >> cargar_singlestore >> limpiar
