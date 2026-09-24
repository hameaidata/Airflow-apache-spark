"""
S2SQL_EXPORT - Exporta tablas de SingleStore a SQL Server 2022.

                                  +-- tabla.extraer -> tabla.cargar --+
    inicio -> preparar_lote ------+-- tabla.extraer -> tabla.cargar --+-> cerrar_lote
                                  +-- tabla.extraer -> tabla.cargar --+        |
                                       (un carril por tabla)                   v
                                                                      limpiar_parquet -> fin

UN CARRIL POR TABLA
-------------------
preparar_lote lee CTL.CTL_S2SQL_CATALOGO y devuelve una lista de trabajos.
Airflow crea un par extraer/cargar por cada elemento de esa lista, en tiempo
de EJECUCION. Dos consecuencias que importan:

  - El grafo no depende de que SQL Server responda cuando el scheduler parsea
    el archivo. Si el destino esta caido, falla la primera tarea con un error
    claro; el DAG no se rompe ni desaparece de la interfaz.

  - Cada tabla es independiente. Si una falla, solo se detiene SU carga; las
    demas siguen. Comprobado: con tres tablas y la del medio rota, las otras
    dos terminan en success y solo la rota queda en failed.

LOS TRES MODOS DE CARGA
-----------------------
Los declara cada fila del catalogo, no el DAG:

    REEMPLAZO     TRUNCATE + INSERT en una transaccion. La destino queda igual
                  que el origen.
    INCREMENTAL   Solo las filas posteriores a la ultima marca de agua cargada.
    MERGE         Actualiza las que cambiaron e inserta las nuevas, por clave.

NOMENCLATURA DE ESTE PIPELINE
-----------------------------
    DAG          S2SQL_EXPORT
    Modulos      etl_s2sql/s2sql_comun.py, s2sql_extraccion.py, s2sql_carga.py
    Variable     S2SQL_EXPORT_CONFIG
    Tablas       CTL.CTL_S2SQL_CATALOGO / _LOTE / _LOG_CARGA   (en SQL Server)
    Parquet      /data/s2sql/<yyyyMMdd>/<TABLA>.parquet
    DDL          sql/s2sql/

CONEXIONES
----------
    Origen   SingleStore  singlestoredb (protocolo MySQL)
    Destino  SQL Server   JDBC, con el driver oficial de Microsoft

El destino va por JDBC, no por ODBC. La imagen trae las dos variantes del
driver y las dos JVM:

    mssql-jdbc.jar       12.8.1.jre11   Java 11+   <- por defecto (JAVA_HOME = 17)
    mssql-jdbc-jre8.jar  12.8.1.jre8    Java 8     <- con JAVA_HOME=$JAVA_HOME_8

El codigo mira con que Java arranco el proceso y elige el jar que toca. Es
automatico, pero conviene saberlo: JPype levanta UNA sola JVM por proceso de
Python y no se puede cambiar en caliente, asi que una tarea corre entera con
Java 8 o entera con Java 17.

CARPETA DE PARQUET (fuera del contenedor)
-----------------------------------------
En .env, una linea por plataforma; se conmutan comentando:

    S2SQL_PARQUET_HOST_DIR=D:/datahub/s2sql          <- Windows
    #S2SQL_PARQUET_HOST_DIR=/datos/datahub/s2sql     <- Red Hat
    S2SQL_PARQUET_CONTAINER_DIR=/data/s2sql          <- igual en ambos

El valor de S2SQL_PARQUET_CONTAINER_DIR tiene que coincidir con
parquet.directorio de la Variable S2SQL_EXPORT_CONFIG.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)

DAG_ID = "S2SQL_EXPORT"

# etl_s2sql/ vive junto a este archivo. Airflow pone dags/ en sys.path, pero no
# dags/production/. Se usa append y no insert(0): insertar al principio pondria
# esta carpeta por delante de la biblioteca estandar, y el dia que alguien cree
# aqui un modulo llamado json.py o logging.py se rompe el interprete entero.
DIR_ACTUAL = os.path.dirname(os.path.abspath(__file__))
if DIR_ACTUAL not in sys.path:
    sys.path.append(DIR_ACTUAL)

# Limita cuantas tareas golpean SQL Server a la vez. Con muchas tablas, el
# mapeo dinamico puede lanzar decenas de cargas en paralelo y saturar el
# destino. Por defecto default_pool para que el DAG funcione tal cual; cuando
# crees el Pool en Admin > Pools pon en .env:
#     AIRFLOW_POOL_SQLSERVER=sqlserver
POOL_SQLSERVER = os.environ.get("AIRFLOW_POOL_SQLSERVER", "default_pool")


# ============================================================================
# CALLABLES
# ============================================================================
# jaydebeapi, pyarrow, pandas y singlestoredb se importan DENTRO de cada
# funcion. Si se importaran arriba, el proceso que parsea los DAGs cargaria las
# cuatro librerias cada 30 segundos para dibujar un grafo de seis tareas.
#
# Con jaydebeapi la razon pesa el doble: importarlo arrastra JPype, y JPype
# levanta una JVM. Una JVM por cada proceso que parsea DAGs, cada 30 segundos,
# es memoria tirada.
#
# Ademas, un driver que falte en un worker hace fallar esa tarea con su traza
# en su log, en vez de tumbar el archivo entero con un Broken DAG.
# ============================================================================
@task(task_id="preparar_lote", execution_timeout=timedelta(minutes=15))
def preparar_lote(fecha_proceso: str | None = None, solo_tablas: list | None = None) -> list[dict]:
    from etl_s2sql.s2sql_extraccion import preparar_lote as _preparar

    return _preparar(fecha_proceso=fecha_proceso, solo_tablas=solo_tablas or None)


@task(task_id="extraer", pool=POOL_SQLSERVER, retries=1,
      execution_timeout=timedelta(hours=3))
def extraer(trabajo: dict) -> dict:
    from etl_s2sql.s2sql_extraccion import extraer_tabla

    return extraer_tabla(trabajo)


@task(task_id="cargar", pool=POOL_SQLSERVER, retries=1,
      execution_timeout=timedelta(hours=3))
def cargar(trabajo: dict) -> dict:
    from etl_s2sql.s2sql_carga import cargar_tabla

    return cargar_tabla(trabajo)


def cerrar_lote(batch_id: str | None = None):
    from etl_s2sql.s2sql_carga import cerrar_lote as _cerrar

    return _cerrar(batch_id=batch_id)


def limpiar_parquet():
    from etl_s2sql.s2sql_extraccion import limpiar_parquet as _limpiar

    return _limpiar()


def alertar_fallo(context) -> None:
    """Una linea grepeable por cada tarea que falla.

    Airflow ya escribe la traza dentro del log de ESA tarea. Esta linea sale en
    el log del scheduler con un prefijo fijo, asi que un 'grep FALLO_S2SQL'
    contesta "que se rompio anoche" sin abrir la interfaz. Es tambien el punto
    unico donde enganchar correo o Teams mas adelante.
    """
    ti = context.get("task_instance")
    logger.error(
        "FALLO_S2SQL dag=%s tarea=%s map_index=%s intento=%s/%s run=%s error=%s",
        getattr(ti, "dag_id", "?"),
        getattr(ti, "task_id", "?"),
        getattr(ti, "map_index", -1),
        getattr(ti, "try_number", "?"),
        getattr(ti, "max_tries", "?"),
        context.get("run_id", "?"),
        context.get("exception"),
    )


# ============================================================================
# REINTENTOS
# ============================================================================
# Un reintento solo sirve si el fallo puede ser pasajero. Los de este pipeline
# casi nunca lo son: una tabla que no existe, una columna renombrada en el
# origen, un permiso que falta. Reintentar tres veces una extraccion de tres
# horas son nueve horas para llegar al mismo error.
#
# extraer y cargar llevan retries=1, declarado en sus decoradores, por si el
# corte es de red. preparar_lote y cerrar_lote llevan 0: si SQL Server no
# responde, esperar cinco minutos no lo arregla y solo retrasa el diagnostico.
# ============================================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alertar_fallo,
}


with DAG(
    dag_id=DAG_ID,
    description="Exporta tablas de SingleStore a SQL Server 2022 (reemplazo, incremental o merge)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Con max_active_runs=1, una corrida atascada bloquea todas las siguientes.
    # execution_timeout protege cada tarea, pero no el tiempo que pasa en queued
    # esperando un slot del pool ni en up_for_retry.
    dagrun_timeout=timedelta(hours=8),
    # Sin esto, "{{ ti.xcom_pull(...) }}" entrega la CADENA 'None' cuando no hay
    # XCom, y un 'if batch_id:' la da por buena. Con el render nativo llega el
    # None de Python. Ver el mismo comentario en dag_bt_parquet_singlestore.py:
    # ya costo un fallo silencioso una vez.
    render_template_as_native_obj=True,
    default_args=default_args,
    doc_md=__doc__,
    params={
        "fecha_proceso": Param(
            None,
            type=["null", "string"],
            format="date",
            title="Fecha de proceso",
            description=(
                "Fecha de negocio del lote (YYYY-MM-DD). Decide el nombre de la "
                "carpeta de parquet. Vacio = hoy."
            ),
        ),
        "solo_tablas": Param(
            [],
            type="array",
            items={"type": "string"},
            title="Solo estas tablas",
            description=(
                "Vacio = todas las activas del catalogo. Con valores, exporta solo "
                "esas. Es lo que se usa para reintentar las tablas que fallaron en "
                "un lote anterior sin volver a mover las que ya cargaron."
            ),
        ),
    },
    tags=["s2sql", "singlestore", "sqlserver", "export", "produccion"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    trabajos = preparar_lote(
        fecha_proceso="{{ params.fecha_proceso }}",
        solo_tablas="{{ params.solo_tablas }}",
    )

    @task_group(group_id="tabla")
    def carril(trabajo: dict):
        """Extraccion y carga de UNA tabla.

        El grupo se mapea sobre la lista de trabajos, asi que la interfaz
        muestra un carril por tabla con su propio map_index. Si una tabla falla
        en extraer, solo su cargar queda en upstream_failed; las demas siguen.
        """
        cargar(extraer(trabajo))

    carriles = carril.expand(trabajo=trabajos)

    # trigger_rule="all_done": tiene que resumir el lote TAMBIEN cuando alguna
    # tabla fallo, que es justo cuando hace falta el resumen. cerrar_lote lanza
    # si encuentra errores, para que la corrida no salga verde con tablas sin
    # cargar.
    cerrar = PythonOperator(
        task_id="cerrar_lote",
        python_callable=cerrar_lote,
        op_kwargs={"batch_id": "{{ ti.xcom_pull(task_ids='preparar_lote')[0]['batch_id'] }}"},
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=10),
        doc_md=(
            "Cuenta en `CTL_S2SQL_LOG_CARGA` como quedo cada tabla del lote y "
            "cierra `CTL_S2SQL_LOTE`.\n\n"
            "**Falla si alguna tabla quedo en ERROR.** Las tablas correctas si se "
            "cargaron: para reintentar solo las que fallaron, relanza el DAG con "
            "el parametro `solo_tablas`."
        ),
    )

    # Corre pase lo que pase: si no, las carpetas viejas se acumulan justo los
    # dias con problemas, que es cuando menos falta hace quedarse sin disco.
    limpiar = PythonOperator(
        task_id="limpiar_parquet",
        python_callable=limpiar_parquet,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=20),
        doc_md=(
            "Borra las carpetas `/data/s2sql/<yyyyMMdd>/` mas viejas que "
            "`parquet.retencion_dias` de la Variable `S2SQL_EXPORT_CONFIG`.\n\n"
            "Solo toca carpetas cuyo nombre son 8 digitos que forman una fecha. "
            "Con `retencion_dias: 0` no borra nada."
        ),
    )

    fin = EmptyOperator(task_id="fin", trigger_rule="all_done")

    inicio >> trabajos
    carriles >> cerrar >> limpiar >> fin
