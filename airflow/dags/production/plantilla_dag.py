"""
PLANTILLA PARA UN DAG NUEVO
===========================================================================

COMO USARLA

  1. Copie este archivo a airflow/dags/production/ con un nombre descriptivo:

         copy airflow\\dags\\templates\\plantilla_dag.py ^
              airflow\\dags\\production\\ventas_diario.py

  2. Cambie dag_id, la descripcion y el responsable
  3. Reemplace el contenido de las tres funciones
  4. Ajuste el horario
  5. Pruebe antes de despausarlo:

         airflow dags test <dag_id> 2026-08-22

  6. Despause el DAG en la interfaz

Tutorial paso a paso: docs/CREAR_UN_DAG.md

---------------------------------------------------------------------------
ESTA PLANTILLA NO SE EJECUTA

El dag_id empieza por "PLANTILLA_", y hay una guarda al final del archivo que
impide que Airflow la registre. Asi aparece como referencia en el repositorio
sin ensuciar la lista de procesos.
===========================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


# ===========================================================================
# CONFIGURACION — lo que hay que cambiar
# ===========================================================================

DAG_ID = "PLANTILLA_cambiar_este_nombre"
DESCRIPCION = "Describa en una linea que hace este proceso"
RESPONSABLE = "cambiar_por_su_usuario"

# Horario en UTC. Peru es UTC-5, asi que sumele 5 horas a la hora local.
#   06:00 Lima -> "0 11 * * *"
#   None       -> solo manual (recomendado mientras lo desarrolla)
HORARIO = None

ETIQUETAS = ["produccion", "cambiar"]


# ===========================================================================
# TAREAS
# ===========================================================================

def extraer(**context) -> dict:
    """Lee los datos de origen.

    Devuelva METADATOS, no los datos. Lo que se devuelve viaja por XCom y se
    guarda en la base de metadatos de Airflow: un DataFrame grande aqui degrada
    el planificador para todos los procesos.

    Si extrae un volumen grande, escribalo a disco y devuelva la ruta.
    """
    fecha = context["ds"]          # fecha logica: '2026-08-22'
    log.info("Extrayendo datos de %s", fecha)

    # --- SU CODIGO AQUI ---------------------------------------------------
    # Ejemplo con base de datos:
    #
    #   from airflow.providers.postgres.hooks.postgres import PostgresHook
    #   hook = PostgresHook(postgres_conn_id="mi_base")
    #   filas = hook.get_records(
    #       "SELECT id, monto FROM ventas WHERE fecha = %s",
    #       parameters=(fecha,),
    #   )
    #
    # Los parametros SIEMPRE con %s. Nunca concatene la fecha en el texto del
    # SQL: eso permite inyeccion.
    # ----------------------------------------------------------------------

    filas = 0

    if filas == 0:
        # Fallar a proposito suele ser mejor que continuar con datos vacios y
        # descubrirlo tres pasos despues.
        log.warning("El origen no devolvio filas para %s", fecha)

    return {"filas": filas, "fecha": fecha}


def transformar(**context) -> dict:
    """Aplica las reglas de negocio."""
    datos = context["ti"].xcom_pull(task_ids="extraer")
    log.info("Transformando %s filas", datos["filas"])

    # --- SU CODIGO AQUI ---------------------------------------------------
    # ----------------------------------------------------------------------

    return {"filas_validas": datos["filas"], "fecha": datos["fecha"]}


def cargar(**context) -> dict:
    """Escribe el resultado en el destino.

    IDEMPOTENCIA: esta tarea debe poder ejecutarse dos veces con la misma fecha
    y dejar el mismo resultado. En la practica, borrar antes de insertar.
    Sin eso, un reintento duplica los datos.
    """
    datos = context["ti"].xcom_pull(task_ids="transformar")
    fecha = datos["fecha"]
    log.info("Cargando %s filas de %s", datos["filas_validas"], fecha)

    # --- SU CODIGO AQUI ---------------------------------------------------
    #   hook.run("DELETE FROM destino WHERE fecha = %s", parameters=(fecha,))
    #   hook.run("INSERT INTO destino ...", parameters=(fecha,))
    # ----------------------------------------------------------------------

    return {"estado": "OK", "filas": datos["filas_validas"]}


def al_fallar(context) -> None:
    """Se ejecuta cuando una tarea falla, tras agotar los reintentos."""
    ti = context["task_instance"]
    log.error(
        "FALLO dag=%s tarea=%s intento=%s fecha=%s url=%s",
        ti.dag_id, ti.task_id, ti.try_number, context["ds"], ti.log_url,
    )
    # Aqui va el envio de correo o la llamada al sistema de alertas.


# ===========================================================================
# DEFINICION DEL DAG
# ===========================================================================

default_args = {
    "owner": RESPONSABLE,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    # execution_timeout es el que mas se olvida. Sin el, una tarea colgada
    # ocupa un worker indefinidamente y nadie se entera.
    "execution_timeout": timedelta(hours=1),
    "depends_on_past": False,
}

with DAG(
    dag_id=DAG_ID,
    description=DESCRIPCION,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=HORARIO,
    # catchup=False NO es opcional. En True, al despausar un DAG diario con
    # start_date de hace dos anios, Airflow encola 730 ejecuciones de golpe.
    catchup=False,
    # Una ejecucion a la vez. Imprescindible si el proceso escribe en una base.
    max_active_runs=1,
    tags=ETIQUETAS,
    on_failure_callback=al_fallar,
    default_view="graph",
) as dag:

    t_extraer = PythonOperator(
        task_id="extraer",
        python_callable=extraer,
    )

    t_transformar = PythonOperator(
        task_id="transformar",
        python_callable=transformar,
    )

    t_cargar = PythonOperator(
        task_id="cargar",
        python_callable=cargar,
    )

    t_extraer >> t_transformar >> t_cargar


# ===========================================================================
# GUARDA - impide que la plantilla se registre como proceso real
#
# Al copiarla, cambie DAG_ID y estas dos lineas dejaran de aplicar solas.
# ===========================================================================

if DAG_ID.startswith("PLANTILLA_"):
    del dag
