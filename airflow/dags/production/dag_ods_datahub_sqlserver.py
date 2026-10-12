"""ODS_DATAHUB_SQLSERVER -- STG -> ODS por stored procedures, en SQL Server.

QUE HACE
--------
Ejecuta los stored procedures declarados en la Variable DAG_ODS_TABLAS, que
leen las tablas STG_* y llenan las ODS_*. Un proceso del catalogo = una tarea.

POR QUE UN DAG NUEVO Y NO TOCAR BDS_DATAHUB_PROCESOS
----------------------------------------------------
Aquel orquesta ODS y BDS a traves de etl-datahub/table_ods.py y table_bds.py,
y esos modulos importan singlestoredb y tienen SINGLESTORE_CONN_ID fijo en el
codigo. No hablan SQL Server y no se les puede pedir que lo hagan cambiando una
Variable. Este DAG no los toca ni los importa.

COMO SE EVITA EL CRUCE CON STG_BT2SQL_CARGA_SPARK
--------------------------------------------------
Este DAG LEE las tablas STG_* que aquel ESCRIBE. Si corrieran a la vez, un SP
de ODS leeria una STG a medio cargar y el resultado seria plausible y
equivocado, que es la peor combinacion.

La separacion es por HORARIO, con holgura, y es responsabilidad de quien
programa el schedule. Este DAG nace con schedule=None: se lanza a mano o se le
pone un cron por encima de la hora en que termina la extraccion.

Si en algun momento esa holgura deja de bastar -- la extraccion crece y un dia
se solapa --, la proteccion robusta no es mas holgura sino una compuerta de
datos como la que lleva el DAG de BDS: comprobar que la STG del dia esta
completa antes de arrancar. Esta pedida y no hecha.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Airflow anade dags/ a sys.path, NO dags/production/. Sin esto, el import de
# etl_datahub_sql falla con ModuleNotFoundError y el DAG no se parsea.
# append y no insert(0): insertar al principio hace que una carpeta de aqui
# pueda tapar un modulo de la biblioteca estandar.
DIR_ACTUAL = str(Path(__file__).resolve().parent)
if DIR_ACTUAL not in sys.path:
    sys.path.append(DIR_ACTUAL)

from etl_datahub_sql import catalogo as cat          # noqa: E402
from etl_datahub_sql.ejecutar import ejecutar_sp     # noqa: E402

DAG_ID = "ODS_DATAHUB_SQLSERVER"
VARIABLE = "DAG_ODS_TABLAS"
CONN_DESTINO = os.environ.get("AIRFLOW_CONN_DATAHUB", "CONEXION_SQLSERVER")
POOL = os.environ.get("AIRFLOW_POOL_SQLSERVER", "sqlserver")

PROCESOS = cat.procesos_de(VARIABLE)
BITACORA = cat.tabla_bitacora(VARIABLE)


def inventario_procesos():
    import logging
    logging.getLogger(__name__).info("\n%s", cat.inventario(VARIABLE))


def _sin_catalogo():
    from airflow.exceptions import AirflowException
    raise AirflowException(
        f"No se pudo leer ningun proceso activo de {VARIABLE}.\n"
        f"Revise airflow/config/json/{VARIABLE}.json, que al menos un proceso "
        f"tenga ACTIVO='S' y TIPO_PROCESO='SP', y publique con:\n"
        f"    python /opt/airflow/scripts/sync_variables.py --solo {VARIABLE}")


with DAG(
    dag_id=DAG_ID,
    description="STG -> ODS por stored procedures en SQL Server",
    # None a proposito: el horario lo pone quien sabe a que hora termina la
    # extraccion. Un cron inventado aqui es la forma mas facil de cruzarse con
    # STG_BT2SQL_CARGA_SPARK el dia que la extraccion tarde de mas.
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,          # dos corridas a la vez sobre las mismas ODS, no
    tags=["datahub", "ods", "sqlserver", "sp"],
    default_args={
        "owner": "datahub",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md=__doc__,
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    tarea_inventario = PythonOperator(
        task_id="inventario_procesos",
        python_callable=inventario_procesos,
        execution_timeout=timedelta(minutes=5),
        doc_md=("Imprime el catalogo entero, activos e inactivos. Un proceso "
                "inactivo no se dibuja, y sin esta tarea nadie sabria que "
                "existe."),
    )

    inicio >> tarea_inventario

    todas: list = []

    if not PROCESOS:
        # El DAG NO desaparece de la interfaz cuando el catalogo no se puede
        # leer: aparece con una tarea que falla diciendo que hacer. Un DAG
        # ausente no se diagnostica; uno en rojo con un mensaje, si.
        unica = PythonOperator(
            task_id="catalogo_no_disponible",
            python_callable=_sin_catalogo,
            retries=0,
            execution_timeout=timedelta(minutes=1),
        )
        tarea_inventario >> unica
    else:
        grupo_anterior = None
        ordenes = sorted({p.orden for p in PROCESOS})

        for orden in ordenes:
            delgrupo = [p for p in PROCESOS if p.orden == orden]
            etiqueta = f"orden_{orden}"

            with TaskGroup(group_id=etiqueta,
                           tooltip=f"{len(delgrupo)} proceso(s)") as grupo:
                anterior_en_grupo = None
                for p in delgrupo:
                    t = PythonOperator(
                        task_id=p.task_id,
                        python_callable=ejecutar_sp,
                        op_kwargs={
                            "conn_id": CONN_DESTINO,
                            "sp": p.sp_calificado,
                            "parametros": p.parametros,
                            "bitacora": BITACORA,
                            "etiqueta": p.nombre,
                        },
                        retries=p.reintentos,
                        execution_timeout=timedelta(minutes=p.timeout_min),
                        pool=POOL,
                        doc_md=(
                            f"**{p.nombre}**  (ID {p.id_proceso})\n\n"
                            f"- SP: `{p.sp_calificado}`\n"
                            f"- origen: `{p.schema_origen}.{p.tabla_origen}`\n"
                            f"- destino: `{p.destino_calificado}`\n"
                            f"- grupo: {p.grupo} (orden {p.orden}, "
                            f"paralelo={p.paralelo})\n"
                        ),
                    )
                    todas.append(t)
                    # PARALELO=false encadena DENTRO del grupo. Es lo que el
                    # catalogo quiere decir con ese campo: no es que el grupo
                    # vaya en serie con otros, es que sus procesos van uno tras
                    # otro porque escriben la misma tabla.
                    if not p.paralelo and anterior_en_grupo is not None:
                        anterior_en_grupo >> t
                    anterior_en_grupo = t

            if grupo_anterior is None:
                tarea_inventario >> grupo
            else:
                grupo_anterior >> grupo
            grupo_anterior = grupo

        fin = EmptyOperator(
            task_id="ods_completo",
            # all_done y no all_success: que un proceso falle no debe impedir
            # que el punto de union exista. Lo que decide si BDS entra es la
            # compuerta de datos del OTRO DAG, que mira las tablas ODS de
            # verdad, no el color de estas tareas.
            trigger_rule="all_done",
            doc_md=("Punto de union. Espera a TODAS las tareas, no a las hojas "
                    "del ultimo grupo."),
        )

        # EL ABANICO CUELGA DE TODAS LAS TAREAS, NO DEL ULTIMO GRUPO.
        #
        # "grupo >> fin" conectaria solo las hojas del ultimo TaskGroup. Si ese
        # grupo entero se salta -- por propagacion de skip desde un grupo
        # anterior --, el punto de union se libera mientras los grupos de antes
        # siguen corriendo. Paso exactamente eso en STG_BT2SQL_CARGA_SPARK el
        # 2026-10-11: la carga se salto con el parquet correcto en disco y sin
        # ninguna tarea en rojo.
        for t in todas:
            t >> fin
