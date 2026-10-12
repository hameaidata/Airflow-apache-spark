"""BDS_DATAHUB_SQLSERVER -- ODS -> BDS por stored procedures, en SQL Server.

QUE HACE
--------
Ejecuta los stored procedures de la Variable DAG_BDS_TABLAS, que leen las
tablas ODS_* y llenan las BDS_*. Un proceso del catalogo = una tarea.

LA COMPUERTA: SOLO CARGA SI ODS TIENE DATOS
-------------------------------------------
Este DAG se programa aparte -- una hora despues de que termine el de ODS -- y
no cuelga de el por el grafo. Un acoplamiento por horario se rompe solo: el dia
que ODS tarde mas de la hora prevista, este arrancaria sobre tablas ODS a medio
llenar o directamente vacias, y los SP de BDS correrian sobre nada. No fallarian
-- un SP sobre una tabla vacia termina en exito y escribe cero filas--, y la capa
BDS quedaria vaciada en silencio.

Por eso la primera tarea no mira el reloj ni el estado del otro DAG: mira los
DATOS. Cuenta las filas de las tablas ODS de las que depende cada proceso BDS,
declaradas en DEPENDE_DE_ODS. Si no hay ninguna fila, la carga se SALTA -- en
rosa, no en rojo-- porque "ODS todavia no cargo" no es un fallo de BDS.

Es el mismo patron que verificar_parquet en STG_BT2SQL_CARGA_SPARK, y por la
misma razon: una compuerta que mira el resultado real aguanta lo que un
acoplamiento por tiempo no.

POR QUE UN DAG NUEVO Y NO BDS_DATAHUB_PROCESOS
-----------------------------------------------
Aquel va por etl-datahub/table_bds.py, que importa singlestoredb y tiene
SINGLESTORE_CONN_ID fijo en el codigo. Este no toca ese paquete.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

DIR_ACTUAL = str(Path(__file__).resolve().parent)
if DIR_ACTUAL not in sys.path:
    sys.path.append(DIR_ACTUAL)

from etl_datahub_sql import catalogo as cat          # noqa: E402
from etl_datahub_sql.ejecutar import ejecutar_sp, filas_de   # noqa: E402

logger = logging.getLogger(__name__)

DAG_ID = "BDS_DATAHUB_SQLSERVER"
VARIABLE = "DAG_BDS_TABLAS"
VARIABLE_ODS = "DAG_ODS_TABLAS"
CONN_DESTINO = os.environ.get("AIRFLOW_CONN_DATAHUB", "CONEXION_SQLSERVER")
POOL = os.environ.get("AIRFLOW_POOL_SQLSERVER", "sqlserver")

PROCESOS = cat.procesos_de(VARIABLE)
BITACORA = cat.tabla_bitacora(VARIABLE)


def tablas_ods_requeridas() -> dict[str, list[str]]:
    """{tabla ODS -> [procesos BDS que la necesitan]}.

    Sale de cruzar DEPENDE_DE_ODS de cada proceso BDS con ID_PROCESO del
    catalogo de ODS. Un proceso BDS sin DEPENDE_DE_ODS no aporta ninguna tabla:
    no se inventa una dependencia que nadie declaro.
    """
    por_id = {p.id_proceso: p for p in cat.procesos_de(VARIABLE_ODS,
                                                       solo_activos=False)}
    requeridas: dict[str, list[str]] = {}
    for bds in PROCESOS:
        for id_ods in bds.depende_de_ods:
            ods = por_id.get(id_ods)
            if ods is None:
                logger.warning(
                    "%s declara DEPENDE_DE_ODS=%s y ese ID_PROCESO no existe "
                    "en %s. No se puede comprobar su insumo.",
                    bds.nombre, id_ods, VARIABLE_ODS)
                continue
            if not ods.tabla_destino:
                logger.warning(
                    "El proceso ODS %s (%s) no declara TABLA_DESTINO: no se "
                    "puede contar su contenido.", id_ods, ods.nombre)
                continue
            requeridas.setdefault(ods.destino_calificado, []).append(bds.nombre)
    return requeridas


def verificar_ods() -> bool:
    """Compuerta: hay datos en ODS?

    Tres salidas, y las tres distintas a proposito:

      - ninguna dependencia declarada -> PASA, con aviso. No se puede
        comprobar lo que nadie declaro, y bloquear por eso convertiria un
        catalogo incompleto en una carga que no corre y nadie sabe por que.

      - alguna tabla con filas -> PASA. Basta una: cada proceso BDS tiene su
        propia dependencia, y exigir que TODAS tengan datos pararia la capa
        entera por una tabla ODS que ese dia no trajo nada.

      - todas vacias o ausentes -> NO pasa, y la carga queda en SKIPPED. No es
        un error: es que ODS todavia no cargo.

    Se distingue 'no existe' de 'existe y tiene cero', porque son dos
    problemas distintos y mandan a buscar a sitios distintos.
    """
    requeridas = tablas_ods_requeridas()

    if not requeridas:
        logger.warning(
            "Ningun proceso activo de %s declara DEPENDE_DE_ODS, asi que no "
            "hay nada que comprobar y la carga continua.\n"
            "Para que esta compuerta sirva, declare en cada proceso BDS los "
            "ID_PROCESO de ODS de los que depende.", VARIABLE)
        return True

    con_datos, vacias, ausentes = [], [], []
    for tabla, quienes in sorted(requeridas.items()):
        n = filas_de(CONN_DESTINO, tabla)
        if n is None:
            ausentes.append(tabla)
            logger.error("  AUSENTE  %-40s  (la necesitan: %s)",
                         tabla, ", ".join(quienes))
        elif n > 0:
            con_datos.append(tabla)
            logger.info("  OK       %-40s  %s fila(s)", tabla, f"{n:,}")
        else:
            vacias.append(tabla)
            logger.warning("  VACIA    %-40s  0 filas  (la necesitan: %s)",
                           tabla, ", ".join(quienes))

    if ausentes:
        logger.error(
            "%s tabla(s) ODS no existen en la base. Eso no es una carga que no "
            "corrio: es un despliegue incompleto. Ejecute el DDL de la capa "
            "ODS antes de volver a lanzar.", len(ausentes))

    if con_datos:
        logger.info("%s de %s tabla(s) ODS tienen datos: la carga continua.",
                    len(con_datos), len(requeridas))
        return True

    logger.info(
        "Ninguna de las %s tabla(s) ODS tiene filas. La carga de BDS se SALTA: "
        "no es un error, es que ODS todavia no cargo. Revise la corrida de "
        "ODS_DATAHUB_SQLSERVER de hoy.", len(requeridas))
    return False


def inventario_procesos():
    logger.info("\n%s", cat.inventario(VARIABLE))
    req = tablas_ods_requeridas()
    if req:
        logger.info("Insumos de ODS que esta corrida va a comprobar:")
        for tabla, quienes in sorted(req.items()):
            logger.info("   %-40s  <- %s", tabla, ", ".join(quienes))


def _sin_catalogo():
    from airflow.exceptions import AirflowException
    raise AirflowException(
        f"No se pudo leer ningun proceso activo de {VARIABLE}.\n"
        f"Revise airflow/config/json/{VARIABLE}.json y publique con:\n"
        f"    python /opt/airflow/scripts/sync_variables.py --solo {VARIABLE}")


with DAG(
    dag_id=DAG_ID,
    description="ODS -> BDS por stored procedures en SQL Server",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["datahub", "bds", "sqlserver", "sp"],
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
    )

    compuerta = ShortCircuitOperator(
        task_id="verificar_ods",
        python_callable=verificar_ods,
        # Sin esto el corto-circuito saltaria tambien el punto de union, y una
        # corrida saltada se veria igual que una que nunca se programo.
        ignore_downstream_trigger_rules=False,
        pool=POOL,
        execution_timeout=timedelta(minutes=15),
        doc_md=("Cuenta las filas de las tablas ODS declaradas en "
                "DEPENDE_DE_ODS. Sin datos, la carga queda en SKIPPED, no en "
                "failed."),
    )

    todas: list = []

    if not PROCESOS:
        unica = PythonOperator(
            task_id="catalogo_no_disponible",
            python_callable=_sin_catalogo,
            retries=0,
            execution_timeout=timedelta(minutes=1),
        )
        inicio >> tarea_inventario >> compuerta >> unica
    else:
        inicio >> tarea_inventario >> compuerta

        grupo_anterior = None
        for orden in sorted({p.orden for p in PROCESOS}):
            delgrupo = [p for p in PROCESOS if p.orden == orden]

            with TaskGroup(group_id=f"orden_{orden}",
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
                            f"- depende de ODS: "
                            f"{p.depende_de_ods or '(no declarado)'}\n"
                            f"- grupo: {p.grupo} (orden {p.orden})\n"
                        ),
                    )
                    todas.append(t)
                    if not p.paralelo and anterior_en_grupo is not None:
                        anterior_en_grupo >> t
                    anterior_en_grupo = t

            if grupo_anterior is None:
                compuerta >> grupo
            else:
                grupo_anterior >> grupo
            grupo_anterior = grupo

        fin = EmptyOperator(
            task_id="bds_completo",
            trigger_rule="all_done",
        )

        # El abanico cuelga de TODAS las tareas, no de las hojas del ultimo
        # grupo. Misma razon que en el DAG de ODS y que en el de Spark: si el
        # ultimo grupo se salta, un punto de union colgado de sus hojas se
        # libera mientras los grupos anteriores siguen corriendo.
        for t in todas:
            t >> fin
