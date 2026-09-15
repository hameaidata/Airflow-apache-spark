from __future__ import annotations

import importlib.util
import logging
import re
from pathlib import Path
from typing import Any, Callable

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

VARIABLE_ODS = "DAG_ODS_TABLAS"
VARIABLE_BDS = "DAG_BDS_TABLAS"

BASE_DIR = Path(__file__).resolve().parent
DATAHUB_DIR = BASE_DIR / "etl-datahub"
TASK_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")

DEFAULT_CONFIG_ODS: dict[str, Any] = {"CAPA": "ODS", "GRUPOS": []}
DEFAULT_CONFIG_BDS: dict[str, Any] = {"CAPA": "BDS", "GRUPOS": []}


def cargar_config(variable: str, default: dict[str, Any]) -> dict[str, Any]:
    try:
        return Variable.get(variable, deserialize_json=True)
    except Exception as exc:
        logger.warning(
            "No se pudo leer Variable %s durante el parseo de dag_bt_datahub: %s",
            variable,
            exc,
        )
        return default


def normalizar_estado_activo(valor: Any, campo: str) -> bool:
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, int):
        return valor == 1
    if isinstance(valor, str):
        normalizado = valor.strip().upper()
        if normalizado in {"S", "SI", "Y", "YES", "TRUE", "1", "ACTIVO", "HABILITADO"}:
            return True
        if normalizado in {"N", "NO", "FALSE", "0", "INACTIVO", "DESHABILITADO"}:
            return False
    raise AirflowException(
        f"{campo} invalido: {valor!r}. Use 'S'/'N', true/false o ACTIVO/INACTIVO."
    )


def proceso_activo(proceso_cfg: dict[str, Any], nombre_grupo: str, variable: str) -> bool:
    for campo in ("ACTIVO", "HABILITADO", "ESTADO"):
        if campo in proceso_cfg:
            return normalizar_estado_activo(
                proceso_cfg[campo],
                f"{variable}.GRUPOS.{nombre_grupo}.PROCESOS.{proceso_cfg.get('ID_PROCESO')}.{campo}",
            )

    raise AirflowException(
        f"El proceso {proceso_cfg.get('ID_PROCESO')!r} / "
        f"{proceso_cfg.get('NOMBRE_PROCESO')!r} del grupo {nombre_grupo!r} "
        f"en {variable} debe declarar ACTIVO='S' o ACTIVO='N'."
    )


def grupos_con_procesos_activos(config: dict[str, Any], variable: str) -> list[dict[str, Any]]:
    grupos = []
    for grupo_cfg in config.get("GRUPOS", []):
        nombre_grupo = grupo_cfg.get("NOMBRE", "sin_nombre")
        procesos_activos = [
            proceso_cfg
            for proceso_cfg in grupo_cfg.get("PROCESOS", [])
            if proceso_activo(proceso_cfg, nombre_grupo, variable)
        ]
        if procesos_activos:
            grupos.append({**grupo_cfg, "PROCESOS": procesos_activos})
    return sorted(grupos, key=lambda item: int(item.get("ORDEN", 0)))


def dependencias_proceso(proceso_cfg: dict[str, Any]) -> list[int]:
    valor = None
    for campo in ("DEPENDE_DE", "DEPENDENCIAS", "BDS_DEPENDE_DE", "DEPENDE_DE_BDS"):
        if campo in proceso_cfg:
            valor = proceso_cfg[campo]
            break

    if valor in (None, "", []):
        return []
    if isinstance(valor, int):
        return [valor]
    if isinstance(valor, str):
        return [int(item.strip()) for item in valor.split(",") if item.strip()]
    if isinstance(valor, list):
        return [int(item) for item in valor]

    raise AirflowException(
        f"Dependencias invalidas para ID_PROCESO={proceso_cfg.get('ID_PROCESO')}: {valor!r}"
    )


def validar_dependencias_bds(grupos_bds: list[dict[str, Any]]) -> None:
    procesos = {
        int(proceso["ID_PROCESO"]): proceso
        for grupo in grupos_bds
        for proceso in grupo.get("PROCESOS", [])
    }

    for proceso_id, proceso in procesos.items():
        for dependencia_id in dependencias_proceso(proceso):
            if dependencia_id not in procesos:
                raise AirflowException(
                    f"BDS ID_PROCESO={proceso_id} depende de ID_PROCESO={dependencia_id}, "
                    f"pero esa dependencia no existe o esta inactiva en {VARIABLE_BDS}."
                )

    visitando: set[int] = set()
    visitados: set[int] = set()

    def visitar(proceso_id: int, ruta: list[int]) -> None:
        if proceso_id in visitados:
            return
        if proceso_id in visitando:
            ciclo = " -> ".join(str(item) for item in [*ruta, proceso_id])
            raise AirflowException(f"Ciclo de dependencias BDS detectado: {ciclo}")

        visitando.add(proceso_id)
        for dependencia_id in dependencias_proceso(procesos[proceso_id]):
            visitar(dependencia_id, [*ruta, proceso_id])
        visitando.remove(proceso_id)
        visitados.add(proceso_id)

    for proceso_id in procesos:
        visitar(proceso_id, [])


def importar_modulo(nombre_archivo: str, nombre_modulo: str):
    ruta = DATAHUB_DIR / nombre_archivo
    if not ruta.exists():
        raise AirflowException(f"No existe el modulo requerido por BT_DATAHUB: {ruta}")
    spec = importlib.util.spec_from_file_location(nombre_modulo, ruta)
    if spec is None or spec.loader is None:
        raise AirflowException(f"No se pudo importar {ruta}")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def ejecutar_ods(id_proceso: int, nombre_grupo: str):
    modulo = importar_modulo("table_ods.py", "_bt_datahub_table_ods_runtime")
    return modulo.ejecutar_proceso_desde_json(id_proceso, nombre_grupo)


def ejecutar_bds(id_proceso: int, nombre_grupo: str):
    modulo = importar_modulo("table_bds.py", "_bt_datahub_table_bds_runtime")
    return modulo.ejecutar_proceso_desde_json(id_proceso, nombre_grupo)


def task_id_grupo(capa: str, nombre: str) -> str:
    task_id = TASK_ID_RE.sub("_", str(nombre).lower()).strip("_.-")
    return f"{capa.lower()}_{task_id or 'sin_nombre'}"


def task_id_proceso(capa: str, proceso_cfg: dict[str, Any]) -> str:
    nombre = str(proceso_cfg.get("NOMBRE_PROCESO") or "proceso")
    nombre = TASK_ID_RE.sub("_", nombre.lower()).strip("_.-")
    return f"{capa.lower()}_p_{int(proceso_cfg['ID_PROCESO'])}_{nombre or 'sin_nombre'}"


def crear_tareas_capa_secuencial(
    *,
    grupos: list[dict[str, Any]],
    capa: str,
    callable_proceso: Callable,
) -> tuple[list, list]:
    primeras_tareas = []
    ultimos_grupos = []
    grupo_anterior = None

    for grupo_cfg in grupos:
        nombre = grupo_cfg["NOMBRE"]
        procesos = sorted(
            grupo_cfg.get("PROCESOS", []),
            key=lambda item: int(item.get("ID_PROCESO", 0)),
        )

        with TaskGroup(group_id=task_id_grupo(capa, nombre), tooltip=f"{capa} - {nombre}") as grupo_task:
            tarea_anterior = None
            primera_tarea_grupo = None

            for proceso_cfg in procesos:
                task = PythonOperator(
                    task_id=task_id_proceso(capa, proceso_cfg),
                    python_callable=callable_proceso,
                    op_kwargs={
                        "id_proceso": int(proceso_cfg["ID_PROCESO"]),
                        "nombre_grupo": nombre,
                    },
                    retries=int(proceso_cfg.get("REINTENTOS", grupo_cfg.get("REINTENTOS", 0))),
                    execution_timeout=timedelta(
                        minutes=max(int(proceso_cfg.get("TIMEOUT_MINUTOS", 30)), 1)
                    ),
                )

                if primera_tarea_grupo is None:
                    primera_tarea_grupo = task
                if not grupo_cfg.get("PARALELO", False) and tarea_anterior:
                    tarea_anterior >> task
                tarea_anterior = task

        if primera_tarea_grupo:
            primeras_tareas.append(primera_tarea_grupo)
        ultimos_grupos.append(grupo_task)

        if grupo_anterior:
            grupo_anterior >> grupo_task
        grupo_anterior = grupo_task

    return primeras_tareas, ultimos_grupos


CONFIG_ODS = cargar_config(VARIABLE_ODS, DEFAULT_CONFIG_ODS)
CONFIG_BDS = cargar_config(VARIABLE_BDS, DEFAULT_CONFIG_BDS)
GRUPOS_ODS = grupos_con_procesos_activos(CONFIG_ODS, VARIABLE_ODS)
GRUPOS_BDS = grupos_con_procesos_activos(CONFIG_BDS, VARIABLE_BDS)
validar_dependencias_bds(GRUPOS_BDS)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="BT_DATAHUB",
    description="Orquesta DataHub completo: procesos ODS y luego procesos BDS con dependencias declaradas",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["BT", "ODS", "BDS", "datahub", "singlestore"],
    default_args=default_args,
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    ods_completo = EmptyOperator(task_id="ods_completo")
    fin = EmptyOperator(task_id="fin")

    primeras_ods, ultimos_ods = crear_tareas_capa_secuencial(
        grupos=GRUPOS_ODS,
        capa="ODS",
        callable_proceso=ejecutar_ods,
    )

    if primeras_ods:
        inicio >> primeras_ods
        ultimos_ods >> ods_completo
    else:
        inicio >> ods_completo

    tareas_bds_por_id = {}
    bds_tasks = []

    for grupo_cfg in GRUPOS_BDS:
        nombre = grupo_cfg["NOMBRE"]
        procesos = sorted(
            grupo_cfg.get("PROCESOS", []),
            key=lambda item: int(item.get("ID_PROCESO", 0)),
        )

        with TaskGroup(group_id=task_id_grupo("BDS", nombre), tooltip=f"BDS - {nombre}"):
            tarea_anterior = None
            for proceso_cfg in procesos:
                proceso_id = int(proceso_cfg["ID_PROCESO"])
                task = PythonOperator(
                    task_id=task_id_proceso("BDS", proceso_cfg),
                    python_callable=ejecutar_bds,
                    op_kwargs={
                        "id_proceso": proceso_id,
                        "nombre_grupo": nombre,
                    },
                    retries=int(proceso_cfg.get("REINTENTOS", grupo_cfg.get("REINTENTOS", 0))),
                    execution_timeout=timedelta(
                        minutes=max(int(proceso_cfg.get("TIMEOUT_MINUTOS", 30)), 1)
                    ),
                )
                tareas_bds_por_id[proceso_id] = task
                bds_tasks.append(task)

                if (
                    not grupo_cfg.get("PARALELO", False)
                    and tarea_anterior
                    and not dependencias_proceso(proceso_cfg)
                ):
                    tarea_anterior >> task
                tarea_anterior = task

    for grupo_cfg in GRUPOS_BDS:
        for proceso_cfg in grupo_cfg.get("PROCESOS", []):
            proceso_id = int(proceso_cfg["ID_PROCESO"])
            dependencias = dependencias_proceso(proceso_cfg)
            if dependencias:
                for dependencia_id in dependencias:
                    tareas_bds_por_id[dependencia_id] >> tareas_bds_por_id[proceso_id]
            else:
                ods_completo >> tareas_bds_por_id[proceso_id]

    if bds_tasks:
        bds_tasks >> fin
    else:
        ods_completo >> fin