"""
BT_DATAHUB - Orquesta la capa ODS y luego la capa BDS en un solo DAG.

Configuracion (Variables de Airflow, versionadas en airflow/config/json/):
    DAG_ODS_TABLAS   grupos y procesos de la capa ODS
    DAG_BDS_TABLAS   grupos y procesos de la capa BDS

La logica de ejecucion vive en etl-datahub/table_ods.py y table_bds.py; este
archivo solo arma el grafo.

COMO SE ARMA EL GRAFO
---------------------
ODS
    Los grupos se ordenan por ORDEN.
      - ORDEN distinto -> los grupos corren uno despues de otro.
      - ORDEN igual     -> los grupos corren en paralelo entre si.
    Dentro del grupo, PARALELO decide:
      - false -> los procesos corren en cadena, por ID_PROCESO
      - true  -> los procesos arrancan todos a la vez
    Al terminar todos los grupos se completa la tarea "ods_completo".

BDS
    ORDEN TAMBIEN ORDENA (cambio respecto de la version anterior, donde ORDEN
    en BDS era decorativo y todo grupo sin DEPENDE_DE arrancaba junto con la
    capa entera). Ahora:
      - Un proceso SIN DEPENDE_DE espera a que termine el nivel de ORDEN
        anterior; si esta en el primer nivel, espera ods_completo.
      - Un proceso CON DEPENDE_DE espera exactamente a esos ID_PROCESO,
        sin importar en que grupo o nivel esten.
    Asi el JSON y el grafo dicen lo mismo.

VALIDACION
----------
Todo se valida ANTES de crear tareas y se reportan TODOS los errores juntos,
no solo el primero: IDs duplicados (dentro de la capa y entre capas), grupos
que generan el mismo group_id, dependencias hacia procesos inexistentes o
inactivos, valores de ACTIVO que nadie contemplo, y ciclos.

Si la configuracion es invalida, el DAG NO desaparece: se publica con una
unica tarea, "configuracion_invalida", que falla mostrando la lista completa
de problemas. Ver el bloque CONSTRUCCION DEL DAG al final del archivo.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


logger = logging.getLogger(__name__)

DAG_ID = "BT_DATAHUB"

VARIABLE_ODS = "DAG_ODS_TABLAS"
VARIABLE_BDS = "DAG_BDS_TABLAS"
VARIABLE_POR_CAPA = {"ODS": VARIABLE_ODS, "BDS": VARIABLE_BDS}

BASE_DIR = Path(__file__).resolve().parent
DATAHUB_DIR = BASE_DIR / "etl-datahub"

# ----------------------------------------------------------------------------
# Pool de Airflow que limita cuantas tareas golpean SingleStore a la vez.
# Sin esto, un grupo con PARALELO=true y 20 procesos abre 20 conexiones de
# golpe. Se puede sobreescribir por proceso o por grupo con la clave POOL.
#
# CUIDADO: si el pool NO existe en Airflow (Admin > Pools), las tareas quedan
# en estado "scheduled" para siempre y la interfaz no dice por que. No es un
# fallo, es una espera silenciosa, que es peor.
#
# Por eso el nombre sale de una variable de entorno: en un stack donde todavia
# no se creo el pool, basta con poner en .env
#
#     AIRFLOW_POOL_SINGLESTORE=default_pool
#
# y las tareas corren sin limite mientras tanto. El valor por defecto sigue
# siendo "singlestore" para no cambiar el comportamiento de lo que ya funciona.
# Se lee del entorno y no de una Variable de Airflow porque esto se evalua en
# cada parseo del archivo.
# ----------------------------------------------------------------------------
POOL_DEFAULT = os.environ.get("AIRFLOW_POOL_SINGLESTORE", "singlestore")

# Airflow valida task_id y group_id con reglas distintas:
#     task_id  -> ^[\w.-]+$   admite punto
#     group_id -> ^[\w-]+$    NO admite punto (el punto separa la jerarquia)
# Se usa la regla mas estricta para ambos.
ID_AIRFLOW_RE = re.compile(r"[^A-Za-z0-9_-]+")
PREFIJOS_REDUNDANTES = ("grupo_", "grp_", "g_")

DEFAULT_CONFIG_ODS: dict[str, Any] = {"CAPA": "ODS", "GRUPOS": []}
DEFAULT_CONFIG_BDS: dict[str, Any] = {"CAPA": "BDS", "GRUPOS": []}


# ============================================================================
# CONFIGURACION
# ============================================================================
def cargar_config(variable: str, default: dict[str, Any]) -> dict[str, Any]:
    """Lee la Variable en tiempo de parseo.

    Nunca lanza: si la Variable falta o esta mal, devuelve el default vacio y
    deja el DAG visible pero sin tareas. Un DAG vacio se diagnostica; un
    archivo que no importa desaparece de la UI sin dejar rastro claro.

    Nota de rendimiento: esto es una consulta a la metadata database en CADA
    parseo (cada 30 s por defecto). Es inevitable en un DAG cuya forma sale de
    la configuracion, pero se puede amortiguar activando la cache de secretos
    de Airflow en airflow.cfg o por entorno:

        AIRFLOW__SECRETS__USE_CACHE=True
        AIRFLOW__SECRETS__CACHE_TTL_SECONDS=900

    Se devuelve una copia del default para que nadie pueda mutar el objeto
    compartido a nivel de modulo.
    """
    try:
        config = Variable.get(variable, deserialize_json=True)
    except Exception as exc:
        logger.warning("No se pudo leer la Variable %s al parsear %s: %s", variable, DAG_ID, exc)
        return dict(default)
    if not isinstance(config, dict):
        logger.warning("La Variable %s no es un objeto JSON; se ignora.", variable)
        return dict(default)
    return config


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
    raise ValueError(
        f"{campo} invalido: {valor!r}. Use 'S'/'N', true/false o ACTIVO/INACTIVO."
    )


def proceso_activo(
    proceso_cfg: dict[str, Any],
    nombre_grupo: str,
    variable: str,
    errores: list[str],
) -> bool:
    """Dice si el proceso esta activo, SIN cortar la validacion.

    Antes esto lanzaba. El efecto era que un solo ACTIVO mal escrito abortaba
    la validacion entera en la primera pasada, asi que el mensaje mostraba ese
    error y ninguno de los demas: el operador corregia uno, volvia a guardar,
    y aparecia el siguiente. Con una lista de errores esto se anota y se sigue,
    que es lo que promete el docstring del modulo.

    Un proceso mal declarado se trata como INACTIVO para que no llegue a crear
    una tarea; de todas formas el DAG no se va a publicar con errores pendientes.
    """
    for campo in ("ACTIVO", "HABILITADO", "ESTADO"):
        if campo in proceso_cfg:
            try:
                return normalizar_estado_activo(
                    proceso_cfg[campo],
                    f"{variable}.GRUPOS.{nombre_grupo}.PROCESOS."
                    f"{proceso_cfg.get('ID_PROCESO')}.{campo}",
                )
            except ValueError as exc:
                errores.append(str(exc))
                return False
    errores.append(
        f"El proceso {proceso_cfg.get('ID_PROCESO')!r} / "
        f"{proceso_cfg.get('NOMBRE_PROCESO')!r} del grupo {nombre_grupo!r} "
        f"en {variable} debe declarar ACTIVO='S' o ACTIVO='N'."
    )
    return False


def grupos_con_procesos_activos(
    config: dict[str, Any], variable: str, errores: list[str]
) -> list[dict[str, Any]]:
    grupos = []
    for grupo_cfg in config.get("GRUPOS", []) or []:
        nombre_grupo = grupo_cfg.get("NOMBRE", "sin_nombre")
        procesos_activos = [
            proceso_cfg
            for proceso_cfg in grupo_cfg.get("PROCESOS", []) or []
            if proceso_activo(proceso_cfg, nombre_grupo, variable, errores)
        ]
        if procesos_activos:
            procesos_activos = sorted(procesos_activos, key=lambda p: int(p["ID_PROCESO"]))
            grupos.append({**grupo_cfg, "PROCESOS": procesos_activos})
    return sorted(grupos, key=lambda item: int(item.get("ORDEN", 0)))


def dependencias_proceso(proceso_cfg: dict[str, Any], errores: list[str]) -> list[int]:
    """Acepta [], 2001, '2001,2002' o [2001, 2002].

    Igual que proceso_activo: un valor mal escrito se anota y se devuelve una
    lista vacia, para no cortar la validacion en el primer error.
    """
    valor = None
    for campo in ("DEPENDE_DE", "DEPENDENCIAS", "BDS_DEPENDE_DE", "DEPENDE_DE_BDS"):
        if campo in proceso_cfg:
            valor = proceso_cfg[campo]
            break

    if valor in (None, "", []):
        return []
    try:
        if isinstance(valor, bool):
            raise ValueError
        if isinstance(valor, int):
            return [valor]
        if isinstance(valor, str):
            return [int(item.strip()) for item in valor.split(",") if item.strip()]
        if isinstance(valor, list):
            return [int(item) for item in valor]
    except (TypeError, ValueError):
        pass
    errores.append(
        f"Dependencias invalidas para ID_PROCESO={proceso_cfg.get('ID_PROCESO')}: {valor!r}"
    )
    return []


# ============================================================================
# IDENTIFICADORES DE AIRFLOW
# ============================================================================
def _slug(texto: Any) -> str:
    return ID_AIRFLOW_RE.sub("_", str(texto).strip().lower()).strip("_-")


def task_id_grupo(capa: str, nombre: str) -> str:
    """('ODS', 'ODS_DATAHUB_ORDEN_1') -> 'ods_datahub_orden_1'

    Quita el prefijo de capa si el nombre del grupo ya lo trae, para no
    terminar con group_ids como 'ods_ods_datahub_orden_1'.
    """
    slug = _slug(nombre)
    for prefijo in PREFIJOS_REDUNDANTES:
        if slug.startswith(prefijo):
            slug = slug[len(prefijo):]
            break
    capa_slug = capa.lower()
    if slug.startswith(f"{capa_slug}_"):
        slug = slug[len(capa_slug) + 1:]
    return f"{capa_slug}_{slug or 'sin_nombre'}"


def task_id_proceso(proceso_cfg: dict[str, Any]) -> str:
    """-> 'p104_carga_stg_ods_tipo_cambio'

    Sin prefijo de capa: el TaskGroup que lo contiene ya lo lleva y Airflow
    compone el id como '<group_id>.<task_id>'. El ID va con ceros a la
    izquierda para que el orden alfabetico de la UI coincida con el numerico.
    """
    nombre = _slug(proceso_cfg.get("NOMBRE_PROCESO") or "proceso")
    return f"p{int(proceso_cfg['ID_PROCESO']):03d}_{nombre or 'sin_nombre'}"


def pool_de(proceso_cfg: dict[str, Any], grupo_cfg: dict[str, Any]) -> str | None:
    for origen in (proceso_cfg, grupo_cfg):
        if "POOL" in origen:
            valor = origen["POOL"]
            return str(valor) if valor else None
    return POOL_DEFAULT


# ============================================================================
# CARGA DE LOS MODULOS DE EJECUCION
# ============================================================================
@lru_cache(maxsize=None)
def importar_modulo(nombre_archivo: str, nombre_modulo: str):
    """Importa table_ods.py / table_bds.py una sola vez por proceso worker.

    Antes se re-ejecutaba el modulo entero en CADA tarea: re-importaba
    singlestoredb y volvia a leer la Variable. Con lru_cache se paga una vez.

    El registro en sys.modules ANTES de exec_module no es opcional: sin el,
    cualquier @dataclass o typing con "from __future__ import annotations"
    dentro del modulo falla con
        'NoneType' object has no attribute '__dict__'
    porque dataclasses busca su propio modulo en sys.modules.
    """
    ruta = DATAHUB_DIR / nombre_archivo
    if not ruta.exists():
        raise AirflowException(f"No existe el modulo requerido por {DAG_ID}: {ruta}")

    spec = importlib.util.spec_from_file_location(nombre_modulo, ruta)
    if spec is None or spec.loader is None:
        raise AirflowException(f"No se pudo importar {ruta}")

    modulo = importlib.util.module_from_spec(spec)
    sys.modules[nombre_modulo] = modulo
    try:
        spec.loader.exec_module(modulo)
    except Exception as exc:
        sys.modules.pop(nombre_modulo, None)
        raise AirflowException(f"Error al importar {ruta}: {exc}") from exc
    return modulo


def ejecutar_ods(id_proceso: int, nombre_grupo: str):
    modulo = importar_modulo("table_ods.py", "_bt_datahub_table_ods_runtime")
    return modulo.ejecutar_proceso_desde_json(id_proceso, nombre_grupo)


def ejecutar_bds(id_proceso: int, nombre_grupo: str):
    modulo = importar_modulo("table_bds.py", "_bt_datahub_table_bds_runtime")
    return modulo.ejecutar_proceso_desde_json(id_proceso, nombre_grupo)


# ============================================================================
# PLAN: el grafo como datos, validado antes de crear tareas
# ============================================================================
@dataclass
class Nodo:
    id_proceso: int
    capa: str
    grupo: str
    orden: int
    cfg: dict[str, Any]
    grupo_cfg: dict[str, Any]


@dataclass
class Plan:
    ods: list[dict[str, Any]] = field(default_factory=list)
    bds: list[dict[str, Any]] = field(default_factory=list)
    nodos: dict[int, Nodo] = field(default_factory=dict)
    # (origen, destino); origen puede ser un ID_PROCESO o "ods_completo".
    aristas_bds: list[tuple[Any, int]] = field(default_factory=list)


def construir_plan(config_ods: dict[str, Any], config_bds: dict[str, Any]) -> Plan:
    errores: list[str] = []

    plan = Plan(
        ods=grupos_con_procesos_activos(config_ods, VARIABLE_ODS, errores),
        bds=grupos_con_procesos_activos(config_bds, VARIABLE_BDS, errores),
    )

    # --- unicidad sobre TODO lo declarado, activo o no ----------------------
    # ID_PROCESO es la clave de CTL_CFG_PROCESOS, asi que un duplicado es un
    # conflicto de catalogo aunque los procesos esten apagados: al activarlos,
    # uno de los dos validaria contra la fila del otro. Lo mismo con los
    # group_id: dos grupos que colapsan al mismo id rompen el DAG el dia que
    # ambos tengan procesos activos. Por eso esta pasada NO filtra por ACTIVO.
    vistos_id: dict[int, str] = {}
    vistos_gid: dict[str, str] = {}
    for capa, cfg in (("ODS", config_ods), ("BDS", config_bds)):
        variable = VARIABLE_POR_CAPA[capa]
        for g in cfg.get("GRUPOS", []) or []:
            nombre_grupo = g.get("NOMBRE", "sin_nombre")
            gid = task_id_grupo(capa, nombre_grupo)
            if gid in vistos_gid and vistos_gid[gid] != f"{capa}.{nombre_grupo}":
                errores.append(
                    f"{variable}: los grupos {vistos_gid[gid]!r} y {capa}.{nombre_grupo!r} "
                    f"generan el mismo group_id {gid!r}. Renombra uno."
                )
            vistos_gid[gid] = f"{capa}.{nombre_grupo}"

            for p in g.get("PROCESOS", []) or []:
                pid = int(p["ID_PROCESO"])
                origen = f"{capa}.{nombre_grupo}"
                if pid in vistos_id and vistos_id[pid] != origen:
                    errores.append(
                        f"{variable}.{nombre_grupo}.ID_PROCESO={pid} repetido, ya declarado en "
                        f"{vistos_id[pid]}. Debe ser unico entre ODS y BDS porque es la clave "
                        f"de CTL_CFG_PROCESOS."
                    )
                vistos_id[pid] = origen

    # --- nodos del grafo (solo los activos) --------------------------------
    for capa, grupos in (("ODS", plan.ods), ("BDS", plan.bds)):
        variable = VARIABLE_POR_CAPA[capa]
        for g in grupos:
            for p in g["PROCESOS"]:
                pid = int(p["ID_PROCESO"])
                donde = f"{variable}.{g['NOMBRE']}.ID_PROCESO={pid}"

                if pid in plan.nodos:
                    continue  # ya reportado arriba como duplicado

                if capa == "ODS" and dependencias_proceso(p, errores):
                    errores.append(
                        f"{donde}: DEPENDE_DE no aplica en ODS. El orden de ODS se controla "
                        f"con ORDEN del grupo y PARALELO."
                    )

                plan.nodos[pid] = Nodo(
                    id_proceso=pid,
                    capa=capa,
                    grupo=g["NOMBRE"],
                    orden=int(g.get("ORDEN", 0)),
                    cfg=p,
                    grupo_cfg=g,
                )

    # --- aristas BDS -------------------------------------------------------
    # ORDEN agrupa en niveles; un proceso sin DEPENDE_DE espera el nivel
    # anterior completo (o ods_completo si esta en el primer nivel).
    niveles_bds = sorted({int(g.get("ORDEN", 0)) for g in plan.bds})
    procesos_por_nivel: dict[int, list[int]] = defaultdict(list)
    for g in plan.bds:
        for p in g["PROCESOS"]:
            procesos_por_nivel[int(g.get("ORDEN", 0))].append(int(p["ID_PROCESO"]))

    # Los errores de ACTIVO ya se anotaron en la primera pasada; aqui solo
    # interesa saber quien quedo fuera, asi que se descarta la lista.
    inactivos_declarados = {
        int(p["ID_PROCESO"])
        for variable, cfg in ((VARIABLE_BDS, config_bds), (VARIABLE_ODS, config_ods))
        for g in (cfg.get("GRUPOS") or [])
        for p in (g.get("PROCESOS") or [])
        if not proceso_activo(p, g.get("NOMBRE", "sin_nombre"), variable, [])
    }

    for g in plan.bds:
        orden = int(g.get("ORDEN", 0))
        paralelo = bool(g.get("PARALELO", False))
        indice_nivel = niveles_bds.index(orden)
        anterior_en_grupo: int | None = None

        for p in g["PROCESOS"]:
            pid = int(p["ID_PROCESO"])
            deps = dependencias_proceso(p, errores)

            if deps:
                for d in deps:
                    if d in plan.nodos:
                        plan.aristas_bds.append((d, pid))
                    elif d in inactivos_declarados:
                        errores.append(
                            f"{VARIABLE_BDS}.{g['NOMBRE']}.ID_PROCESO={pid} depende de "
                            f"ID_PROCESO={d}, que existe pero esta ACTIVO='N'. "
                            f"Activa {d} o quita la dependencia."
                        )
                    else:
                        errores.append(
                            f"{VARIABLE_BDS}.{g['NOMBRE']}.ID_PROCESO={pid} depende de "
                            f"ID_PROCESO={d}, que no existe en ninguna de las dos Variables."
                        )
            elif not paralelo and anterior_en_grupo is not None:
                plan.aristas_bds.append((anterior_en_grupo, pid))
            elif indice_nivel == 0:
                plan.aristas_bds.append(("ods_completo", pid))
            else:
                for previo in procesos_por_nivel[niveles_bds[indice_nivel - 1]]:
                    plan.aristas_bds.append((previo, pid))

            anterior_en_grupo = pid

    # --- ciclos ------------------------------------------------------------
    adyacencia: dict[int, list[int]] = defaultdict(list)
    for origen, destino in plan.aristas_bds:
        if isinstance(origen, int):
            adyacencia[origen].append(destino)

    estado: dict[int, int] = {}

    def visitar(n: int, ruta: list[int]) -> None:
        if estado.get(n) == 2:
            return
        if estado.get(n) == 1:
            ciclo = ruta[ruta.index(n):] + [n]
            errores.append("Ciclo de dependencias BDS: " + " -> ".join(map(str, ciclo)))
            return
        estado[n] = 1
        for m in adyacencia.get(n, []):
            visitar(m, ruta + [n])
        estado[n] = 2

    for n in list(adyacencia):
        visitar(n, [])

    if errores:
        # Se quitan repetidos conservando el orden: un mismo proceso puede
        # recorrerse en dos pasadas y no aporta nada verlo dos veces.
        unicos = list(dict.fromkeys(errores))
        raise AirflowException(
            f"Configuracion invalida de {DAG_ID} ({len(unicos)} error(es)):\n  - "
            + "\n  - ".join(unicos)
        )
    return plan


def plan_a_markdown(plan: Plan) -> str:
    """Documentacion del DAG (pestana Docs de la UI)."""
    lineas = [f"# {DAG_ID}", "", "## ODS", ""]
    if not plan.ods:
        lineas.append(f"_Sin procesos activos en {VARIABLE_ODS}._")
    for g in plan.ods:
        modo = "paralelo" if g.get("PARALELO") else "secuencial"
        lineas.append(f"**{task_id_grupo('ODS', g['NOMBRE'])}** - orden {g.get('ORDEN', 0)}, {modo}")
        for p in g["PROCESOS"]:
            lineas.append(f"- `{task_id_proceso(p)}` -> {p.get('STORED_PROCEDURE')}")
        lineas.append("")

    lineas += ["## BDS", ""]
    if not plan.bds:
        lineas.append(f"_Sin procesos activos en {VARIABLE_BDS}._")
    entrantes: dict[int, list[str]] = defaultdict(list)
    for origen, destino in plan.aristas_bds:
        entrantes[destino].append(str(origen))
    for g in plan.bds:
        modo = "paralelo" if g.get("PARALELO") else "secuencial"
        lineas.append(f"**{task_id_grupo('BDS', g['NOMBRE'])}** - orden {g.get('ORDEN', 0)}, {modo}")
        for p in g["PROCESOS"]:
            pid = int(p["ID_PROCESO"])
            lineas.append(f"- `{task_id_proceso(p)}` <- {', '.join(entrantes[pid]) or 'nada'}")
        lineas.append("")
    return "\n".join(lineas)


def alertar_fallo(context) -> None:
    """Deja una linea de ERROR con todo el contexto del proceso.

    email_on_failure esta en False porque el stack no tiene SMTP configurado.
    Este callback es el punto unico donde enganchar Slack, Teams o PagerDuty
    mas adelante, sin tocar cada tarea.
    """
    ti = context.get("task_instance")
    logger.error(
        "[ALERTA] %s.%s fallo | run_id=%s | intento %s de %s | %s",
        context["dag"].dag_id,
        getattr(ti, "task_id", "?"),
        context.get("run_id"),
        getattr(ti, "try_number", "?"),
        getattr(ti, "max_tries", "?"),
        context.get("exception"),
    )


# ============================================================================
# CONSTRUCCION DEL DAG
# ============================================================================
# construir_plan lanza cuando la configuracion es invalida. Si esa excepcion
# escapa del modulo, Airflow marca el archivo como Broken DAG y BT_DATAHUB
# DESAPARECE de la lista: no se ve el historial, ni las tareas, ni las Docs, y
# el motivo queda en un banner que hay que ir a buscar. Es exactamente lo que
# cargar_config se cuida de evitar ("un DAG vacio se diagnostica; un archivo
# que no importa desaparece de la UI sin dejar rastro claro"), asi que aqui se
# aplica el mismo criterio.
#
# Con la configuracion rota, el DAG se publica con una sola tarea que falla
# citando todos los problemas. Se ve en la lista, se abre, se lee el error en
# su log y en las Docs, y no hay manera de ejecutar procesos por accidente.
#
# Se capturan todas las excepciones, no solo AirflowException: un ID_PROCESO
# que falta o que no es un numero saldria como KeyError o ValueError, y el
# resultado para quien opera debe ser el mismo.
# ============================================================================
ERROR_CONFIG: str | None = None
try:
    PLAN = construir_plan(
        cargar_config(VARIABLE_ODS, DEFAULT_CONFIG_ODS),
        cargar_config(VARIABLE_BDS, DEFAULT_CONFIG_BDS),
    )
    DOC_MD = plan_a_markdown(PLAN)
except Exception as exc:
    PLAN = Plan()
    ERROR_CONFIG = str(exc)
    DOC_MD = (
        f"# {DAG_ID} - configuracion invalida\n\n"
        f"El DAG no se pudo armar. Corrige las Variables `{VARIABLE_ODS}` y/o "
        f"`{VARIABLE_BDS}` (versionadas en `airflow/config/json/`) y vuelve a "
        f"sincronizarlas con `scripts/sync_variables.py`.\n\n"
        f"```\n{ERROR_CONFIG}\n```\n"
    )
    logger.error("%s no se pudo armar. %s", DAG_ID, ERROR_CONFIG)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alertar_fallo,
}


def crear_tarea(nodo: Nodo, callable_proceso) -> PythonOperator:
    cfg, grupo_cfg = nodo.cfg, nodo.grupo_cfg
    return PythonOperator(
        task_id=task_id_proceso(cfg),
        python_callable=callable_proceso,
        op_kwargs={"id_proceso": nodo.id_proceso, "nombre_grupo": nodo.grupo},
        retries=int(cfg.get("REINTENTOS", grupo_cfg.get("REINTENTOS", 0))),
        execution_timeout=timedelta(minutes=max(int(cfg.get("TIMEOUT_MINUTOS", 30)), 1)),
        pool=pool_de(cfg, grupo_cfg),
        doc_md=(
            f"**{nodo.capa} / {nodo.grupo}** - ID_PROCESO {nodo.id_proceso}  \n"
            f"Stored procedure: `{cfg.get('STORED_PROCEDURE')}`  \n"
            f"Origen: `{cfg.get('SCHEMA_ORIGEN')}.{cfg.get('TABLA_ORIGEN')}`  \n"
            f"Destino: `{cfg.get('SCHEMA_DESTINO')}.{cfg.get('TABLA_DESTINO')}`"
        ),
    )


def fallar_por_configuracion() -> None:
    raise AirflowException(ERROR_CONFIG)


with DAG(
    dag_id=DAG_ID,
    description="Orquesta DataHub completo: capa ODS por grupos y capa BDS con dependencias",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Con max_active_runs=1, una corrida atascada bloquea TODAS las siguientes.
    # execution_timeout protege cada tarea por separado, pero no cubre el tiempo
    # que una tarea pasa en queued (por ejemplo esperando un slot del pool) ni
    # en up_for_retry. dagrun_timeout es el tope duro de la corrida completa.
    dagrun_timeout=timedelta(hours=12),
    tags=["BT", "ODS", "BDS", "datahub", "singlestore"],
    default_args=default_args,
    doc_md=DOC_MD,
) as dag:

    if ERROR_CONFIG:
        PythonOperator(
            task_id="configuracion_invalida",
            python_callable=fallar_por_configuracion,
            execution_timeout=timedelta(minutes=1),
            retries=0,
            doc_md=DOC_MD,
        )
    else:
        inicio = EmptyOperator(task_id="inicio")
        ods_completo = EmptyOperator(task_id="ods_completo")
        bds_completo = EmptyOperator(task_id="bds_completo")
        fin = EmptyOperator(task_id="fin")

        tareas: dict[int, PythonOperator] = {}

        # ---------------- ODS: niveles por ORDEN ----------------------------
        niveles_ods: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for grupo_cfg in PLAN.ods:
            niveles_ods[int(grupo_cfg.get("ORDEN", 0))].append(grupo_cfg)

        nivel_anterior: list = [inicio]
        for orden in sorted(niveles_ods):
            nivel_actual = []
            for grupo_cfg in niveles_ods[orden]:
                modo = "paralelo" if grupo_cfg.get("PARALELO") else "secuencial"
                with TaskGroup(
                    group_id=task_id_grupo("ODS", grupo_cfg["NOMBRE"]),
                    tooltip=f"ODS - {grupo_cfg['NOMBRE']} (orden {orden}, {modo})",
                ) as grupo_task:
                    tarea_anterior = None
                    for proceso_cfg in grupo_cfg["PROCESOS"]:
                        pid = int(proceso_cfg["ID_PROCESO"])
                        task = crear_tarea(PLAN.nodos[pid], ejecutar_ods)
                        tareas[pid] = task
                        if not grupo_cfg.get("PARALELO", False) and tarea_anterior is not None:
                            tarea_anterior >> task
                        tarea_anterior = task
                nivel_anterior >> grupo_task
                nivel_actual.append(grupo_task)
            nivel_anterior = nivel_actual
        nivel_anterior >> ods_completo

        # ---------------- BDS: grupos + aristas del plan --------------------
        tareas_bds = []
        for grupo_cfg in PLAN.bds:
            modo = "paralelo" if grupo_cfg.get("PARALELO") else "secuencial"
            with TaskGroup(
                group_id=task_id_grupo("BDS", grupo_cfg["NOMBRE"]),
                tooltip=f"BDS - {grupo_cfg['NOMBRE']} (orden {grupo_cfg.get('ORDEN', 0)}, {modo})",
            ):
                for proceso_cfg in grupo_cfg["PROCESOS"]:
                    pid = int(proceso_cfg["ID_PROCESO"])
                    task = crear_tarea(PLAN.nodos[pid], ejecutar_bds)
                    tareas[pid] = task
                    tareas_bds.append(task)

        for origen, destino in PLAN.aristas_bds:
            (ods_completo if origen == "ods_completo" else tareas[origen]) >> tareas[destino]

        # bds_completo espera a TODAS las tareas BDS, y fin cuelga de el.
        #
        # La version anterior intentaba ademas colgar 'fin' solo de las hojas,
        # pero calculaba esas hojas DESPUES de enlazar bds_completo, asi que
        # para entonces ninguna tarea estaba ya sin downstream y la lista salia
        # siempre vacia: era codigo muerto que caia en este mismo enlace. Se
        # quito porque, ademas, era redundante: si bds_completo ya espera a
        # todas, colgar fin de las hojas no adelanta nada y solo llena el grafo.
        if tareas_bds:
            tareas_bds >> bds_completo
        else:
            ods_completo >> bds_completo
        bds_completo >> fin
