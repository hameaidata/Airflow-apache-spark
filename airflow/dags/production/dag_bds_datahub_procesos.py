"""
BDS_DATAHUB_PROCESOS - Orquesta la capa ODS y luego la capa BDS en un solo DAG.

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

DAG_ID = "BDS_DATAHUB_PROCESOS"

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


CAMPOS_DEPENDE_BDS = ("DEPENDE_DE", "DEPENDENCIAS", "BDS_DEPENDE_DE", "DEPENDE_DE_BDS")
CAMPOS_DEPENDE_ODS = ("DEPENDE_DE_ODS", "ODS_DEPENDE_DE", "REQUIERE_ODS")


def _ids_de(proceso_cfg: dict[str, Any], campos: tuple[str, ...],
            errores: list[str]) -> list[int]:
    """Acepta [], 2001, '2001,2002' o [2001, 2002].

    Igual que proceso_activo: un valor mal escrito se anota y se devuelve una
    lista vacia, para no cortar la validacion en el primer error.
    """
    valor = None
    for campo in campos:
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


def dependencias_proceso(proceso_cfg: dict[str, Any], errores: list[str]) -> list[int]:
    """Dependencias DENTRO de la capa BDS, por ID_PROCESO."""
    return _ids_de(proceso_cfg, CAMPOS_DEPENDE_BDS, errores)


def dependencias_ods(proceso_cfg: dict[str, Any], errores: list[str]) -> list[int]:
    """Procesos de la capa ODS que este proceso BDS necesita.

    POR QUE HACE FALTA UN CAMPO APARTE
    ----------------------------------
    Hasta el 2026-10-07 un proceso BDS colgaba de 'ods_completo' en bloque y no
    sabia de que tabla ODS dependia: no habia donde declararlo. La consecuencia
    es que un fallo en CUALQUIER tabla ODS tenia el mismo efecto sobre todo BDS,
    y al reves, un BDS podia correr sobre una tabla ODS que no se habia cargado.

    No se reutiliza DEPENDE_DE porque ahi los ids son de la capa BDS, y mezclar
    las dos numeraciones en un solo campo obliga a adivinar a que capa pertenece
    cada numero. Hoy no colisionan por casualidad; manana si.
    """
    return _ids_de(proceso_cfg, CAMPOS_DEPENDE_ODS, errores)


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


def ejecutar_bds(id_proceso: int, nombre_grupo: str,
                 tareas_ods: list[str] | None = None, **context):
    """Carga un proceso BDS, pero solo si lo que necesita de ODS salio bien.

    LA DEFENSA, NO LA PUERTA
    ------------------------
    Quien impide de verdad que esto corra es el GRAFO: DEPENDE_DE_ODS crea una
    arista real desde la tarea de ODS, con all_success, asi que si esa tarea
    fallo Airflow marca esta upstream_failed y no la programa. No arranca, no
    ocupa un slot del pool y no abre un log.

    Esta comprobacion es la red de seguridad para el unico caso que se escapa
    del grafo: que alguien haga Clear sobre esta tarea, a mano, mientras su
    insumo de ODS sigue en rojo. Ahi Airflow si la ejecutaria, y sin esto el
    stored procedure correria sobre una tabla ODS que no se cargo.

    Y se SALTA, no se falla. Que una tabla BDS no corra porque su insumo de ODS
    no se cargo no es un fallo de esta tabla: es la consecuencia correcta de un
    fallo que ya esta marcado en rojo en otro sitio. Marcarla en rojo tambien
    multiplicaria el mismo incidente por todas las tablas que cuelgan de el y
    haria mas dificil encontrar la causa.
    """
    from airflow.exceptions import AirflowSkipException

    requeridas = list(tareas_ods or [])
    if requeridas:
        corrida = context.get("dag_run")
        if corrida is None:
            raise AirflowException(
                "ejecutar_bds no recibio el contexto de la corrida y no puede "
                "comprobar sus dependencias de ODS. Es un error de "
                "programacion del DAG, no de configuracion.")

        fallidas = []
        for task_id in requeridas:
            ti = corrida.get_task_instance(task_id)
            estado = getattr(ti, "state", None)
            if str(estado) != "success":
                fallidas.append(f"{task_id} ({estado or 'no ejecutada'})")

        if fallidas:
            logger.warning(
                "ID_PROCESO=%s (%s) NO se ejecuta: su insumo de ODS no termino "
                "bien -> %s", id_proceso, nombre_grupo, ", ".join(fallidas))
            raise AirflowSkipException(
                f"Depende de ODS que no termino en exito: {', '.join(fallidas)}")

        logger.info("ID_PROCESO=%s: sus %s dependencia(s) de ODS estan en exito",
                    id_proceso, len(requeridas))

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
    # ID_PROCESO de BDS -> ID_PROCESO de ODS que necesita para poder correr.
    ods_requeridas: dict[int, list[int]] = field(default_factory=dict)


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
                if capa == "ODS" and dependencias_ods(p, errores):
                    errores.append(
                        f"{donde}: DEPENDE_DE_ODS solo tiene sentido en BDS. Dentro de "
                        f"ODS no hay dependencias de datos declaradas; por eso un fallo "
                        f"en una tabla ODS no detiene a las demas."
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

            # --- lo que este proceso BDS necesita de la capa ODS -----------
            requiere_ods = []
            for d in dependencias_ods(p, errores):
                nodo = plan.nodos.get(d)
                if nodo is not None and nodo.capa == "ODS":
                    requiere_ods.append(d)
                elif nodo is not None:
                    errores.append(
                        f"{VARIABLE_BDS}.{g['NOMBRE']}.ID_PROCESO={pid}: "
                        f"DEPENDE_DE_ODS={d} apunta a un proceso de la capa "
                        f"{nodo.capa}, no a ODS. Para depender de otro proceso "
                        f"BDS use DEPENDE_DE."
                    )
                elif d in inactivos_declarados:
                    errores.append(
                        f"{VARIABLE_BDS}.{g['NOMBRE']}.ID_PROCESO={pid}: "
                        f"DEPENDE_DE_ODS={d} existe pero esta ACTIVO='N'. Este "
                        f"proceso BDS se saltaria SIEMPRE. Active {d} o quite "
                        f"la dependencia."
                    )
                else:
                    errores.append(
                        f"{VARIABLE_BDS}.{g['NOMBRE']}.ID_PROCESO={pid}: "
                        f"DEPENDE_DE_ODS={d} no existe en {VARIABLE_ODS}."
                    )
            if requiere_ods:
                plan.ods_requeridas[pid] = requiere_ods
                # ARISTA REAL del grafo, no una comprobacion en ejecucion.
                #
                # La primera version colgaba estas tareas de 'ods_completo' y
                # comprobaba el estado de ODS dentro del callable. Funcionaba,
                # pero la tarea ARRANCABA: ocupaba un slot del pool, abria su
                # log y se saltaba dentro. Con la arista, Airflow la marca
                # upstream_failed y no la programa siquiera. "Ni siquiera
                # intentarlo" es literal.
                #
                # Y se puede hacer asi porque una dependencia de ODS y una
                # dependencia de BDS son la MISMA clase de cosa -datos- y las
                # dos quieren la misma regla, all_success. Lo que no se podia
                # mezclar era una dependencia de datos con 'ods_completo', que
                # es orden.
                for o in requiere_ods:
                    plan.aristas_bds.append((o, pid))

            if deps or requiere_ods:
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
    # Las aristas que nacen en un proceso de ODS no pueden cerrar un ciclo:
    # ODS no depende de nada y nada de ODS depende de BDS, y las dos cosas
    # estan validadas mas arriba. Se dejan fuera para que el recorrido hable
    # solo de dependencias entre procesos BDS, que es de lo que informa el
    # mensaje de error.
    adyacencia: dict[int, list[int]] = defaultdict(list)
    for origen, destino in plan.aristas_bds:
        if isinstance(origen, int) and plan.nodos[origen].capa == "BDS":
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
# escapa del modulo, Airflow marca el archivo como Broken DAG y BDS_DATAHUB_PROCESOS
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


def task_id_completo(nodo: Nodo) -> str:
    """El task_id tal como lo ve la corrida: '<group_id>.<task_id>'."""
    return f"{task_id_grupo(nodo.capa, nodo.grupo)}.{task_id_proceso(nodo.cfg)}"


def crear_tarea(nodo: Nodo, callable_proceso, trigger_rule: str = "all_success",
                tareas_ods: list[str] | None = None) -> PythonOperator:
    cfg, grupo_cfg = nodo.cfg, nodo.grupo_cfg
    kwargs = {"id_proceso": nodo.id_proceso, "nombre_grupo": nodo.grupo}
    if tareas_ods:
        kwargs["tareas_ods"] = tareas_ods
    return PythonOperator(
        task_id=task_id_proceso(cfg),
        python_callable=callable_proceso,
        trigger_rule=trigger_rule,
        op_kwargs=kwargs,
        retries=int(cfg.get("REINTENTOS", grupo_cfg.get("REINTENTOS", 0))),
        execution_timeout=timedelta(minutes=max(int(cfg.get("TIMEOUT_MINUTOS", 30)), 1)),
        pool=pool_de(cfg, grupo_cfg),
        doc_md=(
            f"**{nodo.capa} / {nodo.grupo}** - ID_PROCESO {nodo.id_proceso}  \n"
            f"Stored procedure: `{cfg.get('STORED_PROCEDURE')}`  \n"
            f"Origen: `{cfg.get('SCHEMA_ORIGEN')}.{cfg.get('TABLA_ORIGEN')}`  \n"
            f"Destino: `{cfg.get('SCHEMA_DESTINO')}.{cfg.get('TABLA_DESTINO')}`"
            + (f"  \nRequiere de ODS: `{', '.join(tareas_ods)}`" if tareas_ods else "")
            + (f"  \nRegla de disparo: `{trigger_rule}`"
               if trigger_rule != "all_success" else "")
        ),
    )


def fallar_por_configuracion() -> None:
    raise AirflowException(ERROR_CONFIG)


def resumen_de_la_corrida(**context) -> dict:
    """Cuenta como acabo cada tabla y falla si alguna fallo.

    POR QUE ESTA TAREA EXISTE
    -------------------------
    Que un fallo no trunque el proceso no quiere decir que se oculte. Con
    all_done repartido por el grafo, la ultima tarea terminaria siempre en
    exito y Airflow marcaria la CORRIDA ENTERA en verde aunque dentro hubiera
    tablas en rojo. Nadie mira el detalle de una corrida verde.

    Asi que aqui se recorre el estado real de las tareas, se deja un resumen
    legible en el log -que es lo que alguien quiere ver a las siete de la
    manana- y se falla si hubo algun fallo.
    """
    corrida = context["dag_run"]
    por_estado: dict[str, list[str]] = defaultdict(list)
    for ti in corrida.get_task_instances():
        if ti.task_id in ("inicio", "ods_completo", "bds_completo", "fin"):
            continue
        por_estado[str(ti.state)].append(ti.task_id)

    total = sum(len(v) for v in por_estado.values())
    logger.info("RESUMEN DE LA CORRIDA  (%s tabla(s))", total)
    for estado in sorted(por_estado):
        logger.info("  %-14s %s", estado, len(por_estado[estado]))
        for task_id in sorted(por_estado[estado]):
            logger.info("      %s", task_id)

    fallidas = por_estado.get("failed", []) + por_estado.get("upstream_failed", [])
    saltadas = por_estado.get("skipped", [])
    if saltadas:
        logger.warning(
            "%s tabla(s) no se ejecutaron porque su insumo no estaba: %s",
            len(saltadas), ", ".join(sorted(saltadas)))

    resumen = {estado: len(tareas) for estado, tareas in por_estado.items()}
    if fallidas:
        raise AirflowException(
            f"La corrida termino con {len(fallidas)} tabla(s) en error: "
            f"{', '.join(sorted(fallidas))}.\n"
            f"Las demas SI se cargaron -por eso el proceso no se trunco- y "
            f"{len(saltadas)} se saltaron por depender de estas. "
            f"Reintente solo las fallidas y despues sus dependientes.")
    logger.info("Todas las tablas que entraron en la corrida terminaron bien.")
    return resumen


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
    tags=["produccion", "manual", "bds", "ods", "datahub", "bantotal", "singlestore"],
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
        # all_done en los tres: la capa ODS tiene que TERMINAR, y terminar
        # incluye el caso de que alguna tabla se haya caido. Con all_success,
        # ods_completo se quedaba sin ejecutar y arrastraba a toda la capa BDS.
        ods_completo = EmptyOperator(
            task_id="ods_completo", trigger_rule="all_done",
            doc_md=("La capa ODS termino. **No** significa que todas sus tablas "
                    "hayan salido bien: significa que ninguna quedo pendiente. "
                    "Quien decide si una tabla BDS puede correr es su propio "
                    "`DEPENDE_DE_ODS`."))
        bds_completo = EmptyOperator(task_id="bds_completo", trigger_rule="all_done")
        fin = PythonOperator(
            task_id="fin",
            python_callable=resumen_de_la_corrida,
            trigger_rule="all_done",
            retries=0,
            execution_timeout=timedelta(minutes=5),
            doc_md=("Resumen de la corrida. Falla si alguna tabla fallo: que un "
                    "fallo no trunque el proceso no quiere decir que se oculte. "
                    "Sin esta tarea la corrida saldria en verde con tablas en "
                    "rojo dentro."))

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
                        # all_done en TODA la capa ODS, y es deliberado.
                        #
                        # ODS no declara dependencias de datos -DEPENDE_DE esta
                        # prohibido ahi-, asi que el encadenamiento por ORDEN y
                        # por PARALELO=false es orden de ejecucion, no de datos:
                        # existe para no abrir veinte conexiones al core a la
                        # vez, no porque una tabla necesite a la anterior.
                        #
                        # Con all_success, una tabla caida bloqueaba el resto de
                        # su cadena, el siguiente nivel de ORDEN, ods_completo y
                        # con eso la capa BDS entera. Un incidente en una tabla
                        # se llevaba por delante la carga del dia.
                        task = crear_tarea(PLAN.nodos[pid], ejecutar_ods,
                                           trigger_rule="all_done")
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
                    # Los padres de una tarea BDS son de UNA de dos clases, y
                    # nunca de las dos a la vez (lo garantiza construir_plan):
                    #
                    #   con DEPENDE_DE -> dependencias de DATOS entre procesos
                    #       BDS. all_success: si el insumo fallo, esta no corre.
                    #   sin DEPENDE_DE -> solo ORDEN (el nivel anterior, o
                    #       ods_completo). all_done: el nivel anterior puede
                    #       traer tablas caidas y eso no es razon para no
                    #       intentar esta, que no depende de ellas.
                    #
                    # Lo que esta tarea necesite de la capa ODS NO se expresa
                    # aqui: va por DEPENDE_DE_ODS y lo comprueba ejecutar_bds,
                    # porque una sola trigger_rule no puede decir dos cosas
                    # distintas a padres distintos.
                    deps_bds = dependencias_proceso(proceso_cfg, [])
                    requiere = [task_id_completo(PLAN.nodos[d])
                                for d in PLAN.ods_requeridas.get(pid, [])]
                    # Una dependencia de ODS y una de BDS son la misma clase de
                    # cosa -datos- y piden la misma regla. Solo quien no declara
                    # ninguna cuelga de 'ods_completo', que es orden.
                    con_datos = bool(deps_bds or requiere)
                    task = crear_tarea(
                        PLAN.nodos[pid], ejecutar_bds,
                        trigger_rule="all_success" if con_datos else "all_done",
                        tareas_ods=requiere,
                    )
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
