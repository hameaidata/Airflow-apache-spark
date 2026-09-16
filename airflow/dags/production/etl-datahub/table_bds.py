from __future__ import annotations

import logging
import re
import socket
from datetime import datetime, timedelta
from typing import Any

import singlestoredb as s2
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.task_group import TaskGroup


logger = logging.getLogger(__name__)

VARIABLE_CONFIG = "DAG_BDS_TABLAS"
SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"
TABLA_CFG_PROCESOS = "CTL_CFG_PROCESOS"
CAPA_DEFAULT = "BDS"
AMBIENTE_DEFAULT = "PRD"

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
TASK_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")

DEFAULT_AUDITORIA: dict[str, Any] = {
    "REGISTRAR_LOG": True,
    "TABLA_LOG": "CONTROL_EJECUCIONES",
    "REGISTRAR_DURACION": True,
    "REGISTRAR_ERROR": True,
}

DEFAULT_CONFIG: dict[str, Any] = {
    "CAPA": CAPA_DEFAULT,
    "AMBIENTE": AMBIENTE_DEFAULT,
    "EJECUCION": {
        "MODO": "NORMAL",
        "RUN_ID": "{{ run_id }}",
        "FECHA_PROCESO": "{{ ds }}",
    },
    "GRUPOS": [],
    "AUDITORIA": DEFAULT_AUDITORIA,
}


def cargar_config(parse_time: bool = False) -> dict[str, Any]:
    """Lee la Variable JSON que declara los procesos BDS."""
    try:
        return Variable.get(VARIABLE_CONFIG, deserialize_json=True)
    except Exception as exc:
        if parse_time:
            logger.warning(
                "No se pudo leer Variable %s durante el parseo: %s",
                VARIABLE_CONFIG,
                exc,
            )
            return DEFAULT_CONFIG
        raise AirflowException(
            f"No se pudo leer la Variable {VARIABLE_CONFIG} como JSON valido."
        ) from exc


CONFIG_PARSE = cargar_config(parse_time=True)


class SingleStoreConnection:
    @staticmethod
    def get_connection():
        try:
            conn_airflow = BaseHook.get_connection(SINGLESTORE_CONN_ID)
            conn = s2.connect(
                host=conn_airflow.host,
                port=conn_airflow.port,
                user=conn_airflow.login,
                password=conn_airflow.password,
                database=conn_airflow.schema,
                connect_timeout=30,
                autocommit=False,
            )
            logger.info(
                "Conexion SingleStore establecida [%s:%s/%s]",
                conn_airflow.host,
                conn_airflow.port,
                conn_airflow.schema,
            )
            return conn
        except Exception as exc:
            logger.exception("No se pudo obtener conexion SingleStore")
            raise AirflowException(str(exc)) from exc

    @staticmethod
    def validar_conectividad() -> None:
        try:
            conn_airflow = BaseHook.get_connection(SINGLESTORE_CONN_ID)
            sock = socket.create_connection(
                (conn_airflow.host, int(conn_airflow.port)),
                timeout=10,
            )
            sock.close()
            logger.info(
                "Validacion TCP exitosa [%s:%s]",
                conn_airflow.host,
                conn_airflow.port,
            )
        except Exception as exc:
            raise AirflowException(f"[ERROR] conectividad TCP: {exc}") from exc


def validar_identificador(valor: str, campo: str) -> str:
    if not valor or not IDENTIFIER_RE.match(str(valor)):
        raise AirflowException(f"{campo} invalido: {valor!r}")
    return str(valor)


def validar_identificadores_opcionales(proceso: dict[str, Any]) -> None:
    for campo in (
        "SCHEMA_ORIGEN",
        "SCHEMA_DESTINO",
        "TABLA_ORIGEN",
        "TABLA_DESTINO",
    ):
        valor = proceso.get(campo)
        if valor:
            validar_identificador(str(valor), campo)


def nombre_sp(proceso: dict[str, Any]) -> str:
    sp = validar_identificador(proceso["STORED_PROCEDURE"], "STORED_PROCEDURE")
    if "." in sp:
        return sp

    schema = proceso.get("SCHEMA_DESTINO") or proceso.get("SCHEMA_SP")
    if schema:
        return f"{validar_identificador(str(schema), 'SCHEMA_DESTINO')}.{sp}"
    return sp


def render_valor(valor: Any, context: dict[str, Any]) -> Any:
    """Render simple para placeholders usados dentro del JSON."""
    if isinstance(valor, str):
        return (
            valor.replace("{{ run_id }}", context["run_id"])
            .replace("{{run_id}}", context["run_id"])
            .replace("{{ ds }}", context["ds"])
            .replace("{{ds}}", context["ds"])
            .replace("{{ ts }}", context["ts"])
            .replace("{{ts}}", context["ts"])
        )
    if isinstance(valor, list):
        return [render_valor(item, context) for item in valor]
    if isinstance(valor, dict):
        return {key: render_valor(item, context) for key, item in valor.items()}
    return valor


def normalizar_estado_activo(valor: Any, campo: str) -> bool:
    """Convierte banderas de activacion del JSON a booleano."""
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


def proceso_activo(proceso_cfg: dict[str, Any], nombre_grupo: str) -> bool:
    """Indica si un proceso declarado en DAG_BDS_TABLAS debe entrar al DAG."""
    for campo in ("ACTIVO", "HABILITADO", "ESTADO"):
        if campo in proceso_cfg:
            return normalizar_estado_activo(
                proceso_cfg[campo],
                f"GRUPOS.{nombre_grupo}.PROCESOS.{proceso_cfg.get('ID_PROCESO')}.{campo}",
            )

    raise AirflowException(
        f"El proceso {proceso_cfg.get('ID_PROCESO')!r} / "
        f"{proceso_cfg.get('NOMBRE_PROCESO')!r} del grupo {nombre_grupo!r} "
        f"en {VARIABLE_CONFIG} debe declarar ACTIVO='S' o ACTIVO='N'."
    )


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
                    f"pero esa dependencia no existe o esta inactiva en {VARIABLE_CONFIG}."
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


class ConfigRepository:
    @staticmethod
    def obtener_activos(capa: str) -> dict[int, dict[str, Any]]:
        SingleStoreConnection.validar_conectividad()
        conn = SingleStoreConnection.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT  *
                FROM    {TABLA_CFG_PROCESOS}
                WHERE   ACTIVO = 'S'
                  AND   CAPA = %s
                """,
                (capa,),
            )
            columnas = [col[0].upper() for col in cursor.description]
            if "ID_PROCESO" not in columnas:
                raise AirflowException(
                    f"{TABLA_CFG_PROCESOS} debe devolver la columna ID_PROCESO."
                )

            id_idx = columnas.index("ID_PROCESO")
            return {
                int(row[id_idx]): {
                    **{columna: row[idx] for idx, columna in enumerate(columnas)},
                    "ID_PROCESO": int(row[id_idx]),
                }
                for row in cursor.fetchall()
            }
        finally:
            conn.close()


class LogRepository:
    @staticmethod
    def registrar(
        conn,
        proceso: dict[str, Any],
        auditoria: dict[str, Any],
        airflow_ctx: dict[str, str],
        estado: str,
        inicio: datetime,
        fin: datetime,
        error: str | None = None,
    ) -> None:
        if not auditoria.get("REGISTRAR_LOG", True):
            return

        tabla_log = validar_identificador(
            auditoria.get("TABLA_LOG", "CONTROL_EJECUCIONES"),
            "AUDITORIA.TABLA_LOG",
        )
        duracion = round((fin - inicio).total_seconds(), 2)
        mensaje_error = (error or "")[:4000] if auditoria.get("REGISTRAR_ERROR", True) else None
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO {tabla_log}
                (ID_PROCESO, DAG_ID, RUN_ID, TASK_ID, CAPA, GRUPO_PROCESO,
                 NOMBRE_PROCESO, FECHA_INICIO, FECHA_FIN, DURACION_SEGUNDOS,
                 ESTADO, MENSAJE_ERROR)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                proceso["ID_PROCESO"],
                airflow_ctx["dag_id"],
                airflow_ctx["run_id"],
                airflow_ctx["task_id"],
                proceso["CAPA"],
                proceso["_GRUPO_JSON"],
                proceso["NOMBRE_PROCESO"],
                inicio,
                fin,
                duracion if auditoria.get("REGISTRAR_DURACION", True) else None,
                estado,
                mensaje_error,
            ),
        )
        conn.commit()


class ProcesoExecutor:
    @staticmethod
    def ejecutar_sqls(cursor, sqls: list[str], etapa: str, proceso: dict[str, Any]) -> None:
        for sql in sqls or []:
            if not sql or not str(sql).strip():
                continue
            logger.info(
                "[%s] Ejecutando %s",
                proceso["NOMBRE_PROCESO"],
                etapa,
            )
            cursor.execute(sql)

    @staticmethod
    def ejecutar_sp(cursor, proceso: dict[str, Any]) -> None:
        parametros = tuple(proceso.get("PARAMETROS") or [])
        marcadores = ", ".join(["%s"] * len(parametros))
        llamada = f"CALL {nombre_sp(proceso)}({marcadores})"
        logger.info(
            "[%s] Ejecutando stored procedure BDS: %s",
            proceso["NOMBRE_PROCESO"],
            llamada,
        )
        cursor.execute(llamada, parametros)

    @staticmethod
    def ejecutar_singlestore(
        proceso: dict[str, Any],
        auditoria: dict[str, Any],
        airflow_ctx: dict[str, str],
    ) -> dict[str, Any]:
        inicio = datetime.now()
        estado = "SUCCESS"
        error = None
        conn = SingleStoreConnection.get_connection()
        try:
            cursor = conn.cursor()
            ProcesoExecutor.ejecutar_sqls(cursor, proceso.get("PRE_SQL", []), "PRE_SQL", proceso)
            ProcesoExecutor.ejecutar_sp(cursor, proceso)
            ProcesoExecutor.ejecutar_sqls(cursor, proceso.get("POST_SQL", []), "POST_SQL", proceso)
            conn.commit()
            return {"ID_PROCESO": proceso["ID_PROCESO"], "ESTADO": estado}
        except Exception as exc:
            conn.rollback()
            estado = "FAILED"
            error = str(exc)
            raise
        finally:
            try:
                LogRepository.registrar(
                    conn=conn,
                    proceso=proceso,
                    auditoria=auditoria,
                    airflow_ctx=airflow_ctx,
                    estado=estado,
                    inicio=inicio,
                    fin=datetime.now(),
                    error=error,
                )
            finally:
                conn.close()


def procesos_json_por_grupo(config: dict[str, Any], nombre_grupo: str) -> list[dict[str, Any]]:
    for grupo in config.get("GRUPOS", []):
        if grupo.get("NOMBRE") == nombre_grupo:
            procesos = grupo.get("PROCESOS", [])
            return sorted(procesos, key=lambda item: int(item.get("ID_PROCESO", 0)))
    return []


def validar_y_enriquecer(
    proceso_json: dict[str, Any],
    proceso_db: dict[str, Any] | None,
    config: dict[str, Any],
    nombre_grupo: str,
) -> dict[str, Any] | None:
    validar_cfg = proceso_json.get("VALIDAR_CFG", True)

    if proceso_db is None:
        mensaje = (
            f"Proceso {proceso_json.get('ID_PROCESO')} / "
            f"{proceso_json.get('NOMBRE_PROCESO')} no esta activo en "
            f"{TABLA_CFG_PROCESOS} para CAPA={CAPA_DEFAULT}."
        )
        if validar_cfg:
            raise AirflowException(mensaje)
        logger.warning("%s Se omite porque VALIDAR_CFG=false.", mensaje)
        return None

    diferencias = []
    for campo in ("NOMBRE_PROCESO", "STORED_PROCEDURE", "TIPO_PROCESO"):
        valor_json = proceso_json.get(campo)
        valor_db = proceso_db.get(campo)
        if valor_json and valor_db and str(valor_json).upper() != str(valor_db).upper():
            diferencias.append(f"{campo}: JSON={valor_json} DB={valor_db}")

    grupo_db = proceso_db.get("GRUPO_PROCESO")
    if grupo_db and str(grupo_db).upper() != str(nombre_grupo).upper():
        diferencias.append(f"GRUPO_PROCESO: JSON={nombre_grupo} DB={grupo_db}")

    capa_db = proceso_db.get("CAPA")
    if capa_db and str(capa_db).upper() != CAPA_DEFAULT:
        diferencias.append(f"CAPA: esperado={CAPA_DEFAULT} DB={capa_db}")

    if diferencias and validar_cfg:
        raise AirflowException(
            f"Configuracion inconsistente para ID_PROCESO={proceso_json['ID_PROCESO']}: "
            + "; ".join(diferencias)
        )

    proceso = {**proceso_db, **proceso_json}
    proceso["CAPA"] = CAPA_DEFAULT
    proceso["AMBIENTE"] = config.get("AMBIENTE", AMBIENTE_DEFAULT)
    proceso["_GRUPO_JSON"] = nombre_grupo
    proceso["PARAMETROS"] = proceso.get("PARAMETROS", [])
    validar_identificadores_opcionales(proceso)
    return proceso


def ejecutar_proceso(
    proceso: dict[str, Any],
    auditoria: dict[str, Any],
    airflow_ctx: dict[str, str],
) -> dict[str, Any]:
    tipo = str(proceso.get("TIPO_PROCESO", "SP")).upper()
    if tipo != "SP":
        raise AirflowException(
            f"TIPO_PROCESO no soportado para BDS en {proceso['NOMBRE_PROCESO']}: {tipo}. "
            "Este DAG ejecuta procedimientos almacenados en SingleStore."
        )

    return ProcesoExecutor.ejecutar_singlestore(proceso, auditoria, airflow_ctx)


def buscar_proceso_json(
    config: dict[str, Any],
    nombre_grupo: str,
    id_proceso: int,
) -> dict[str, Any]:
    for proceso in procesos_json_por_grupo(config, nombre_grupo):
        if int(proceso["ID_PROCESO"]) == int(id_proceso):
            return proceso
    raise AirflowException(
        f"ID_PROCESO={id_proceso} no existe en el grupo {nombre_grupo!r} "
        f"de la Variable {VARIABLE_CONFIG}."
    )


def ejecutar_proceso_desde_json(id_proceso: int, nombre_grupo: str) -> dict[str, Any] | None:
    context = get_current_context()
    config = render_valor(cargar_config(parse_time=False), context)
    auditoria = config.get("AUDITORIA", DEFAULT_AUDITORIA)
    airflow_ctx = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "task_id": context["task"].task_id,
    }

    grupo = next(
        (item for item in config.get("GRUPOS", []) if item.get("NOMBRE") == nombre_grupo),
        None,
    )
    if not grupo:
        raise AirflowException(f"Grupo {nombre_grupo!r} no existe en {VARIABLE_CONFIG}.")

    activos_db = ConfigRepository.obtener_activos(CAPA_DEFAULT)
    proceso_json = buscar_proceso_json(config, nombre_grupo, id_proceso)
    if not proceso_activo(proceso_json, nombre_grupo):
        raise AirflowSkipException(
            f"Proceso {id_proceso} del grupo {nombre_grupo!r} esta inactivo "
            f"en {VARIABLE_CONFIG}; no se ejecuta."
        )

    proceso_db = activos_db.get(int(proceso_json["ID_PROCESO"]))
    proceso = validar_y_enriquecer(
        proceso_json,
        proceso_db,
        config,
        nombre_grupo,
    )
    if not proceso:
        return None

    logger.info(
        "Ejecutando proceso BDS | grupo=%s id=%s nombre=%s sp=%s",
        nombre_grupo,
        proceso["ID_PROCESO"],
        proceso["NOMBRE_PROCESO"],
        proceso["STORED_PROCEDURE"],
    )
    return ejecutar_proceso(proceso, auditoria, airflow_ctx)


def grupos_ordenados(config: dict[str, Any]) -> list[dict[str, Any]]:
    grupos_con_procesos_activos = []
    for grupo_cfg in config.get("GRUPOS", []):
        nombre_grupo = grupo_cfg.get("NOMBRE", "sin_nombre")
        procesos_activos = []
        for proceso_cfg in grupo_cfg.get("PROCESOS", []):
            if proceso_activo(proceso_cfg, nombre_grupo):
                procesos_activos.append(proceso_cfg)
            else:
                logger.info(
                    "Proceso BDS omitido por configuracion: grupo=%s id=%s nombre=%s activo=%s",
                    nombre_grupo,
                    proceso_cfg.get("ID_PROCESO"),
                    proceso_cfg.get("NOMBRE_PROCESO"),
                    proceso_cfg.get(
                        "ACTIVO",
                        proceso_cfg.get("HABILITADO", proceso_cfg.get("ESTADO")),
                    ),
                )

        if procesos_activos:
            grupos_con_procesos_activos.append({**grupo_cfg, "PROCESOS": procesos_activos})

    return sorted(grupos_con_procesos_activos, key=lambda item: int(item.get("ORDEN", 0)))


def task_id_grupo(nombre: str) -> str:
    task_id = TASK_ID_RE.sub("_", nombre.lower()).strip("_.-")
    return f"grupo_{task_id or 'sin_nombre'}"


def task_id_proceso(proceso_cfg: dict[str, Any]) -> str:
    nombre = str(proceso_cfg.get("NOMBRE_PROCESO") or "proceso")
    nombre = TASK_ID_RE.sub("_", nombre.lower()).strip("_.-")
    return f"p_{int(proceso_cfg['ID_PROCESO'])}_{nombre or 'sin_nombre'}"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


GRUPOS_PARSE = grupos_ordenados(CONFIG_PARSE)
validar_dependencias_bds(GRUPOS_PARSE)


with DAG(
    dag_id="BDS_PROCESOS_DATAHUB",
    description="Orquesta procesos BDS desde ODS usando stored procedures en SingleStore",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["BDS", "ODS", "datahub", "singlestore"],
    default_args=default_args,
) as dag:
    tareas_por_id = {}
    tareas_con_dependencias = set()

    for grupo_cfg in GRUPOS_PARSE:
        nombre = grupo_cfg["NOMBRE"]
        procesos = sorted(
            grupo_cfg.get("PROCESOS", []),
            key=lambda item: int(item.get("ID_PROCESO", 0)),
        )

        with TaskGroup(group_id=task_id_grupo(nombre), tooltip=nombre) as grupo_task:
            tarea_anterior = None

            for proceso_cfg in procesos:
                task = PythonOperator(
                    task_id=task_id_proceso(proceso_cfg),
                    python_callable=ejecutar_proceso_desde_json,
                    op_kwargs={
                        "id_proceso": int(proceso_cfg["ID_PROCESO"]),
                        "nombre_grupo": nombre,
                    },
                    retries=int(proceso_cfg.get("REINTENTOS", grupo_cfg.get("REINTENTOS", 0))),
                    execution_timeout=timedelta(
                        minutes=max(int(proceso_cfg.get("TIMEOUT_MINUTOS", 30)), 1)
                    ),
                )
                tareas_por_id[int(proceso_cfg["ID_PROCESO"])] = task
                if dependencias_proceso(proceso_cfg):
                    tareas_con_dependencias.add(int(proceso_cfg["ID_PROCESO"]))

                if (
                    not grupo_cfg.get("PARALELO", False)
                    and tarea_anterior
                    and int(proceso_cfg["ID_PROCESO"]) not in tareas_con_dependencias
                ):
                    tarea_anterior >> task
                tarea_anterior = task

    for grupo_cfg in GRUPOS_PARSE:
        for proceso_cfg in grupo_cfg.get("PROCESOS", []):
            proceso_id = int(proceso_cfg["ID_PROCESO"])
            for dependencia_id in dependencias_proceso(proceso_cfg):
                if dependencia_id not in tareas_por_id:
                    raise AirflowException(
                        f"ID_PROCESO={proceso_id} depende de ID_PROCESO={dependencia_id}, "
                        f"pero la dependencia no existe o esta inactiva en {VARIABLE_CONFIG}."
                    )
<<<<<<< HEAD
                tareas_por_id[dependencia_id] >> tareas_por_id[proceso_id]
=======
                tareas_por_id[dependencia_id] >> tareas_por_id[proceso_id]
>>>>>>> f5bf0dba400d07a1260b49ac053c79a744d8ad46
