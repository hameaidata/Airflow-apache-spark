from __future__ import annotations

import logging
import re
import socket
from datetime import datetime, timedelta
from typing import Any

import singlestoredb as s2
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.task_group import TaskGroup


logger = logging.getLogger(__name__)

VARIABLE_CONFIG = "DAG_ODS_TABLAS"
SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"
TABLA_CFG_PROCESOS = "CTL_CFG_PROCESOS"

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")

DEFAULT_CONFIG: dict[str, Any] = {
    "CAPA": "ODS",
    "AMBIENTE": "PRD",
    "EJECUCION": {
        "MODO": "NORMAL",
        "RUN_ID": "{{ run_id }}",
        "FECHA_PROCESO": "{{ ds }}",
    },
    "GRUPOS": [],
    "AUDITORIA": {
        "REGISTRAR_LOG": True,
        "TABLA_LOG": "CONTROL_EJECUCIONES",
        "REGISTRAR_REGISTROS_PROCESADOS": True,
        "REGISTRAR_DURACION": True,
        "REGISTRAR_ERROR": True,
    },
    "SPARK": {
        "HABILITADO": False,
        "CORES": 4,
        "EXECUTORES": 2,
        "MEMORY_GB": 8,
    },
}


def cargar_config(parse_time: bool = False) -> dict[str, Any]:
    """Lee el JSON de Airflow usado como contrato del DAG."""
    try:
        logger.info(f"[INFO] Iniciando la ejecucion -->>")
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
logger.info(
    "CONFIG_PARSE: %s",
    CONFIG_PARSE
)

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
            logger.exception("[ERROR] Obtencion conexion SingleStore")
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
    if not valor or not IDENTIFIER_RE.match(valor):
        raise AirflowException(f"{campo} invalido: {valor!r}")
    return valor


def nombre_sp(proceso: dict[str, Any]) -> str:
    logger.info(f"Mostrando el flujo de nombre_sp")
    sp = validar_identificador(proceso["STORED_PROCEDURE"], "STORED_PROCEDURE")
    if "." in sp:
        return sp
    schema = proceso.get("SCHEMA_DESTINO")
    if schema:
        return f"{validar_identificador(schema, 'SCHEMA_DESTINO')}.{sp}"
    return sp


def render_valor(valor: Any, context: dict[str, Any]) -> Any:
    """Render simple para los placeholders guardados dentro del JSON."""
    if isinstance(valor, str):
        return (
            valor.replace("{{ run_id }}", context["run_id"])
            .replace("{{run_id}}", context["run_id"])
            .replace("{{ ds }}", context["ds"])
            .replace("{{ds}}", context["ds"])
        )
    if isinstance(valor, list):
        return [render_valor(item, context) for item in valor]
    if isinstance(valor, dict):
        return {k: render_valor(v, context) for k, v in valor.items()}
    return valor


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
            logger.info(f"Columnas {columnas}")
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
            auditoria.get("TABLA_LOG", "MON_EJECUCIONES"),
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
                "[%s] Ejecutando %s: %s",
                proceso["NOMBRE_PROCESO"],
                etapa,
                sql,
            )
            cursor.execute(sql)

    @staticmethod
    def ejecutar_sp(cursor, proceso: dict[str, Any]) -> None:
        parametros = proceso.get("PARAMETROS") or []
        logger.info(f"Mostrando los parametros {parametros}")
        marcadores = ", ".join(["%s"] * len(parametros))
        llamada = f"CALL {nombre_sp(proceso)}({marcadores})"
        logger.info("[%s] Ejecutando %s", proceso["NOMBRE_PROCESO"], llamada)
        cursor.execute(llamada, tuple(parametros))

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
            logger.info(f"Mostrando la secuencia de la linea 1 {proceso}")
            ProcesoExecutor.ejecutar_sp(cursor, proceso)
            logger.info(f"Mostrando la secuencia de la linea 2")
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
            f"{proceso_json.get('NOMBRE_PROCESO')} no esta activo en {TABLA_CFG_PROCESOS}."
        )
        if validar_cfg:
            raise AirflowException(mensaje)
        logger.warning("%s Se omite porque VALIDAR_CFG=false.", mensaje)
        return None

    diferencias = []
    for campo_json, campo_db in (("NOMBRE_PROCESO", "NOMBRE_PROCESO"), ("STORED_PROCEDURE", "STORED_PROCEDURE"), ("TIPO_PROCESO", "TIPO_PROCESO"),):
        valor_json = proceso_json.get(campo_json)
        valor_db = proceso_db.get(campo_db)
        if valor_json and valor_db and str(valor_json).upper() != str(valor_db).upper():
            diferencias.append(f"{campo_json}: JSON={valor_json} DB={valor_db}")

    grupo_db = proceso_db.get("GRUPO_PROCESO")
    if grupo_db and str(grupo_db).upper() != str(nombre_grupo).upper():
        diferencias.append(f"GRUPO_PROCESO: JSON={nombre_grupo} DB={grupo_db}")

    if diferencias and validar_cfg:
        raise AirflowException(
            f"Configuracion inconsistente para ID_PROCESO={proceso_json['ID_PROCESO']}: "
            + "; ".join(diferencias)
        )

    proceso = {**proceso_db, **proceso_json}
    proceso["CAPA"] = config.get("CAPA", proceso_db.get("CAPA", "ODS"))
    proceso["AMBIENTE"] = config.get("AMBIENTE", "PRD")
    proceso["_GRUPO_JSON"] = nombre_grupo
    proceso["PARAMETROS"] = proceso.get("PARAMETROS", [])
    return proceso


def ejecutar_proceso(
    proceso: dict[str, Any],
    auditoria: dict[str, Any],
    spark_cfg: dict[str, Any],
    airflow_ctx: dict[str, str],
) -> dict[str, Any]:
    tipo = str(proceso.get("TIPO_PROCESO", "SP")).upper()
    usar_spark = tipo == "SPARK" or proceso.get("USAR_SPARK") is True
    logger.info(f"Mostrando la ejecucion de procesos{proceso} ")
    if usar_spark or (spark_cfg.get("HABILITADO") and tipo != "SP"):
        raise AirflowException(
            "Este proceso esta marcado para Spark, pero table_ods.py todavia "
            "ejecuta ODS directo en SingleStore. El siguiente paso es derivar "
            "estos procesos al plugin BsgSparkJdbcOperator."
        )

    if tipo != "SP":
        raise AirflowException(
            f"TIPO_PROCESO no soportado para {proceso['NOMBRE_PROCESO']}: {tipo}"
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
    logger.info(f"[INFO] Inciando proceso de ejecucion desde el archivo")
    context = get_current_context()
    config = render_valor(cargar_config(parse_time=False), context)
    auditoria = config.get("AUDITORIA", DEFAULT_CONFIG["AUDITORIA"])
    spark_cfg = config.get("SPARK", DEFAULT_CONFIG["SPARK"])
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

    SingleStoreConnection.validar_conectividad()
    activos_db = ConfigRepository.obtener_activos(config.get("CAPA", "ODS"))
    proceso_json = buscar_proceso_json(config, nombre_grupo, id_proceso)
    logger.info(f"[INFO] Mostrando la linea de comentario")
    proceso_db = activos_db.get(int(proceso_json["ID_PROCESO"]))
    proceso = validar_y_enriquecer(
        proceso_json,
        proceso_db,
        config,
        nombre_grupo,
    )
    logger.info(f"[INFO] Mostrandome el contenido de la respuesta {proceso}")
    if not proceso:
        return None

    logger.info(
        "Ejecutando proceso ODS | grupo=%s id=%s nombre=%s",
        nombre_grupo,
        proceso["ID_PROCESO"],
        proceso["NOMBRE_PROCESO"],
    )
    return ejecutar_proceso(proceso, auditoria, spark_cfg, airflow_ctx)


def grupos_ordenados(config: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(config.get("GRUPOS", []), key=lambda item: int(item.get("ORDEN", 0)))


def task_id_grupo(nombre: str) -> str:
    task_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", nombre.lower()).strip("_.-")
    return f"grupo_{task_id or 'sin_nombre'}"


def task_id_proceso(proceso_cfg: dict[str, Any]) -> str:
    nombre = str(proceso_cfg.get("NOMBRE_PROCESO") or "proceso")
    nombre = re.sub(r"[^A-Za-z0-9_.-]+", "_", nombre.lower()).strip("_.-")
    return f"p_{int(proceso_cfg['ID_PROCESO'])}_{nombre or 'sin_nombre'}"


def timeout_grupo_minutos(grupo_cfg: dict[str, Any]) -> int:
    procesos = grupo_cfg.get("PROCESOS") or []
    if not procesos:
        return 30
    return max(int(proceso.get("TIMEOUT_MINUTOS", 30)) for proceso in procesos)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ODS_PROCESOS_DATAHUB",
    description="Orquesta procesos ODS validados contra CTL_CFG_PROCESOS y DAG_ODS_TABLAS",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ODS", "datahub", "singlestore"],
    default_args=default_args,
) as dag:
    grupo_anterior = None

    for grupo_cfg in grupos_ordenados(CONFIG_PARSE):
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

                if not grupo_cfg.get("PARALELO", False) and tarea_anterior:
                    tarea_anterior >> task
                tarea_anterior = task

        if grupo_anterior:
            grupo_anterior >> grupo_task
        grupo_anterior = grupo_task
