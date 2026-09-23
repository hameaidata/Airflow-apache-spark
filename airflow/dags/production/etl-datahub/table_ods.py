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

VARIABLE_CONFIG = "DAG_ODS_TABLAS"
SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"
TABLA_CFG_PROCESOS = "CTL_CFG_PROCESOS"

# Nombre unico de la tabla de log. Antes habia dos valores distintos en este
# mismo archivo (CONTROL_EJECUCIONES_SP en DEFAULT_CONFIG y MON_EJECUCIONES
# como fallback de LogRepository), asi que segun si la Variable traia
# AUDITORIA.TABLA_LOG o no, los logs se repartian entre dos tablas.
TABLA_LOG_DEFAULT = "CONTROL_EJECUCIONES_SP"

# Columnas que el codigo necesita de CTL_CFG_PROCESOS. Explicitas y no
# SELECT *: con SELECT *, cualquier columna nueva de la tabla entra al
# diccionario del proceso y puede pisar una clave del JSON en el merge
# {**proceso_db, **proceso_json}.
COLUMNAS_CFG_PROCESOS = (
    "ID_PROCESO",
    "CAPA",
    "GRUPO_PROCESO",
    "NOMBRE_PROCESO",
    "TIPO_PROCESO",
    "STORED_PROCEDURE",
    "SCHEMA_ORIGEN",
    "TABLA_ORIGEN",
    "SCHEMA_DESTINO",
    "TABLA_DESTINO",
    "ACTIVO",
)

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
        "TABLA_LOG": TABLA_LOG_DEFAULT,
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
    """Indica si un proceso declarado en DAG_ODS_TABLAS debe entrar al DAG."""
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


class ConfigRepository:
    @staticmethod
    def obtener_proceso(conn, id_proceso: int, capa: str) -> dict[str, Any] | None:
        """Lee UNA fila del catalogo, reutilizando la conexion de la tarea.

        Antes esto hacia SELECT * de todo el catalogo y abria dos conexiones
        propias (una para el pre-check TCP y otra para la consulta), en cada
        tarea. Con 22 procesos eso eran 22 escaneos completos y 44 conexiones
        por corrida, ademas de la conexion de ejecucion.
        """
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT {', '.join(COLUMNAS_CFG_PROCESOS)} "
            f"FROM {TABLA_CFG_PROCESOS} "
            f"WHERE ID_PROCESO = %s AND CAPA = %s AND ACTIVO = 'S'",
            (id_proceso, capa),
        )
        fila = cursor.fetchone()
        if fila is None:
            return None
        columnas = [col[0].upper() for col in cursor.description]
        proceso = dict(zip(columnas, fila))
        proceso["ID_PROCESO"] = int(proceso["ID_PROCESO"])
        return proceso


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
            auditoria.get("TABLA_LOG") or TABLA_LOG_DEFAULT,
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
        marcadores = ", ".join(["%s"] * len(parametros))
        llamada = f"CALL {nombre_sp(proceso)}({marcadores})"
        logger.info("[%s] %s  params=%s", proceso["NOMBRE_PROCESO"], llamada, parametros)
        cursor.execute(llamada, tuple(parametros))

    @staticmethod
    def ejecutar_singlestore(
        conn,
        proceso: dict[str, Any],
        auditoria: dict[str, Any],
        airflow_ctx: dict[str, str],
    ) -> dict[str, Any]:
        """Ejecuta PRE_SQL + CALL + POST_SQL sobre la conexion ya abierta."""
        inicio = datetime.now()
        estado = "SUCCESS"
        error = None
        try:
            cursor = conn.cursor()
            ProcesoExecutor.ejecutar_sqls(cursor, proceso.get("PRE_SQL", []), "PRE_SQL", proceso)
            ProcesoExecutor.ejecutar_sp(cursor, proceso)
            ProcesoExecutor.ejecutar_sqls(cursor, proceso.get("POST_SQL", []), "POST_SQL", proceso)
            conn.commit()
            return {"ID_PROCESO": proceso["ID_PROCESO"], "ESTADO": estado}
        except Exception as exc:
            estado = "FAILED"
            error = str(exc)
            try:
                conn.rollback()
            except Exception:
                logger.warning("Fallo el rollback", exc_info=True)
            raise
        finally:
            # El log se escribe en finally, pero NO puede tapar la excepcion
            # real del proceso: si registrar() falla (tabla inexistente,
            # conexion caida), esa excepcion reemplazaria a la original y se
            # perderia la causa raiz.
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
            except Exception:
                logger.exception(
                    "No se pudo registrar el log de ID_PROCESO=%s. Resultado real: %s",
                    proceso.get("ID_PROCESO"),
                    estado,
                )


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
    nombre_grupo: str,) -> dict[str, Any] | None:
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
    conn,
    proceso: dict[str, Any],
    auditoria: dict[str, Any],
    spark_cfg: dict[str, Any],
    airflow_ctx: dict[str, str],) -> dict[str, Any]:
    tipo = str(proceso.get("TIPO_PROCESO", "SP")).upper()
    usar_spark = tipo == "SPARK" or proceso.get("USAR_SPARK") is True
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

    return ProcesoExecutor.ejecutar_singlestore(conn, proceso, auditoria, airflow_ctx)


def buscar_proceso_json(
    config: dict[str, Any],
    nombre_grupo: str,
    id_proceso: int,) -> dict[str, Any]:
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

    # El chequeo de "esta activo en el JSON" no necesita base de datos, asi
    # que va antes de conectarse: un proceso desactivado no debe gastar una
    # conexion para terminar en skip.
    proceso_json = buscar_proceso_json(config, nombre_grupo, id_proceso)
    if not proceso_activo(proceso_json, nombre_grupo):
        raise AirflowSkipException(
            f"Proceso {id_proceso} del grupo {nombre_grupo!r} esta inactivo "
            f"en {VARIABLE_CONFIG}; no se ejecuta."
        )

    SingleStoreConnection.validar_conectividad()

    # UNA sola conexion para todo: leer el catalogo, ejecutar y registrar.
    conn = SingleStoreConnection.get_connection()
    try:
        proceso_db = ConfigRepository.obtener_proceso(
            conn, int(proceso_json["ID_PROCESO"]), config.get("CAPA", "ODS")
        )
        proceso = validar_y_enriquecer(proceso_json, proceso_db, config, nombre_grupo)
        if not proceso:
            return None

        logger.info(
            "Ejecutando proceso ODS | grupo=%s id=%s nombre=%s sp=%s",
            nombre_grupo,
            proceso["ID_PROCESO"],
            proceso["NOMBRE_PROCESO"],
            proceso.get("STORED_PROCEDURE"),
        )
        return ejecutar_proceso(conn, proceso, auditoria, spark_cfg, airflow_ctx)
    finally:
        try:
            conn.close()
        except Exception:
            logger.warning("Fallo al cerrar la conexion SingleStore", exc_info=True)


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
                    "Proceso ODS omitido por configuracion: grupo=%s id=%s nombre=%s activo=%s",
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

"""
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
"""