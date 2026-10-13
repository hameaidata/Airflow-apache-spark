"""Ejecuta un stored procedure en SQL Server y lo registra.

POR QUE NO SE USA EjecutarSPOperator NI UN HOOK DE AIRFLOW
---------------------------------------------------------
plugins/operators/sp_operator.py resuelve el motor con conn.conn_type CRUDO y
pide el hook con conn.get_hook(). Las Connections de este proyecto son de tipo
Generic -- el motor va declarado en el Extra, {"motor": "mssql"} --, asi que
conn_type vale el literal "generic" y ese operador falla antes de ejecutar
nada, con "El motor 'generic' no esta soportado". Es el mismo fallo que ya
tenia el operador de Spark y que se arreglo alli con motor_de_conexion().

Aqui se usa conexion_sqlserver() de etl_bt2sql.bt2sql_comun, que es el camino
que el resto del pipeline lleva usando contra este mismo SQL Server: va por
JDBC, entiende las Connections Generic, y soporta jdbc_url en el Extra para
instancias con nombre o Always On.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


def _existe_tabla(cur, nombre: str) -> bool:
    """INFORMATION_SCHEMA, no un SELECT a ciegas.

    Un SELECT contra una tabla inexistente aborta la transaccion en algunos
    motores y deja la conexion inservible para lo que viene detras.
    """
    partes = nombre.split(".")
    tabla = partes[-1]
    esquema = partes[-2] if len(partes) > 1 else None
    if esquema:
        cur.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", (esquema, tabla))
    else:
        cur.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_NAME = ?", (tabla,))
    return int(cur.fetchall()[0][0]) > 0


def filas_de(conn_id: str, tabla: str, filtro_fecha: str = "",
             fecha=None) -> int | None:
    """Cuenta filas de una tabla. None si la tabla no existe.

    Se distingue 'no existe' de 'existe y tiene cero' a proposito: son dos
    problemas distintos. Cero filas es una carga que no trajo nada; tabla
    ausente es un despliegue incompleto, y confundirlos manda a buscar el
    error al sitio equivocado.
    """
    from etl_bt2sql.bt2sql_comun import conexion_sqlserver

    conn = conexion_sqlserver({"conn_id_destino": conn_id})
    try:
        cur = conn.cursor()
        if not _existe_tabla(cur, tabla):
            return None
        if filtro_fecha and fecha is not None:
            cur.execute(f"SELECT COUNT(*) FROM {tabla} "
                        f"WHERE {filtro_fecha} = ?", (str(fecha),))
        else:
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
        return int(cur.fetchall()[0][0])
    finally:
        conn.close()


def ejecutar_sp(conn_id: str, sp: str, parametros: list | None = None,
                bitacora: str = "", etiqueta: str = "", **context) -> dict:
    """Llama al procedimiento y devuelve {'filas': n, 'segundos': s}.

    LOS PARAMETROS NUNCA SE CONCATENAN
    ----------------------------------
    Van como marcadores '?'. Concatenarlos permitiria inyeccion a traves del
    valor de un parametro del catalogo, y quien pueda editar esa Variable no
    tiene por que poder ejecutar SQL arbitrario en el servidor.

    El nombre del SP si se concatena -- ningun motor lo admite como parametro --
    asi que viene de STORED_PROCEDURE del catalogo y de ningun otro sitio.
    """
    from airflow.exceptions import AirflowException

    from etl_bt2sql.bt2sql_comun import conexion_sqlserver

    params = tuple(parametros or ())
    marcadores = ", ".join(["?"] * len(params))
    # Llave de escape JDBC/ODBC. El driver de Microsoft la traduce a EXEC, y
    # funciona igual con y sin parametros, que EXEC no.
    sentencia = f"{{call {sp}({marcadores})}}" if params else f"{{call {sp}}}"

    logger.info("=" * 72)
    logger.info("EJECUTANDO  %s", etiqueta or sp)
    logger.info("   sentencia : %s", sentencia)
    if params:
        logger.info("   parametros: %s", list(params))

    conn = conexion_sqlserver({"conn_id_destino": conn_id}, autocommit=True)
    inicio = time.monotonic()
    try:
        cur = conn.cursor()
        cur.execute(sentencia, params) if params else cur.execute(sentencia)
        # rowcount es -1 cuando el motor no lo informa. Se normaliza a None
        # para no publicar un -1 que parece un conteo.
        filas = getattr(cur, "rowcount", -1)
        filas = None if filas is None or filas < 0 else int(filas)
        segundos = time.monotonic() - inicio
    except Exception as exc:                                     # noqa: BLE001
        segundos = time.monotonic() - inicio
        _registrar(conn_id, bitacora, sp, etiqueta, "ERROR", segundos,
                   None, str(exc), **context)
        try:
            conn.close()
        except Exception:                                        # noqa: BLE001
            pass
        raise AirflowException(
            f"{etiqueta or sp} fallo tras {segundos:.1f} s: {exc}") from exc
    else:
        conn.close()

    logger.info("TERMINADO   %s   %s  (%.1f s)", etiqueta or sp,
                f"{filas:,} fila(s)" if filas is not None else
                "sin conteo de filas", segundos)
    _registrar(conn_id, bitacora, sp, etiqueta, "OK", segundos, filas, None,
               **context)
    return {"filas": filas, "segundos": round(segundos, 2)}


def _registrar(conn_id: str, bitacora: str, sp: str, etiqueta: str,
               estado: str, segundos: float, filas: int | None,
               error: str | None, **context) -> None:
    """Escribe en la bitacora. Nunca tumba la corrida.

    Si la tabla no existe se avisa UNA vez con el nombre y se sigue: que falte
    la bitacora es un problema de despliegue, no una razon para no cargar los
    datos. Y la DDL NO se crea sola, a diferencia de sp_operator: inventar una
    tabla de auditoria con las columnas que a uno le parezcan, en una base
    bancaria, es peor que no tenerla.
    """
    if not bitacora:
        return

    from etl_bt2sql.bt2sql_comun import conexion_sqlserver

    try:
        conn = conexion_sqlserver({"conn_id_destino": conn_id}, autocommit=True)
    except Exception as exc:                                     # noqa: BLE001
        logger.warning("No se pudo abrir conexion para la bitacora: %s", exc)
        return

    try:
        cur = conn.cursor()
        if not _existe_tabla(cur, bitacora):
            logger.warning(
                "La tabla de bitacora %s no existe: la ejecucion NO queda "
                "registrada. Creela, o quite TABLA_LOG de AUDITORIA en la "
                "Variable para dejar de ver este aviso.", bitacora)
            return
        cur.execute(
            f"INSERT INTO {bitacora} "
            f"(NOMBRE_SP, NOMBRE_PROCESO, ESTADO, DURACION_SEGUNDOS, "
            f" FILAS_AFECTADAS, MSG_ERROR, RUN_ID, FECHA_REGISTRO) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())",
            (sp, etiqueta or sp, estado, round(segundos, 2), filas,
             (error or "")[:4000] or None,
             str((context.get("run_id") or ""))[:200]))
    except Exception as exc:                                     # noqa: BLE001
        # El mensaje nombra las columnas esperadas: si la bitacora existe con
        # otro esquema, es lo primero que hay que saber.
        logger.warning(
            "No se pudo registrar en %s (%s). Columnas que espera este "
            "proceso: NOMBRE_SP, NOMBRE_PROCESO, ESTADO, DURACION_SEGUNDOS, "
            "FILAS_AFECTADAS, MSG_ERROR, RUN_ID, FECHA_REGISTRO.",
            bitacora, exc)
    finally:
        try:
            conn.close()
        except Exception:                                        # noqa: BLE001
            pass


def sp_faltantes(conn_id: str, procesos: list) -> tuple[list, list]:
    """Cuales de los SP declarados NO existen en la base. (existen, faltan).

    POR QUE ESTA COMPROBACION ES LA MAS IMPORTANTE DE ESTE PIPELINE
    ---------------------------------------------------------------
    El catalogo (DAG_ODS_TABLAS / DAG_BDS_TABLAS) es una DECLARACION, no un
    inventario: nombra 22 procedimientos, y que esten desplegados en la base es
    otra cosa. El 2026-10-13 ninguno de esos 22 nombres existia como archivo en
    sql/ del repositorio, y de los 11 que si existen, 8 estan escritos en
    sintaxis de SingleStore -DELIMITER //, DECLARE ... DEFAULT- que SQL Server
    no acepta.

    Sin esta tarea, un SP ausente se descubre asi: la tarea del proceso falla
    con "Invalid object name 'DATAHUB.SP_ODS_TIPO_CAMBIO'", a los veinte
    segundos, despues de haber abierto una conexion y escrito en la bitacora.
    Con veinte procesos activos son veinte tareas en rojo diciendo lo mismo, y
    hay que abrir veinte logs para saber cuales faltan.

    Con esta tarea es UNA lista, antes de tocar nada.

    SE CONSULTA sys.procedures Y NO SE INTENTA EJECUTAR
    --------------------------------------------------
    Es la unica forma de preguntar "existe?" sin efectos. Un EXEC de prueba
    ejecutaria el procedimiento, y estos escriben en tablas.

    OBJECT_ID() seria mas corto pero devuelve NULL tanto si el objeto no existe
    como si existe y el usuario no tiene permiso para verlo, y esos son dos
    problemas distintos. sys.procedures al menos lista lo que SI hay, asi que
    el mensaje puede decir que procedimientos encontro.
    """
    from etl_bt2sql.bt2sql_comun import conexion_sqlserver

    conn = conexion_sqlserver({"conn_id_destino": conn_id})
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT LOWER(s.name) + '.' + LOWER(p.name) "
            "FROM sys.procedures p "
            "JOIN sys.schemas s ON s.schema_id = p.schema_id")
        en_la_base = {str(f[0]) for f in cur.fetchall()}
    finally:
        conn.close()

    existen, faltan = [], []
    for p in procesos:
        nombre = p.sp_calificado
        # Un SP sin esquema se busca en dbo, que es donde SQL Server lo
        # resolveria con el esquema por defecto de un login normal.
        clave = nombre.lower() if "." in nombre else f"dbo.{nombre.lower()}"
        (existen if clave in en_la_base else faltan).append(p)

    logger.info("Procedimientos en la base: %s", len(en_la_base))
    return existen, faltan
