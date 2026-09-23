from __future__ import annotations

import logging
import re
import socket
import time
from datetime import datetime, timedelta
from pathlib import Path

# pyarrow.dataset es lo unico que se usa de pyarrow aqui.
# Se quitaron pyodbc, pyarrow.parquet, json, os, re y ThreadPoolExecutor:
# ninguno se usaba, y un import a nivel de modulo que falla (por ejemplo
# pyodbc sin el driver instalado) hace que el DAG entero desaparezca de la UI.
import pandas as pd
import pyarrow.dataset as ds

# Librerias de bases de datos
import singlestoredb as s2

# Librerias de airflow
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)
# Las Variables se leen en EJECUCION, no al importar el modulo.
#
# Antes estaban a nivel de modulo: Airflow importa este archivo en cada ciclo
# de parseo (30 s por defecto), asi que consultaba la metadata database todo
# el tiempo, y si la Variable faltaba el DAG que lo importa desaparecia de la
# UI con un error de import en vez de fallar en la tarea.
config: dict = {}
NOM_PROCESO = ""
TABLA_CONTROL = ""
SQL_JOBS = ""
ESTADO_INICIADO = "INICIADO"
ESTADO_EJECUTADO = "EJECUTADO"
ESTADO_FINALIZADO = "FINALIZADO"
ESTADO_ERROR = "ERROR"
ERROR_SIZE_LIMIT = 4000
COMMIT_DEFAULT = 10
BATCH_DEFAULT = 5000

# Tabla donde la extraccion deja la ruta completa de cada parquet.
# Debe ser la misma que tb_proceso_parquet de EXTRACCION_BT_STG.
TABLA_CONTROL_EXTRACCION = "CTL_PROCESO_PARQUET"


def cargar_variables_config() -> dict:
    """Carga CARGAR_PARQUET_CONFIG. Se llama al inicio de ejecutar_carga()."""
    global config, NOM_PROCESO, TABLA_CONTROL, SQL_JOBS, ESTADO_INICIADO
    global ESTADO_EJECUTADO, ESTADO_FINALIZADO, ESTADO_ERROR, ERROR_SIZE_LIMIT
    global COMMIT_DEFAULT, BATCH_DEFAULT, TABLA_CONTROL_EXTRACCION

    config = Variable.get("CARGAR_PARQUET_CONFIG", deserialize_json=True)
    NOM_PROCESO = config["nom_proceso"]
    TABLA_CONTROL = config["tabla_control"]
    SQL_JOBS = config["sql_jobs"]
    ESTADO_INICIADO = config["estado_iniciado"]
    ESTADO_EJECUTADO = config["estado_ejecutado"]
    ESTADO_FINALIZADO = config["estado_finalizado"]
    ESTADO_ERROR = config["estado_error"]
    ERROR_SIZE_LIMIT = int(config["error_size_limit"])
    COMMIT_DEFAULT = int(config["commit_default"])
    BATCH_DEFAULT = int(config["batch_default"])
    TABLA_CONTROL_EXTRACCION = config.get(
        "tabla_control_extraccion", "CTL_PROCESO_PARQUET"
    )
    return config

# CONEXIONES A LA BASE DE DATOS
SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure":False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay":timedelta(minutes=5),

}
# Funciones de auditoria

def obtener_conexion():
    """
    
    """
    try:
        conn_airflow = BaseHook.get_connection(
            SINGLESTORE_CONN_ID
        )
        logger.info("Conectando a Singlestore [%s: %s]",conn_airflow.host, conn_airflow.port)
        conn = s2.connect(
            host= conn_airflow.host,
            port=conn_airflow.port,
            user=conn_airflow.login,
            password=conn_airflow.password,
            database=conn_airflow.schema,
            connect_timeout =30,
            autocommit=False
        )
        logger.info("Conexion Singlestore establecida correctamente")
        return conn
    except Exception as e:
        logger.exception("[Error] Obtencion conexion Singlestore")
        raise Exception(str(e))

def validar_conectividad():
    """
    
    """
    
    try:
        conn_airflow= BaseHook.get_connection(
        SINGLESTORE_CONN_ID
        )
        sock = socket.create_connection((conn_airflow.host, int(conn_airflow.port)),timeout=10)
        sock.close()
        logger.info("Validacion TCP exitosa [%s:%s]",conn_airflow.host, conn_airflow.port)
    except Exception as e:
        raise Exception(f"[ERROR] conectividad TCP:{str(e)}")


# Token que se reemplaza en sql_jobs por el batch de la corrida. Se enlaza
# como parametro (%s), nunca se concatena.
TOKEN_BATCH = "{BATCH_ID}"

# Si la carga corre sin batch (por ejemplo, ejecutando solo esa tarea), se usa
# esta condicion en lugar del filtro por batch: la ultima extraccion TERMINADA
# de cada tabla.
FALLBACK_ULTIMO_BATCH = """p.id_log IN (
        SELECT MAX(id_log) FROM {tabla} WHERE estado = 'TERMINADO' GROUP BY tabla_origen
    )"""


def obtener_jobs(cur, batch_id=None, permitir_vacio=False):
    """Obtiene los archivos a cargar leyendo la RUTA desde la tabla de control.

    La extraccion guarda en CTL_PROCESO_PARQUET la ruta completa de cada
    parquet que escribio. sql_jobs une esa tabla con ETL_CONFIG (que dice a
    que tabla STG va cada origen) y devuelve, EN ESTE ORDEN:

        ruta_parquet, tabla_destino, batch_size, commit_every

    porque procesar_jobs las desempaqueta por posicion.

    Si sql_jobs contiene el token {BATCH_ID}, se sustituye por un parametro
    enlazado con el batch que devolvio la extraccion. Asi la carga sube
    exactamente los archivos de esta corrida y no los de ayer si la
    extraccion de hoy fallo.
    """
    sql = SQL_JOBS
    params = ()

    if TOKEN_BATCH in sql:
        if batch_id:
            sql = sql.replace(TOKEN_BATCH, "%s")
            params = (batch_id,)
            logger.info("Cargando los archivos del batch %s", batch_id)
        else:
            # Sin batch: se cambia la comparacion completa por el fallback.
            sql = re.sub(
                r"[\w.]*batch_id\s*=\s*\{BATCH_ID\}",
                FALLBACK_ULTIMO_BATCH.format(tabla=TABLA_CONTROL_EXTRACCION),
                sql,
            )
            logger.warning(
                "No se recibio batch_id: se cargara la ultima extraccion TERMINADA "
                "de cada tabla. Si esto no es lo que quieres, ejecuta el DAG completo."
            )

    cur.execute(sql, params)
    jobs = cur.fetchall()
    logger.info("Archivos a cargar: %s", len(jobs))

    if not jobs and not permitir_vacio:
        raise Exception(
            "sql_jobs no devolvio ningun archivo. Causas habituales: la extraccion "
            "no dejo filas en estado TERMINADO para este batch, o falta la fila "
            "correspondiente en ETL_CONFIG con activo = 1."
        )

    for indice, job in enumerate(jobs):
        if len(job) != 4:
            raise Exception(
                f"SQL_JOBS devolvio {len(job)} columnas en la fila {indice}; se esperaban "
                f"exactamente 4 y en este orden: ruta_parquet, tabla_destino, batch_size, "
                f"commit_every. Revisa CARGAR_PARQUET_CONFIG -> sql_jobs."
            )
        if not job[0]:
            raise Exception(
                f"La fila {indice} ({job[1]}) no tiene ruta de parquet en "
                f"{TABLA_CONTROL_EXTRACCION}.archivo_parquet. La extraccion no la registro."
            )
    return jobs


def verificar_parquet(batch_id: str | None = None) -> bool:
    """Comprueba que haya parquet para cargar ANTES de tocar las tablas STG.

    Lo usa un ShortCircuitOperator, asi que el resultado decide el DAG:

        True  -> la carga se ejecuta
        False -> la carga se SALTA (queda en skipped, rosa en la UI),
                 no falla. El log explica por que.

    Revisa las dos cosas, porque pueden discrepar:
      1. lo que la extraccion registro en la tabla de control
      2. lo que de verdad hay en la carpeta del dia

    Que la tabla diga TERMINADO no garantiza que el archivo siga ahi: puede
    haberlo borrado la retencion, o la carpeta externa puede no estar montada
    en este worker. Por eso se comprueba el disco.
    """
    cargar_variables_config()
    validar_conectividad()

    cargar_parcial = bool(config.get("cargar_parcial", False))
    conn = cur = None
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        jobs = obtener_jobs(cur, batch_id, permitir_vacio=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    # --- 1. Lo que dice la tabla -------------------------------------------
    if not jobs:
        logger.warning(
            "NO HAY NADA QUE CARGAR: %s no tiene filas en estado TERMINADO%s. "
            "La carga se salta (no es un error).",
            TABLA_CONTROL_EXTRACCION,
            f" para el batch {batch_id}" if batch_id else "",
        )
        logger.warning(
            "Revisa: (a) que la extraccion haya terminado bien, "
            "(b) que exista la fila en ETL_CONFIG con activo = 1."
        )
        return False

    # --- 2. Lo que hay en el disco -----------------------------------------
    presentes, faltantes, total_mb = [], [], 0.0
    logger.info("Archivos registrados para el batch %s:", batch_id or "(ultimo)")
    for ruta, tabla_destino, _, _ in jobs:
        archivo = Path(ruta)
        if archivo.exists():
            mb = sum(f.stat().st_size for f in ([archivo] if archivo.is_file()
                                                else archivo.rglob("*")) if f.is_file())
            mb = mb / 1024 / 1024
            total_mb += mb
            presentes.append((ruta, tabla_destino))
            logger.info("   OK      %-28s %8.2f MB  -> %s", archivo.name, mb, tabla_destino)
        else:
            faltantes.append((ruta, tabla_destino))
            logger.error("   FALTA   %-28s            -> %s   (%s)",
                         archivo.name, tabla_destino, ruta)

    # Listado de la carpeta del dia, para diagnosticar cuando algo no cuadra.
    carpetas = {str(Path(r).parent) for r, _, _, _ in jobs}
    for carpeta in sorted(carpetas):
        ruta = Path(carpeta)
        if ruta.is_dir():
            encontrados = sorted(f.name for f in ruta.glob("*.parquet"))
            logger.info("Carpeta %s: %s archivo(s) %s", carpeta, len(encontrados), encontrados)
        else:
            logger.error(
                "La carpeta %s NO EXISTE en este worker. Si en el servidor si existe, "
                "falta el montaje: revisa PARQUET_HOST_DIR en .env y que el "
                "docker-compose monte esa carpeta en TODOS los contenedores.",
                carpeta,
            )

    # --- 3. Decision --------------------------------------------------------
    if not presentes:
        logger.warning(
            "NO HAY NADA QUE CARGAR: los %s archivo(s) registrados no estan en disco. "
            "La carga se salta (no es un error).", len(faltantes)
        )
        return False

    if faltantes and not cargar_parcial:
        logger.error(
            "CARGA CANCELADA: faltan %s de %s archivos. No se carga parcialmente "
            "porque cada tabla destino se TRUNCA antes de insertar, y quedaria "
            "vacia sin datos con que rellenarla. Para permitirlo igualmente, pon "
            "\"cargar_parcial\": true en CARGAR_PARQUET_CONFIG.",
            len(faltantes), len(jobs),
        )
        return False

    if faltantes:
        logger.warning(
            "Se cargaran %s de %s archivos (cargar_parcial=true). Faltan: %s",
            len(presentes), len(jobs), [Path(r).name for r, _ in faltantes],
        )

    logger.info(
        "HAY %s archivo(s) para cargar, %.2f MB en total. Se ejecuta la carga.",
        len(presentes), total_mb,
    )
    return True


def gestionar_control_proceso(conn, cur, accion, id_log=None, tabla_destino=None,
                              ruta=None, host_name=None, estado=None,
                              mensaje_error=None, total_rows=None, total_time=None):

    tiempo = round(float(total_time), 2) if total_time else 0

    if accion == "INICIO":
        cur.execute(
            f"""INSERT INTO {TABLA_CONTROL}
            (nom_proceso,tabla_destino,archivo_parquet,fec_inicio,estado,host_name)
            VALUES (%s,%s,%s,NOW(6),%s,%s)""",
            (NOM_PROCESO, tabla_destino, ruta, ESTADO_INICIADO, host_name)
        )
        conn.commit()
        return cur.lastrowid

    elif accion == "ESTADO":
        cur.execute(f"UPDATE {TABLA_CONTROL} SET estado=%s WHERE id_log=%s", (estado, id_log))

    elif accion == "ERROR":
        cur.execute(
            f"UPDATE {TABLA_CONTROL} SET fec_termino=NOW(6),estado=%s,msg_error=%s WHERE id_log=%s",
            (ESTADO_ERROR, (mensaje_error or '')[:ERROR_SIZE_LIMIT], id_log)
        )

    elif accion == "FIN":
        cur.execute(
            f"UPDATE {TABLA_CONTROL} SET fec_termino=NOW(6),estado=%s,filas_cargadas=%s,duracion_segundos=%s WHERE id_log=%s",
            (ESTADO_FINALIZADO, total_rows, tiempo, id_log)
        )

    conn.commit()



def validar_archivo_parquet(ruta: str) -> ds.Dataset:
    """
    Valida la existencia de un parquet y devuelve
    el dataset para su posterior procesamiento.
    Parameters
    ----------
    ruta : str
        Ruta absoluta o relativa al archivo/directorio parquet.
    Returns
    -------
    pyarrow.dataset.Dataset
    """
    ruta_path = Path(ruta)
    logger.info("Leyendo parquet: %s", ruta_path)
    if not ruta_path.exists():
        raise FileNotFoundError(
            f"No existe el parquet {ruta}. La ruta sale de "
            f"{TABLA_CONTROL_EXTRACCION}.archivo_parquet, que llena la extraccion. "
            f"Comprueba que la carpeta externa este montada en los contenedores "
            f"(PARQUET_HOST_DIR en .env) y que la extraccion haya terminado bien."
        )

    return ds.dataset(
        str(ruta_path),
        format="parquet"
    )
def truncar_tabla_destino(conn, cur, tabla_destino):
    try:
        logger.info("Truncando la tabla %s", tabla_destino)
        cur.execute(f"TRUNCATE TABLE {tabla_destino}")
        conn.commit()
        logger.info("[INFO] Tabla truncada correctamente %s", tabla_destino)
    except Exception as e:
        logger.error("Error truncando tabla %s: %s", tabla_destino, str(e))
        raise


def cargar_batch(cur, tabla_destino, batch):
    logger.info("[INFO]: Inicio de proceso de insercion gracias")
    cols= batch.schema.names
    cols_sql = ",".join(cols)
    logger.info(f"Mostrando el contenido de { cols_sql}")
    placeholders = ",".join(["%s"]*len(cols))

    sql = f"""INSERT INTO {tabla_destino} ({cols_sql}) VALUES ({placeholders})"""
    logger.info(f"Mostrando el sql de la  tabla {sql}")
    columns = [batch.column(i).to_pylist() for i in range(len(cols))]
    data = list(zip(*columns))
    cur.executemany(sql,data)
    return len(data)

def cargar_dataset(conn, cur, dataset, tabla_destino, batch_size, commit_every):
    try:
        total_rows = 0
        batch_count = 0
        inicio = time.time()
        for batch in dataset.to_batches(batch_size=batch_size):
            start_batch = time.time()
            filas = cargar_batch(cur, tabla_destino, batch)
            total_rows += filas
            batch_count += 1
            batch_time = time.time() - start_batch
            rows_per_sec = filas / batch_time if batch_time > 0 else 0
            logger.info(
                "%s | Batch %s | Filas=%s | %.0f filas/s | Total=%s",
                tabla_destino,
                batch_count,
                filas,
                rows_per_sec,
                total_rows
            )
            if batch_count % commit_every == 0:
                conn.commit()
        conn.commit()
        logger.info("Commit final ejecutado")
        return total_rows, time.time() - inicio
    except Exception as e:
        logger.exception("Error en cargar_dataset. Tabla=%s, Batch=%s",tabla_destino,batch_count if 'batch_count' in locals() else 0)
        conn.rollback()
        raise

def procesar_jobs(conn, cur, job, host_name):
    ruta, tabla_destino, batch_size, commit_every = job

    # Si sql_jobs devuelve NULL, se usan los defaults de la Variable. Antes
    # commit_default y batch_default se leian y no se usaban nunca.
    batch_size = int(batch_size or BATCH_DEFAULT)
    commit_every = int(commit_every or COMMIT_DEFAULT)

    logger.info("Iniciando carga de %s desde %s (batch=%s, commit cada %s)",
                tabla_destino, ruta, batch_size, commit_every)
    id_log = gestionar_control_proceso(
        conn=conn, cur=cur, accion='INICIO',
        tabla_destino=tabla_destino, ruta=ruta, host_name=host_name,
    )
    try:
        # El parquet se valida ANTES de truncar: TRUNCATE lleva commit
        # implicito y no se revierte, asi que si el archivo no existe la tabla
        # destino quedaria vacia sin nada con que rellenarla.
        dataset = validar_archivo_parquet(ruta)

        # Antes decia estado='ESTADO_EJECUTADO', entre comillas: guardaba el
        # NOMBRE de la constante en la tabla de control en vez de su valor.
        gestionar_control_proceso(
            conn=conn, cur=cur, accion='ESTADO', id_log=id_log, estado=ESTADO_EJECUTADO,
        )
        truncar_tabla_destino(conn=conn, cur=cur, tabla_destino=tabla_destino)
        total_rows, total_time = cargar_dataset(
            conn=conn, cur=cur, dataset=dataset, tabla_destino=tabla_destino,
            batch_size=batch_size, commit_every=commit_every,
        )
        gestionar_control_proceso(
            conn=conn, cur=cur, accion='FIN', id_log=id_log,
            total_rows=total_rows, total_time=total_time,
        )

    except Exception as e:
        conn.rollback()
        # El log de ERROR no puede tapar la excepcion real.
        try:
            gestionar_control_proceso(
                conn=conn, cur=cur, accion='ERROR', id_log=id_log, mensaje_error=str(e),
            )
        except Exception:
            logger.exception(
                "Ademas del fallo de carga de %s, no se pudo escribir ERROR en %s (id_log=%s)",
                tabla_destino, TABLA_CONTROL, id_log,
            )
        # Sin este raise el fallo se quedaba aqui: ejecutar_carga seguia con
        # el siguiente job y la tarea terminaba en SUCCESS con la tabla vacia.
        raise



def ejecutar_carga(batch_id: str | None = None):
    """Carga a STG los parquet cuya ruta dejo registrada la extraccion.

    batch_id llega por XCom desde la tarea de extraccion. Si no llega, se
    cargan los ultimos archivos TERMINADOS de cada tabla (ver obtener_jobs).
    """
    cargar_variables_config()
    validar_conectividad()
    conn = None
    cur = None
    errores: list[str] = []
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        host_name = socket.gethostname()
        jobs = obtener_jobs(cur, batch_id)
        cargar_parcial = bool(config.get("cargar_parcial", False))

        for job in jobs:
            tabla_destino = job[1] if len(job) > 1 else "?"

            # Con cargar_parcial, un archivo ausente se omite con aviso en vez
            # de fallar. verificar_parquet ya lo advirtio antes de llegar aqui.
            if cargar_parcial and not Path(job[0]).exists():
                logger.warning("Se omite %s: no existe %s", tabla_destino, job[0])
                continue

            try:
                procesar_jobs(conn, cur, job, host_name)
            except Exception as exc:
                # Se intentan todos los jobs y se reportan juntos al final,
                # en vez de cortar en el primero.
                logger.exception("Fallo la carga de %s", tabla_destino)
                errores.append(f"{tabla_destino}: {exc}")

        if errores:
            raise Exception("Fallaron procesos de carga: " + " | ".join(errores))
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


"""with DAG(
    dag_id="carga_parquet_singlestore",
    description=(
        "Carga masiva de archivos parquet "
        "hacia SingleStore"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "etl",
        "parquet",
        "singlestore",
        "data-engineering",
        "produccion"
    ]
) as dag:
    ejecutar_carga_parquet = PythonOperator(
        task_id="ejecutar_carga_parquet",
        python_callable=ejecutar_cargar_parquet,
        execution_timeout=timedelta(hours=4)

    )"""