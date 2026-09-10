from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta
import os
import re
import time
import socket

from concurrent.futures import ThreadPoolExecutor, as_completed
# Librerias de tablas 
import pandas as  pd
import pyodbc
import pyarrow as pq
from pathlib import Path
import pyarrow.dataset as ds

# Librerias de bases de datos
import singlestoredb as s2

# Librerias de airflow
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)
## Lectura de variables
config = {}
NOM_PROCESO = ""
TABLA_CONTROL = ""
SQL_JOBS = ""
ESTADO_INICIADO = "INICIADO"
ESTADO_EJECUTADO = "EJECUTADO"
ESTADO_FINALIZADO = "TERMINADO"
ESTADO_ERROR = "ERROR"
ERROR_SIZE_LIMIT = 4000
COMMIT_DEFAULT = 1
BATCH_DEFAULT = 10000


def cargar_variables_config():
    global config, NOM_PROCESO, TABLA_CONTROL, SQL_JOBS, ESTADO_INICIADO
    global ESTADO_EJECUTADO, ESTADO_FINALIZADO, ESTADO_ERROR, ERROR_SIZE_LIMIT
    global COMMIT_DEFAULT, BATCH_DEFAULT

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
    return config

# CONEXIONES A LA BASE DE DATOS
SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure":False,
    "emial_on_retry": False,
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


def obtener_jobs(conn):
    """
    Obtiene configuraciones activas desde etl_config
    """
    conn.execute(SQL_JOBS)
    jobs = conn.fetchall()
    logger.info("Cantidad de jobs encontrados :%s", len(jobs))
    return jobs

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
    logger.info(f"Mostrando la primera linea desde la funcion validar_archivo_parquet {ruta_path}")
    if not ruta_path.exists():
        raise FileNotFoundError(
            f"No existe la ruta parquet: {ruta}"
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

    logger.info("[INFO] Iniciando proceso %s",tabla_destino)
    id_log = gestionar_control_proceso(conn= conn , cur=cur,accion='INICIO',tabla_destino=tabla_destino,ruta= ruta, host_name=host_name)
    logger.info("Mostrando la linea 0 desde la funcion procesar_jobs")
    try:
        dataset = validar_archivo_parquet(ruta)
        gestionar_control_proceso(conn =conn, cur=cur,accion='ESTADO', id_log=id_log,estado= 'ESTADO_EJECUTADO')
        truncar_tabla_destino(conn =conn, cur =cur, tabla_destino=tabla_destino)
        total_rows, total_time  = cargar_dataset(conn= conn, cur= cur, dataset =dataset, tabla_destino=tabla_destino, batch_size=batch_size, commit_every=commit_every)
        gestionar_control_proceso(conn=conn, cur=cur,accion='FIN', id_log=id_log, total_rows=total_rows, total_time=total_time)

    except Exception as e:
        conn.rollback()
        gestionar_control_proceso(conn = conn, cur=cur, accion='ERROR',id_log= id_log, mensaje_error =str(e))
        raise



def ejecutar_carga():
    cargar_variables_config()
    validar_conectividad()
    conn =None
    cur = None
    errores = []
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        host_name = socket.gethostname()
        jobs =  obtener_jobs(cur)
        for job in jobs:
            try:
                procesar_jobs(
                    conn,
                    cur,
                    job,
                    host_name
                )
            except Exception as exc:
                tabla_destino = job[1] if len(job) > 1 else "?"
                logger.exception("Fallo carga de %s", tabla_destino)
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
