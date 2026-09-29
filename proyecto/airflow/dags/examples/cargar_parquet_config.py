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
config=Variable.get("CARGAR_PARQUET_CONFIG", deserialize_json=True)
NOM_PROCESO = config["nom_proceso"]
TABLA_CONTROL = config["tabla_control"]
SQL_JOBS = config["sql_jobs"]
ESTADO_INICIADO = config["estado_iniciado"]
ESTADO_EJECUTADO = config["estado_ejecutado"]
ESTADO_FINALIZADO = config["estado_finalizado"]
ESTADO_ERROR = config["estado_error"]
ERROR_SIZE_LIMIT=config["error_size_limit"]
COMMIT_DEFAULT = config["commit_default"]
BATCH_DEFAULT = config["batch_default"]

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

def gestionar_control_proceso(
    conn, cur, accion, id_log=None, tabla_destino=None,
    ruta=None, host_name=None, estado=None,
    mensaje_error=None, total_rows=None, total_time=None
):

    if accion == "INICIO":
        cur.execute(
            f"""INSERT INTO {TABLA_CONTROL}
            (nom_proceso,tabla_destino,archivo_parquet,fec_inicio,estado,host_name)
            VALUES (%s,%s,%s,NOW(6),%s,%s)""",
            (NOM_PROCESO, tabla_destino, ruta, ESTADO_INICIADO, host_name)
        )
        conn.commit()
        return cur.lastrowid
    tiempo = round(float(total_time), 2) if total_time is not None else 0
    sqls = {
        "ESTADO": (
            f"UPDATE {TABLA_CONTROL} SET estado=%s WHERE id_log=%s",
            (estado, id_log)
        ),
        "ERROR": (
            f"""UPDATE {TABLA_CONTROL}
            SET fec_termino=NOW(6),estado=%s,msg_error=%s
            WHERE id_log=%s""",
            (ESTADO_ERROR, mensaje_error[:ERROR_SIZE_LIMIT], id_log)
        ),
        "FIN": (
            f"""UPDATE {TABLA_CONTROL}
            SET fec_termino=NOW(6),estado=%s,filas_cargadas=%s,
                duracion_segundos=%s
            WHERE id_log=%s""",

            (ESTADO_FINALIZADO, total_rows,tiempo , id_log)
        )
    }

    if accion not in sqls:
        raise ValueError(f"Acción no soportada: {accion}")

    cur.execute(*sqls[accion])
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

    if not ruta_path.exists():
        raise FileNotFoundError(
            f"No existe la ruta parquet: {ruta}"
        )

    return ds.dataset(
        str(ruta_path),
        format="parquet"
    )
def truncar_tabla_destino(conn,cur, tabla_destino):
    logger.info("Truncando la tabla %s", tabla_destino)
    cur.execute(f"""TRUNCATE TABLE {tabla_destino}""")
    conn.commit()
    logger.info("[INFO] Tabla truncada correstamente", tabla_destino)


def cargar_batch(cur, tabla_destino, batch):
    info.log("[INFO]: Inicio de proceso de insercion gracias")
    cols= batch.schema.names
    cols_sql = ",".join(cols)
    placeholders = ",".join(
        ["%s"]*len(cols)
    )
    sql = f"""INSERT INTO {tabla_destino} ({cols_sql}) VALUES ({placeholders})"""
    columns = [batch.column(i).to_pylist() for i in range(len(cols))]
    data = list(zip(*columns))
    cur.executemany(sql,data)
    return len(data)

def cargar_dataset(conn, cur, dataset, tabla_destino, batch_size, commit_every):
    total_rows = 0
    batch_count = 0 
    inicio = time.time()
    for batch in dataset.to_batches(batch_size=batch_size):
        start_batch = time.time()
        filas = cargar_batch(cur, tabla_destino, batch)
        total_rows += filas
        batch_count +=1
        batch_time = (time.time()-start_batch)
        rows_par_sec = (filas/batch_time if batch_time >0 else 0)

        lof.info("%s | Batch %s | Filas=%s | %.0f filas/s | Total=%s", tabla_destino, batch_count, filas, rows_per_sec, total_rows)
        if batch %commit_every==0:
            conn.commit()
    conn.commit()
    return total_rows, time.time() -inicio

def procesar_jobs(conn, cur, job, host_name):
    ruta, tabla_detino, batch_size, commit_every = job

    logger.info("[INFO] Iniciando proceso %s",tabla_detino)
    id_log = gestionar_control_proceso(conn= conn , cur=cur,accion='INICIO',tabla_destino=tabla_detino,ruta= ruta, host_name=host_name)
    try:
        dataset = validar_archivo_parquet(ruta)
        gestionar_control_proceso(conn =conn, cur=cur,accion='ESTADO', id_log=id_log,estado= 'ESTADO_EJECUTADO')
        truncar_tabla_destino(conn =conn, cur =cur, tabla_detino=tabla_destino)
        total_rows, total_time  = cargar_dataset(conn= conn, cur= cur, dataset =dataset, tabla_destino=tabla_destino, batch_size=batch_size, commit_every=commit_every)
        gestionar_control_proceso(conn=conn, cur=cur,accion='FIN', id_log=id_log, total_rows=total_rows, total_time=total_time)
        logger.info("[INFO] Proceso %s Finalizado",tabla_destino)
    except Exception as e:
        conn.rollback()
        gestionar_control_proceso(conn = conn, cur=cur, accion='ERROR',id_log= id_log, mensaje_error =str(e))

def ejecutar_cargar_parquet():
    validar_conectividad()
    conn =None
    cur = None
    try:
        conn = obtener_conexion()
        cur = conn.cursor()
        host_name = socket.gethostname()
        jobs =  obtener_jobs(cur)
        for job in jobs:
            procesar_jobs(
                conn,
                cur,
                job,
                host_name
            )
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


with DAG(
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

    )