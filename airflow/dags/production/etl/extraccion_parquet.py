from __future__ import annotations

import logging
import os
import re
import socket
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset  as ds
import singlestoredb as s2
import ibm_db

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)



# ===================================================
# CONEXIONES A LAS BASES DE DATOS
# ===================================================
SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"
BT_CONN_ID = "BT_CONEXION_PREPRODUCTION"


NOM_PROCESO = "GENERACION_PARQUET"
ESTADO_INICIADO = "INICIADO"
ESTADO_EJECUTANDO = "EJECUTANDO"
ESTADO_FINALIZADO = "TERMINADO"
ESTADO_ERROR = "ERROR"
ERROR_SIZE_LIMIT = 4000
FECHA_PROCESO_GLOBAL = None
BATCH_ID_GLOBAL = None
# ===================================================
# VARIABLES AIRFLOW
# ===================================================
variables_config = Variable.get("EXTRACCION_BT_STG",deserialize_json=True)
MAX_WORKERS = variables_config["max_worker"]
CHUNK_SIZE= variables_config["chunk_size"]
OUTPUT_DIR= variables_config["output_dir"]
SQL_FECHA_PROCESO= variables_config["sql_fecha"]
PROCESOS = variables_config["procesos"] #LISTA DE PROCESOS


TABLA_AUDITORIA_PARQUET = variables_config["tb_proceso_parquet"]
SQL_PARAMETROS_PARQUET = variables_config["sql_parametros_parquet"]
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}


class FechaProceso:

    def __init__(self):
        self.fecha_proceso = self._obtener_fecha_proceso()
    def _obtener_fecha_proceso(self):
        logger.info("Inicio de proceso")
        # conn = obtener_conexion_bt()

        conn = obtener_conexion_singlestore()
        try:
            cursor = conn.cursor()
            cursor.execute(SQL_FECHA_PROCESO)
            row = cursor.fetchone()
            if row is None:
                raise Exception("No se obtuvo fecha proceso")
            return row[0]
        finally:
            conn.close()
# LECTURA DE TABLA DE CONTROL

def validar_conectividad():
    try:
        conn_airflow= BaseHook.get_connection(
        SINGLESTORE_CONN_ID
        )
        sock = socket.create_connection((conn_airflow.host, int(conn_airflow.port)),timeout=10)
        sock.close()
        logger.info("Validacion TCP exitosa [%s:%s]",conn_airflow.host, conn_airflow.port)
    except Exception as e:
        raise Exception(f"[ERROR] conectividad TCP:{str(e)}")


#Funciones de establecer coenxion con base de datos B204648v

def obtener_conexion_singlestore():
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


def obtener_conexion_bt():
    conn_bt = BaseHook.get_connection(
        BT_CONN_ID
    )
    logger.info(f"Estableciendo conexion con BT {conn_bt.host}, {conn_bt.port}")
    try: 
        conn_bt =(
            f"DATABASE={conn_bt.schema};"
            f"HOSTNAME={conn_bt.host};"
            f"PORT={conn_bt.port};"
            "PROTOCOL=TCPIP;"
            "CONNECTTIMEOUT=10;"
            f"UID={conn_bt.login};"
            f"PWD={conn.password};"
        )
        
        connection = ibm_db.connect(conn_str,"","")
        return connection
    except Exception as e:
        Exception(f"{e}")
        raise

def gestionar_control_proceso(
    conn, cur, accion, id_log=None, tabla_origen=None,
    ruta=None, host_name=None, estado=None,
    mensaje_error=None, total_rows=None, total_time=None
):

    if accion == "INICIO":
        cur.execute(
            f"""INSERT INTO {TABLA_AUDITORIA_PARQUET}
            (nom_proceso,tabla_origen,archivo_parquet,fec_inicio,estado,host_name)
            VALUES (%s,%s,%s,NOW(6),%s,%s)""",
            (NOM_PROCESO, tabla_origen, ruta, ESTADO_INICIADO, host_name)
        )
        conn.commit()
        return cur.lastrowid
    tiempo = round(float(total_time), 2) if total_time is not None else 0
    if accion == "ESTADO":
        cur.execute(
            f"UPDATE {TABLA_AUDITORIA_PARQUET} SET estado=%s WHERE id_log=%s",
            (estado, id_log)
        )

    elif accion == "ERROR":
        cur.execute(
            f"""UPDATE {TABLA_AUDITORIA_PARQUET}
            SET fec_termino=NOW(6),estado=%s,msg_error=%s
            WHERE id_log=%s""",
            (
                ESTADO_ERROR,
                (mensaje_error or "")[:ERROR_SIZE_LIMIT],
                id_log
            )
        )

    elif accion == "FIN":
        cur.execute( f"""UPDATE {TABLA_AUDITORIA_PARQUET}
            SET fec_termino=NOW(6),estado=%s,filas_procesadas=%s,
                duracion_segundos=%s
            WHERE id_log=%s""",

            (ESTADO_FINALIZADO, total_rows,tiempo , id_log)
        )
    conn.commit()


def parse_dtype_map(tipos_str):

    dtype_map = {}

    if not tipos_str:
        return dtype_map

    for item in tipos_str.split("|"):

        if not item.strip():
            continue

        col, typ = item.split(":")
        col, typ = col.strip(), typ.strip().lower()

        if typ.startswith(("char", "varchar")):
            dtype_map[col] = {"type": "string"}

        elif typ.startswith(("decimal", "numeric")):

            match = re.search(
                r"\((\d+),(\d+)\)",
                typ
            )

            if not match:
                raise Exception(f"Decimal sin precision: {typ}")

            dtype_map[col] = {
                "type": "decimal",
                "precision": int(match.group(1)),
                "scale": int(match.group(2))
            }

        elif typ in ("int", "integer"):
            dtype_map[col] = {"type": "int"}

        elif typ == "bigint":
            dtype_map[col] = {"type": "bigint"}

        elif typ in ("float", "double"):
            dtype_map[col] = {"type": "float"}

        else:
            dtype_map[col] = {"type": "string"}

    return dtype_map


def build_fixed_schema(table, dtype_map):
    fields = []

    for field  in table.schema:
        if field.name  in dtype_map:
            config = dtype_map[field.name]
            if config["type"] =="decimal":
                fields.append(pa.field(field.name, pa.decimal128(config["precision"],config["scale"]), nullable=True))
                continue
        fields.append(field)
    return pa.schema(fields)


def apply_dtype_rules(chunk, dtype_map, tabla):
    for col, config in dtype_map.items():
        if col not in chunk.columns:
            continue
        typ = config["type"]
        if typ =="string":
            chunk[col] = chunk[col].where(pd.notna(chunk[col]), None)
        elif typ in ("int","float"):
            empty_before=(chunk[col] =="").sum()
            chunk[col] = chunk[col].replace("",None)
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
            if empty_before > 0:
                print(f"{tabla}: {col} empty->numeric NULLS {empty_before}")
        elif typ =="decimal":
            empty_before = (chunk[col] =="").sum()
            chunk[col] = chunk[col].replace("",None)
            chunk[col] =chunk[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else None)
            if empty_before > 0:
                print(f"{tabla}: {col} empty->decimal NULLS  {empty_before}")
    return chunk

def procesar_tabla_incremental(row):

    tabla, archivo = row["TABLA"], row["NOMBRE_PARQUET"]
    conn_bt = conn_log = writer = None
    id_log, total = None, 0
    inicio = time.time()

    try:
        conn_log = obtener_conexion_singlestore()
        cur_log = conn_log.cursor()

        id_log = gestionar_control_proceso(conn=conn_log, cur=cur_log, accion="INICIO",tabla_origen=tabla, ruta=archivo)

        gestionar_control_proceso(conn= conn_log, cur= cur_log, accion="ESTADO", id_log=id_log, estado=ESTADO_EJECUTANDO)
        

        conn_s2 = obtener_conexion_singlestore()
        # conn_bt = obtener_conexion_bt() -- 1
        dtype_map = parse_dtype_map(row["TIPOS"])

        query = f"SELECT {row['COLUMNAS']} FROM {row['ESQUEMA']}.{tabla}"
        if row["FILTRO"]:
            query += f" WHERE {row['FILTRO']}"

        output_path = os.path.join(OUTPUT_DIR, archivo)

        # for chunk in pd.read_sql(query, conn_bt, chunksize=CHUNK_SIZE): -- 1
        for chunk in pd.read_sql(query, conn_s2, chunksize=CHUNK_SIZE):
            total += len(chunk)
            chunk = apply_dtype_rules(chunk, dtype_map, tabla)

            chunk.insert(0, "FECHA_PROCESO", FECHA_PROCESO_GLOBAL)
            chunk["BATCH_ID"] = BATCH_ID_GLOBAL

            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if writer is None:
                schema = build_fixed_schema(table, dtype_map)
                writer = pq.ParquetWriter(output_path, schema, compression="snappy")

            writer.write_table(table.cast(writer.schema))

        gestionar_control_proceso(
            conn_log, cur_log, "FIN",
            id_log=id_log,
            total_rows=total,
            total_time=time.time() - inicio
        )

        return f"TERMINADO -> {tabla}"

    except Exception as e:

        logger.exception("[%s]", tabla)

        if conn_log and id_log:
            gestionar_control_proceso(
                conn_log,
                cur_log,
                "ERROR",
                id_log=id_log,
                mensaje_error=str(e)
            )

        return f"ERROR -> {tabla}: {str(e)}"

    finally:

        if writer:
            writer.close()

        if conn_bt:
            conn_bt.close()

        if conn_log:
            conn_log.close()

def filtrar_procesos(param_df, procesos, tipo_ejecucion="diario"):

    # Crear lookup de procesos
    procesos_dict = { p["nombre_proceso"]: p for p in procesos }
    procesos_validos = []
    for _, row in param_df.iterrows():
        print("Mostrando el contenido del row")
        print(row)
        # Validar ACTIVO
        if row["ACTIVO"] != "S":
            continue
        nombre_proceso = row["TABLA"].replace(".parquet", "")

        # Validar existencia en configuración JSON
        if nombre_proceso not in procesos_dict: 
            logger.warning( f"Proceso {nombre_proceso} no existe en configuración")
            continue
        proc = procesos_dict[nombre_proceso]
        # Validar tipo de ejecución
        ejecutar = False
        if tipo_ejecucion == "diario":
            ejecutar = proc.get("estado_diario", 0) == 1
        elif tipo_ejecucion == "semanal":
            ejecutar = proc.get("estado_semanal", 0) == 1
        elif tipo_ejecucion == "mensual":
            ejecutar = proc.get("estado_mensual", 0) == 1
        if not ejecutar:
            continue
        # Combinar información de tabla + json
        procesos_validos.append({ "row": row, "prioridad": proc["prioridad"],  "nombre_proceso": proc["nombre_proceso"]})

    prioridad_1 = [ p["row"] for p in procesos_validos if p["prioridad"] == 1 ]
    prioridad_2 = [ p["row"] for p in procesos_validos if p["prioridad"] == 2 ]
    return prioridad_1, prioridad_2


def ejecutar_extraccion():
    start_time = time.time()
    FECHA_PROCESO_GLOBAL = FechaProceso().fecha_proceso
    BATCH_ID_GLOBAL = datetime.now().strftime("%Y%m%d%H%M%S")
    validar_conectividad()
    conn_s2 = obtener_conexion_singlestore()
    param_df = pd.read_sql(SQL_PARAMETROS_PARQUET, conn_s2)

    prioridad_1, prioridad_2 = filtrar_procesos(
        param_df,
        PROCESOS,    # lista de diccionarios
        tipo_ejecucion="diario"
    )

    logger.info(f"Prioridad 1: {len(prioridad_1)}")
    logger.info(f"Prioridad 2: {len(prioridad_2)}")


    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [ executor.submit(procesar_tabla_incremental, row) for row in prioridad_1 ]

        for future in futures:
            future.result()

        futures = [executor.submit(procesar_tabla_incremental, row) for row in prioridad_2]
        for future in futures:
            future.result()

"""with DAG(
    dag_id = "Extraer_datos_bt",
    description="Carga Masiva de archivos parquet",
    start_date =datetime(2026,1,1),
    schedule= None,
    catchup= False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl","parquet","singlestore"]
    ) as dag:
    start_process = PythonOperator(
        task_id = "start_process",
        python_callable=start_process,
        execution_timeout=timedelta(hours=4)
    )"""