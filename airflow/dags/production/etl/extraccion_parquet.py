from __future__ import annotations

import logging
import os
import re
import shutil
import socket
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import singlestoredb as s2

# Se quitaron pyodbc y pyarrow.dataset: no se usaban.
#
# ibm_db (cliente DB2 para BT) se importa DENTRO de obtener_conexion_bt().
# El Dockerfile lo instala tolerando el fallo:
#     RUN pip install "ibm-db" || echo "AVISO: ibm-db no se instalo"
# asi que puede faltar en la imagen sin que el build haya fallado. Con el
# import a nivel de modulo, eso hacia desaparecer de la UI al DAG que
# importa este archivo. Ahora solo falla si de verdad se usa BT, y con un
# mensaje que dice que hacer.

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
# Se leen en ejecucion, NO al importar el modulo.
#
# Antes estaban a nivel de modulo: Airflow ejecuta este archivo en cada ciclo
# de parseo (cada 30 s por defecto), asi que golpeaba la metadata database
# constantemente, y si la Variable faltaba el DAG que importa este modulo
# desaparecia de la UI con un error de import en vez de fallar en la tarea.
variables_config: dict = {}
MAX_WORKERS = 1
CHUNK_SIZE = 10000
OUTPUT_DIR = "/opt/spark-data/parquet"
SQL_FECHA_PROCESO = ""
PROCESOS: list = []
TABLA_AUDITORIA_PARQUET = ""
SQL_PARAMETROS_PARQUET = ""


def cargar_variables_config() -> dict:
    """Carga EXTRACCION_BT_STG. Se llama al inicio de ejecutar_extraccion()."""
    global variables_config, MAX_WORKERS, CHUNK_SIZE, OUTPUT_DIR
    global SQL_FECHA_PROCESO, PROCESOS, TABLA_AUDITORIA_PARQUET
    global SQL_PARAMETROS_PARQUET, ERROR_SIZE_LIMIT

    variables_config = Variable.get("EXTRACCION_BT_STG", deserialize_json=True)
    MAX_WORKERS = int(variables_config["max_worker"])
    CHUNK_SIZE = int(variables_config["chunk_size"])
    OUTPUT_DIR = variables_config["output_dir"]
    SQL_FECHA_PROCESO = variables_config["sql_fecha"]
    PROCESOS = variables_config["procesos"]
    TABLA_AUDITORIA_PARQUET = variables_config["tb_proceso_parquet"]
    SQL_PARAMETROS_PARQUET = variables_config["sql_parametros_parquet"]
    # Antes estaba hardcodeado en 4000 aqui y en 65000 en el JSON de carga:
    # dos verdades para el mismo limite. Ahora manda la Variable.
    ERROR_SIZE_LIMIT = int(variables_config.get("error_size_limit", 4000))
    return variables_config


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
    """Conexion DB2 a BT.

    Tenia tres errores que garantizaban un NameError si se llamaba:
      - reasignaba conn_bt (la Connection) a la cadena de conexion
      - usaba conn.password, variable que no existe
      - pasaba conn_str a ibm_db.connect, que tampoco existia
    """
    try:
        import ibm_db
    except ImportError as exc:
        raise Exception(
            "El cliente DB2 (ibm_db) no esta instalado en la imagen, asi que no se "
            "puede conectar a BT. El Dockerfile lo instala tolerando el fallo; "
            "revisa el log del build o instalalo con: pip install ibm-db ibm-db-sa"
        ) from exc

    conn_airflow = BaseHook.get_connection(BT_CONN_ID)
    logger.info("Estableciendo conexion con BT %s:%s", conn_airflow.host, conn_airflow.port)
    try:
        conn_str = (
            f"DATABASE={conn_airflow.schema};"
            f"HOSTNAME={conn_airflow.host};"
            f"PORT={conn_airflow.port};"
            "PROTOCOL=TCPIP;"
            "CONNECTTIMEOUT=10;"
            f"UID={conn_airflow.login};"
            f"PWD={conn_airflow.password};"
        )
        return ibm_db.connect(conn_str, "", "")
    except Exception:
        logger.exception("[ERROR] Obtencion conexion BT")
        raise

def gestionar_control_proceso(
    conn, cur, accion, id_log=None, tabla_origen=None,
    ruta=None, host_name=None, estado=None,
    mensaje_error=None, total_rows=None, total_time=None,
    batch_id=None, fecha_proceso=None
):
    """Gestiona la fila de CTL_PROCESO_PARQUET de una tabla.

    archivo_parquet guarda la RUTA COMPLETA dentro del contenedor
    (/data/parquet/<yyyyMMdd>/<archivo>.parquet), no solo el nombre. Esa es
    la ruta que despues lee la carga para subir a STG: la tabla es el punto
    de encuentro entre las dos mitades del pipeline.
    """
    if accion == "INICIO":
        cur.execute(
            f"""INSERT INTO {TABLA_AUDITORIA_PARQUET}
            (nom_proceso,tabla_origen,archivo_parquet,batch_id,fecha_proceso,
             fec_inicio,estado,host_name)
            VALUES (%s,%s,%s,%s,%s,NOW(6),%s,%s)""",
            (NOM_PROCESO, tabla_origen, ruta, batch_id, fecha_proceso,
             ESTADO_INICIADO, host_name)
        )
        conn.commit()
        return cur.lastrowid
    tiempo = round(float(total_time), 2) if total_time is not None else 0

    if accion == "RUTA":
        # La ruta definitiva se conoce despues del INSERT (depende de la fecha
        # de proceso, que se lee de la base). Se actualiza aqui.
        cur.execute(
            f"UPDATE {TABLA_AUDITORIA_PARQUET} SET archivo_parquet=%s WHERE id_log=%s",
            (ruta, id_log)
        )

    elif accion == "ESTADO":
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

def carpeta_del_dia() -> str:
    """Carpeta de salida de esta corrida: OUTPUT_DIR/<yyyyMMdd>.

    Usa FECHA_PROCESO_GLOBAL (la fecha de negocio que devuelve sql_fecha), no
    datetime.now(). Con el reloj, una corrida que cruza la medianoche partia
    los archivos en dos carpetas, y un reproceso de una fecha pasada los
    dejaba en la carpeta de hoy mientras la columna FECHA_PROCESO de adentro
    del parquet decia otra cosa.
    """
    fecha = FECHA_PROCESO_GLOBAL
    if hasattr(fecha, "strftime"):
        sufijo = fecha.strftime("%Y%m%d")
    else:
        # sql_fecha puede devolver texto; se normaliza quitando separadores.
        sufijo = str(fecha or "").strip()[:10].replace("-", "").replace("/", "")
    if not sufijo:
        raise Exception(
            "No se pudo determinar la fecha de proceso para la carpeta de salida. "
            "Revisa sql_fecha en la Variable EXTRACCION_BT_STG."
        )
    return os.path.join(OUTPUT_DIR, sufijo)


def procesar_tabla_incremental(row):

    tabla, archivo = row["TABLA"], row["NOMBRE_PARQUET"]
    conn_bt = conn_s2 = conn_log = writer = None
    cur_log = None
    id_log, total = None, 0
    inicio = time.time()

    try:
        conn_log = obtener_conexion_singlestore()
        cur_log = conn_log.cursor()

        # La ruta definitiva se calcula ANTES del INSERT para que la tabla de
        # control tenga desde el primer momento la RUTA COMPLETA. Esa es la
        # ruta que la carga va a leer despues para subir a STG.
        directorio_salida = carpeta_del_dia()
        os.makedirs(directorio_salida, exist_ok=True)
        output_path = os.path.join(directorio_salida, archivo)

        # host_name antes no se pasaba nunca, asi que la columna quedaba en
        # NULL y era imposible saber que worker ejecuto cada extraccion.
        id_log = gestionar_control_proceso(
            conn=conn_log, cur=cur_log, accion="INICIO",
            tabla_origen=tabla,
            ruta=output_path,
            host_name=socket.gethostname(),
            batch_id=BATCH_ID_GLOBAL,
            fecha_proceso=FECHA_PROCESO_GLOBAL,
        )
        logger.info("[%s] destino %s (batch %s)", tabla, output_path, BATCH_ID_GLOBAL)

        gestionar_control_proceso(conn=conn_log, cur=cur_log, accion="ESTADO",
                                  id_log=id_log, estado=ESTADO_EJECUTANDO)

        conn_s2 = obtener_conexion_singlestore()
        # conn_bt = obtener_conexion_bt() -- 1
        dtype_map = parse_dtype_map(row["TIPOS"])

        query = f"SELECT {row['COLUMNAS']} FROM {row['ESQUEMA']}.{tabla}"
        if row["FILTRO"]:
            query += f" WHERE {row['FILTRO']}"

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
            try:
                gestionar_control_proceso(
                    conn_log,
                    cur_log,
                    "ERROR",
                    id_log=id_log,
                    mensaje_error=str(e)
                )
            except Exception:
                logger.exception(
                    "Ademas del fallo de %s, no se pudo escribir ERROR en %s (id_log=%s)",
                    tabla, TABLA_AUDITORIA_PARQUET, id_log,
                )

        # Antes hacia "return f'ERROR -> ...'": la excepcion no llegaba al
        # llamador, future.result() no lanzaba nada y la tarea terminaba en
        # SUCCESS con tablas sin extraer.
        raise

    finally:

        if writer:
            writer.close()

        if cur_log:
            cur_log.close()

        if conn_bt:
            conn_bt.close()

        # conn_s2 no estaba en este finally: se filtraba una conexion por
        # tabla en cada corrida.
        if conn_s2:
            conn_s2.close()

        if conn_log:
            conn_log.close()

def filtrar_procesos(param_df, procesos, tipo_ejecucion="diario"):
    procesos_dict = {p["nombre_proceso"]: p for p in procesos}
    estado_ejecucion = {"diario":"estado_diario","semanal":"estado_semanal","mensual":"estado_mensual"}
    flag_ejecucion =  estado_ejecucion.get(tipo_ejecucion)
    if flag_ejecucion is None:
        raise ValueError(f"Tipo de ejecucion no soportada: {tipo_ejecucion}")
    procesos_validos = []
    for _, row in param_df.iterrows():
        nombre_proceso =  row["TABLA"]
        proc =  procesos_dict.get(nombre_proceso)
        if proc  is None:
            logger.warning("Proceso [%s] no encontrado en configuracion JSON",nombre_proceso)
            continue
        #Proceso activo en tabla
        if row["ACTIVO"] !="S":
            logger.info("Proceso [%s] descartado: ACTIVO != 'S' en la tabla de parametros", nombre_proceso)
            continue
        # Activo en variable airflow
        if proc.get("estado",0) !=1:
            logger.info("Proceso[%s] descartado estado=0 en JSON", nombre_proceso)
            continue
        # Activo para la frecuencia de ejecucion
        if proc.get(flag_ejecucion, 0) !=1:
            logger.info("Proceso [%s] descartado .%s=0", nombre_proceso, flag_ejecucion)
            continue
        procesos_validos.append({"row": row, "prioridad": proc.get("prioridad",2),"nombre_proceso":nombre_proceso})
        logger.info("Proceso [%s] habilitado para ejecucion", nombre_proceso)

    # OJO: estas tres lineas estaban DENTRO del for, con el return incluido.
    # Eso hacia que la funcion terminara al encontrar el primer proceso
    # valido, asi que solo se extraia UNA tabla por corrida sin importar
    # cuantas estuvieran activas. Y si ninguna era valida, devolvia None y
    # el desempaquetado del llamador reventaba con TypeError.
    prioridad_1 = [p["row"] for p in procesos_validos if p["prioridad"] == 1]
    prioridad_2 = [p["row"] for p in procesos_validos if p["prioridad"] == 2]

    otras = [p["nombre_proceso"] for p in procesos_validos if p["prioridad"] not in (1, 2)]
    if otras:
        logger.warning("Procesos descartados por prioridad fuera de (1,2): %s", otras)

    return prioridad_1, prioridad_2

def ejecutar_extraccion(tipo_ejecucion: str = "diario"):
    # global es obligatorio: sin el, estas dos asignaciones creaban variables
    # LOCALES y las globales seguian en None, asi que cada parquet se escribia
    # con FECHA_PROCESO y BATCH_ID nulos. Era un bug de datos silencioso: el
    # proceso terminaba en TERMINADO con los archivos mal.
    global FECHA_PROCESO_GLOBAL, BATCH_ID_GLOBAL

    start_time = time.time()
    cargar_variables_config()
    validar_conectividad()

    FECHA_PROCESO_GLOBAL = FechaProceso().fecha_proceso
    BATCH_ID_GLOBAL = datetime.now().strftime("%Y%m%d%H%M%S")
    logger.info("Fecha de proceso=%s batch_id=%s", FECHA_PROCESO_GLOBAL, BATCH_ID_GLOBAL)

    conn_s2 = None
    try:
        conn_s2 = obtener_conexion_singlestore()
        param_df = pd.read_sql(SQL_PARAMETROS_PARQUET, conn_s2)
    finally:
        if conn_s2:
            conn_s2.close()

    prioridad_1, prioridad_2 = filtrar_procesos(
        param_df,
        PROCESOS,
        tipo_ejecucion=tipo_ejecucion,
    )
    logger.info("Tablas a extraer | prioridad 1: %s | prioridad 2: %s",
                len(prioridad_1), len(prioridad_2))

    errores: list[str] = []

    def ejecutar_lote(nombre_lote: str, filas) -> None:
        if not filas:
            return
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futuros = {executor.submit(procesar_tabla_incremental, row): row for row in filas}
            for futuro in as_completed(futuros):
                tabla = futuros[futuro].get("TABLA", "?")
                try:
                    logger.info(futuro.result())
                except Exception as exc:
                    errores.append(f"{nombre_lote}:{tabla}: {exc}")

    ejecutar_lote("prioridad_1", prioridad_1)
    ejecutar_lote("prioridad_2", prioridad_2)

    logger.info("Extraccion finalizada en %.2f s", time.time() - start_time)

    # Sin esto la tarea terminaba en SUCCESS aunque fallaran tablas.
    if errores:
        raise Exception("Fallaron procesos de extraccion: " + " | ".join(errores))

    # Se devuelve por XCom para que la carga sepa QUE batch tiene que subir.
    # Sin esto la carga tendria que adivinar, y si la extraccion de hoy fallo
    # podria terminar recargando los archivos de ayer.
    return BATCH_ID_GLOBAL


def limpiar_parquet(dias_retencion: int | None = None) -> dict:
    """Borra las carpetas de parquet mas viejas que la retencion configurada.

    La carpeta esta FUERA del contenedor (bind mount de PARQUET_HOST_DIR),
    asi que sin esto crece indefinidamente en el disco del servidor: mover el
    problema de sitio no lo resuelve.

    Con dias_retencion = 0 no borra nada, solo informa cuanto ocupa.
    """
    cargar_variables_config()

    if dias_retencion is None:
        dias_retencion = int(variables_config.get("retencion_dias", 7))

    if not os.path.isdir(OUTPUT_DIR):
        logger.warning("No existe %s; nada que limpiar.", OUTPUT_DIR)
        return {"borradas": 0, "conservadas": 0, "bytes_liberados": 0}

    limite = (datetime.now() - timedelta(days=dias_retencion)).strftime("%Y%m%d")
    borradas, conservadas, liberados = [], 0, 0

    for nombre in sorted(os.listdir(OUTPUT_DIR)):
        ruta = os.path.join(OUTPUT_DIR, nombre)
        # Solo carpetas con formato yyyyMMdd: nunca se toca nada mas, para que
        # un error de configuracion no borre datos ajenos.
        if not (os.path.isdir(ruta) and len(nombre) == 8 and nombre.isdigit()):
            continue

        if dias_retencion > 0 and nombre < limite:
            tam = sum(
                os.path.getsize(os.path.join(raiz, f))
                for raiz, _, archivos in os.walk(ruta)
                for f in archivos
            )
            shutil.rmtree(ruta)
            borradas.append(nombre)
            liberados += tam
        else:
            conservadas += 1

    resumen = {
        "borradas": len(borradas),
        "carpetas": borradas,
        "conservadas": conservadas,
        "mb_liberados": round(liberados / 1024 / 1024, 2),
        "retencion_dias": dias_retencion,
    }
    logger.info("Limpieza de %s: %s", OUTPUT_DIR, resumen)
    return resumen

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