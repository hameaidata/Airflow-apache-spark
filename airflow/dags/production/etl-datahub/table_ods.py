from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.hooks.base import BaseHook
import singlestoredb as s2
import logging
import os
import re
import socket
import time


logger = logging.getLogger(__name__)
#=======================================
#           Variables
#=======================================
variables_ods_tablas =Variable.get("DAG_ODS_TABLAS")
CAPA = variables_ods_tablas["CAPA"]
AMBIENTE = variable_ods_tablas["AMBIENTE"]
EJECUCION = variable_ods_tablas["EJECUCION"]
GRUPOS = variable_ods_tablas["GRUPOS"]
AUDITORIA = variables_ods_tablas["AUDITORIA"]
VARIABLES_DEFAULT =variable_ods_tablas["SPARK"]
#=======================================
#           Conecciones
#=======================================

SINGLESTORE_CONN_ID = "CONEXION_SINGLESTORE"

class SingleStoreConnection:
    @staticmethod
    def get_connection():
        try:
            conn_airflow = BaseHook.get_connection(SINGLESTORE_CONN_ID)
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

    @staticmethod
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

class ConfigRepository:
    @staticmethod 
    def obtener_procesos( capa:str):
        #Validar conexion
        SingleStoreConnection.validar_conectividad()
        #Conectar a singlestore
        try:
            conn = SingleStoreConnection.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT ID_PROCESO, CAPA, GRUPO_PROCESO, NOMBRE_PROCESO, STORED_PROCEDURE FROM CTL_CFG_PROCESOS WHERE ACTIVO ='S' AND CAPA=%s ORDER BY GRUPO_PROCESO, ORDEN_EJECUCION",(capa, ))
            return [{
                "id_proceso":row[0],
                "capa":row[1],
                "grupo": row[2],
                "nombre": row[3],
                "sp": row[4]
            } for row in cursor.fetchall()
            ]
        finally:
            conn.close()

class LogRepository:
    @staticmethod
    def registrar(cfg, dag_id, run_id, estado, inicio, fin, error=None):
        # Validaicon de conexion a al base de datos
        SingleStoreConnection.validar_conectividad()
        conn = SingleStoreConnection.get_connection()
        try:
            
            cursor.execute(
                """INSERT INTO LOG_NOM_EJECUCIONES(ID_PROCESO, DAG_ID, RUN_ID, TASK_ID, CAPA, GRUPO_PROCESO, NOMBRE_PROCESO, FECHA_INICIO, FECHA_FIN, DURACION_SEGUNDOS, ESTADO, MENSAJE_ERROR) VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(
                cfg["id_proceso"],dag_id, run_id, cfg["nombre"],cfg["capa"],cfg["grupo"],cfg["nombre"], inicio, fin, round((fin-inicio).total_seconds(),2),estado, error
                )
            )
            conn.commit()
        finally:
            conn.close()

class ProcesoExecutor:
    @staticmethod
    def ejecutar(cfg):
        SingleStoreConnection.validar_conectividad()
        conn = SingleStoreConnection.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"CALL {cfg['sp']}()")
            conn.commit()
        finally:
            conn.close()

@task
def obtener_procesos():
    return ConfigRepository.obtener_procesos(capa="ODS")


@task
def ejecutar_proceso(cfg):
    inicio =datetime.now()
    estado = "SUCCESS"
    error = None
    try:
        ProcesoExecutor.ejecutar(cfg)
    except Exception as e:
        logger.info(f"[ERROR] :{e}")
        estado= "FAILED"
        error = str(e)
        raise
    finally:
        LogRepository.registrar(
            cfg =cfg,
            dag_id = "{{dag.dag_id}}",
            run_id = "{{run_id}}",
            estado = estado,
            inicio = inicio,
            fin = datetime.now(),
            error = error
        )
with DAG(
    dag_id="ODS_PROCESOS",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ODS"]
) as dag:

    ejecutar_proceso.expand(
        cfg=obtener_procesos()
    )