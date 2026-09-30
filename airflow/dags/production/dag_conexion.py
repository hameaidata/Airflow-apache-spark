"""
bt_odbc_test - comprobacion de conectividad ODBC contra Bantotal.

QUE HACE
    Abre una conexion por el DSN de IBM i y pide la fecha del servidor. Nada
    mas. Sirve para separar "el driver ODBC esta bien" de "la consulta esta
    mal" cuando algo falla en los pipelines que si hacen trabajo.

POR QUE SE REESCRIBIO (2026-09-30)
    La version anterior tenia el usuario y la contrasena de BT PREPRODUCCION
    escritos en el propio archivo. Eso los ponia en tres sitios a la vez: en
    git con todo su historial, en la imagen de Docker, y en el traceback que
    Airflow muestra en la interfaz cuando la tarea falla -donde lo ve
    cualquiera que tenga acceso de lectura a los logs-.

    ESA CLAVE HAY QUE ROTARLA. Sacarla del archivo no la borra del historial
    de git: sigue estando en cada commit anterior.

    Ademas importaba pyodbc en la primera linea del modulo. El scheduler
    importa cada archivo de dags/ cada 30 segundos, asi que cargaba el driver
    ODBC entero unas 2.880 veces al dia sin usarlo nunca. Ahora el import esta
    dentro de la funcion.

QUE NECESITA
    Una Connection de Airflow llamada AF_BANTOTAL_PRE (tipo ODBC) con el DSN,
    el usuario y la clave. Se crea en Admin -> Connections, o con:

        airflow connections add AF_BANTOTAL_PRE \
            --conn-type odbc --conn-host "BT PREPRODUCCION" \
            --conn-login USUARIO --conn-password CLAVE

    El prefijo AF_ dice que la usa Airflow directamente, no Spark. Ver
    docs/CONVENCIONES.md.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# El DSN, el usuario y la clave viven en esta Connection, no aqui.
CONN_ID = "AF_BANTOTAL_PRE"


def probar_conexion() -> str:
    """Pide la fecha del servidor y la devuelve.

    El import de pyodbc va aqui dentro a proposito: a nivel de modulo lo
    ejecutaria el scheduler en cada pasada del parser.
    """
    from airflow.providers.odbc.hooks.odbc import OdbcHook

    hook = OdbcHook(odbc_conn_id=CONN_ID)

    # SYSIBM.SYSDUMMY1 es la tabla de una sola fila de DB2. Es la consulta mas
    # barata que confirma que la sesion quedo abierta de verdad: un connect que
    # devuelve sin error no siempre significa que el servidor respondio.
    with hook.get_conn() as conexion:
        cursor = conexion.cursor()
        try:
            cursor.execute("SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1")
            fila = cursor.fetchone()
        finally:
            cursor.close()

    if not fila:
        raise RuntimeError(
            f"{CONN_ID}: la conexion abrio pero SYSIBM.SYSDUMMY1 no devolvio "
            f"ninguna fila. Revise que el usuario tenga permiso de lectura."
        )

    fecha = str(fila[0])
    log.info("%s responde. Fecha del servidor: %s", CONN_ID, fecha)
    return fecha


default_args = {
    "owner": "datahub",
    "retries": 0,               # es un diagnostico: si falla, se quiere ver el fallo
    "execution_timeout": timedelta(minutes=2),
}

with DAG(
    dag_id="bt_odbc_test",
    description="Comprobacion de conectividad ODBC contra Bantotal (diagnostico)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    # Dos minutos de tarea, cinco de tope para la corrida. Si una comprobacion
    # de conectividad tarda mas que eso, ya contesto la pregunta.
    dagrun_timeout=timedelta(minutes=5),
    max_active_runs=1,
    default_args=default_args,
    tags=["manual", "diagnostico", "bantotal", "odbc"],
) as dag:

    PythonOperator(
        task_id="probar_conexion",
        python_callable=probar_conexion,
    )
