"""
22 — DB2 de IBM

Hay dos caminos y conviene elegir a conciencia:

  A) JDBC (JdbcHook)  -> driver jcc.jar, requiere Java. Es el que recomiendo:
                         no depende de librerias nativas de IBM y es el mismo
                         driver que usara Spark.

  B) ibm_db (nativo)  -> mas rapido, pero necesita el IBM Data Server Driver
                         instalado. Su descarga suele estar bloqueada en redes
                         corporativas. El Dockerfile lo intenta y tolera fallo.

Este ejemplo usa JDBC.

--- Aviso importante sobre el provider JDBC ---------------------------------
Desde apache-airflow-providers-jdbc 4.0.0, poner driver_path o driver_class en
el "extra" de la Connection esta DESHABILITADO por defecto (fue un vector de
ejecucion remota de codigo). Hay que activarlo explicitamente con estas dos
variables de entorno en los contenedores de Airflow:

    AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA=true
    AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA=true

Si no las pones, el hook ignora en silencio el driver del extra y falla con un
error de "driver not found" que no explica el motivo real.

La alternativa mas segura (y la que preferiria un auditor) es fijar el driver
en airflow.cfg, donde solo un administrador puede tocarlo:

    [providers.jdbc]
    allow_driver_path_in_extra = False
    allow_driver_class_in_extra = False

y definir el driver en la propia Connection desde la UI.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.jdbc.hooks.jdbc import JdbcHook

log = logging.getLogger(__name__)

CONN_ID = "db2_core"


def extraer_de_db2(**context):
    """Lectura sencilla desde DB2."""
    hook = JdbcHook(jdbc_conn_id=CONN_ID)

    sql = """
        SELECT NUM_CUENTA, SALDO, MONEDA, FECHA_CORTE
        FROM ESQUEMA.CUENTAS
        WHERE FECHA_CORTE = ?
        FETCH FIRST 1000 ROWS ONLY
    """
    filas = hook.get_records(sql, parameters=(context["ds"],))

    log.info("DB2 devolvio %d filas", len(filas))
    return {"filas": len(filas)}


def ejecutar_sp_db2(**context):
    """Procedimiento almacenado en DB2.

    La sintaxis es CALL, no EXEC (eso es de SQL Server).
    """
    hook = JdbcHook(jdbc_conn_id=CONN_ID)

    conn = hook.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("CALL ESQUEMA.SP_CONSOLIDAR_SALDOS(?)", (context["ds"],))

        # Si el SP devuelve un cursor de resultados:
        try:
            filas = cur.fetchall()
            log.info("El SP devolvio %d filas", len(filas))
        except Exception:
            # Muchos SPs de DB2 no devuelven filas; fetchall lanza excepcion.
            log.info("El SP no devolvio conjunto de resultados (es lo normal)")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def carga_incremental_db2(**context):
    """Patron incremental: leer solo lo nuevo desde la ultima marca.

    Evita traer la tabla completa cada dia, que es lo que mata estos procesos
    cuando la tabla crece.
    """
    from airflow.models import Variable

    hook = JdbcHook(jdbc_conn_id=CONN_ID)

    ultima_marca = Variable.get("db2_ultima_marca", default_var="1900-01-01 00:00:00")
    log.info("Leyendo registros posteriores a %s", ultima_marca)

    sql = """
        SELECT ID, NUM_CUENTA, IMPORTE, TS_ACTUALIZACION
        FROM ESQUEMA.MOVIMIENTOS
        WHERE TS_ACTUALIZACION > ?
        ORDER BY TS_ACTUALIZACION
        FETCH FIRST 50000 ROWS ONLY
    """
    filas = hook.get_records(sql, parameters=(ultima_marca,))

    if not filas:
        log.info("Sin registros nuevos")
        return {"filas": 0}

    # La nueva marca es el TS mas alto que efectivamente leimos.
    # Importante: se actualiza DESPUES de procesar, no antes. Si la tarea falla
    # a mitad, el reintento vuelve a leer desde la marca anterior y no se pierde
    # nada. Al reves perderias registros silenciosamente.
    nueva_marca = str(max(f[3] for f in filas))

    # ... aqui iria el procesamiento real ...

    Variable.set("db2_ultima_marca", nueva_marca)
    log.info("Procesadas %d filas. Nueva marca: %s", len(filas), nueva_marca)

    return {"filas": len(filas), "marca": nueva_marca}


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ej22_db2_jdbc",
    description="DB2 de IBM via JDBC",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ejemplo", "db2"],
) as dag:

    extraer = PythonOperator(task_id="extraer", python_callable=extraer_de_db2)
    sp = PythonOperator(task_id="ejecutar_sp", python_callable=ejecutar_sp_db2)
    incremental = PythonOperator(
        task_id="carga_incremental", python_callable=carga_incremental_db2
    )

    extraer >> sp >> incremental


# =============================================================================
# CREAR LA CONNECTION DE DB2
# =============================================================================
#
#   docker compose exec airflow-scheduler airflow connections add db2_core \
#     --conn-type jdbc \
#     --conn-host 'jdbc:db2://servidor-db2:50000/BDNOMBRE' \
#     --conn-login usuario \
#     --conn-password 'clave' \
#     --conn-extra '{
#         "driver_path": "/opt/airflow/jars/db2-jcc.jar",
#         "driver_class": "com.ibm.db2.jcc.DB2Driver"
#     }'
#
# Fijate en que para JDBC el "host" es la URL JDBC COMPLETA, no solo el nombre
# del servidor. Es la confusion mas frecuente con este hook.
#
# Con SSL (lo habitual en banca):
#   jdbc:db2://servidor:50001/BD:sslConnection=true;
#   (los dos puntos y el punto y coma finales son parte de la sintaxis de DB2)
