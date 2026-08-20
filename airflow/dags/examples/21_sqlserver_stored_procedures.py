"""
21 — SQL Server: consultas y procedimientos almacenados

Cubre:
  - SELECT sencillo con SQLExecuteQueryOperator
  - EXEC de un SP sin resultado (carga, mantenimiento)
  - EXEC de un SP CON resultado, leyendo las filas
  - SP con parametro de salida (OUTPUT)
  - Manejo de transacciones y errores

Requiere: Connection "sqlserver_core" de tipo mssql, y la imagen personalizada
(el provider microsoft-mssql trae pymssql, que no viene en la imagen oficial).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

log = logging.getLogger(__name__)

CONN_ID = "sqlserver_core"


# =============================================================================
# 1. SP sin resultado — el caso mas comun (recalculo, carga, purga)
# =============================================================================

def ejecutar_sp_sin_resultado(**context):
    """Ejecuta un SP que no devuelve filas.

    hook.run() abre conexion, ejecuta, hace commit y cierra.
    """
    hook = MsSqlHook(mssql_conn_id=CONN_ID)

    fecha_proceso = context["ds"]  # 'YYYY-MM-DD' de la fecha logica del run

    # SET NOCOUNT ON evita que los mensajes "N rows affected" confundan al
    # driver. Sin esto, algunos SPs hacen que pymssql devuelva resultados vacios
    # antes del real, y acabas leyendo None sin entender por que.
    sql = "SET NOCOUNT ON; EXEC dbo.sp_procesar_movimientos_diarios @fecha = %s"

    log.info("Ejecutando SP para la fecha %s", fecha_proceso)
    hook.run(sql, parameters=(fecha_proceso,), autocommit=True)
    log.info("SP completado")


# =============================================================================
# 2. SP CON resultado — leer las filas que devuelve
# =============================================================================

def ejecutar_sp_con_resultado(**context):
    """Ejecuta un SP y recoge el conjunto de filas que devuelve."""
    hook = MsSqlHook(mssql_conn_id=CONN_ID)

    sql = "SET NOCOUNT ON; EXEC dbo.sp_resumen_cartera @fecha = %s, @sucursal = %s"
    filas = hook.get_records(sql, parameters=(context["ds"], "LIMA-01"))

    log.info("El SP devolvio %d filas", len(filas))
    for fila in filas[:5]:
        log.info("  %s", fila)

    # get_pandas_df() si prefieres un DataFrame:
    #   df = hook.get_pandas_df(sql, parameters=(...))
    #
    # OJO con el tamano: get_records y get_pandas_df traen TODO a memoria del
    # worker de Airflow. Para volumenes grandes usa Spark (ejemplo 23) o
    # escribe el resultado a una tabla y leela por partes.

    # Solo devolver metadatos por XCom. XCom se guarda en la base de datos de
    # Airflow: meter ahi un dataset completo la infla y degrada el scheduler.
    return {"filas": len(filas), "fecha": context["ds"]}


# =============================================================================
# 3. SP con parametros OUTPUT
# =============================================================================

def ejecutar_sp_con_output(**context):
    """SP que devuelve valores por parametros OUTPUT.

    Aqui hay que bajar al cursor: hook.run() no expone los OUTPUT.
    """
    hook = MsSqlHook(mssql_conn_id=CONN_ID)

    conn = hook.get_conn()
    try:
        cur = conn.cursor()

        # Se declaran variables T-SQL, se llama al SP y se hace SELECT de ellas.
        # Es mas portable que callproc() y funciona con cualquier driver.
        cur.execute(
            """
            SET NOCOUNT ON;
            DECLARE @total INT, @estado VARCHAR(50);
            EXEC dbo.sp_validar_lote
                 @lote_id = %s,
                 @total_registros = @total OUTPUT,
                 @estado = @estado OUTPUT;
            SELECT @total AS total_registros, @estado AS estado;
            """,
            (context["run_id"],),
        )
        resultado = cur.fetchone()
        conn.commit()

        total, estado = resultado[0], resultado[1]
        log.info("total_registros=%s estado=%s", total, estado)

        if estado != "OK":
            raise ValueError(f"El SP reporto estado '{estado}' — se aborta el flujo")

        return {"total": total, "estado": estado}

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# =============================================================================
# 4. Varias sentencias en una sola transaccion
# =============================================================================

def transaccion_multiple(**context):
    """Varias operaciones que deben confirmarse o revertirse juntas.

    autocommit=False es la clave: sin eso, cada sentencia se confirma sola y
    un fallo a mitad deja la base en un estado inconsistente.
    """
    hook = MsSqlHook(mssql_conn_id=CONN_ID)
    conn = hook.get_conn()
    conn.autocommit(False)

    try:
        cur = conn.cursor()
        cur.execute("SET NOCOUNT ON;")

        cur.execute(
            "INSERT INTO dbo.control_cargas (fecha, estado, iniciado_en) "
            "VALUES (%s, 'EN_PROCESO', GETDATE())",
            (context["ds"],),
        )
        cur.execute("EXEC dbo.sp_cargar_staging @fecha = %s", (context["ds"],))
        cur.execute("EXEC dbo.sp_promover_a_produccion @fecha = %s", (context["ds"],))
        cur.execute(
            "UPDATE dbo.control_cargas SET estado='OK', finalizado_en=GETDATE() "
            "WHERE fecha = %s",
            (context["ds"],),
        )

        conn.commit()
        log.info("Transaccion confirmada")

    except Exception as exc:
        conn.rollback()
        log.error("Transaccion revertida: %s", exc)
        raise
    finally:
        conn.close()


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="ej21_sqlserver_sp",
    description="SQL Server: consultas y procedimientos almacenados",
    default_args=default_args,
    schedule="0 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ejemplo", "sqlserver"],
) as dag:

    # -------------------------------------------------------------------------
    # SELECT declarativo. El campo "sql" es templated: puedes usar Jinja dentro.
    # -------------------------------------------------------------------------
    consulta_simple = SQLExecuteQueryOperator(
        task_id="consulta_simple",
        conn_id=CONN_ID,
        sql="""
            SELECT TOP 10 cuenta_id, saldo, moneda
            FROM dbo.cuentas
            WHERE fecha_corte = '{{ ds }}'
            ORDER BY saldo DESC
        """,
        show_return_value_in_logs=False,   # el resultado puede traer datos sensibles
    )

    # También puedes apuntar a un archivo .sql en dags/ (mas mantenible):
    #   sql="sql/consulta_cartera.sql"

    sp_sin_resultado = PythonOperator(
        task_id="sp_sin_resultado",
        python_callable=ejecutar_sp_sin_resultado,
    )

    sp_con_resultado = PythonOperator(
        task_id="sp_con_resultado",
        python_callable=ejecutar_sp_con_resultado,
    )

    sp_con_output = PythonOperator(
        task_id="sp_con_output",
        python_callable=ejecutar_sp_con_output,
    )

    tx = PythonOperator(
        task_id="transaccion_multiple",
        python_callable=transaccion_multiple,
    )

    consulta_simple >> sp_sin_resultado >> sp_con_resultado >> sp_con_output >> tx
