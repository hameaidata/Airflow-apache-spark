from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pyodbc


def conectar_bt():

    dsn = "BT PREPRODUCCION"
    user = "G48124738"
    password = "Whisper2@7"

    conn = pyodbc.connect(
        f"DSN={dsn};UID={user};PWD={password}",
        timeout=120
    )

    cursor = conn.cursor()

    cursor.execute("""
        SELECT CURRENT DATE
        FROM SYSIBM.SYSDUMMY1
    """)

    for row in cursor.fetchall():
        print(row)

    cursor.close()
    conn.close()


with DAG(
    dag_id="bt_odbc_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    task_bt = PythonOperator(
        task_id="conectar_bt",
        python_callable=conectar_bt
    )

    task_bt