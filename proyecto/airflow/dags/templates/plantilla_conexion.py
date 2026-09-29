from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

def validar_bt():
    hook = OdbcHook(
        odbc_conn_id ="BT_PREPRODUCTION"
    )
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT  CURRENT DATE")
    resultado =cursor.fetchone()

    print("Conexion a BT exitosa")
    print(resultado)
    cursor.close()
    conn.close()

def validar_singlestore():
    hook = MySqlHook(
        mysql_conn_id = "CONEXION_SINGLESTORE"
    )
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT  NOW()")
    resultado = cursor.fetchone()
    print("Conexion a Singlestore exitosa")
    print(resultado)
    cursor.close()
    conn.close()

with DAG(
    dag_id ="VALIDAR_CONEXIONES",
    start_date = datetime(2026,1,1),
    schedule=None,
    catchup = False,
    tags= {"validar",'bt','singlestore'}
) as dag:
    test_bt = PythonOperator(
        task_id = "validar_bt"
        python_callable =validar_bt
    )
    test_singlestore =PythonOperator(
        task_id = "validar_singlestore",
        python_callable = validar_singlestore
    )
    [test_bt, test_singlestore]