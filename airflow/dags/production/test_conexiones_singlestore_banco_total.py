from datetime import datetime
import os

import jaydebeapi

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


# ============================================================
# DRIVERS JDBC
# ============================================================

DRIVERS = {
    # ========================================================
    # SQL SERVER
    # ========================================================
    "mssql": (
        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "/opt/airflow/jars/mssql-jdbc.jar",
    ),

    # ========================================================
    # BANCO TOTAL
    #
    # BT utiliza el driver configurado como "odbc".
    #
    # IMPORTANTE:
    # El endpoint JDBC debe estar disponible en:
    #
    # conn.extra_dejson["jdbc_url"]
    #
    # porque BT solo tiene usuario y contraseña.
    # ========================================================
    "odbc": (
        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "/opt/airflow/jars/mssql-jdbc.jar",
    ),

    # ========================================================
    # SINGLESTORE
    # ========================================================
    "singlestore": (
        "com.singlestore.jdbc.Driver",
        "/opt/airflow/jars/singlestore-jdbc.jar",
    ),
}


# ============================================================
# AIRFLOW CONNECTIONS
# ============================================================

SQLSERVER_CONN_ID = "sqlserver_connection"
SINGLESTORE_CONN_ID = "singlestore_connection"
BT_CONN_ID = "banco_total_connection"


# ============================================================
# VALIDAR DRIVER / JAR
# ============================================================

def validar_driver(driver_class, driver_jar, conn_id):

    print(f"Driver : {driver_class}")
    print(f"JAR    : {driver_jar}")

    if not os.path.isfile(driver_jar):
        raise FileNotFoundError(
            f"[{conn_id}] No existe el JAR del driver: "
            f"{driver_jar}"
        )


# ============================================================
# CONSTRUIR JDBC URL - SQL SERVER
# ============================================================

def construir_sqlserver_jdbc_url(conn):

    if not conn.host:
        raise ValueError(
            f"[{conn.conn_id}] Host no configurado."
        )

    if not conn.port:
        raise ValueError(
            f"[{conn.conn_id}] Port no configurado."
        )

    if not conn.schema:
        raise ValueError(
            f"[{conn.conn_id}] Database/schema no configurado."
        )

    return (
        f"jdbc:sqlserver://"
        f"{conn.host}:{conn.port};"
        f"databaseName={conn.schema};"
        f"encrypt=false;"
        f"trustServerCertificate=true"
    )


# ============================================================
# CONSTRUIR JDBC URL - SINGLESTORE
# ============================================================

def construir_singlestore_jdbc_url(conn):

    if not conn.host:
        raise ValueError(
            f"[{conn.conn_id}] Host no configurado."
        )

    if not conn.port:
        raise ValueError(
            f"[{conn.conn_id}] Port no configurado."
        )

    if not conn.schema:
        raise ValueError(
            f"[{conn.conn_id}] Database/schema no configurado."
        )

    return (
        f"jdbc:singlestore://"
        f"{conn.host}:{conn.port}/"
        f"{conn.schema}"
    )


# ============================================================
# OBTENER JDBC URL - BANCO TOTAL
# ============================================================

def obtener_bt_jdbc_url(conn):

    extra = conn.extra_dejson

    jdbc_url = extra.get("jdbc_url")

    if not jdbc_url:
        raise ValueError(
            f"[{conn.conn_id}] Banco Total solo tiene "
            "usuario y contraseña, por lo que debe existir "
            "'jdbc_url' dentro de Extra de la Airflow Connection."
        )

    return jdbc_url


# ============================================================
# VALIDAR CREDENCIALES
# ============================================================

def validar_credenciales(conn):

    if not conn.login:
        raise ValueError(
            f"[{conn.conn_id}] Usuario no configurado."
        )

    if not conn.password:
        raise ValueError(
            f"[{conn.conn_id}] Password no configurado."
        )


# ============================================================
# TEST JDBC GENÉRICO
# ============================================================

def ejecutar_test_jdbc(
    conn,
    driver_class,
    driver_jar,
    jdbc_url,
):

    validar_credenciales(conn)

    validar_driver(
        driver_class,
        driver_jar,
        conn.conn_id,
    )

    print("-" * 80)
    print(f"Connection ID : {conn.conn_id}")
    print(f"Conn Type     : {conn.conn_type}")
    print(f"User          : {conn.login}")
    print(f"Driver        : {driver_class}")
    print(f"JDBC URL      : {jdbc_url}")
    print("-" * 80)

    connection = None
    cursor = None

    try:

        # ====================================================
        # CONEXIÓN JDBC
        # ====================================================

        print("Cargando driver JDBC...")

        connection = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            [
                conn.login,
                conn.password,
            ],
            driver_jar,
        )

        print(
            "Driver JDBC cargado correctamente."
        )

        print(
            "Conexión JDBC establecida correctamente."
        )

        # ====================================================
        # TEST REAL
        # ====================================================

        cursor = connection.cursor()

        print(
            "Ejecutando SELECT 1..."
        )

        cursor.execute(
            "SELECT 1"
        )

        result = cursor.fetchone()

        print(
            f"Resultado SELECT 1: {result}"
        )

        if result is None:
            raise RuntimeError(
                f"[{conn.conn_id}] "
                "SELECT 1 no devolvió resultados."
            )

        print("-" * 80)
        print(
            f"CONEXIÓN EXITOSA: {conn.conn_id}"
        )
        print("-" * 80)

    except Exception as e:

        print("=" * 80)
        print(
            f"ERROR DE CONEXIÓN: {conn.conn_id}"
        )
        print("=" * 80)

        print(
            f"Tipo   : {type(e).__name__}"
        )

        print(
            f"Detalle: {e}"
        )

        print("=" * 80)

        raise

    finally:

        if cursor is not None:

            try:
                cursor.close()

            except Exception:
                pass

        if connection is not None:

            try:
                connection.close()

                print(
                    f"Conexión cerrada: {conn.conn_id}"
                )

            except Exception:
                pass


# ============================================================
# TEST SQL SERVER
# ============================================================

def test_sqlserver():

    print("\n")
    print("=" * 80)
    print("TEST DE CONEXIÓN - SQL SERVER")
    print("=" * 80)

    conn = BaseHook.get_connection(
        SQLSERVER_CONN_ID
    )

    print(
        f"Connection ID : {conn.conn_id}"
    )

    print(
        f"Conn Type     : {conn.conn_type}"
    )

    print(
        f"Host          : {conn.host}"
    )

    print(
        f"Port          : {conn.port}"
    )

    print(
        f"Database      : {conn.schema}"
    )

    print(
        f"User          : {conn.login}"
    )

    driver_class, driver_jar = DRIVERS["mssql"]

    jdbc_url = construir_sqlserver_jdbc_url(
        conn
    )

    ejecutar_test_jdbc(
        conn=conn,
        driver_class=driver_class,
        driver_jar=driver_jar,
        jdbc_url=jdbc_url,
    )


# ============================================================
# TEST SINGLESTORE
# ============================================================

def test_singlestore():

    print("\n")
    print("=" * 80)
    print("TEST DE CONEXIÓN - SINGLESTORE")
    print("=" * 80)

    conn = BaseHook.get_connection(
        SINGLESTORE_CONN_ID
    )

    print(
        f"Connection ID : {conn.conn_id}"
    )

    print(
        f"Conn Type     : {conn.conn_type}"
    )

    print(
        f"Host          : {conn.host}"
    )

    print(
        f"Port          : {conn.port}"
    )

    print(
        f"Database      : {conn.schema}"
    )

    print(
        f"User          : {conn.login}"
    )

    driver_class, driver_jar = DRIVERS[
        "singlestore"
    ]

    jdbc_url = construir_singlestore_jdbc_url(
        conn
    )

    ejecutar_test_jdbc(
        conn=conn,
        driver_class=driver_class,
        driver_jar=driver_jar,
        jdbc_url=jdbc_url,
    )


# ============================================================
# TEST BANCO TOTAL
# ============================================================

def test_banco_total():

    print("\n")
    print("=" * 80)
    print("TEST DE CONEXIÓN - BANCO TOTAL")
    print("=" * 80)

    conn = BaseHook.get_connection(
        BT_CONN_ID
    )

    print(
        f"Connection ID : {conn.conn_id}"
    )

    print(
        f"Conn Type     : {conn.conn_type}"
    )

    print(
        f"User          : {conn.login}"
    )

    # ========================================================
    # BT NO TIENE:
    #
    # host
    # port
    # database
    #
    # Por eso NO usamos conn.host / conn.port /
    # conn.schema.
    # ========================================================

    driver_class, driver_jar = DRIVERS["odbc"]

    jdbc_url = obtener_bt_jdbc_url(
        conn
    )

    ejecutar_test_jdbc(
        conn=conn,
        driver_class=driver_class,
        driver_jar=driver_jar,
        jdbc_url=jdbc_url,
    )


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="test_conexiones_bases_datos",

    description=(
        "Test de conexión JDBC para "
        "SQL Server, SingleStore y Banco Total"
    ),

    start_date=datetime(
        2026,
        1,
        1,
    ),

    schedule=None,

    catchup=False,

    tags=[
        "jdbc",
        "sqlserver",
        "singlestore",
        "banco-total",
    ],

) as dag:

    # ========================================================
    # SQL SERVER
    # ========================================================

    test_sqlserver_task = PythonOperator(
        task_id="test_sqlserver",

        python_callable=test_sqlserver,
    )

    # ========================================================
    # SINGLESTORE
    # ========================================================

    test_singlestore_task = PythonOperator(
        task_id="test_singlestore",

        python_callable=test_singlestore,
    )

    # ========================================================
    # BANCO TOTAL
    # ========================================================

    test_banco_total_task = PythonOperator(
        task_id="test_banco_total",

        python_callable=test_banco_total,
    )

    # ========================================================
    # LAS TRES TASKS SON INDEPENDIENTES
    # ========================================================

    test_sqlserver_task

    test_singlestore_task

    test_banco_total_task