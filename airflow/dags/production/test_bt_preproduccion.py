from pyspark.sql import SparkSession

HOST = "166.73.3.22"
PORT = 8471

USER = "G48124738"
PASSWORD = "Whisper2@7"

JAR = "/opt/airflow/jars/jt400-11.2.jar"

spark = (
    SparkSession.builder
    .appName("IBMi JDBC Test")
    .config("spark.jars", JAR)
    .config(
        "spark.driver.extraJavaOptions",
        "-Djava.awt.headless=true"
    )
    .config(
        "spark.executor.extraJavaOptions",
        "-Djava.awt.headless=true"
    )
    .getOrCreate()
)

jdbc_url = (
    f"jdbc:as400://{HOST}:{PORT};"
    "naming=system;"
    "errors=full;"
    "prompt=false;"
    "date format=iso;"
)

properties = {
    "user": USER,
    "password": PASSWORD,
    "driver": "com.ibm.as400.access.AS400JDBCDriver"
}

try:

    query = """
    (
        SELECT *
        FROM GPPPBTDB.FSR011
        FETCH FIRST 10 ROWS ONLY
    ) T
    """

    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )

    print("=" * 60)
    print("CONEXION EXITOSA")
    print("=" * 60)

    print("Registros:", df.count())

    df.show(10, False)

except Exception as e:

    print("=" * 60)
    print("ERROR DE CONEXION")
    print("=" * 60)
    print(type(e).__name__)
    print(str(e))

    raise

finally:
    spark.stop()