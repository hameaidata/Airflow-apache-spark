from pyspark.sql import SparkSession

HOST = "166.72.194.22"


USER = "G48124738"
PASSWORD = "Credinka5$"

JAR = "/opt/airflow/jars/jt400-11.2.jar"

spark = (
    SparkSession.builder
    .appName("IBMi JDBC Test")
    .config("spark.jars", JAR)
    .getOrCreate()
)

jdbc_url = (
    f"jdbc:as400://{HOST};"
    "naming=system;"
    "errors=full"
)

properties = {
    "user": USER,
    "password": PASSWORD,
    "driver": "com.ibm.as400.access.AS400JDBCDriver"
}

try:
            
    df = spark.read.jdbc(
        url=jdbc_url,
        table="BTN_DB.FST017",
        properties=properties
    )

    print("================================================")
    print("CONEXION EXITOSA")
    print("================================================")

    df.show(10, False)

except Exception as e:
    print("ERROR DE CONEXION")
    print(str(e))
    raise

finally:
    spark.stop()



