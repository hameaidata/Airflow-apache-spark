from pyspark.sql import SparkSession

SERVER = "PEDL27H0029WDB"
INSTANCE = "BBDDW03"
DATABASE = "GNBPE_DATAHUB"

USER = "usrairflowsqlcon"
PASSWORD = "airflow123"

JAR = "/opt/airflow/jars/mssql-jdbc.jar"

spark = (
    SparkSession.builder
    .appName("SQLServer JDBC Test")
    .config("spark.jars", JAR)
    .getOrCreate()
)

jdbc_url = (
    f"jdbc:sqlserver://{SERVER};"
    f"instanceName={INSTANCE};"
    f"databaseName={DATABASE};"
    "encrypt=true;"
    "trustServerCertificate=true;"
)

properties = {
    "user": USER,
    "password": PASSWORD,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="dbo.BDS_SALDOS_DIARIOS_CONSOLIDADOS",
    properties=properties
)

df.show(10, False)

spark.stop()