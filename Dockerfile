# ============================================================================
# IMAGEN AIRFLOW PERSONALIZADA
# ----------------------------------------------------------------------------
# La imagen oficial apache/airflow NO trae:
#   - spark-submit          (lo necesita SparkSubmitOperator)
#   - drivers JDBC          (SQL Server, DB2)
#   - clientes Python de BD (pymssql, ibm_db)
#
# Construir:
#   docker build -t airflow-bsg:2.11.2 .
#
# Luego en tu .env:
#   AIRFLOW_IMAGE=airflow-bsg:2.11.2
#
# Y levantar normal. Los contenedores usaran esta imagen en vez de la oficial.
# ============================================================================

FROM apache/airflow:2.11.2-python3.11

# ----------------------------------------------------------------------------
# Capa root: paquetes de sistema, Java y Spark
# ----------------------------------------------------------------------------
USER root

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3

# Java es requisito de spark-submit y del puente JDBC (JayDeBeApi/JPype).
# procps lo usa spark-submit internamente (llama a "ps").
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        gcc g++ \
        unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- Cliente de Spark -------------------------------------------------------
# Debe coincidir con la version del cluster (SPARK_IMAGE_TAG en tu .env).
# Si no coinciden, spark-submit falla con errores de serializacion poco claros.
RUN curl -fSL \
      "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
      -o /tmp/spark.tgz \
 || curl -fSL \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
      -o /tmp/spark.tgz \
 && tar -xzf /tmp/spark.tgz -C /opt \
 && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark \
 && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# --- Drivers JDBC -----------------------------------------------------------
# OJO: no pude verificar estas URLs desde donde construi el archivo (sin salida
# a Maven Central). Si alguna da 404, busca la version vigente en:
#   https://central.sonatype.com/artifact/com.microsoft.sqlserver/mssql-jdbc
#   https://central.sonatype.com/artifact/com.ibm.db2/jcc
ARG MSSQL_JDBC_VERSION=12.8.1.jre11
ARG DB2_JCC_VERSION=11.5.9.0
ARG PG_JDBC_VERSION=42.7.4

RUN mkdir -p /opt/airflow/jars && cd /opt/airflow/jars \
 && curl -fSL -o mssql-jdbc.jar \
      "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar" \
 && curl -fSL -o db2-jcc.jar \
      "https://repo1.maven.org/maven2/com/ibm/db2/jcc/${DB2_JCC_VERSION}/jcc-${DB2_JCC_VERSION}.jar" \
 && curl -fSL -o postgresql-jdbc.jar \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
 && chown -R airflow:root /opt/airflow/jars \
 && chmod 644 /opt/airflow/jars/*.jar

# ----------------------------------------------------------------------------
# Capa airflow: paquetes de Python
# ----------------------------------------------------------------------------
# Nunca instalar como root: rompe los permisos del site-packages de la imagen.
USER airflow

# El archivo de constraints es lo que evita que pip rompa las dependencias de
# Airflow al resolver las de los providers. Sin esto, tarde o temprano te
# quedas con una version incompatible de alguna libreria comun.
ARG AIRFLOW_VERSION=2.11.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
        "apache-airflow-providers-microsoft-mssql==4.7.0" \
        "apache-airflow-providers-jdbc==5.5.0" \
        "apache-airflow-providers-apache-spark==6.3.1" \
        "apache-airflow-providers-common-sql" \
        "apache-airflow-providers-postgres" \
    && pip install --no-cache-dir \
        "pyspark==3.5.3" \
        "pandas" \
        "pyarrow"

# ibm_db (cliente nativo de DB2) descarga el driver de IBM al instalarse.
# Va aparte y tolera fallo: en muchas redes corporativas esa descarga esta
# bloqueada, y el JDBC de arriba ya cubre DB2. Si falla, no rompe el build.
RUN pip install --no-cache-dir "ibm-db" "ibm-db-sa" || \
    echo "AVISO: ibm-db no se instalo. Usa DB2 via JDBC (JdbcHook)."

# Ruta donde los DAGs esperan encontrar los jars
ENV JDBC_DRIVER_PATH=/opt/airflow/jars

# Comprobacion de que lo esencial quedo instalado
RUN python -c "import pymssql; print('pymssql', pymssql.__version__)" \
 && java -version \
 && spark-submit --version 2>&1 | head -3
