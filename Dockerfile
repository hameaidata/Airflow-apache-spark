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
        gnupg2 \
        gcc g++ \
        unixodbc unixodbc-dev \
        pkg-config \
        default-libmysqlclient-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------------------------
# ODBC Driver 18 de Microsoft + cliente Kerberos
# ----------------------------------------------------------------------------
# NECESARIO si tu SQL Server usa autenticacion integrada de Windows/AD.
# pymssql (que es lo que trae el provider mssql) NO soporta auth integrada:
# solo usuario y contrasena de SQL. Con AD hay que ir por ODBC + Kerberos.
#
# AVISO: este paso descarga de packages.microsoft.com. En muchas redes
# corporativas ese host esta bloqueado y el build falla aqui. Si te pasa,
# descarga los .deb desde una maquina con salida y copialos con COPY.
#
# La imagen base es Debian 12 (bookworm). Si cambias de imagen base, ajusta
# la ruta de la lista de paquetes.
ARG INSTALAR_ODBC=true
RUN if [ "$INSTALAR_ODBC" = "true" ]; then \
      set -e; \
      curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
      echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list && \
      apt-get update && \
      ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
      DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends krb5-user && \
      apt-get clean && rm -rf /var/lib/apt/lists/*; \
    else \
      echo "ODBC omitido (INSTALAR_ODBC=false)"; \
    fi

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

# ============================================================================
# DRIVERS JDBC — se descargan UNA VEZ, al construir la imagen
# ----------------------------------------------------------------------------
# Formato de cada entrada:   grupo:artefacto:version:nombre_destino.jar
#
# Anadir un motor nuevo es agregar una linea. Los jars quedan en
# /opt/airflow/jars y los usan tanto Airflow (provider jdbc) como Spark
# (spark-submit --jars).
#
# VERSIONES COMPROBADAS CONTRA MAVEN CENTRAL
#
# Las cinco coordenadas se verificaron una por una en repo1.maven.org. Existen,
# y estos son sus tamanos y fechas de publicacion reales:
#
#   mssql-jdbc               12.8.1.jre11            publicado 2024-08-22
#   jcc (DB2)                11.5.9.0    6.5 MB      publicado 2023-11-17
#   mysql-connector-j        9.1.0       2.6 MB      publicado 2024-10-14
#   postgresql               42.7.4      1.1 MB      publicado 2024-08-22
#   singlestore-jdbc-client  1.2.7       713 KB      publicado 2025-01-08
#
# Si alguna diera 404 al construir seria por un bloqueo de red, no por una
# coordenada mal escrita. En ese caso descargue los jars desde una maquina con
# salida a Internet y copielos con COPY en vez de usar curl.
#
# Nota sobre mssql-jdbc: el sufijo .jre11 forma parte de la VERSION, no del
# nombre del artefacto. Por eso la URL lo lleva dos veces
# (.../12.8.1.jre11/mssql-jdbc-12.8.1.jre11.jar) y asi debe ser.
# ============================================================================

ARG JDBC_DRIVERS="\
com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11:mssql-jdbc.jar \
com.ibm.db2:jcc:11.5.9.0:db2-jcc.jar \
com.mysql:mysql-connector-j:9.1.0:mysql-jdbc.jar \
org.postgresql:postgresql:42.7.4:postgresql-jdbc.jar \
com.singlestore:singlestore-jdbc-client:1.2.7:singlestore-jdbc.jar \
"

ARG MAVEN_REPO=https://repo1.maven.org/maven2




RUN set -e; \
    mkdir -p /opt/airflow/jars; \
    fallidos=""; \
    for spec in ${JDBC_DRIVERS}; do \
        grupo=$(echo "$spec"    | cut -d: -f1); \
        artefacto=$(echo "$spec"| cut -d: -f2); \
        version=$(echo "$spec"  | cut -d: -f3); \
        destino=$(echo "$spec"  | cut -d: -f4); \
        ruta=$(echo "$grupo" | tr '.' '/'); \
        url="${MAVEN_REPO}/${ruta}/${artefacto}/${version}/${artefacto}-${version}.jar"; \
        echo ">>> ${destino}  <-  ${artefacto} ${version}"; \
        if curl -fSL --retry 3 --retry-delay 2 -o "/opt/airflow/jars/${destino}" "$url"; then \
            echo "    ok  $(du -h /opt/airflow/jars/${destino} | cut -f1)"; \
        else \
            echo "    FALLO: $url"; \
            rm -f "/opt/airflow/jars/${destino}"; \
            fallidos="${fallidos} ${artefacto}"; \
        fi; \
    done; \
    chown -R airflow:root /opt/airflow/jars; \
    chmod 644 /opt/airflow/jars/*.jar 2>/dev/null || true; \
    echo ""; \
    echo "=== jars instalados ==="; \
    ls -la /opt/airflow/jars/; \
    if [ -n "${fallidos}" ]; then \
        echo ""; \
        echo "AVISO: no se descargaron:${fallidos}"; \
        echo "La imagen se construye igual. Los motores afectados no funcionaran"; \
        echo "hasta que corrija la version en JDBC_DRIVERS y reconstruya."; \
    fi

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

# ----------------------------------------------------------------------------
# Un provider por motor. Con esto, cada Connection de Airflow resuelve su hook
# automaticamente segun el conn_type, sin codigo adicional.
#
# SIN FIJAR VERSIONES, A PROPOSITO.
#
# El archivo de constraints ya trae un conjunto de versiones probadas entre si
# para esta version de Airflow. Fijar ademas una version a mano produce esto:
#
#     The user requested apache-airflow-providers-microsoft-mssql==4.7.0
#     The user requested (constraint) ...==4.5.0
#     ERROR: ResolutionImpossible
#
# O se usa el archivo de constraints, o se fijan versiones. Las dos cosas a la
# vez se contradicen. Se elige el archivo: es el conjunto que Apache probo.
#
# Versiones que resultan con Airflow 2.11.2 (comprobadas en el archivo):
#     microsoft-mssql 4.5.0   postgres 6.6.0   mysql 6.5.0
#     jdbc 5.4.0              odbc 4.12.0      common-sql 1.32.0
#     apache-spark 5.5.1
# ----------------------------------------------------------------------------
RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
        "apache-airflow-providers-microsoft-mssql" \
        "apache-airflow-providers-postgres" \
        "apache-airflow-providers-mysql" \
        "apache-airflow-providers-jdbc" \
        "apache-airflow-providers-odbc" \
        "apache-airflow-providers-common-sql" \
        "apache-airflow-providers-apache-spark" \
        "pyodbc" \
        "pymssql" \
        "psycopg2-binary" \
        "mysqlclient" \
        "jaydebeapi" \
        "pandas" \
        "pyarrow"

RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" \
        "apache-airflow-providers-microsoft-mssql" \
        "apache-airflow-providers-postgres" \
        "apache-airflow-providers-mysql" \
        "apache-airflow-providers-jdbc" \
        "apache-airflow-providers-odbc" \
        "apache-airflow-providers-common-sql" \
        "apache-airflow-providers-apache-spark" \
        "pyodbc" \
        "pandas" \
        "pyarrow"

# ----------------------------------------------------------------------------
# pyspark: DEBE coincidir con la version del cluster, y por eso va aparte.
#
# El archivo de constraints fija pyspark 4.1.1, pero el cluster de este
# proyecto es Spark 3.5.3. Con versiones distintas entre cliente y cluster, el
# envio de trabajos falla con errores de serializacion que en ningun momento
# mencionan la version — es de los diagnosticos mas largos que hay.
#
# Se instala sin --constraint para poder bajarlo a 3.5.3. El provider de Spark
# pide pyspark>=3.5.2, asi que 3.5.3 lo satisface.
# ----------------------------------------------------------------------------
RUN pip install --no-cache-dir "pyspark==${SPARK_VERSION}" \
 && python -c "import pyspark; print('pyspark', pyspark.__version__)"

# SingleStore habla el protocolo de MySQL, asi que el provider de MySQL le
# sirve para la mayoria de los casos. Este cliente propio anade lo especifico
# (tipos vectoriales, notas de version). Tolera fallo: no es imprescindible.
RUN pip install --no-cache-dir "singlestoredb" || \
    echo "AVISO: singlestoredb no se instalo. Use el provider de MySQL para SingleStore."

# ibm_db (cliente nativo de DB2) descarga el driver de IBM al instalarse.
# Va aparte y tolera fallo: en muchas redes corporativas esa descarga esta
# bloqueada, y el JDBC de arriba ya cubre DB2. Si falla, no rompe el build.
RUN pip install --no-cache-dir "ibm-db" "ibm-db-sa" || \
    echo "AVISO: ibm-db no se instalo. Usa DB2 via JDBC (JdbcHook)."

# Ruta donde los DAGs esperan encontrar los jars
ENV JDBC_DRIVER_PATH=/opt/airflow/jars

# Comprobacion de que lo esencial quedo instalado
COPY --chown=airflow:root scripts/verificar_drivers.py /opt/airflow/verificar_drivers.py

RUN python /opt/airflow/verificar_drivers.py --build
