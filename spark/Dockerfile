# ============================================================================
# Imagen de Spark con Python — SOLO si usas UDFs de Python
# ----------------------------------------------------------------------------
# apache/spark:3.5.3 no incluye interprete de Python. Para la API de DataFrames
# no hace falta (todo corre en la JVM), pero en cuanto uses una UDF de Python,
# rdd.map() o applyInPandas, los executors fallan con:
#     Cannot run program "python3": error=2, No such file or directory
#
# Construir:
#   docker build -t spark-bsg:3.5.3 -f spark/Dockerfile .
#
# Usar:  en el .env  ->  SPARK_IMAGE=spark-bsg:3.5.3
# ============================================================================

ARG SPARK_BASE=apache/spark:3.5.3
FROM ${SPARK_BASE}

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Las librerias que usen tus UDFs tienen que estar en TODOS los executors,
# no solo en el driver. Es el olvido mas comun al pasar de local a cluster.
RUN pip3 install --no-cache-dir --break-system-packages \
        pyarrow \
        pandas \
 || pip3 install --no-cache-dir pyarrow pandas

# ============================================================================
# DRIVERS JDBC — TAMBIEN AQUI, no solo en la imagen de Airflow
# ----------------------------------------------------------------------------
# EL PROBLEMA QUE RESUELVE
#
# Los jars se descargan en la imagen de Airflow, en /opt/airflow/jars. Esa ruta
# NO EXISTE en los contenedores de Spark: el compose solo les monta
# ./spark/jobs, los volumenes de eventos y datos, y la carpeta de parquet.
#
# En client mode, spark-submit distribuye lo que va en --jars a los executors,
# asi que muchas veces funciona. Pero con JDBC falla de una forma
# especialmente molesta: el jar llega al executor, y aun asi DriverManager no
# encuentra la clase, porque el jar distribuido se carga con un classloader
# distinto del que usa DriverManager. El sintoma es
#
#     java.sql.SQLException: No suitable driver
#
# en un job al que SI se le pasaron los jars, lo cual manda a cualquiera a
# buscar el problema donde no esta.
#
# LA SOLUCION
# Poner los jars en $SPARK_HOME/jars. Todo lo que este ahi entra en el
# classpath del driver Y de los executors automaticamente, sin --jars, sin
# spark.jars y sin extraClassPath. Es la unica forma que no depende de como se
# haya enviado el trabajo.
#
# La lista es LA MISMA que la del Dockerfile principal, a proposito. Hay un
# test (airflow/tests/unit/test_drivers_jdbc.py) que compara las dos y falla
# si divergen: duplicar esta bien mientras no pueda pasar en silencio.
# ============================================================================
ARG JDBC_DRIVERS="\
net.sf.jt400:jt400:21.0.7:jt400.jar \
com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11:mssql-jdbc.jar \
com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre8:mssql-jdbc-jre8.jar \
com.ibm.db2:jcc:11.5.9.0:db2-jcc.jar \
com.mysql:mysql-connector-j:9.1.0:mysql-jdbc.jar \
org.postgresql:postgresql:42.7.4:postgresql-jdbc.jar \
com.singlestore:singlestore-jdbc-client:1.2.7:singlestore-jdbc.jar \
"
ARG MAVEN_REPO=https://repo1.maven.org/maven2

RUN set -e; \
    fallidos=""; \
    for spec in ${JDBC_DRIVERS}; do \
        grupo=$(echo "$spec"    | cut -d: -f1); \
        artefacto=$(echo "$spec"| cut -d: -f2); \
        version=$(echo "$spec"  | cut -d: -f3); \
        destino=$(echo "$spec"  | cut -d: -f4); \
        ruta=$(echo "$grupo" | tr '.' '/'); \
        url="${MAVEN_REPO}/${ruta}/${artefacto}/${version}/${artefacto}-${version}.jar"; \
        echo ">>> ${destino}  <-  ${artefacto} ${version}"; \
        if curl -fSL --retry 3 --retry-delay 2 -o "${SPARK_HOME}/jars/${destino}" "$url"; then \
            echo "    ok  $(du -h ${SPARK_HOME}/jars/${destino} | cut -f1)"; \
        else \
            echo "    FALLO: $url"; \
            rm -f "${SPARK_HOME}/jars/${destino}"; \
            fallidos="${fallidos} ${artefacto}"; \
        fi; \
    done; \
    chown 185:185 ${SPARK_HOME}/jars/*.jar; \
    chmod 644 ${SPARK_HOME}/jars/*.jar; \
    if [ -n "${fallidos}" ]; then \
        echo "AVISO: no se descargaron:${fallidos}"; \
        echo "Los jobs Spark contra esos motores fallaran con 'No suitable driver'."; \
    fi

# ============================================================================
# JARS PROPIOS — airflow/jars/
# ----------------------------------------------------------------------------
# La MISMA carpeta que usa el Dockerfile principal, a proposito. Un driver que
# solo estuviera en la imagen de Airflow haria que el job arrancara bien y
# muriera al llegar al executor con "No suitable driver", que es justo el error
# que manda a buscar el problema donde no esta.
#
# Precedencia identica: se copia DESPUES de la descarga, asi que un archivo con
# el mismo nombre reemplaza al descargado.
#
# Aqui van a $SPARK_HOME/jars, que entra en el classpath del driver Y de los
# executors sin --jars ni extraClassPath.
# ============================================================================
COPY airflow/jars/ /tmp/jars-propios/

RUN set -e; \
    propios=$(find /tmp/jars-propios -maxdepth 1 -name "*.jar" 2>/dev/null | sort); \
    if [ -z "$propios" ]; then \
        echo "No hay jars propios en airflow/jars/."; \
    else \
        echo "=== jars propios de airflow/jars/ ==="; \
        for jar in $propios; do \
            nombre=$(basename "$jar"); \
            if [ -f "${SPARK_HOME}/jars/${nombre}" ]; then \
                echo "  ${nombre}  REEMPLAZA al descargado"; \
            else \
                echo "  ${nombre}  nuevo"; \
            fi; \
            cp "$jar" "${SPARK_HOME}/jars/"; \
        done; \
    fi; \
    rm -rf /tmp/jars-propios; \
    chown 185:185 ${SPARK_HOME}/jars/*.jar; \
    chmod 644 ${SPARK_HOME}/jars/*.jar

# Directorios de trabajo con el propietario correcto (uid 185 = spark).
# Asi no dependes del contenedor spark-init para estas rutas.
RUN mkdir -p /opt/spark-events /opt/spark-data \
 && chown -R 185:185 /opt/spark-events /opt/spark-data

USER spark

RUN python3 --version && java -version && \
    echo "=== drivers JDBC en el classpath de Spark ===" && \
    ls -1 ${SPARK_HOME}/jars/ | grep -E "jt400|jdbc|jcc" || true
