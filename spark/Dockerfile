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

# Directorios de trabajo con el propietario correcto (uid 185 = spark).
# Asi no dependes del contenedor spark-init para estas rutas.
RUN mkdir -p /opt/spark-events /opt/spark-data \
 && chown -R 185:185 /opt/spark-events /opt/spark-data

USER spark

RUN python3 --version && java -version
