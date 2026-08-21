#!/usr/bin/env bash
# ============================================================================
# preparar-bundle-offline.sh
# ----------------------------------------------------------------------------
# Genera un paquete con TODO lo que la plataforma descarga de Internet, para
# poder instalarla y mantenerla en una red sin salida.
#
# DONDE SE EJECUTA: en una maquina CON Internet y CON Docker.
#                   NO en el servidor del banco.
#
# QUE PRODUCE:      una carpeta bundle/ y un archivo bundle-<fecha>.tar.gz
#                   que se traslada al banco por el canal autorizado.
#
# Uso:
#   ./preparar-bundle-offline.sh                 # todo
#   ./preparar-bundle-offline.sh solo-imagen     # solo la imagen final (rapido)
# ============================================================================

set -euo pipefail

# --- Versiones. Deben coincidir con las del .env y el Dockerfile ------------
AIRFLOW_VERSION="${AIRFLOW_VERSION:-2.11.2}"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
AIRFLOW_BASE="apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}"
SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
SPARK_IMAGE="apache/spark:${SPARK_VERSION}"
POSTGRES_IMAGE="postgres:16-alpine"
REDIS_IMAGE="redis:7-alpine"
BUSYBOX_IMAGE="busybox:1.36"

IMAGEN_PROPIA="airflow-bsg:${AIRFLOW_VERSION}"

MSSQL_JDBC_VERSION="${MSSQL_JDBC_VERSION:-12.8.1.jre11}"
DB2_JCC_VERSION="${DB2_JCC_VERSION:-11.5.9.0}"
PG_JDBC_VERSION="${PG_JDBC_VERSION:-42.7.4}"

MODO="${1:-completo}"
DESTINO="bundle"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[ok]${NC} $*"; }
info() { echo -e "${CYAN}[..]${NC} $*"; }
warn() { echo -e "${YELLOW}[aviso]${NC} $*"; }
err()  { echo -e "${RED}[error]${NC} $*"; }

# --- Comprobaciones previas -------------------------------------------------
command -v docker >/dev/null || { err "Falta Docker"; exit 1; }
docker info >/dev/null 2>&1 || { err "El daemon de Docker no responde"; exit 1; }
command -v curl >/dev/null || { err "Falta curl"; exit 1; }

echo -e "${CYAN}=== Preparando paquete sin conexion ===${NC}"
echo "  Airflow : ${AIRFLOW_VERSION}"
echo "  Spark   : ${SPARK_VERSION}"
echo "  Modo    : ${MODO}"
echo ""

mkdir -p "${DESTINO}"/{imagenes,jars,spark,wheelhouse,docs}

# ============================================================================
# 1. IMAGEN PROPIA — es la via recomendada
# ============================================================================
# Construirla aqui, donde SI hay Internet, y trasladar el resultado ya armado
# evita tener que replicar PyPI, Maven, los repositorios del sistema operativo
# y el de Microsoft dentro del banco. Es, con diferencia, el camino mas corto
# para la primera puesta en marcha.

info "Construyendo la imagen propia de Airflow (tarda 10-20 min)..."
if [ ! -f Dockerfile ]; then
    err "No encuentro el Dockerfile. Ejecuta este script desde la raiz del proyecto."
    exit 1
fi

docker build -t "${IMAGEN_PROPIA}" .
ok "Imagen construida: ${IMAGEN_PROPIA}"

info "Exportando la imagen propia..."
docker save "${IMAGEN_PROPIA}" | gzip > "${DESTINO}/imagenes/airflow-bsg.tar.gz"
ok "$(du -h "${DESTINO}/imagenes/airflow-bsg.tar.gz" | cut -f1) — airflow-bsg.tar.gz"

# ============================================================================
# 2. IMAGENES BASE
# ============================================================================
info "Descargando y exportando las imagenes base..."
for imagen in "${SPARK_IMAGE}" "${POSTGRES_IMAGE}" "${REDIS_IMAGE}" "${BUSYBOX_IMAGE}"; do
    nombre_archivo=$(echo "${imagen}" | tr '/:' '__')
    info "  ${imagen}"
    docker pull "${imagen}"
    docker save "${imagen}" | gzip > "${DESTINO}/imagenes/${nombre_archivo}.tar.gz"
done
ok "Imagenes base exportadas"

if [ "${MODO}" = "solo-imagen" ]; then
    warn "Modo solo-imagen: se omiten jars, wheels y tarball de Spark"
else

# ============================================================================
# 3. CONTROLADORES JDBC
# ============================================================================
info "Descargando controladores JDBC..."
descargar_jar() {
    local url="$1" salida="$2"
    if curl -fSL --retry 3 -o "${DESTINO}/jars/${salida}" "${url}"; then
        ok "  ${salida} ($(du -h "${DESTINO}/jars/${salida}" | cut -f1))"
    else
        warn "  FALLO ${salida} — verifica la version en central.sonatype.com"
    fi
}
descargar_jar \
  "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar" \
  "mssql-jdbc.jar"
descargar_jar \
  "https://repo1.maven.org/maven2/com/ibm/db2/jcc/${DB2_JCC_VERSION}/jcc-${DB2_JCC_VERSION}.jar" \
  "db2-jcc.jar"
descargar_jar \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
  "postgresql-jdbc.jar"

# ============================================================================
# 4. DISTRIBUCION DE SPARK
# ============================================================================
info "Descargando la distribucion de Spark..."
SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
curl -fSL --retry 3 -o "${DESTINO}/spark/${SPARK_TGZ}" \
  "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}" \
  || curl -fSL --retry 3 -o "${DESTINO}/spark/${SPARK_TGZ}" \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
ok "  ${SPARK_TGZ} ($(du -h "${DESTINO}/spark/${SPARK_TGZ}" | cut -f1))"

# ============================================================================
# 5. ARCHIVO DE RESTRICCIONES DE AIRFLOW
# ============================================================================
# Es lo que impide que pip rompa las dependencias de Airflow al instalar
# providers. Sin el, tarde o temprano acabas con una version incompatible de
# alguna libreria comun.
info "Descargando el archivo de restricciones..."
curl -fSL --retry 3 -o "${DESTINO}/constraints-${PYTHON_VERSION}.txt" \
  "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
ok "  constraints-${PYTHON_VERSION}.txt ($(wc -l < "${DESTINO}/constraints-${PYTHON_VERSION}.txt") lineas)"

# ============================================================================
# 6. PAQUETES DE PYTHON (wheelhouse)
# ============================================================================
# Se descargan DENTRO de un contenedor de la misma imagen base. Es la unica
# forma de garantizar que las ruedas compiladas sirvan: si se descargan desde
# otro sistema operativo o version de Python, algunas no seran compatibles y
# el fallo aparece recien al instalar, dentro del banco.
info "Descargando paquetes de Python dentro de la imagen base..."
docker run --rm \
  -v "$(pwd)/${DESTINO}/wheelhouse:/wheelhouse" \
  -v "$(pwd)/${DESTINO}/constraints-${PYTHON_VERSION}.txt:/constraints.txt:ro" \
  --entrypoint /bin/bash \
  "${AIRFLOW_BASE}" -c '
    set -e
    pip download --dest /wheelhouse --constraint /constraints.txt \
        "apache-airflow-providers-microsoft-mssql==4.7.0" \
        "apache-airflow-providers-jdbc==5.5.0" \
        "apache-airflow-providers-apache-spark==6.3.1" \
        "apache-airflow-providers-common-sql" \
        "apache-airflow-providers-postgres" \
        "apache-airflow-providers-odbc"
    pip download --dest /wheelhouse \
        "pyspark==3.5.3" "pyodbc" "pandas" "pyarrow"
    # ibm-db se intenta aparte: descarga binarios de IBM y suele fallar.
    pip download --dest /wheelhouse "ibm-db" "ibm-db-sa" \
        || echo "AVISO: ibm-db no se pudo descargar. DB2 por JDBC no lo necesita."
  '
ok "  $(find "${DESTINO}/wheelhouse" -type f | wc -l) archivos en el wheelhouse"

fi  # fin del modo completo

# ============================================================================
# 7. SUMAS DE VERIFICACION
# ============================================================================
# El area de seguridad del banco las pedira para validar que lo que entra es
# lo mismo que salio.
info "Calculando sumas de verificacion..."
( cd "${DESTINO}" && find . -type f ! -name SHA256SUMS.txt -exec sha256sum {} \; \
    | sort -k2 > SHA256SUMS.txt )
ok "  SHA256SUMS.txt con $(wc -l < "${DESTINO}/SHA256SUMS.txt") entradas"

# ============================================================================
# 8. INSTRUCCIONES PARA EL DESTINO
# ============================================================================
cat > "${DESTINO}/LEEME-INSTALACION.txt" <<EOF
PAQUETE SIN CONEXION — Plataforma Airflow + Spark
Generado el: $(date '+%Y-%m-%d %H:%M')
Airflow ${AIRFLOW_VERSION} / Spark ${SPARK_VERSION}

-------------------------------------------------------------------------
1. VERIFICAR INTEGRIDAD (antes de nada)

   sha256sum -c SHA256SUMS.txt

   Si alguna linea dice FALLO, no continuar: el traslado corrompio archivos.

-------------------------------------------------------------------------
2. CARGAR LAS IMAGENES

   for archivo in imagenes/*.tar.gz; do
       echo "Cargando \$archivo..."
       gunzip -c "\$archivo" | docker load
   done

   docker images    # verificar que aparecen las cinco

-------------------------------------------------------------------------
3. CONFIGURAR

   En el archivo .env del proyecto:

       AIRFLOW_IMAGE=${IMAGEN_PROPIA}

   Esto hace que se use la imagen que viene en este paquete, ya construida,
   en vez de intentar descargar nada.

-------------------------------------------------------------------------
4. LEVANTAR

   docker compose -f docker-compose.ubuntu.yml up -d

   No requiere ninguna descarga: todas las imagenes ya estan cargadas.

-------------------------------------------------------------------------
CONTENIDO DEL PAQUETE

   imagenes/       Imagenes de contenedor listas para cargar
   jars/           Controladores JDBC (por si se reconstruye internamente)
   spark/          Distribucion de Spark (idem)
   wheelhouse/     Paquetes de Python (idem)
   constraints-*   Restricciones de dependencias de Airflow

   Las carpetas jars/, spark/ y wheelhouse/ NO son necesarias si se usa la
   imagen ya construida. Sirven para reconstruirla dentro del banco usando
   Dockerfile.offline.
EOF
ok "  LEEME-INSTALACION.txt"

# ============================================================================
# 9. EMPAQUETAR
# ============================================================================
FECHA=$(date '+%Y%m%d')
ARCHIVO_FINAL="bundle-airflow-${AIRFLOW_VERSION}-${FECHA}.tar.gz"
info "Empaquetando en ${ARCHIVO_FINAL}..."
tar -czf "${ARCHIVO_FINAL}" "${DESTINO}"

echo ""
echo -e "${GREEN}=== Listo ===${NC}"
echo "  Archivo : ${ARCHIVO_FINAL}"
echo "  Tamano  : $(du -h "${ARCHIVO_FINAL}" | cut -f1)"
echo ""
echo "  Suma de verificacion del paquete completo:"
sha256sum "${ARCHIVO_FINAL}" | sed 's/^/    /'
echo ""
echo "  Siguiente paso: trasladarlo por el canal autorizado del banco"
echo "  (analisis antivirus previo) y seguir bundle/LEEME-INSTALACION.txt"
