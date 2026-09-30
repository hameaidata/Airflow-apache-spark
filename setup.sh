#!/usr/bin/env bash
# ============================================================================
# setup.sh  ::  Preparacion del stack en LINUX (Ubuntu y RHEL/Podman)
# ----------------------------------------------------------------------------
# Uso:  chmod +x setup.sh && ./setup.sh          # Ubuntu / Debian, con Docker
#       chmod +x setup.sh && ./setup.sh --rhel   # RHEL 9 con Podman
#
# El modo --rhel cambia tres cosas, no mas:
#   - el archivo de compose al que apunta el resumen
#   - el prefijo localhost/ en la sugerencia de AIRFLOW_IMAGE
#   - los mensajes de instalacion (dnf en vez de apt-get)
# Los secretos, las carpetas y el UID se generan igual en los dos casos.
#
# Genera el archivo .env con secretos aleatorios creados EN TU MAQUINA.
# Ningun secreto viaja por la red ni pasa por herramientas remotas.
# ============================================================================

set -euo pipefail

# --- Modo: ubuntu (por defecto) o rhel --------------------------------------
MODO="ubuntu"
COMPOSE_FILE="docker-compose.ubuntu.yml"
PREFIJO_IMAGEN=""
for arg in "$@"; do
    case "$arg" in
        --rhel)   MODO="rhel"; COMPOSE_FILE="docker-compose.rhel.yml"
                  PREFIJO_IMAGEN="localhost/" ;;
        --ubuntu) MODO="ubuntu" ;;
        -h|--help|--ayuda)
            sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Opcion desconocida: $arg  (use --rhel, --ubuntu o --ayuda)"; exit 2 ;;
    esac
done

# --- Rutas de parquet segun el modo -----------------------------------------
# En Ubuntu se dejan DENTRO del proyecto, que es lo comodo para desarrollo.
# En RHEL van a /datos, que es donde suele estar el disco de datos de verdad y
# no la particion del sistema: estos parquet crecen todos los dias.
if [ "$MODO" = "rhel" ]; then
    RUTA_PARQUET="/datos/datahub/parquet"
    RUTA_S2SQL="/datos/datahub/s2sql"
    RUTA_BT2SQL="/datos/datahub/bt2sql"
else
    RUTA_PARQUET="./data/parquet"
    RUTA_S2SQL="./data/s2sql"
    RUTA_BT2SQL="./data/bt2sql"
fi

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[ok]${NC} $*"; }
warn() { echo -e "${YELLOW}[aviso]${NC} $*"; }
err()  { echo -e "${RED}[error]${NC} $*"; }

echo -e "${CYAN}=== Preparando stack Airflow + Spark (modo: ${MODO}) ===${NC}"

# --- Helpers de generacion de secretos --------------------------------------
gen_fernet() {
    # Una clave Fernet son 32 bytes aleatorios en base64 url-safe.
    openssl rand -base64 32 | tr '+/' '-_'
}
gen_hex()  { openssl rand -hex "${1:-32}"; }
gen_pass() { openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c "${1:-24}"; }

# --- 1. Verificar Docker ----------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
    err "No encuentro el comando 'docker'."
    if [ "$MODO" = "rhel" ]; then
        echo "  En RHEL no hace falta Docker: se usa Podman, ya incluido."
        echo "    sudo dnf install -y container-tools podman-docker"
        echo "    systemctl --user enable --now podman.socket"
        echo "    export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/podman/podman.sock"
        echo
        echo "  Lo prepara todo de una vez:  ./scripts/preparar_rhel_podman.sh"
    else
        echo "  Instalalo con:"
        echo "    curl -fsSL https://get.docker.com | sudo sh"
        echo "    sudo usermod -aG docker \$USER   # cierra sesion y vuelve a entrar"
    fi
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    err "El comando existe, pero el motor no responde."
    if [ "$MODO" = "rhel" ]; then
        echo "  Podman sin privilegios necesita su socket activo:"
        echo "    systemctl --user enable --now podman.socket"
        echo "    export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/podman/podman.sock"
        echo
        echo "  Compruebe el resto con:  ./scripts/preparar_rhel_podman.sh"
    else
        echo "  Prueba:  sudo systemctl start docker"
        echo "  Si pide permisos:  sudo usermod -aG docker \$USER  (y reinicia sesion)"
    fi
    exit 1
fi
ok "Docker responde"

# --- 2. Verificar el plugin compose ----------------------------------------
if ! docker compose version >/dev/null 2>&1; then
    err "Falta 'docker compose' (v2)."
    if [ "$MODO" = "rhel" ]; then
        echo "  No forma parte de RHEL. Dos salidas:"
        echo "   a) instalar el binario de compose v2 (un solo archivo), o"
        echo "   b) usar unidades Quadlet:  ver docs/RHEL_SIN_DOCKER.md"
    else
        echo "  sudo apt-get install -y docker-compose-plugin"
    fi
    exit 1
fi
ok "docker compose $(docker compose version --short)"

# --- 3. Verificar openssl ---------------------------------------------------
if ! command -v openssl >/dev/null 2>&1; then
    err "Falta openssl (se usa para generar los secretos)."
    echo "  sudo apt-get install -y openssl"
    exit 1
fi

# --- 4. Verificar memoria ---------------------------------------------------
MEM_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
MEM_GB=$(( MEM_KB / 1024 / 1024 ))
echo "[info] RAM del host: ${MEM_GB} GB"
if [ "$MEM_GB" -lt 7 ]; then
    warn "Con menos de 8 GB el stack completo puede quedarse sin memoria."
    warn "Reduce replicas de worker o SPARK_WORKER_MEMORY en el .env."
fi

# --- 5. Crear estructura de carpetas ---------------------------------------
mkdir -p airflow/dags/production airflow/dags/examples airflow/dags/templates \
         airflow/plugins airflow/logs airflow/config \
         spark/jobs spark/config
ok "estructura de carpetas creada"

# --- 6. Generar .env con el UID correcto -----------------------------------
# ESTO ES LO CRITICO EN LINUX: si AIRFLOW_UID no coincide con tu usuario,
# los archivos de log se crean como root y no los puedes ni leer ni borrar.
CURRENT_UID=$(id -u)

if [ -f .env ]; then
    ok ".env ya existe, no lo toco"
    ENV_UID=$(grep -E '^AIRFLOW_UID=' .env | cut -d= -f2 || echo "")
    if [ "$ENV_UID" != "$CURRENT_UID" ]; then
        warn "AIRFLOW_UID en .env es '${ENV_UID}' pero tu UID es '${CURRENT_UID}'."
        warn "Corrige con: sed -i 's/^AIRFLOW_UID=.*/AIRFLOW_UID=${CURRENT_UID}/' .env"
    fi
else
    cat > .env <<EOF
# ============================================================================
# CONFIGURACION DEL STACK AIRFLOW + SPARK  ::  UBUNTU / LINUX
# ----------------------------------------------------------------------------
# Generado por setup.sh el $(date '+%Y-%m-%d %H:%M')
# Los secretos se generaron localmente en esta maquina.
# NUNCA subas este archivo a git (ya esta en .gitignore).
# ============================================================================

COMPOSE_PROJECT_NAME=airflow-spark

# --- Versiones de imagen ----------------------------------------------------
# LAS DOS IMAGENES PROPIAS SON OBLIGATORIAS, no opcionales.
#
# Antes venian comentadas, con un "descomenta si construiste tu imagen". Ya no:
# el stack DEPENDE de ellas, y dejarlas comentadas es como se llega a un
# "No suitable driver" que manda a buscar el problema donde no esta.
#
#   airflow-bsg  drivers JDBC (jt400 para Bantotal, mssql-jdbc, singlestore,
#                postgres, mysql, db2), el ODBC de IBM i, Java 8 junto a
#                Java 17, y spark-submit.
#   spark-bsg    Python -que la imagen oficial de Apache NO trae- y los mismos
#                drivers en \$SPARK_HOME/jars, que es la unica ruta que entra
#                en el classpath del driver Y de los executors.
#
# Se construyen con:
#   docker build -t airflow-bsg:2.11.2 .
#   docker build -t spark-bsg:3.5.3 -f spark/Dockerfile .
AIRFLOW_IMAGE_TAG=2.11.2-python3.11
AIRFLOW_IMAGE=${PREFIJO_IMAGEN}airflow-bsg:2.11.2
SPARK_IMAGE_TAG=3.5.3
SPARK_IMAGE=${PREFIJO_IMAGEN}spark-bsg:3.5.3
POSTGRES_IMAGE_TAG=16-alpine
REDIS_IMAGE_TAG=7-alpine

# --- Autenticacion del cluster de Spark -------------------------------------
# DESACTIVADA por defecto para que el cluster arranque sin friccion.
# Para activarla hay que ponerla en LOS TRES SITIOS o las tareas fallaran:
#   1) SPARK_MASTER_OPTS y SPARK_WORKER_OPTS aqui abajo
#   2) el conf del SparkSubmitOperator en el DAG:
#        "spark.authenticate": "true"
#        "spark.authenticate.secret": "<el mismo valor>"
# Aviso: el secreto pasado por -D es visible con `ps` dentro del contenedor.
# Lo correcto en produccion es un spark-defaults.conf con permisos 600.
#   SPARK_MASTER_OPTS=-Dspark.authenticate=true -Dspark.authenticate.secret=EL_SECRETO
#   SPARK_WORKER_OPTS=-Dspark.authenticate=true -Dspark.authenticate.secret=EL_SECRETO
SPARK_MASTER_OPTS=
SPARK_WORKER_OPTS=

# --- Claves de cifrado ------------------------------------------------------
# AIRFLOW_FERNET_KEY cifra Connections y Variables en la base de datos.
# Si la pierdes o la cambias, TODAS las credenciales guardadas quedan ilegibles.
AIRFLOW_FERNET_KEY=$(gen_fernet)
AIRFLOW_WEBSERVER_SECRET_KEY=$(gen_hex 32)

# --- PostgreSQL (metastore) -------------------------------------------------
POSTGRES_USER=airflow
POSTGRES_PASSWORD=$(gen_pass 24)
POSTGRES_DB=airflow
POSTGRES_PORT=5432

# --- Redis (broker de Celery) -----------------------------------------------
REDIS_PASSWORD=$(gen_pass 24)

# --- Usuario admin inicial de la UI -----------------------------------------
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$(gen_pass 16)
AIRFLOW_ADMIN_EMAIL=admin@example.com

# --- Puertos publicados en el host ------------------------------------------
AIRFLOW_WEB_PORT=8080
FLOWER_PORT=5555
SPARK_MASTER_UI_PORT=8082
SPARK_MASTER_PORT=7077
# History Server: donde se ven los jobs Spark YA TERMINADOS. Sin el, cuando un
# job falla su interfaz muere con el job y no queda nada que mirar.
SPARK_HISTORY_PORT=18080

# ============================================================================
# CARPETAS DE PARQUET
# ----------------------------------------------------------------------------
# Cada pipeline escribe en la suya. El lado IZQUIERDO es la ruta en esta
# maquina; el DERECHO es la ruta dentro del contenedor y NO se debe cambiar:
# es la que guardan las tablas de control y la que abre despues la carga.
#
# Si cambia el lado derecho, tiene que cambiar tambien output_dir en la
# Variable de Airflow correspondiente, o la extraccion escribira en una ruta
# que no esta montada. Eso no da error: Docker crea la carpeta DENTRO del
# contenedor, la corrida sale verde, y los archivos desaparecen al reiniciar.
# ============================================================================

# Bantotal -> parquet -> STG en SingleStore
PARQUET_HOST_DIR=${RUTA_PARQUET}
PARQUET_CONTAINER_DIR=/data/parquet

# SingleStore -> parquet -> SQL Server
S2SQL_PARQUET_HOST_DIR=${RUTA_S2SQL}
S2SQL_PARQUET_CONTAINER_DIR=/data/s2sql

# Bantotal -> parquet -> STG en SQL Server
BT2SQL_PARQUET_HOST_DIR=${RUTA_BT2SQL}
BT2SQL_PARQUET_CONTAINER_DIR=/data/bt2sql

# --- Pools de Airflow -------------------------------------------------------
# Limitan cuantas tareas golpean cada motor a la vez. Por defecto default_pool
# para que todo funcione recien instalado.
#
# Cuando cree los pools en Admin > Pools, ponga aqui sus nombres. El compose ya
# los pasa a los contenedores; antes no lo hacia y cambiarlos aqui no tenia
# ningun efecto, sin dar tampoco ningun error.
AIRFLOW_POOL_SINGLESTORE=default_pool
AIRFLOW_POOL_SQLSERVER=default_pool

# --- Escalado ---------------------------------------------------------------
# Capacidad total = replicas x WORKER_CONCURRENCY
WORKER_CONCURRENCY=8
WORKER_QUEUES=default

# Replicas al arrancar. En caliente: up -d --scale airflow-worker=N
AIRFLOW_WORKER_REPLICAS=2
SPARK_WORKER_REPLICAS=1

SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2
SPARK_RPC_SECRET=$(gen_pass 32)

# --- Auto-discovery de DAGs -------------------------------------------------
# Segundos entre escaneos de dags/. Bajalo a 10 solo en desarrollo.
DAG_DIR_LIST_INTERVAL=30

# --- Providers extra (opcional) ---------------------------------------------
# Se instalan con pip AL ARRANCAR cada contenedor: lento y fragil.
# Para produccion, construye una imagen propia.
# PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-apache-spark==4.11.3
PIP_ADDITIONAL_REQUIREMENTS=

# --- Especifico de Linux ----------------------------------------------------
# Debe coincidir con tu usuario o los logs quedaran como root.
AIRFLOW_UID=${CURRENT_UID}

# ============================================================================
# TLS CON NGINX  (opcional, docker-compose.tls.yml)
# ----------------------------------------------------------------------------
# Descomente el bloque entero si va a levantar el proxy TLS por delante.
# nginx 1.27 y no una version anterior: "http2 on" necesita >= 1.25.1.
# ============================================================================
# NGINX_IMAGE=nginx:1.27-alpine
# HTTP_PORT=80
# HTTPS_PORT=443
# DOMINIO_AIRFLOW=airflow.suempresa.local
# DOMINIO_FLOWER=flower.suempresa.local
# DOMINIO_SPARK=spark.suempresa.local
EOF
    chmod 600 .env
    ok ".env generado con secretos aleatorios (AIRFLOW_UID=${CURRENT_UID}, permisos 600)"
fi

# --- 6b. Las carpetas de parquet deben existir ANTES de levantar ------------
# Si no existen, Docker las crea como root al montar el bind mount, y entonces
# el worker -que corre con otro uid- no puede escribir dentro. El sintoma es un
# "Permission denied" a mitad de la extraccion, cuando ya se consulto el core.
for d in "$RUTA_PARQUET" "$RUTA_S2SQL" "$RUTA_BT2SQL"; do
    if [ ! -d "$d" ]; then
        mkdir -p "$d" 2>/dev/null && ok "carpeta creada: $d" \
            || warn "no pude crear $d  (creela a mano: sudo mkdir -p $d)"
    fi
    # 2775: el 2 hace que lo que se cree dentro herede el grupo. Sin el, las
    # subcarpetas por dia nacen con el grupo del proceso que las crea y Spark
    # -que corre con otro uid- no puede escribir en ellas.
    chmod 2775 "$d" 2>/dev/null || true
    chown -R "${CURRENT_UID}:0" "$d" 2>/dev/null || true
done

# --- 6c. Comprobar que las imagenes propias existen -------------------------
# Esta comprobacion existe porque el fallo contrario es silencioso y caro: el
# .env nombra airflow-bsg y spark-bsg, y si no estan construidas el compose
# falla con "image not found" sin decir que hay que construirlas.
FALTAN_IMAGENES=""
for img in "${PREFIJO_IMAGEN}airflow-bsg:2.11.2" "${PREFIJO_IMAGEN}spark-bsg:3.5.3"; do
    docker image inspect "$img" >/dev/null 2>&1 || FALTAN_IMAGENES="${FALTAN_IMAGENES} ${img}"
done
if [ -n "$FALTAN_IMAGENES" ]; then
    warn "Faltan imagenes que el .env ya referencia:${FALTAN_IMAGENES}"
    echo "  Construyalas antes de levantar el stack:"
    echo "    docker build -t airflow-bsg:2.11.2 ."
    echo "    docker build -t spark-bsg:3.5.3 -f spark/Dockerfile ."
    echo
    echo "  La de Spark es la que mas se olvida, y su ausencia se manifiesta"
    echo "  como 'No suitable driver' dentro de un job, no al arrancar."
else
    ok "las dos imagenes propias estan construidas"
fi

# --- 7. Permisos de la carpeta de logs -------------------------------------
chown -R "${CURRENT_UID}:0" airflow/logs airflow/config 2>/dev/null || \
    warn "no pude hacer chown de airflow/logs (usa sudo si falla el arranque)"

# --- 8. vm.max_map_count (solo si vas a usar Elasticsearch) ----------------
CURRENT_MMC=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
if [ "$CURRENT_MMC" -lt 262144 ]; then
    warn "vm.max_map_count = ${CURRENT_MMC}. Elasticsearch necesita >= 262144."
    echo "  Si luego levantas el overlay de observabilidad:"
    echo "    sudo sysctl -w vm.max_map_count=262144"
    echo "    echo 'vm.max_map_count=262144' | sudo tee -a /etc/sysctl.conf"
fi

# --- 9. Resumen -------------------------------------------------------------
ADMIN_USER=$(grep -E '^AIRFLOW_ADMIN_USER=' .env | cut -d= -f2)
ADMIN_PASS=$(grep -E '^AIRFLOW_ADMIN_PASSWORD=' .env | cut -d= -f2)

cat <<EOF

$(echo -e "${CYAN}=== Listo. Para arrancar: ===${NC}")
  docker compose -f ${COMPOSE_FILE} up -d

La primera vez tarda 5-10 min (descarga ~3 GB de imagenes).

Ver progreso:
  docker compose -f ${COMPOSE_FILE} ps
  docker compose -f ${COMPOSE_FILE} logs -f airflow-init

Cuando airflow-webserver este 'healthy':
  UI Airflow : http://localhost:8080   ($ADMIN_USER / $ADMIN_PASS)
  Flower     : http://localhost:5555
  Spark      : http://localhost:8082

Escalar workers sin parar nada:
  docker compose -f ${COMPOSE_FILE} up -d --scale airflow-worker=5

EOF
