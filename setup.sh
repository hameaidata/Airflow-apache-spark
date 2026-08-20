#!/usr/bin/env bash
# ============================================================================
# setup.sh  ::  Preparacion del stack en UBUNTU / LINUX
# ----------------------------------------------------------------------------
# Uso:  chmod +x setup.sh && ./setup.sh
#
# Genera el archivo .env con secretos aleatorios creados EN TU MAQUINA.
# Ningun secreto viaja por la red ni pasa por herramientas remotas.
# ============================================================================

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[ok]${NC} $*"; }
warn() { echo -e "${YELLOW}[aviso]${NC} $*"; }
err()  { echo -e "${RED}[error]${NC} $*"; }

echo -e "${CYAN}=== Preparando stack Airflow + Spark (Ubuntu/Linux) ===${NC}"

# --- Helpers de generacion de secretos --------------------------------------
gen_fernet() {
    # Una clave Fernet son 32 bytes aleatorios en base64 url-safe.
    openssl rand -base64 32 | tr '+/' '-_'
}
gen_hex()  { openssl rand -hex "${1:-32}"; }
gen_pass() { openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c "${1:-24}"; }

# --- 1. Verificar Docker ----------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
    err "Docker no esta instalado."
    echo "  Instalalo con:"
    echo "    curl -fsSL https://get.docker.com | sudo sh"
    echo "    sudo usermod -aG docker \$USER   # luego cierra sesion y vuelve a entrar"
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    err "No puedo hablar con el daemon de Docker."
    echo "  Prueba:  sudo systemctl start docker"
    echo "  Si pide permisos:  sudo usermod -aG docker \$USER  (y reinicia sesion)"
    exit 1
fi
ok "Docker responde"

# --- 2. Verificar el plugin compose ----------------------------------------
if ! docker compose version >/dev/null 2>&1; then
    err "Falta el plugin 'docker compose' (v2)."
    echo "  sudo apt-get install -y docker-compose-plugin"
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
AIRFLOW_IMAGE_TAG=2.10.5-python3.11
POSTGRES_IMAGE_TAG=16-alpine
REDIS_IMAGE_TAG=7-alpine
SPARK_IMAGE_TAG=3.5.3

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

# --- Escalado ---------------------------------------------------------------
# Capacidad total = replicas x WORKER_CONCURRENCY
WORKER_CONCURRENCY=8
WORKER_QUEUES=default

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
EOF
    chmod 600 .env
    ok ".env generado con secretos aleatorios (AIRFLOW_UID=${CURRENT_UID}, permisos 600)"
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
  docker compose -f docker-compose.ubuntu.yml up -d

La primera vez tarda 5-10 min (descarga ~3 GB de imagenes).

Ver progreso:
  docker compose -f docker-compose.ubuntu.yml ps
  docker compose -f docker-compose.ubuntu.yml logs -f airflow-init

Cuando airflow-webserver este 'healthy':
  UI Airflow : http://localhost:8080   ($ADMIN_USER / $ADMIN_PASS)
  Flower     : http://localhost:5555
  Spark      : http://localhost:8082

Escalar workers sin parar nada:
  docker compose -f docker-compose.ubuntu.yml up -d --scale airflow-worker=5

EOF
