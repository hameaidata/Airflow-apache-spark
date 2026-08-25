#!/usr/bin/env bash
# =============================================================================
# CONSTRUIR LA IMAGEN PROPIA DE AIRFLOW  ::  UBUNTU / LINUX
# -----------------------------------------------------------------------------
# Equivalente exacto de scripts/construir_imagen.ps1 (Windows).
#
# La imagen oficial apache/airflow NO trae spark-submit, ni los drivers JDBC,
# ni los clientes de base de datos. Este script construye la imagen que si los
# trae, y deja el .env apuntando a ella.
#
# Uso:
#     ./scripts/construir_imagen.sh                 construir y activar
#     ./scripts/construir_imagen.sh --sin-odbc      omitir el ODBC de Microsoft
#     ./scripts/construir_imagen.sh --forzar        reconstruir aunque ya exista
#     ./scripts/construir_imagen.sh --tag otro:1.0  usar otro nombre de imagen
#     ./scripts/construir_imagen.sh --ayuda
#
# Tarda entre 15 y 25 minutos la primera vez. Despues, con la cache de Docker,
# unos pocos minutos.
# =============================================================================

# Sin `set -e` a proposito. Aqui queremos comprobar codigos de salida y dar un
# mensaje util, no morir en silencio a mitad de una comprobacion.
set -uo pipefail

VERDE='\033[0;32m'; ROJO='\033[0;31m'; AMARILLO='\033[1;33m'
CYAN='\033[0;36m'; GRIS='\033[0;90m'; NC='\033[0m'

ok()    { echo -e "${VERDE}[ok]${NC} $1"; }
aviso() { echo -e "${AMARILLO}[aviso]${NC} $1"; }
fallo() { echo -e "${ROJO}[error]${NC} $1"; }
info()  { echo -e "${CYAN}[..]${NC} $1"; }

# --- El script se ubica solo -------------------------------------------------
# Sin esto, ejecutarlo desde scripts/ en vez de la raiz rompe todas las rutas.
# Ya nos paso con crear_roles: se corrige de origen, no recordandolo.
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TAG="airflow-bsg:2.11.2"
INSTALAR_ODBC="true"
FORZAR="no"

ayuda() {
    sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --sin-odbc) INSTALAR_ODBC="false"; shift ;;
        --forzar)   FORZAR="si"; shift ;;
        --tag)      TAG="${2:-}"; shift 2 ;;
        --ayuda|-h|--help) ayuda ;;
        *) fallo "Opcion desconocida: $1"; echo "Use --ayuda"; exit 2 ;;
    esac
done

echo
echo -e "${CYAN}==============================================================${NC}"
echo -e "${CYAN}  CONSTRUIR IMAGEN DE AIRFLOW  (Ubuntu / Linux)${NC}"
echo -e "${CYAN}==============================================================${NC}"
echo "  Proyecto: ${RAIZ}"
echo "  Imagen:   ${TAG}"
echo "  ODBC:     ${INSTALAR_ODBC}"
echo

# --- 1. Docker existe y responde ---------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
    fallo "No encuentro el comando 'docker'."
    echo
    echo "  Instalar en Ubuntu:"
    echo "    sudo apt-get update"
    echo "    sudo apt-get install -y docker.io docker-compose-v2"
    echo
    echo "  Y anadase al grupo docker para no depender de sudo:"
    echo "    sudo usermod -aG docker \$USER"
    echo "    newgrp docker"
    exit 1
fi

# `docker info` es la prueba real: comprueba que el demonio responde, no solo
# que el binario existe.
if ! docker info >/dev/null 2>&1; then
    fallo "El comando docker existe, pero el demonio no responde."
    echo
    echo "  Causa mas comun en Ubuntu: su usuario no esta en el grupo docker."
    echo "    sudo usermod -aG docker \$USER"
    echo "    newgrp docker          # o cerrar sesion y volver a entrar"
    echo
    echo "  Si el servicio esta parado:"
    echo "    sudo systemctl enable --now docker"
    exit 1
fi
ok "Docker responde ($(docker --version | cut -d' ' -f3 | tr -d ','))"

# --- 2. El Dockerfile esta donde debe ----------------------------------------
if [ ! -f "${RAIZ}/Dockerfile" ]; then
    fallo "No encuentro ${RAIZ}/Dockerfile"
    exit 1
fi
ok "Dockerfile encontrado"

# --- 3. Trampa clasica de Linux: el UID --------------------------------------
# En Windows da igual. En Linux, si AIRFLOW_UID no coincide con su usuario, los
# logs se crean como root y usted no puede ni leerlos ni borrarlos desde el
# host. El sintoma aparece mucho despues y no apunta hacia aqui.
ENV_FILE="${RAIZ}/.env"
if [ -f "${ENV_FILE}" ]; then
    UID_ENV="$(grep -E '^[[:space:]]*AIRFLOW_UID[[:space:]]*=' "${ENV_FILE}" \
               | tail -1 | cut -d= -f2 | tr -d '[:space:]\r')"
    UID_REAL="$(id -u)"
    if [ -n "${UID_ENV}" ] && [ "${UID_ENV}" != "${UID_REAL}" ]; then
        aviso "AIRFLOW_UID=${UID_ENV} en .env, pero su usuario es ${UID_REAL}."
        aviso "En Linux esto deja los logs como root. Corrijalo con:"
        echo "      sed -i 's/^AIRFLOW_UID=.*/AIRFLOW_UID=${UID_REAL}/' .env"
        echo
    else
        ok "AIRFLOW_UID coincide con su usuario (${UID_REAL})"
    fi
else
    aviso ".env no existe todavia. Ejecute ./setup.sh antes de levantar el stack."
fi

# --- 4. La imagen ya existe? -------------------------------------------------
if docker image inspect "${TAG}" >/dev/null 2>&1; then
    CREADA="$(docker image inspect "${TAG}" --format '{{.Created}}' | cut -c1-19)"
    if [ "${FORZAR}" = "no" ]; then
        echo
        aviso "La imagen ${TAG} ya existe (creada ${CREADA})."
        echo "  Para reconstruirla:  ./scripts/construir_imagen.sh --forzar"
        echo
        echo -e "${CYAN}  Para comprobar sus drivers ahora mismo:${NC}"
        echo "    docker compose -f docker-compose.ubuntu.yml exec airflow-webserver \\"
        echo "        python /opt/airflow/verificar_drivers.py"
        echo
        exit 0
    fi
    info "La imagen existe (${CREADA}); se reconstruye por --forzar"
fi

# --- 5. Construir ------------------------------------------------------------
echo
echo -e "${CYAN}--------------------------------------------------------------${NC}"
echo -e "${CYAN}  Construyendo. Entre 15 y 25 minutos la primera vez.${NC}"
echo -e "${GRIS}  Descarga Spark (~400 MB), Java 17, 5 drivers JDBC y los${NC}"
echo -e "${GRIS}  providers de Airflow. Puede dejarlo corriendo.${NC}"
echo -e "${CYAN}--------------------------------------------------------------${NC}"
echo

INICIO=$(date +%s)

docker build \
    --build-arg "INSTALAR_ODBC=${INSTALAR_ODBC}" \
    -t "${TAG}" \
    "${RAIZ}"

CODIGO=$?
DURACION=$(( $(date +%s) - INICIO ))

echo
if [ ${CODIGO} -ne 0 ]; then
    fallo "La construccion fallo (codigo ${CODIGO}) tras $((DURACION / 60)) min."
    echo
    echo -e "${AMARILLO}  Los dos fallos mas frecuentes:${NC}"
    echo
    echo "  1. Se corta en el paso del ODBC de Microsoft."
    echo "     Descarga de packages.microsoft.com, que muchas redes"
    echo "     corporativas bloquean. Reintente sin el:"
    echo "         ./scripts/construir_imagen.sh --sin-odbc"
    echo "     Solo pierde la autenticacion integrada de AD contra SQL Server;"
    echo "     usuario y contrasena siguen funcionando."
    echo
    echo "  2. Se corta descargando un jar de repo1.maven.org."
    echo "     Las 5 coordenadas estan verificadas, asi que un 404 aqui es"
    echo "     bloqueo de red, no una version mal escrita."
    echo
    exit ${CODIGO}
fi

ok "Imagen construida en $((DURACION / 60)) min $((DURACION % 60)) s"
docker image inspect "${TAG}" --format '     tamano: {{.Size}} bytes' 2>/dev/null \
    | numfmt --to=iec --field=3 2>/dev/null || true

# --- 6. Activar la imagen en .env --------------------------------------------
# Construir la imagen no sirve de nada si el compose sigue usando la oficial.
# Esto es lo que se olvida y produce el "pero si ya la construi".
if [ -f "${ENV_FILE}" ]; then
    cp "${ENV_FILE}" "${ENV_FILE}.bak"

    # Ojo con el anclaje: AIRFLOW_IMAGE_TAG NO debe coincidir. Exigir que tras
    # AIRFLOW_IMAGE venga espacio o '=' lo garantiza, porque en _TAG viene '_'.
    if grep -qE '^[[:space:]]*AIRFLOW_IMAGE[[:space:]]*=' "${ENV_FILE}"; then
        sed -i -E "s|^[[:space:]]*AIRFLOW_IMAGE[[:space:]]*=.*|AIRFLOW_IMAGE=${TAG}|" "${ENV_FILE}"
        ok "AIRFLOW_IMAGE actualizada en .env"
    elif grep -qE '^[[:space:]]*#[[:space:]]*AIRFLOW_IMAGE[[:space:]]*=' "${ENV_FILE}"; then
        sed -i -E "s|^[[:space:]]*#[[:space:]]*AIRFLOW_IMAGE[[:space:]]*=.*|AIRFLOW_IMAGE=${TAG}|" "${ENV_FILE}"
        ok "AIRFLOW_IMAGE descomentada en .env"
    else
        printf '\nAIRFLOW_IMAGE=%s\n' "${TAG}" >> "${ENV_FILE}"
        ok "AIRFLOW_IMAGE anadida a .env"
    fi
    echo -e "${GRIS}     copia previa en .env.bak${NC}"
else
    aviso "No hay .env. Ejecute ./setup.sh y luego ponga a mano:"
    echo "      AIRFLOW_IMAGE=${TAG}"
fi

# --- 7. Que sigue ------------------------------------------------------------
echo
echo -e "${CYAN}==============================================================${NC}"
echo -e "${VERDE}  LISTO${NC}"
echo -e "${CYAN}==============================================================${NC}"
echo
echo "  1. Levantar el stack:"
echo "       docker compose -f docker-compose.ubuntu.yml up -d"
echo
echo "  2. Comprobar los drivers (dentro del contenedor, que es donde viven):"
echo "       docker compose -f docker-compose.ubuntu.yml exec airflow-webserver \\"
echo "           python /opt/airflow/verificar_drivers.py"
echo
echo "  3. Aplicar los roles:"
echo "       ./scripts/crear_roles.sh --simular"
echo "       ./scripts/crear_roles.sh --aplicar"
echo
exit 0
