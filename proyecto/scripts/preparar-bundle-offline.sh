#!/usr/bin/env bash
# =============================================================================
# PAQUETE PARA RED SIN INTERNET  ::  se ejecuta en la maquina CON Internet
# -----------------------------------------------------------------------------
# Equivalente exacto de scripts/preparar-bundle-offline.ps1 (Windows).
#
# Uso:
#     ./scripts/preparar-bundle-offline.sh [carpeta-destino]
#
# Produce una carpeta que se copia tal cual al servidor aislado. Alli no hace
# falta descargar absolutamente nada.
#
# -----------------------------------------------------------------------------
# POR QUE ESTE SCRIPT SE REESCRIBIO POR COMPLETO
#
# La version anterior intentaba armar un "kit de construccion": descargaba el
# tarball de Spark, el archivo de constraints y ruedas de pip, para CONSTRUIR
# la imagen en el servidor aislado. Dos problemas serios:
#
#   1. No funcionaba. Fijaba versiones de providers (mssql==4.7.0) que
#      contradicen el archivo de constraints, y pip terminaba en
#      ResolutionImpossible - el mismo fallo que ya nos costo un build.
#
#   2. Aunque hubiera funcionado, es la estrategia equivocada. Construir en el
#      servidor aislado significa reproducir alli media cadena de suministro:
#      apt, pip, Maven y el CDN de Apache. Cada uno es un punto de fallo, y
#      diagnosticarlos sin Internet es miserable.
#
# La estrategia correcta es la contraria: CONSTRUIR AQUI, donde hay Internet y
# donde ya sabemos que funciona, y llevarse la imagen terminada. Lo que cruza
# a la red aislada son archivos .tar.gz, no una lista de dependencias con la
# esperanza de que resuelvan.
# =============================================================================

set -uo pipefail

VERDE='\033[0;32m'; ROJO='\033[0;31m'; AMARILLO='\033[1;33m'
CYAN='\033[0;36m'; GRIS='\033[0;90m'; NC='\033[0m'
ok()    { echo -e "${VERDE}[ok]${NC} $1"; }
aviso() { echo -e "${AMARILLO}[aviso]${NC} $1"; }
fallo() { echo -e "${ROJO}[error]${NC} $1"; }
info()  { echo -e "${CYAN}[..]${NC} $1"; }

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DESTINO="${1:-${RAIZ}/bundle-offline}"

# Deben coincidir con el .env y el Dockerfile. Si cambia una version alli,
# cambiela aqui tambien o el paquete llevara una imagen distinta a la esperada.
IMAGEN_PROPIA="airflow-bsg:2.11.2"
IMAGENES_BASE=(
    "postgres:16-alpine"
    "redis:7-alpine"
    "apache/spark:3.5.3"
    "busybox:1.36"     # lo usa el contenedor de permisos; sin el, no arranca
    "nginx:1.27-alpine"  # proxy TLS; 1.27 porque http2 on necesita >= 1.25.1
)

echo
echo -e "${CYAN}==============================================================${NC}"
echo -e "${CYAN}  PAQUETE PARA RED AISLADA${NC}"
echo -e "${CYAN}==============================================================${NC}"
echo "  Origen:  ${RAIZ}"
echo "  Destino: ${DESTINO}"
echo

# --- Comprobaciones previas --------------------------------------------------
if ! docker info >/dev/null 2>&1; then
    fallo "Docker no responde. Este script se ejecuta en la maquina CON Internet."
    exit 1
fi
ok "Docker responde"

# La imagen propia es el corazon del paquete: lleva Java, Spark, los 5 drivers
# JDBC y los providers. Si no existe, no hay nada que empaquetar.
if ! docker image inspect "${IMAGEN_PROPIA}" >/dev/null 2>&1; then
    fallo "No existe la imagen ${IMAGEN_PROPIA}."
    echo
    echo "  Construyala primero, en esta misma maquina:"
    echo "      ./scripts/construir_imagen.sh"
    echo
    echo "  Es el paso que NO se puede hacer en el servidor aislado."
    exit 1
fi
ok "Imagen propia encontrada: ${IMAGEN_PROPIA}"

mkdir -p "${DESTINO}/imagenes" "${DESTINO}/proyecto"

# --- 1. Imagenes -------------------------------------------------------------
echo
info "Guardando imagenes. Es lo lento y lo pesado del paquete."
echo

guardar() {
    local imagen="$1" archivo="$2"
    if ! docker image inspect "${imagen}" >/dev/null 2>&1; then
        info "  descargando ${imagen}"
        if ! docker pull "${imagen}" >/dev/null 2>&1; then
            fallo "  no se pudo descargar ${imagen}"
            return 1
        fi
    fi
    docker save "${imagen}" | gzip > "${DESTINO}/imagenes/${archivo}.tar.gz"
    ok "  $(du -h "${DESTINO}/imagenes/${archivo}.tar.gz" | cut -f1)  ${archivo}.tar.gz"
}

FALLIDAS=""
guardar "${IMAGEN_PROPIA}" "airflow-bsg" || FALLIDAS="${FALLIDAS} ${IMAGEN_PROPIA}"
for img in "${IMAGENES_BASE[@]}"; do
    nombre="$(echo "${img}" | tr '/:' '__')"
    guardar "${img}" "${nombre}" || FALLIDAS="${FALLIDAS} ${img}"
done

# --- 2. Archivos del proyecto ------------------------------------------------
echo
info "Copiando archivos del proyecto"

# Lista explicita y no "copiar todo": evita arrastrar .git, logs, parquet de
# pruebas y -lo importante- un .env con secretos reales de desarrollo.
for ruta in \
    docker-compose.ubuntu.yml docker-compose.windows.yml docker-compose.rhel.yml \
    .env.ubuntu .env.windows .env.rhel \
    Dockerfile setup.sh setup.ps1 docker-compose.tls.yml nginx \
    README.md README-TI.md README-TI-REDUCIDO.md \
    README-REQUERIMIENTOS-FUNCIONAMIENTO.md README_DOCKER.md README-NGINX.md \
    airflow scripts spark docs
do
    if [ -e "${RAIZ}/${ruta}" ]; then
        cp -r "${RAIZ}/${ruta}" "${DESTINO}/proyecto/"
    else
        aviso "  no existe, se omite: ${ruta}"
    fi
done

# Nunca viaja un .env real: lleva secretos y el destino debe generar los suyos.
rm -f "${DESTINO}/proyecto/.env"
# Ni logs, ni cache de Python, ni datos de pruebas.
find "${DESTINO}/proyecto" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find "${DESTINO}/proyecto" -type d -name "logs" -exec rm -rf {} + 2>/dev/null
ok "Proyecto copiado (sin .env, sin logs, sin __pycache__)"

# --- 3. Script de carga en el destino ----------------------------------------
cat > "${DESTINO}/cargar_bundle.sh" <<'CARGADOR'
#!/usr/bin/env bash
# Se ejecuta EN EL SERVIDOR AISLADO. No necesita Internet.
set -uo pipefail
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo
echo "=== Cargando imagenes en el motor de contenedores ==="
echo

# podman entiende los mismos comandos; si esta instalado podman-docker, el
# ejecutable 'docker' ya apunta ahi y esto funciona sin cambios.
for archivo in "${AQUI}"/imagenes/*.tar.gz; do
    echo "  cargando $(basename "${archivo}")"
    gunzip -c "${archivo}" | docker load
done

# --- Nombres cortos: la diferencia entre Docker y Podman ---------------------
# Docker asume Docker Hub cuando ve "postgres:16-alpine". Podman NO: exige el
# registro completo, y al cargar un archivo puede dejar la imagen como
# localhost/postgres:16-alpine o docker.io/library/postgres:16-alpine.
# El compose dice "postgres:16-alpine" a secas, asi que si el nombre no
# coincide el arranque falla con "short-name resolution failed".
#
# Volver a etiquetar es inofensivo en Docker (etiquetar algo con su propio
# nombre no hace nada) y arregla el caso de Podman. Se hace siempre.
echo
echo "=== Normalizando nombres de imagen ==="
for par in \
    "postgres:16-alpine" "redis:7-alpine" "apache/spark:3.5.3" "busybox:1.36" "nginx:1.27-alpine"
do
    for prefijo in "docker.io/library/" "docker.io/" "localhost/"; do
        if docker image inspect "${prefijo}${par}" >/dev/null 2>&1; then
            docker tag "${prefijo}${par}" "${par}" 2>/dev/null && \
                echo "  ${prefijo}${par}  ->  ${par}"
            break
        fi
    done
done

echo
echo "=== Imagenes disponibles ==="
docker images | grep -E "airflow-bsg|postgres|redis|spark|busybox|nginx"

echo
echo "=== Que sigue ==="
echo "  1. cd proyecto"
echo "  2. ./setup.sh                 genera .env con secretos NUEVOS"
echo "  3. Confirme en .env:  AIRFLOW_IMAGE=airflow-bsg:2.11.2"
echo "  4. docker compose -f docker-compose.ubuntu.yml up -d"
echo "  5. docker compose -f docker-compose.ubuntu.yml exec airflow-webserver \\"
echo "         python /opt/airflow/verificar_drivers.py"
echo
echo "  NO ejecute 'docker compose pull': intentaria salir a Internet y fallara."
echo "  Las imagenes ya estan cargadas; up -d las usa directamente."
echo
CARGADOR
chmod +x "${DESTINO}/cargar_bundle.sh"
ok "cargar_bundle.sh generado"

# --- 4. Manifiesto con sumas de verificacion ---------------------------------
# El traslado suele ser por USB o por un recinto de transferencia. Un archivo
# truncado ahi produce, dias despues, un "docker load" que falla sin explicar
# por que. Las sumas convierten eso en una comprobacion de treinta segundos.
{
    echo "PAQUETE SIN CONEXION - Airflow + Spark"
    echo "Generado: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "Origen:   $(hostname)"
    echo
    echo "IMAGENES"
    (cd "${DESTINO}/imagenes" && sha256sum *.tar.gz)
    echo
    echo "TAMANO TOTAL: $(du -sh "${DESTINO}" | cut -f1)"
} > "${DESTINO}/MANIFIESTO.txt"

cat > "${DESTINO}/verificar_bundle.sh" <<'VERIF'
#!/usr/bin/env bash
# Comprueba que el traslado no corrompio ningun archivo.
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${AQUI}/imagenes" || exit 1
echo "Verificando sumas de los archivos de imagen..."
grep "tar.gz" "${AQUI}/MANIFIESTO.txt" | sha256sum -c - \
    && echo "OK: el traslado esta integro."
VERIF
chmod +x "${DESTINO}/verificar_bundle.sh"
ok "MANIFIESTO.txt y verificar_bundle.sh generados"

# --- 5. Resumen --------------------------------------------------------------
echo
echo -e "${CYAN}==============================================================${NC}"
if [ -n "${FALLIDAS}" ]; then
    fallo "No se pudieron empaquetar:${FALLIDAS}"
    echo "  El paquete esta INCOMPLETO. Corrijalo antes de trasladarlo."
    echo -e "${CYAN}==============================================================${NC}"
    exit 1
fi
echo -e "${VERDE}  PAQUETE COMPLETO${NC}"
echo -e "${CYAN}==============================================================${NC}"
echo
echo "  Carpeta: ${DESTINO}"
echo "  Tamano:  $(du -sh "${DESTINO}" | cut -f1)"
echo
echo "  En el servidor aislado:"
echo "      ./verificar_bundle.sh      comprobar que el traslado esta integro"
echo "      ./cargar_bundle.sh         cargar las imagenes"
echo
echo -e "${GRIS}  Recuerde: los secretos NO viajan. setup.sh genera unos nuevos${NC}"
echo -e "${GRIS}  en el destino, que es lo correcto para produccion.${NC}"
echo
