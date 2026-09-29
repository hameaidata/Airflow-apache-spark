#!/usr/bin/env bash
# ============================================================================
# crear_roles.sh - Aplica la matriz de roles de Airflow (Linux)
# ----------------------------------------------------------------------------
# Uso (desde cualquier carpeta):
#   ./scripts/crear_roles.sh              # SIMULA, no cambia nada
#   ./scripts/crear_roles.sh --aplicar    # ejecuta los cambios
#   ./scripts/crear_roles.sh --verificar  # compara con la matriz
#
# Equivalente a scripts/crear_roles.ps1 para Windows. Si cambia uno, cambie
# el otro: ambos deben hacer exactamente lo mismo.
# ============================================================================

set -euo pipefail

# La raiz del proyecto es la carpeta padre de scripts/, sin importar desde
# donde se invoque el script.
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$RAIZ"

COMPOSE="${COMPOSE_FILE:-docker-compose.ubuntu.yml}"

VERDE='\033[0;32m'; AMARILLO='\033[1;33m'; ROJO='\033[0;31m'; CYAN='\033[0;36m'; GRIS='\033[0;90m'; NC='\033[0m'

if [ ! -f "$COMPOSE" ]; then
    echo -e "${ROJO}ERROR:${NC} no encuentro $COMPOSE"
    echo -e "  Buscado en: $RAIZ"
    exit 1
fi

if [ ! -f airflow/config/aplicar_roles.py ]; then
    echo -e "${ROJO}ERROR:${NC} falta airflow/config/aplicar_roles.py"
    exit 1
fi

# Comprobar que el webserver responde.
#
# NO se analiza la salida de 'docker compose ps': el campo .State del formato
# no existe en todas las versiones de Compose. En su lugar se prueba
# directamente lo que el script necesita: entrar al contenedor y ejecutar
# airflow. Si eso funciona, todo lo demas funciona.
if ! docker compose -f "$COMPOSE" exec -T airflow-webserver airflow version >/dev/null 2>&1; then
    echo -e "${ROJO}ERROR:${NC} no puedo ejecutar airflow dentro de airflow-webserver."
    echo ""
    echo -e "${AMARILLO}Revise el estado de los contenedores:${NC}"
    echo "  docker compose -f $COMPOSE ps"
    echo ""
    echo -e "${AMARILLO}Si el webserver aparece como Created o Restarting, aun esta${NC}"
    echo -e "${AMARILLO}arrancando. La primera vez tarda 1-2 minutos. Reintente luego.${NC}"
    exit 1
fi

case "${1:-}" in
    --aplicar)   MODO='--aplicar' ;;
    --verificar) MODO='--verificar' ;;
    *)           MODO='--simular'
                 echo ""
                 echo -e "${AMARILLO}MODO SIMULACION - no se cambiara nada.${NC}"
                 echo -e "${AMARILLO}Para aplicar de verdad:  ./scripts/crear_roles.sh --aplicar${NC}"
                 echo "" ;;
esac

# set -e haria abortar el script si el python devuelve != 0, y perderiamos el
# mensaje de ayuda. Se captura el codigo a proposito.
set +e
docker compose -f "$COMPOSE" exec -T airflow-webserver \
    python /opt/airflow/config/aplicar_roles.py "$MODO"
CODIGO=$?
set -e

if [ "$CODIGO" -eq 0 ] && [ "$MODO" = '--aplicar' ]; then
    echo ""
    echo -e "${VERDE}Roles creados.${NC} Verifique en la interfaz:"
    echo "  http://localhost:8080  ->  Security  ->  List Roles"
    echo ""
    echo -e "${CYAN}Para asignar un rol a una persona:${NC}"
    echo "  docker compose -f $COMPOSE exec airflow-webserver \\"
    echo "    airflow users add-role -u USUARIO -r BSG_IngenieroDatos"
    echo ""
    echo -e "${GRIS}Reemplace USUARIO por el nombre real de la cuenta.${NC}"
fi

exit "$CODIGO"
