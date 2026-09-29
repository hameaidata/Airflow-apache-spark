#!/usr/bin/env bash
# Se ejecuta EN EL SERVIDOR AISLADO. No necesita Internet.
set -uo pipefail
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo
echo "=== Cargando imagenes en el motor de contenedores ==="
echo

for archivo in "${AQUI}"/imagenes/*.tar; do
    [ -e "${archivo}" ] || continue
    echo "  cargando $(basename "${archivo}")"
    docker load -i "${archivo}"
done
for archivo in "${AQUI}"/imagenes/*.tar.gz; do
    [ -e "${archivo}" ] || continue
    echo "  cargando $(basename "${archivo}")"
    gunzip -c "${archivo}" | docker load
done

echo
echo "=== Imagenes disponibles ==="
docker images | grep -E "airflow-bsg|postgres|redis|spark|busybox"

echo
echo "=== Que sigue ==="
echo "  1. cd proyecto"
echo "  2. ./setup.sh                 genera .env con secretos NUEVOS"
echo "  3. Confirme en .env:  AIRFLOW_IMAGE=airflow-bsg:2.11.2"
echo "  4. docker compose -f docker-compose.ubuntu.yml up -d"
echo
echo "  NO ejecute 'docker compose pull': intentaria salir a Internet y fallara."
echo