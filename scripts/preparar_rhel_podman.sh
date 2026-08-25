#!/usr/bin/env bash
# ============================================================================
# preparar_rhel_podman.sh - Deja RHEL 9.x listo para correr el stack
# ----------------------------------------------------------------------------
# NO instala Docker. Usa Podman, que RHEL ya incluye y Red Hat soporta.
#
# Uso:
#   ./scripts/preparar_rhel_podman.sh                 # solo diagnostica
#   sudo ./scripts/preparar_rhel_podman.sh --aplicar  # aplica los cambios
#
# Lo que hace con --aplicar:
#   1. Instala container-tools si falta
#   2. Configura rangos subuid/subgid para la cuenta de servicio
#   3. Activa lingering (que los servicios sobrevivan al cierre de sesion)
#   4. Activa el socket compatible con la API de Docker
#
# Detalle y alternativas: docs/RHEL_SIN_DOCKER.md
# ============================================================================

set -uo pipefail

USUARIO="${USUARIO_SERVICIO:-svc_airflow}"
APLICAR=false
[ "${1:-}" = "--aplicar" ] && APLICAR=true

V='\033[0;32m'; A='\033[1;33m'; R='\033[0;31m'; C='\033[0;36m'; G='\033[0;90m'; N='\033[0m'
ok()    { echo -e "  ${V}ok${N}     $*"; }
falta() { echo -e "  ${R}falta${N}  $*"; }
aviso() { echo -e "  ${A}aviso${N}  $*"; }
paso()  { echo -e "  ${G}       $*${N}"; }

pendientes=0

echo ""
echo -e "${C}======================================================================${N}"
echo -e "${C}  PREPARAR RHEL PARA EL STACK (sin Docker)${N}"
echo -e "${C}======================================================================${N}"
$APLICAR && echo -e "  ${A}MODO APLICAR${N}" || echo -e "  ${G}Solo diagnostico. Use --aplicar para cambiar algo.${N}"
echo ""

# --- 1. Version del sistema -------------------------------------------------
echo -e "${C}-- Sistema --${N}"
if [ -f /etc/redhat-release ]; then
    ok "$(cat /etc/redhat-release)"
else
    aviso "No parece RHEL. Este script asume RHEL 9.x o compatible."
fi

# --- 2. Podman --------------------------------------------------------------
echo ""
echo -e "${C}-- Podman --${N}"
if command -v podman >/dev/null 2>&1; then
    ok "podman $(podman --version | awk '{print $3}')"
else
    falta "podman no esta instalado"
    paso "sudo dnf install -y container-tools"
    pendientes=$((pendientes+1))
    if $APLICAR; then
        dnf install -y container-tools && ok "instalado" || falta "no se pudo instalar"
    fi
fi

for extra in buildah skopeo; do
    command -v $extra >/dev/null 2>&1 && ok "$extra presente" || aviso "$extra no esta (opcional)"
done

# El paquete que da un comando 'docker' que redirige a podman
if command -v docker >/dev/null 2>&1; then
    if docker --version 2>/dev/null | grep -qi podman; then
        ok "el comando 'docker' redirige a podman (podman-docker)"
    else
        aviso "hay un 'docker' que NO es podman. Confirme cual quiere usar."
    fi
else
    aviso "sin comando 'docker'. Opcional:  sudo dnf install -y podman-docker"
    paso "con eso, todos los comandos del proyecto funcionan escritos igual"
fi

# --- 3. Rangos subuid / subgid ----------------------------------------------
echo ""
echo -e "${C}-- Rangos de identificadores para contenedores sin privilegios --${N}"
if id "$USUARIO" >/dev/null 2>&1; then
    ok "la cuenta '$USUARIO' existe"
    if grep -q "^${USUARIO}:" /etc/subuid 2>/dev/null && grep -q "^${USUARIO}:" /etc/subgid 2>/dev/null; then
        ok "subuid: $(grep "^${USUARIO}:" /etc/subuid)"
        ok "subgid: $(grep "^${USUARIO}:" /etc/subgid)"
    else
        falta "sin rangos subuid/subgid — los contenedores no arrancaran"
        paso "sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 $USUARIO"
        pendientes=$((pendientes+1))
        if $APLICAR; then
            usermod --add-subuids 100000-165535 --add-subgids 100000-165535 "$USUARIO" \
                && ok "rangos asignados" || falta "no se pudo"
        fi
    fi
else
    falta "la cuenta '$USUARIO' no existe"
    paso "sudo useradd --system --shell /sbin/nologin $USUARIO"
    paso "o defina otra:  USUARIO_SERVICIO=micuenta ./scripts/preparar_rhel_podman.sh"
    pendientes=$((pendientes+1))
fi

# --- 4. Lingering -----------------------------------------------------------
echo ""
echo -e "${C}-- Persistencia de los servicios --${N}"
if id "$USUARIO" >/dev/null 2>&1; then
    if [ -f "/var/lib/systemd/linger/${USUARIO}" ]; then
        ok "lingering activo: los contenedores sobreviven al cierre de sesion"
    else
        falta "lingering NO activo"
        paso "Sin esto los contenedores se detienen al cerrar sesion y no vuelven"
        paso "tras reiniciar. Es el fallo mas frecuente, y el sintoma no apunta ahi."
        paso "sudo loginctl enable-linger $USUARIO"
        pendientes=$((pendientes+1))
        $APLICAR && { loginctl enable-linger "$USUARIO" && ok "activado" || falta "no se pudo"; }
    fi
fi

# --- 5. Puertos bajos -------------------------------------------------------
echo ""
echo -e "${C}-- Puertos por debajo de 1024 --${N}"
inicio=$(sysctl -n net.ipv4.ip_unprivileged_port_start 2>/dev/null || echo 1024)
if [ "$inicio" -le 443 ]; then
    ok "permitidos desde el $inicio: se puede publicar el 443 sin privilegios"
else
    aviso "solo desde el $inicio. El stack usa 8080, asi que funciona igual."
    paso "Para publicar el 443 directamente:"
    paso "  echo 'net.ipv4.ip_unprivileged_port_start=80' | sudo tee /etc/sysctl.d/99-podman.conf"
    paso "Alternativa preferible en banca: nginx delante, en el 443."
fi

# --- 6. SELinux -------------------------------------------------------------
echo ""
echo -e "${C}-- SELinux --${N}"
if command -v getenforce >/dev/null 2>&1; then
    modo=$(getenforce)
    if [ "$modo" = "Enforcing" ]; then
        ok "Enforcing — como debe ser"
        paso "Los montajes de volumen necesitan etiqueta :Z (privado) o :z (compartido)."
        paso "Sin ella el sintoma es 'Permission denied' con permisos aparentemente correctos."
    else
        aviso "SELinux en $modo. No pedimos desactivarlo; conviene volver a Enforcing."
    fi
fi

# --- 7. Socket compatible con la API de Docker ------------------------------
echo ""
echo -e "${C}-- Socket compatible (para usar docker compose) --${N}"
if systemctl --user is-active podman.socket >/dev/null 2>&1; then
    ok "podman.socket activo"
    ok "DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/podman/podman.sock"
else
    aviso "podman.socket no esta activo"
    paso "systemctl --user enable --now podman.socket"
    paso "export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/podman/podman.sock"
    if $APLICAR && [ "$(id -u)" != "0" ]; then
        systemctl --user enable --now podman.socket && ok "activado" || aviso "no se pudo"
    elif $APLICAR; then
        aviso "esto va como el usuario de servicio, no como root. Omitido."
    fi
fi

# --- 8. Sincronizacion horaria ----------------------------------------------
echo ""
echo -e "${C}-- Reloj --${N}"
if command -v timedatectl >/dev/null 2>&1; then
    if timedatectl show -p NTPSynchronized --value 2>/dev/null | grep -q yes; then
        ok "sincronizado"
    else
        falta "el reloj NO esta sincronizado"
        paso "Kerberos rechaza toda autenticacion con mas de 5 min de desfase,"
        paso "y el error nunca menciona la hora."
        paso "sudo systemctl enable --now chronyd"
        pendientes=$((pendientes+1))
    fi
fi

# --- Resumen ----------------------------------------------------------------
echo ""
echo -e "${C}======================================================================${N}"
if [ "$pendientes" -eq 0 ]; then
    echo -e "  ${V}Sin pendientes. El servidor puede correr el stack.${N}"
    echo ""
    echo "  Construir la imagen:"
    echo "    podman build -t airflow-bsg:2.11.2 ."
    echo ""
    echo "  Levantar (con el socket activo):"
    echo "    export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/podman/podman.sock"
    echo "    docker compose -f docker-compose.ubuntu.yml up -d"
else
    echo -e "  ${A}${pendientes} punto(s) pendientes.${N} Las lineas grises de arriba son los comandos."
    $APLICAR || echo -e "  Para aplicarlos:  ${C}sudo ./scripts/preparar_rhel_podman.sh --aplicar${N}"
fi
echo -e "${C}======================================================================${N}"
echo ""
