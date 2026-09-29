#!/bin/sh
# =============================================================================
# ESCRIBE LA DIRECTIVA resolver DE NGINX A PARTIR DEL DNS DEL CONTENEDOR
# -----------------------------------------------------------------------------
# La imagen oficial de nginx ejecuta todo lo que haya en /docker-entrypoint.d/
# antes de arrancar el servidor. Esto corre ahi.
#
# EL PROBLEMA QUE RESUELVE
#
# Nginx resuelve los nombres de los upstream UNA VEZ, al arrancar. Si el
# contenedor de Airflow todavia no existe, nginx no arranca "degradado": se
# niega a arrancar del todo, con
#
#     [emerg] host not found in upstream "airflow-webserver"
#
# En un arranque en frio -docker compose up con todo apagado- eso es una
# carrera que se pierde a menudo. Y aunque se gane, si mas tarde se recrea el
# contenedor de Airflow, cambia su IP y nginx sigue usando la vieja: la
# interfaz devuelve 502 hasta que alguien reinicia nginx.
#
# LA SOLUCION
#
# Declarar un  resolver  y usar VARIABLES en proxy_pass. Con una variable,
# nginx resuelve el nombre en cada peticion en vez de al arrancar. Arranca
# aunque los backends no existan todavia, y se entera solo cuando cambian.
#
# POR QUE SE LEE DE /etc/resolv.conf Y NO SE FIJA 127.0.0.11
#
# 127.0.0.11 es el DNS interno de Docker. Podman usa aardvark-dns en otra
# direccion. Fijar la de Docker haria que esto funcionara en Windows y Ubuntu
# y fallara en RHEL, que es justo donde menos ganas hay de diagnosticar.
# Leyendolo del resolv.conf del propio contenedor funciona en los tres.
# =============================================================================

set -e

SALIDA=/etc/nginx/conf.d/00-resolver.conf

# Primer nameserver del contenedor. Vale tanto para Docker como para Podman.
DNS=$(awk '/^nameserver/ { print $2; exit }' /etc/resolv.conf 2>/dev/null || true)

if [ -z "${DNS}" ]; then
    echo "[resolver] AVISO: no encontre nameserver en /etc/resolv.conf."
    echo "[resolver] Se omite la directiva resolver; nginx resolvera al arrancar"
    echo "[resolver] y podria fallar si los backends aun no existen."
    : > "${SALIDA}"
    exit 0
fi

# Una direccion IPv6 tiene que ir entre corchetes en la directiva resolver.
case "${DNS}" in
    *:*) DNS="[${DNS}]" ;;
esac

# valid=10s: si un backend se recrea y cambia de IP, nginx lo nota en 10
# segundos sin que nadie tenga que reiniciar nada.
# ipv6=off: las redes de compose son IPv4; pedir AAAA solo anade latencia y
# ruido de "no resuelto" en la bitacora de errores.
cat > "${SALIDA}" <<EOF
# Generado en el arranque por /docker-entrypoint.d/10-resolver.sh
resolver ${DNS} valid=10s ipv6=off;
resolver_timeout 5s;
EOF

echo "[resolver] DNS del contenedor: ${DNS}"
