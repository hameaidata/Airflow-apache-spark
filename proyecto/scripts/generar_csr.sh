#!/usr/bin/env bash
# =============================================================================
# GENERAR CLAVE PRIVADA Y CSR PARA QUE LO FIRME LA CA DEL BANCO
# -----------------------------------------------------------------------------
# Equivalente exacto de scripts/generar_csr.ps1 (Windows).
#
# Uso:
#     ./scripts/generar_csr.sh --dominio bsg.banco.local
#     ./scripts/generar_csr.sh --dominio bsg.banco.local --org "Banco XYZ" --pais PE
#     ./scripts/generar_csr.sh --ayuda
#
# Produce en tls/:
#     plataforma.key   LA CLAVE PRIVADA. Nunca sale de este servidor.
#     plataforma.csr   Esto es lo que se le envia al banco para firmar.
#     plataforma.cnf   La configuracion usada, para poder repetirlo igual.
#
# -----------------------------------------------------------------------------
# UN SOLO CERTIFICADO PARA LAS TRES INTERFACES
#
# Airflow, Spark y Flower son tres nombres de host distintos, pero NO hacen
# falta tres certificados. Un certificado con varios SAN (Subject Alternative
# Name) los cubre a los tres:
#
#     airflow.<dominio>     interfaz de Airflow
#     spark.<dominio>       interfaz del master de Spark
#     flower.<dominio>      monitor de Celery
#
# Ventaja practica: se hace UN tramite con la CA del banco en vez de tres, y
# hay una sola fecha de caducidad que vigilar en vez de tres.
#
# -----------------------------------------------------------------------------
# POR QUE EL CN YA NO BASTA
#
# El Common Name (CN) del subject esta obsoleto para validar nombres de host.
# Chrome lo ignora por completo desde la version 58; Firefox y Edge igual. Un
# certificado con CN correcto pero SIN la extension SAN produce
# ERR_CERT_COMMON_NAME_INVALID aunque el nombre coincida exactamente.
#
# Por eso este script SIEMPRE escribe la extension subjectAltName, y por eso
# hay que insistirle a la CA del banco en que la conserve al firmar. Algunas
# plantillas de Microsoft AD CS descartan las extensiones del CSR y generan
# las suyas: si eso pasa, el certificado vuelve sin SAN y no sirve.
# =============================================================================

set -uo pipefail

VERDE='\033[0;32m'; ROJO='\033[0;31m'; AMARILLO='\033[1;33m'
CYAN='\033[0;36m'; GRIS='\033[0;90m'; NC='\033[0m'
ok()    { echo -e "${VERDE}[ok]${NC} $1"; }
aviso() { echo -e "${AMARILLO}[aviso]${NC} $1"; }
fallo() { echo -e "${ROJO}[error]${NC} $1"; }
info()  { echo -e "${CYAN}[..]${NC} $1"; }

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TLS="${RAIZ}/tls"

DOMINIO=""
ORG="Banco"
UNIDAD="Tecnologia"
PAIS="PE"
ESTADO="Lima"
CIUDAD="Lima"
BITS=2048

ayuda() { sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }

while [ $# -gt 0 ]; do
    case "$1" in
        --dominio) DOMINIO="${2:-}"; shift 2 ;;
        --org)     ORG="${2:-}"; shift 2 ;;
        --unidad)  UNIDAD="${2:-}"; shift 2 ;;
        --pais)    PAIS="${2:-}"; shift 2 ;;
        --estado)  ESTADO="${2:-}"; shift 2 ;;
        --ciudad)  CIUDAD="${2:-}"; shift 2 ;;
        --bits)    BITS="${2:-}"; shift 2 ;;
        --ayuda|-h|--help) ayuda ;;
        *) fallo "Opcion desconocida: $1"; echo "Use --ayuda"; exit 2 ;;
    esac
done

if [ -z "${DOMINIO}" ]; then
    fallo "Falta --dominio"
    echo
    echo "  Ejemplo:  ./scripts/generar_csr.sh --dominio bsg.banco.local"
    echo
    echo "  Es el dominio interno del banco bajo el que colgaran las tres"
    echo "  interfaces. Pidaselo a TI junto con las entradas de DNS."
    exit 2
fi

if ! command -v openssl >/dev/null 2>&1; then
    fallo "No encuentro openssl."
    echo "  sudo apt-get install -y openssl     (Debian/Ubuntu)"
    echo "  sudo dnf install -y openssl         (RHEL)"
    exit 1
fi

# 2048 es el minimo que aceptan las CA modernas; por debajo, muchas rechazan.
if [ "${BITS}" -lt 2048 ]; then
    fallo "Una clave de ${BITS} bits sera rechazada. El minimo real es 2048."
    exit 2
fi

HOST_AIRFLOW="airflow.${DOMINIO}"
HOST_SPARK="spark.${DOMINIO}"
HOST_FLOWER="flower.${DOMINIO}"

mkdir -p "${TLS}"

echo
echo -e "${CYAN}==============================================================${NC}"
echo -e "${CYAN}  GENERAR CSR PARA LA CA DEL BANCO${NC}"
echo -e "${CYAN}==============================================================${NC}"
echo "  Dominio base : ${DOMINIO}"
echo "  Nombres SAN  : ${HOST_AIRFLOW}"
echo "                 ${HOST_SPARK}"
echo "                 ${HOST_FLOWER}"
echo "  Clave        : RSA ${BITS} bits"
echo "  Organizacion : ${ORG} / ${UNIDAD}"
echo

# --- Aviso si ya existe una clave --------------------------------------------
# Regenerar la clave invalida cualquier certificado ya firmado con ella. Si eso
# pasa despues de un tramite de dos semanas con la CA, hay que repetirlo.
if [ -f "${TLS}/plataforma.key" ]; then
    aviso "Ya existe ${TLS}/plataforma.key"
    echo
    echo "  Si la regenera, CUALQUIER certificado ya firmado con ella deja de"
    echo "  servir y habra que repetir el tramite con la CA."
    echo
    read -r -p "  Escriba REGENERAR para continuar: " respuesta
    if [ "${respuesta}" != "REGENERAR" ]; then
        echo "  Cancelado. No se toco nada."
        exit 0
    fi
    cp "${TLS}/plataforma.key" "${TLS}/plataforma.key.anterior"
    aviso "Copia de la clave anterior en plataforma.key.anterior"
fi

# --- Archivo de configuracion de openssl -------------------------------------
# Se deja escrito en disco a proposito: si dentro de un ano hay que renovar,
# se repite exactamente el mismo CSR sin recordar que parametros se usaron.
cat > "${TLS}/plataforma.cnf" <<EOF
# Generado por scripts/generar_csr.sh para el dominio ${DOMINIO}
[ req ]
default_bits        = ${BITS}
prompt              = no
default_md          = sha256
distinguished_name  = dn
req_extensions      = req_ext

[ dn ]
C  = ${PAIS}
ST = ${ESTADO}
L  = ${CIUDAD}
O  = ${ORG}
OU = ${UNIDAD}
CN = ${HOST_AIRFLOW}

[ req_ext ]
# LA EXTENSION QUE DE VERDAD IMPORTA.
# Los navegadores validan el nombre contra esta lista, no contra el CN.
subjectAltName   = @alt_names
keyUsage         = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth

[ alt_names ]
DNS.1 = ${HOST_AIRFLOW}
DNS.2 = ${HOST_SPARK}
DNS.3 = ${HOST_FLOWER}
EOF
ok "plataforma.cnf escrito"

# --- Clave privada -----------------------------------------------------------
# SIN contrasena, a proposito. Una clave con passphrase obliga a teclearla en
# cada arranque de nginx, lo que impide que el servidor levante solo tras un
# reinicio. La proteccion aqui son los permisos del archivo, no una frase que
# alguien acabaria escribiendo en un script.
if ! openssl genrsa -out "${TLS}/plataforma.key" "${BITS}" 2>/dev/null; then
    fallo "No se pudo generar la clave"
    exit 1
fi
chmod 600 "${TLS}/plataforma.key"
ok "plataforma.key generada (permisos 600)"

# --- CSR ---------------------------------------------------------------------
if ! openssl req -new \
        -key "${TLS}/plataforma.key" \
        -out "${TLS}/plataforma.csr" \
        -config "${TLS}/plataforma.cnf"; then
    fallo "No se pudo generar el CSR"
    exit 1
fi
ok "plataforma.csr generado"

# --- Comprobar que el SAN quedo dentro ---------------------------------------
# Un CSR sin SAN se ve perfectamente normal y produce un certificado inservible.
# Mejor descubrirlo ahora que despues de dos semanas de tramite.
echo
info "Comprobando que el CSR lleva la extension SAN"
SALIDA="$(openssl req -in "${TLS}/plataforma.csr" -noout -text 2>/dev/null)"
if echo "${SALIDA}" | grep -q "Subject Alternative Name"; then
    ok "SAN presente:"
    echo "${SALIDA}" | grep -A1 "Subject Alternative Name" | tail -1 | sed 's/^/       /'
else
    fallo "El CSR NO lleva SAN. No lo envie: no serviria."
    exit 1
fi

# --- spark-defaults.conf -----------------------------------------------------
# Spark necesita saber por que URL lo van a ver, o genera enlaces internos que
# el navegador no puede seguir. Se escribe como archivo y no como variable de
# entorno porque spark-defaults.conf lo leen SIEMPRE el master y los workers,
# mientras que el paso de opciones por entorno depende del arranque.
mkdir -p "${RAIZ}/spark/conf"
cat > "${RAIZ}/spark/conf/spark-defaults.conf" <<EOF
# Generado por scripts/generar_csr.sh
#
# Spark detras de un proxy inverso. Estas dos opciones DEBEN estar puestas
# igual en el master y en TODOS los workers, o los enlaces de la interfaz
# apuntan a nombres internos que el navegador no resuelve.
spark.ui.reverseProxy      true
spark.ui.reverseProxyUrl   https://${HOST_SPARK}

# Con reverseProxy activo, las interfaces de los workers dejan de ser
# accesibles directamente y solo se llega a ellas A TRAVES del master. Es
# justo lo que queremos: una sola puerta de entrada.
EOF
ok "spark/conf/spark-defaults.conf generado"

# --- Que sigue ---------------------------------------------------------------
echo
echo -e "${CYAN}==============================================================${NC}"
echo -e "${VERDE}  CSR LISTO PARA ENVIAR${NC}"
echo -e "${CYAN}==============================================================${NC}"
echo
echo "  1. ENVIE a la CA del banco unicamente este archivo:"
echo "       ${TLS}/plataforma.csr"
echo
echo -e "${ROJO}     NUNCA envie plataforma.key. Esa clave no sale de este servidor.${NC}"
echo "     Si alguien se la pide, esta pidiendo algo que no necesita: una CA"
echo "     firma un CSR sin ver jamas la clave privada."
echo
echo "  2. PIDA EXPRESAMENTE que el certificado firmado conserve los tres SAN."
echo "     Algunas plantillas de AD CS descartan las extensiones del CSR y"
echo "     generan las suyas. Si vuelve sin SAN, no sirve y hay que repetirlo."
echo
echo "  3. PIDA TAMBIEN la cadena de la CA (raiz e intermedias) en formato PEM."
echo "     Sin ella, los navegadores del banco desconfiaran del certificado"
echo "     aunque sea correcto."
echo
echo "  4. Cuando lo devuelvan:"
echo "       ./scripts/instalar_certificado.sh --cert <archivo> --cadena <archivo>"
echo
echo "  5. Y pida a TI las tres entradas de DNS apuntando a este servidor:"
echo "       ${HOST_AIRFLOW}"
echo "       ${HOST_SPARK}"
echo "       ${HOST_FLOWER}"
echo
echo -e "${GRIS}  Ver el CSR en texto:  openssl req -in tls/plataforma.csr -noout -text${NC}"
echo
