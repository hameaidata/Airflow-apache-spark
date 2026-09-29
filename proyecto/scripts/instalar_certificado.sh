#!/usr/bin/env bash
# =============================================================================
# INSTALAR EL CERTIFICADO QUE DEVOLVIO LA CA DEL BANCO
# -----------------------------------------------------------------------------
# Equivalente exacto de scripts/instalar_certificado.ps1 (Windows).
#
# Uso:
#     ./scripts/instalar_certificado.sh --cert firmado.cer
#     ./scripts/instalar_certificado.sh --cert firmado.cer --cadena ca.pem
#     ./scripts/instalar_certificado.sh --ayuda
#
# -----------------------------------------------------------------------------
# LAS CUATRO COMPROBACIONES, Y POR QUE CADA UNA
#
# Instalar un certificado equivocado no da un error claro: nginx arranca y el
# navegador muestra un aviso que no dice cual de las cuatro cosas fallo. Este
# script las separa ANTES de tocar nada.
#
#   1. FORMATO. Una CA puede devolver PEM, DER o PKCS#7. Nginx solo entiende
#      PEM. Los otros dos se convierten aqui automaticamente.
#
#   2. QUE EL CERTIFICADO CORRESPONDA A LA CLAVE. Es el fallo mas caro: si
#      alguien regenero la clave despues de enviar el CSR, el certificado que
#      vuelve es inutil y hay que repetir el tramite entero. Se comparan los
#      modulos, que es la prueba definitiva.
#
#   3. QUE CONSERVE LOS TRES SAN. Algunas plantillas de AD CS descartan las
#      extensiones del CSR y generan las suyas. El certificado se ve correcto
#      y el navegador lo rechaza igual.
#
#   4. QUE NO ESTE CADUCADO. Suena absurdo, pero una CA interna puede firmar
#      con fechas heredadas de una plantilla vieja.
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

CERT=""
CADENA=""

ayuda() { sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }

while [ $# -gt 0 ]; do
    case "$1" in
        --cert)   CERT="${2:-}"; shift 2 ;;
        --cadena) CADENA="${2:-}"; shift 2 ;;
        --ayuda|-h|--help) ayuda ;;
        *) fallo "Opcion desconocida: $1"; echo "Use --ayuda"; exit 2 ;;
    esac
done

[ -z "${CERT}" ]     && { fallo "Falta --cert"; echo "  Use --ayuda"; exit 2; }
[ ! -f "${CERT}" ]   && { fallo "No existe: ${CERT}"; exit 1; }
[ ! -f "${TLS}/plataforma.key" ] && {
    fallo "No existe ${TLS}/plataforma.key"
    echo "  Genere primero la clave y el CSR:"
    echo "      ./scripts/generar_csr.sh --dominio bsg.banco.local"
    exit 1
}

echo
echo -e "${CYAN}==============================================================${NC}"
echo -e "${CYAN}  INSTALAR CERTIFICADO FIRMADO${NC}"
echo -e "${CYAN}==============================================================${NC}"
echo "  Certificado: ${CERT}"
echo "  Cadena:      ${CADENA:-(no indicada)}"
echo

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

# --- 1. Normalizar a PEM -----------------------------------------------------
info "Detectando formato"
if openssl x509 -in "${CERT}" -noout >/dev/null 2>&1; then
    cp "${CERT}" "${TMP}/cert.pem"
    ok "Formato PEM"
elif openssl x509 -inform DER -in "${CERT}" -out "${TMP}/cert.pem" >/dev/null 2>&1; then
    ok "Formato DER, convertido a PEM"
elif openssl pkcs7 -inform DER -in "${CERT}" -print_certs -out "${TMP}/p7.pem" >/dev/null 2>&1 \
  || openssl pkcs7 -in "${CERT}" -print_certs -out "${TMP}/p7.pem" >/dev/null 2>&1; then
    # PKCS#7 es lo que suele devolver Microsoft AD CS (.p7b). Trae el
    # certificado del servidor Y la cadena en un solo archivo. El primero es
    # el del servidor; el resto, la cadena.
    ok "Formato PKCS#7 (tipico de AD CS)"
    awk '/BEGIN CERTIFICATE/{n++} n==1' "${TMP}/p7.pem" > "${TMP}/cert.pem"
    awk '/BEGIN CERTIFICATE/{n++} n>1'  "${TMP}/p7.pem" > "${TMP}/cadena_p7.pem"
    if [ -s "${TMP}/cadena_p7.pem" ] && [ -z "${CADENA}" ]; then
        CADENA="${TMP}/cadena_p7.pem"
        info "La cadena venia dentro del PKCS#7; se usara"
    fi
else
    fallo "No reconozco el formato de ${CERT}"
    echo "  Se admiten PEM, DER y PKCS#7. Pida a la CA que lo entregue en PEM."
    exit 1
fi

# --- 2. El certificado tiene que corresponder a la clave ---------------------
# La prueba definitiva: el modulo de la clave publica del certificado y el de
# la clave privada tienen que ser el mismo numero.
info "Comprobando que el certificado corresponda a la clave privada"
MOD_CERT="$(openssl x509 -in "${TMP}/cert.pem" -noout -modulus 2>/dev/null | openssl md5)"
MOD_KEY="$(openssl rsa  -in "${TLS}/plataforma.key" -noout -modulus 2>/dev/null | openssl md5)"

if [ -z "${MOD_CERT}" ] || [ "${MOD_CERT}" != "${MOD_KEY}" ]; then
    fallo "EL CERTIFICADO NO CORRESPONDE A ESTA CLAVE PRIVADA."
    echo
    echo "  cert: ${MOD_CERT}"
    echo "  key:  ${MOD_KEY}"
    echo
    echo "  Casi siempre significa una de dos cosas:"
    echo "    - se regenero la clave despues de enviar el CSR, o"
    echo "    - la CA firmo un CSR distinto al que usted envio."
    echo
    echo "  En ambos casos hay que rehacer el CSR y repetir el tramite. No"
    echo "  hay forma de arreglarlo desde aqui."
    exit 1
fi
ok "Certificado y clave corresponden"

# --- 3. Los tres SAN ---------------------------------------------------------
info "Comprobando los nombres alternativos (SAN)"
SAN="$(openssl x509 -in "${TMP}/cert.pem" -noout -ext subjectAltName 2>/dev/null | tail -n +2 | tr -d ' ')"
if [ -z "${SAN}" ]; then
    fallo "El certificado NO tiene extension SAN."
    echo
    echo "  Los navegadores modernos validan el nombre contra el SAN, no contra"
    echo "  el CN. Sin SAN, Chrome da ERR_CERT_COMMON_NAME_INVALID aunque el"
    echo "  nombre coincida."
    echo
    echo "  La plantilla de la CA descarto las extensiones del CSR. Hay que"
    echo "  pedir que lo firmen con una plantilla que las conserve."
    exit 1
fi
ok "SAN presente: ${SAN}"

FALTAN=""
for host in $(grep -oE "DNS\.[0-9]+ = [^ ]+" "${TLS}/plataforma.cnf" 2>/dev/null | awk '{print $3}'); do
    echo "${SAN}" | grep -q "DNS:${host}" || FALTAN="${FALTAN} ${host}"
done
if [ -n "${FALTAN}" ]; then
    aviso "Estos nombres del CSR NO estan en el certificado:${FALTAN}"
    aviso "Las interfaces de esos nombres daran aviso de certificado."
else
    ok "Los tres nombres del CSR estan en el certificado"
fi

# --- 4. Vigencia -------------------------------------------------------------
info "Comprobando fechas"
if ! openssl x509 -in "${TMP}/cert.pem" -noout -checkend 0 >/dev/null 2>&1; then
    fallo "EL CERTIFICADO YA ESTA CADUCADO."
    openssl x509 -in "${TMP}/cert.pem" -noout -dates | sed 's/^/    /'
    exit 1
fi
DESDE="$(openssl x509 -in "${TMP}/cert.pem" -noout -startdate | cut -d= -f2)"
HASTA="$(openssl x509 -in "${TMP}/cert.pem" -noout -enddate  | cut -d= -f2)"
ok "Vigente: ${DESDE}  ->  ${HASTA}"
# 30 dias
if ! openssl x509 -in "${TMP}/cert.pem" -noout -checkend 2592000 >/dev/null 2>&1; then
    aviso "Caduca en menos de 30 dias. Empiece ya la renovacion."
fi

# --- 5. Armar el archivo final ----------------------------------------------
# Nginx quiere UN archivo con el certificado del servidor primero y la cadena
# despues, en ese orden. Al reves, los clientes no validan.
if [ -n "${CADENA}" ] && [ -f "${CADENA}" ]; then
    cat "${TMP}/cert.pem" "${CADENA}" > "${TMP}/final.pem"
    N="$(grep -c "BEGIN CERTIFICATE" "${TMP}/final.pem")"
    ok "Certificado + cadena: ${N} certificados en el archivo"
else
    cp "${TMP}/cert.pem" "${TMP}/final.pem"
    aviso "Sin cadena. Los navegadores del banco pueden desconfiar aunque el"
    aviso "certificado sea correcto. Pida a la CA la raiz y las intermedias."
fi

# Copia de seguridad del anterior, si lo habia.
[ -f "${TLS}/plataforma.crt" ] && {
    cp "${TLS}/plataforma.crt" "${TLS}/plataforma.crt.anterior"
    info "El certificado anterior quedo en plataforma.crt.anterior"
}

cp "${TMP}/final.pem" "${TLS}/plataforma.crt"
chmod 644 "${TLS}/plataforma.crt"
ok "Instalado en ${TLS}/plataforma.crt"

# --- Que sigue ---------------------------------------------------------------
echo
echo -e "${CYAN}==============================================================${NC}"
echo -e "${VERDE}  CERTIFICADO INSTALADO${NC}"
echo -e "${CYAN}==============================================================${NC}"
echo
echo "  Levantar con TLS (elija su compose):"
echo "    docker compose -f docker-compose.ubuntu.yml -f docker-compose.tls.yml up -d"
echo
echo "  Comprobar que nginx acepto la configuracion:"
echo "    docker compose -f docker-compose.tls.yml exec nginx nginx -t"
echo
echo "  Y desde un equipo del banco, con el DNS ya publicado:"
for host in $(grep -oE "DNS\.[0-9]+ = [^ ]+" "${TLS}/plataforma.cnf" 2>/dev/null | awk '{print $3}'); do
    echo "    https://${host}"
done
echo
echo -e "${GRIS}  Anote la fecha de caducidad: ${HASTA}${NC}"
echo
