#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# unir_archivo.sh - Rearma un archivo partido por partir_archivo.ps1
#
#   ./unir_archivo.sh airflow-bsg_2.11.2.tar
#
# Espera encontrar en la carpeta actual:
#   <nombre>.part001, .part002, ...
#   <nombre>.sha256            (el manifiesto)
#
# POR QUE UN SCRIPT Y NO UN 'cat'
# -------------------------------
# 'cat *.part* > archivo' funciona y es lo que casi todo el mundo hace. El
# problema es que funciona TAMBIEN cuando falta un trozo o cuando uno llego
# truncado: cat concatena lo que haya y no se queja. El fallo aparece mucho
# despues, como un 'docker load' que dice que el tar esta corrupto -o peor,
# como una imagen que carga y se comporta de forma rara-, y a esas alturas
# nadie sospecha del traslado.
#
# Esto comprueba tres cosas antes de dar el archivo por bueno:
#   1. que esten TODOS los trozos que dice el manifiesto, sin huecos;
#   2. que cada trozo tenga su SHA256;
#   3. que el archivo rearmado tenga el SHA256 del original.
#
# Si falta el .sha256 avisa y une sin comprobar, porque es mejor que nada,
# pero lo dice.
# ---------------------------------------------------------------------------
set -uo pipefail

rojo()  { printf '\033[31m%s\033[0m\n' "$*"; }
verde() { printf '\033[32m%s\033[0m\n' "$*"; }
gris()  { printf '\033[90m%s\033[0m\n' "$*"; }

if [ $# -lt 1 ]; then
    echo "Uso: $0 <nombre-del-archivo-original>"
    echo "Ejemplo: $0 airflow-bsg_2.11.2.tar"
    echo
    echo "Archivos partidos que se ven aqui:"
    ls -1 *.part001 2>/dev/null | sed 's/\.part001$//; s/^/  /' || echo "  (ninguno)"
    exit 2
fi

NOMBRE="$1"
MANIFIESTO="${NOMBRE}.sha256"

# ---- que herramienta de sha256 hay -------------------------------------
if command -v sha256sum >/dev/null 2>&1; then
    suma() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
    suma() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
    rojo "No hay sha256sum ni shasum. No se puede verificar nada."
    echo "Instale coreutils, o una sin comprobar con:"
    echo "    cat ${NOMBRE}.part* > ${NOMBRE}"
    exit 3
fi

# ---- los trozos --------------------------------------------------------
# El glob va ordenado porque partir_archivo.ps1 numera con tres digitos
# (part001). Con numeracion sin rellenar, part10 iria antes de part2 y el
# archivo saldria mal ordenado SIN que nada avise: por eso los tres digitos.
shopt -s nullglob
TROZOS=( "${NOMBRE}".part[0-9][0-9][0-9] )
shopt -u nullglob

if [ ${#TROZOS[@]} -eq 0 ]; then
    rojo "No se encontro ningun ${NOMBRE}.partNNN en $(pwd)"
    exit 3
fi

echo
echo "  archivo : ${NOMBRE}"
echo "  trozos  : ${#TROZOS[@]} encontrados"

# ---- el manifiesto -----------------------------------------------------
SUMA_ORIGINAL=""
ESPERADOS=0
BYTES_ESPERADOS=0
declare -A SUMA_DE_TROZO=()

if [ -f "$MANIFIESTO" ]; then
    while read -r clave resto; do
        case "$clave" in
            ORIGINAL) SUMA_ORIGINAL=$(echo "$resto" | awk '{print $1}') ;;
            TROZOS)   ESPERADOS=$(echo "$resto" | awk '{print $1}') ;;
            BYTES)    BYTES_ESPERADOS=$(echo "$resto" | awk '{print $1}') ;;
            PARTE)
                s=$(echo "$resto" | awk '{print $1}')
                n=$(echo "$resto" | awk '{print $2}')
                SUMA_DE_TROZO["$n"]="$s"
                ;;
        esac
    done < "$MANIFIESTO"
    echo "  manifiesto: $MANIFIESTO  (espera $ESPERADOS trozo(s))"
else
    rojo "  FALTA $MANIFIESTO"
    echo "  Se unira SIN comprobar nada. Copie tambien ese archivo y repita"
    echo "  si quiere la verificacion."
fi
echo

# ---- faltan trozos? ----------------------------------------------------
if [ "$ESPERADOS" -gt 0 ] && [ "${#TROZOS[@]}" -ne "$ESPERADOS" ]; then
    rojo "Faltan trozos: hay ${#TROZOS[@]} y el manifiesto dice $ESPERADOS."
    echo "Los que faltan:"
    for i in $(seq 1 "$ESPERADOS"); do
        t=$(printf "%s.part%03d" "$NOMBRE" "$i")
        [ -f "$t" ] || echo "    $t"
    done
    exit 4
fi

# ---- cada trozo --------------------------------------------------------
if [ ${#SUMA_DE_TROZO[@]} -gt 0 ]; then
    echo "  comprobando cada trozo..."
    malos=0
    for t in "${TROZOS[@]}"; do
        esperada="${SUMA_DE_TROZO[$t]:-}"
        if [ -z "$esperada" ]; then
            rojo "    $t  no esta en el manifiesto"
            malos=$((malos + 1)); continue
        fi
        real=$(suma "$t")
        if [ "${real,,}" = "${esperada,,}" ]; then
            gris "    ok  $t"
        else
            rojo "    MAL $t"
            echo "        esperado: $esperada"
            echo "        real    : $real"
            malos=$((malos + 1))
        fi
    done
    if [ "$malos" -gt 0 ]; then
        echo
        rojo "$malos trozo(s) no cuadran. Vuelva a copiarlos; NO se une nada."
        exit 5
    fi
    echo
fi

# ---- unir --------------------------------------------------------------
if [ -e "$NOMBRE" ]; then
    rojo "Ya existe $NOMBRE. Muevalo o borrelo antes de rearmar."
    exit 6
fi

echo "  uniendo..."
cat "${TROZOS[@]}" > "$NOMBRE"

if [ "$BYTES_ESPERADOS" -gt 0 ]; then
    reales=$(wc -c < "$NOMBRE")
    if [ "$reales" -ne "$BYTES_ESPERADOS" ]; then
        rojo "El tamano no cuadra: $reales bytes, se esperaban $BYTES_ESPERADOS."
        exit 7
    fi
fi

# ---- el original -------------------------------------------------------
if [ -n "$SUMA_ORIGINAL" ]; then
    echo "  comprobando el archivo rearmado (1 pasada sobre el disco)..."
    real=$(suma "$NOMBRE")
    if [ "${real,,}" != "${SUMA_ORIGINAL,,}" ]; then
        rojo "EL ARCHIVO REARMADO NO ES EL ORIGINAL."
        echo "  esperado: $SUMA_ORIGINAL"
        echo "  real    : $real"
        echo
        echo "Los trozos cuadraban uno a uno, asi que lo mas probable es que"
        echo "esten en otro orden o que falte uno al final sin que el"
        echo "manifiesto lo diga. No use este archivo."
        exit 8
    fi
    verde "  SHA256 correcto: $real"
fi

echo
verde "  Listo: $NOMBRE  ($(du -h "$NOMBRE" | cut -f1))"
echo
echo "  Si es una imagen de Docker:"
echo "      docker load -i $NOMBRE"
echo
echo "  Y despues ya puede borrar los trozos:"
echo "      rm ${NOMBRE}.part[0-9][0-9][0-9]"
