# =============================================================================
# PAQUETE PARA RED SIN INTERNET  ::  se ejecuta en la maquina CON Internet
# -----------------------------------------------------------------------------
# Equivalente exacto de scripts/preparar-bundle-offline.sh (Ubuntu / Linux).
#
# Uso:
#     .\scripts\preparar-bundle-offline.ps1
#     .\scripts\preparar-bundle-offline.ps1 -Destino D:\traslado\bundle
#
# Produce una carpeta que se copia tal cual al servidor aislado. Alli no hace
# falta descargar absolutamente nada.
#
# La estrategia es CONSTRUIR AQUI, donde hay Internet y donde ya sabemos que
# funciona, y llevarse la imagen terminada. Lo que cruza a la red aislada son
# archivos .tar.gz, no una lista de dependencias con la esperanza de que
# resuelvan en el destino.
# =============================================================================

param(
    [string]$Destino,
    [switch]$Ayuda
)

# 'Continue' y NO 'Stop': docker escribe avisos por stderr durante operaciones
# perfectamente correctas, y con 'Stop' PowerShell los convierte en errores
# terminantes. Ya nos ocurrio antes.
$ErrorActionPreference = 'Continue'

function Escribir-Ok    { param($m) Write-Host "[ok] $m"    -ForegroundColor Green }
function Escribir-Aviso { param($m) Write-Host "[aviso] $m" -ForegroundColor Yellow }
function Escribir-Fallo { param($m) Write-Host "[error] $m" -ForegroundColor Red }
function Escribir-Info  { param($m) Write-Host "[..] $m"    -ForegroundColor Cyan }

if ($Ayuda) {
    Get-Content $PSCommandPath | Select-Object -First 17 |
        ForEach-Object { $_ -replace '^#\s?', '' }
    exit 0
}

$Raiz = Split-Path -Parent $PSScriptRoot
if (-not $Destino) { $Destino = Join-Path $Raiz 'bundle-offline' }

# Deben coincidir con el .env y el Dockerfile.
$ImagenPropia = 'airflow-bsg:2.11.2'
$ImagenesBase = @(
    'postgres:16-alpine',
    'redis:7-alpine',
    'apache/spark:3.5.3',
    'busybox:1.36'#,         # lo usa el contenedor de permisos; sin el, no arranca
    # 'nginx:1.27-alpine'     # proxy TLS; 1.27 porque http2 on necesita >= 1.25.1
)

Write-Host ''
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host '  PAQUETE PARA RED AISLADA'                                     -ForegroundColor Cyan
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host "  Origen:  $Raiz"
Write-Host "  Destino: $Destino"
Write-Host ''

# --- Comprobaciones previas --------------------------------------------------
docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Escribir-Fallo 'Docker no responde. Este script se ejecuta en la maquina CON Internet.'
    exit 1
}
Escribir-Ok 'Docker responde'

docker image inspect $ImagenPropia 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Escribir-Fallo "No existe la imagen $ImagenPropia."
    Write-Host ''
    Write-Host '  Construyala primero, en esta misma maquina:'
    Write-Host '      .\scripts\construir_imagen.ps1'
    Write-Host ''
    Write-Host '  Es el paso que NO se puede hacer en el servidor aislado.'
    exit 1
}
Escribir-Ok "Imagen propia encontrada: $ImagenPropia"

$dirImagenes = Join-Path $Destino 'imagenes'
$dirProyecto = Join-Path $Destino 'proyecto'
New-Item -ItemType Directory -Force -Path $dirImagenes, $dirProyecto | Out-Null

# --- 1. Imagenes -------------------------------------------------------------
Write-Host ''
Escribir-Info 'Guardando imagenes. Es lo lento y lo pesado del paquete.'
Write-Host ''

$fallidas = @()

function Guardar-Imagen {
    param($Imagen, $Archivo)
    docker image inspect $Imagen 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Escribir-Info "  descargando $Imagen"
        docker pull $Imagen 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Escribir-Fallo "  no se pudo descargar $Imagen"
            return $false
        }
    }
    # En Windows no hay gzip fiable en la tuberia: se guarda el .tar y se
    # comprime despues con Compress-Archive. El resultado es un .zip en vez de
    # un .tar.gz, y cargar_bundle.sh lo contempla.
    $tar = Join-Path $dirImagenes "$Archivo.tar"
    docker save -o $tar $Imagen
    if ($LASTEXITCODE -ne 0) {
        Escribir-Fallo "  fallo docker save de $Imagen"
        return $false
    }
    $mb = [math]::Round((Get-Item $tar).Length / 1MB, 0)
    Escribir-Ok ("  {0} MB  {1}.tar" -f $mb, $Archivo)
    return $true
}

if (-not (Guardar-Imagen $ImagenPropia 'airflow-bsg')) { $fallidas += $ImagenPropia }
foreach ($img in $ImagenesBase) {
    $nombre = $img -replace '[/:]', '_'
    if (-not (Guardar-Imagen $img $nombre)) { $fallidas += $img }
}

# --- 2. Archivos del proyecto ------------------------------------------------
Write-Host ''
Escribir-Info 'Copiando archivos del proyecto'

# Lista explicita y no "copiar todo": evita arrastrar .git, logs, parquet de
# pruebas y -lo importante- un .env con secretos reales de desarrollo.
$rutas = @(
    'docker-compose.ubuntu.yml', 'docker-compose.windows.yml', 'docker-compose.rhel.yml',
    '.env.ubuntu', '.env.windows', '.env.rhel',
    'Dockerfile', 'setup.sh', 'setup.ps1', 'docker-compose.tls.yml', 'nginx',
    'README.md', 'README-TI.md', 'README-TI-REDUCIDO.md', 'README-NGINX.md',
    'README-REQUERIMIENTOS-FUNCIONAMIENTO.md', 'README_DOCKER.md',
    'airflow', 'scripts', 'spark', 'docs'
)
foreach ($r in $rutas) {
    $origen = Join-Path $Raiz $r
    if (Test-Path $origen) {
        Copy-Item $origen -Destination $dirProyecto -Recurse -Force
    } else {
        Escribir-Aviso "  no existe, se omite: $r"
    }
}

# Nunca viaja un .env real: lleva secretos y el destino debe generar los suyos.
Remove-Item (Join-Path $dirProyecto '.env') -Force -ErrorAction SilentlyContinue
Get-ChildItem $dirProyecto -Recurse -Directory -Filter '__pycache__' -ErrorAction SilentlyContinue |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem $dirProyecto -Recurse -Directory -Filter 'logs' -ErrorAction SilentlyContinue |
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Escribir-Ok 'Proyecto copiado (sin .env, sin logs, sin __pycache__)'

# --- 3. Script de carga en el destino ----------------------------------------
# Se escribe con salto de linea LF: el destino es Linux, y un .sh con CRLF
# falla con "bash: $'\r': command not found", que no dice nada util.
$cargador = @'
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
'@
$rutaCargador = Join-Path $Destino 'cargar_bundle.sh'
[System.IO.File]::WriteAllText($rutaCargador, ($cargador -replace "`r`n", "`n"))
Escribir-Ok 'cargar_bundle.sh generado (con saltos de linea LF)'

# --- 4. Manifiesto con sumas de verificacion ---------------------------------
# El traslado suele ser por USB o por un recinto de transferencia. Un archivo
# truncado ahi produce, dias despues, un "docker load" que falla sin explicar
# por que. Las sumas convierten eso en una comprobacion de treinta segundos.
$lineas = @()
$lineas += 'PAQUETE SIN CONEXION - Airflow + Spark'
$lineas += "Generado: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
$lineas += "Origen:   $env:COMPUTERNAME"
$lineas += ''
$lineas += 'IMAGENES'
Get-ChildItem $dirImagenes -Filter '*.tar' | ForEach-Object {
    $h = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLower()
    $lineas += "$h  $($_.Name)"
}
$total = (Get-ChildItem $Destino -Recurse -File | Measure-Object Length -Sum).Sum
$lineas += ''
$lineas += ("TAMANO TOTAL: {0} MB" -f [math]::Round($total / 1MB, 0))
Set-Content -Path (Join-Path $Destino 'MANIFIESTO.txt') -Value $lineas -Encoding ASCII
Escribir-Ok 'MANIFIESTO.txt generado'

# --- 5. Resumen --------------------------------------------------------------
Write-Host ''
Write-Host '==============================================================' -ForegroundColor Cyan
if ($fallidas.Count -gt 0) {
    Escribir-Fallo ("No se pudieron empaquetar: " + ($fallidas -join ', '))
    Write-Host '  El paquete esta INCOMPLETO. Corrijalo antes de trasladarlo.'
    Write-Host '==============================================================' -ForegroundColor Cyan
    exit 1
}
Write-Host '  PAQUETE COMPLETO' -ForegroundColor Green
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host ''
Write-Host "  Carpeta: $Destino"
Write-Host ("  Tamano:  {0} MB" -f [math]::Round($total / 1MB, 0))
Write-Host ''
Write-Host '  En el servidor aislado:  ./cargar_bundle.sh'
Write-Host ''
Write-Host '  Recuerde: los secretos NO viajan. setup.sh genera unos nuevos' -ForegroundColor DarkGray
Write-Host '  en el destino, que es lo correcto para produccion.'            -ForegroundColor DarkGray
Write-Host ''
exit 0
