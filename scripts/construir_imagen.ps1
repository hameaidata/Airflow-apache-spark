# =============================================================================
# CONSTRUIR LA IMAGEN PROPIA DE AIRFLOW  ::  WINDOWS
# -----------------------------------------------------------------------------
# Equivalente exacto de scripts/construir_imagen.sh (Ubuntu / Linux).
#
# La imagen oficial apache/airflow NO trae spark-submit, ni los drivers JDBC,
# ni los clientes de base de datos. Este script construye la imagen que si los
# trae, y deja el .env apuntando a ella.
#
# Uso:
#     .\scripts\construir_imagen.ps1                 construir y activar
#     .\scripts\construir_imagen.ps1 -SinOdbc        omitir el ODBC de Microsoft
#     .\scripts\construir_imagen.ps1 -Forzar         reconstruir aunque ya exista
#     .\scripts\construir_imagen.ps1 -Tag otro:1.0   usar otro nombre de imagen
#     .\scripts\construir_imagen.ps1 -Ayuda
#
# Tarda entre 15 y 25 minutos la primera vez. Despues, con la cache de Docker,
# unos pocos minutos.
# =============================================================================

param(
    [switch]$SinOdbc,
    [switch]$Forzar,
    [string]$Tag = 'airflow-bsg:2.11.2',
    [switch]$Ayuda
)

# 'Continue' y NO 'Stop', a proposito.
#
# Docker escribe avisos por stderr durante una construccion perfectamente
# correcta. Con 'Stop', PowerShell convierte ese aviso en un error terminante y
# aborta el script cuando en realidad no pasaba nada. Ya nos ocurrio con el
# RequestsDependencyWarning en crear_roles.ps1.
$ErrorActionPreference = 'Continue'

function Escribir-Ok    { param($m) Write-Host "[ok] $m"    -ForegroundColor Green }
function Escribir-Aviso { param($m) Write-Host "[aviso] $m" -ForegroundColor Yellow }
function Escribir-Fallo { param($m) Write-Host "[error] $m" -ForegroundColor Red }
function Escribir-Info  { param($m) Write-Host "[..] $m"    -ForegroundColor Cyan }

if ($Ayuda) {
    Get-Content $PSCommandPath | Select-Object -First 19 |
        ForEach-Object { $_ -replace '^#\s?', '' }
    exit 0
}

# --- El script se ubica solo -------------------------------------------------
# Sin esto, ejecutarlo desde scripts\ en vez de la raiz rompe todas las rutas.
$Raiz = Split-Path -Parent $PSScriptRoot
$InstalarOdbc = if ($SinOdbc) { 'false' } else { 'true' }

Write-Host ''
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host '  CONSTRUIR IMAGEN DE AIRFLOW  (Windows)'                       -ForegroundColor Cyan
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host "  Proyecto: $Raiz"
Write-Host "  Imagen:   $Tag"
Write-Host "  ODBC:     $InstalarOdbc"
Write-Host ''

# --- 1. Docker existe y responde ---------------------------------------------
$docker = Get-Command docker -ErrorAction SilentlyContinue
if (-not $docker) {
    Escribir-Fallo "No encuentro el comando 'docker'."
    Write-Host ''
    Write-Host '  Instale Docker Desktop y asegurese de que el backend sea WSL2:'
    Write-Host '    Settings > General > Use the WSL 2 based engine'
    exit 1
}

# 'docker info' es la prueba real: comprueba que el demonio responde, no solo
# que el binario existe. Se descarta la salida y se mira el codigo.
docker info 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Escribir-Fallo 'El comando docker existe, pero el demonio no responde.'
    Write-Host ''
    Write-Host '  Abra Docker Desktop y espere a que el icono deje de girar.'
    exit 1
}
$version = (docker --version)
Escribir-Ok "Docker responde ($version)"

# --- 2. El Dockerfile esta donde debe ----------------------------------------
$dockerfile = Join-Path $Raiz 'Dockerfile'
if (-not (Test-Path $dockerfile)) {
    Escribir-Fallo "No encuentro $dockerfile"
    exit 1
}
Escribir-Ok 'Dockerfile encontrado'

# --- 3. Nota sobre el UID ----------------------------------------------------
# La version de Linux comprueba aqui que AIRFLOW_UID coincida con 'id -u'.
# En Windows esa comprobacion no aplica: Docker Desktop no mapea UIDs de POSIX
# y el .env de Windows fija AIRFLOW_UID=50000 a proposito. Es la unica
# diferencia real de comportamiento entre los dos scripts.
$envFile = Join-Path $Raiz '.env'
if (-not (Test-Path $envFile)) {
    Escribir-Aviso '.env no existe todavia. Ejecute .\setup.ps1 antes de levantar el stack.'
}

# --- 4. La imagen ya existe? -------------------------------------------------
docker image inspect $Tag 2>&1 | Out-Null
$existe = ($LASTEXITCODE -eq 0)

if ($existe) {
    $creada = (docker image inspect $Tag --format '{{.Created}}')
    if (-not $Forzar) {
        Write-Host ''
        Escribir-Aviso "La imagen $Tag ya existe (creada $creada)."
        Write-Host '  Para reconstruirla:  .\scripts\construir_imagen.ps1 -Forzar'
        Write-Host ''
        Write-Host '  Para comprobar sus drivers ahora mismo:' -ForegroundColor Cyan
        Write-Host '    docker compose -f docker-compose.windows.yml exec airflow-webserver python /opt/airflow/verificar_drivers.py'
        Write-Host ''
        exit 0
    }
    Escribir-Info "La imagen existe ($creada); se reconstruye por -Forzar"
}

# --- 5. Construir ------------------------------------------------------------
Write-Host ''
Write-Host '--------------------------------------------------------------' -ForegroundColor Cyan
Write-Host '  Construyendo. Entre 15 y 25 minutos la primera vez.'          -ForegroundColor Cyan
Write-Host '  Descarga Spark (~400 MB), Java 17, 5 drivers JDBC y los'      -ForegroundColor DarkGray
Write-Host '  providers de Airflow. Puede dejarlo corriendo.'               -ForegroundColor DarkGray
Write-Host '--------------------------------------------------------------' -ForegroundColor Cyan
Write-Host ''

$inicio = Get-Date

docker build --build-arg "INSTALAR_ODBC=$InstalarOdbc" -t $Tag $Raiz

$codigo = $LASTEXITCODE
$duracion = (Get-Date) - $inicio

Write-Host ''
if ($codigo -ne 0) {
    Escribir-Fallo ("La construccion fallo (codigo {0}) tras {1:N0} min." -f $codigo, $duracion.TotalMinutes)
    Write-Host ''
    Write-Host '  Los dos fallos mas frecuentes:' -ForegroundColor Yellow
    Write-Host ''
    Write-Host '  1. Se corta en el paso del ODBC de Microsoft.'
    Write-Host '     Descarga de packages.microsoft.com, que muchas redes'
    Write-Host '     corporativas bloquean. Reintente sin el:'
    Write-Host '         .\scripts\construir_imagen.ps1 -SinOdbc'
    Write-Host '     Solo pierde la autenticacion integrada de AD contra SQL Server;'
    Write-Host '     usuario y contrasena siguen funcionando.'
    Write-Host ''
    Write-Host '  2. Se corta descargando un jar de repo1.maven.org.'
    Write-Host '     Las 5 coordenadas estan verificadas, asi que un 404 aqui es'
    Write-Host '     bloqueo de red, no una version mal escrita.'
    Write-Host ''
    exit $codigo
}

# En una sola linea a proposito: el acento grave de continuacion de PowerShell
# es una fuente de errores de sintaxis que ya nos costo tiempo. No se usa.
$min = [math]::Floor($duracion.TotalMinutes)
Escribir-Ok ("Imagen construida en {0:N0} min {1:N0} s" -f $min, $duracion.Seconds)

# --- 6. Activar la imagen en .env --------------------------------------------
# Construir la imagen no sirve de nada si el compose sigue usando la oficial.
# Esto es lo que se olvida y produce el "pero si ya la construi".
if (Test-Path $envFile) {
    Copy-Item $envFile "$envFile.bak" -Force
    $lineas = Get-Content $envFile

    # Ojo con el anclaje: AIRFLOW_IMAGE_TAG NO debe coincidir. Exigir que tras
    # AIRFLOW_IMAGE venga espacio o '=' lo garantiza, porque en _TAG viene '_'.
    $reActiva   = '^\s*AIRFLOW_IMAGE\s*='
    $reComentada = '^\s*#\s*AIRFLOW_IMAGE\s*='

    if ($lineas -match $reActiva) {
        $nuevas = $lineas | ForEach-Object {
            if ($_ -match $reActiva) { "AIRFLOW_IMAGE=$Tag" } else { $_ }
        }
        Set-Content -Path $envFile -Value $nuevas -Encoding ASCII
        Escribir-Ok 'AIRFLOW_IMAGE actualizada en .env'
    }
    elseif ($lineas -match $reComentada) {
        $nuevas = $lineas | ForEach-Object {
            if ($_ -match $reComentada) { "AIRFLOW_IMAGE=$Tag" } else { $_ }
        }
        Set-Content -Path $envFile -Value $nuevas -Encoding ASCII
        Escribir-Ok 'AIRFLOW_IMAGE descomentada en .env'
    }
    else {
        # Add-Content ya anade el salto de linea; no hace falta escaparlo.
        Add-Content -Path $envFile -Value "AIRFLOW_IMAGE=$Tag" -Encoding ASCII
        Escribir-Ok 'AIRFLOW_IMAGE anadida a .env'
    }
    Write-Host '     copia previa en .env.bak' -ForegroundColor DarkGray
}
else {
    Escribir-Aviso 'No hay .env. Ejecute .\setup.ps1 y luego ponga a mano:'
    Write-Host "      AIRFLOW_IMAGE=$Tag"
}

# --- 7. Que sigue ------------------------------------------------------------
Write-Host ''
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host '  LISTO'                                                        -ForegroundColor Green
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host ''
Write-Host '  1. Levantar el stack:'
Write-Host '       docker compose -f docker-compose.windows.yml up -d'
Write-Host ''
Write-Host '  2. Comprobar los drivers (dentro del contenedor, que es donde viven):'
Write-Host '       docker compose -f docker-compose.windows.yml exec airflow-webserver python /opt/airflow/verificar_drivers.py'
Write-Host ''
Write-Host '  3. Aplicar los roles:'
Write-Host '       .\scripts\crear_roles.ps1 -Simular'
Write-Host '       .\scripts\crear_roles.ps1 -Aplicar'
Write-Host ''
exit 0
