# ============================================================================
# setup.ps1  ::  Preparacion del stack en WINDOWS (Docker Desktop + WSL2)
# ============================================================================
# VERSIÓN CORREGIDA: Incluye estructura completa de Spark + SPARK_RPC_SECRET
# ============================================================================

$ErrorActionPreference = "Stop"

Write-Host "=== Preparando stack Airflow + Spark (Windows) - VERSIÓN MEJORADA ===" -ForegroundColor Cyan

# --- Helpers de generacion de secretos --------------------------------------
function New-RandomBytes([int]$n) {
    $bytes = New-Object byte[] $n
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    $rng.Dispose()
    return $bytes
}

function New-FernetKey {
    # Una clave Fernet son 32 bytes aleatorios en base64 url-safe.
    $b64 = [Convert]::ToBase64String((New-RandomBytes 32))
    return $b64.Replace('+', '-').Replace('/', '_')
}

function New-HexKey([int]$bytes) {
    return -join ((New-RandomBytes $bytes) | ForEach-Object { $_.ToString("x2") })
}

function New-Password([int]$len) {
    $chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    $bytes = New-RandomBytes $len
    return -join ($bytes | ForEach-Object { $chars[$_ % $chars.Length] })
}

# --- 1. Verificar Docker ----------------------------------------------------
try {
    docker version --format '{{.Server.Version}}' | Out-Null
} catch {
    Write-Host "ERROR: Docker Desktop no responde." -ForegroundColor Red
    Write-Host "  Abre Docker Desktop y espera a que diga 'Engine running'." -ForegroundColor Yellow
    exit 1
}
Write-Host "[ok] Docker responde"

# --- 2. Verificar backend WSL2 ---------------------------------------------
$info = docker info 2>$null | Out-String
if ($info -notmatch "wsl") {
    Write-Host "AVISO: no se detecto el backend WSL2." -ForegroundColor Yellow
    Write-Host "  Docker Desktop > Settings > General > 'Use the WSL 2 based engine'" -ForegroundColor Yellow
    Write-Host "  Con Hyper-V el rendimiento de los bind mounts es mucho peor." -ForegroundColor Yellow
}

# --- 3. Verificar memoria asignada -----------------------------------------
$memBytes = (docker info --format '{{.MemTotal}}' 2>$null)
if ($memBytes) {
    $memGB = [math]::Round([int64]$memBytes / 1GB, 1)
    Write-Host "[info] Memoria disponible para Docker: $memGB GB"
    if ($memGB -lt 7) {
        Write-Host "AVISO: con menos de 8 GB el stack puede quedarse sin memoria." -ForegroundColor Yellow
        Write-Host "  Crea C:\Users\$env:USERNAME\.wslconfig con:" -ForegroundColor Yellow
        Write-Host "      [wsl2]" -ForegroundColor Gray
        Write-Host "      memory=8GB" -ForegroundColor Gray
        Write-Host "      processors=4" -ForegroundColor Gray
        Write-Host "  Luego: wsl --shutdown  y reinicia Docker Desktop." -ForegroundColor Yellow
    }
}

# --- 4. Advertencia sobre OneDrive -----------------------------------------
if ($PWD.Path -match "OneDrive") {
    Write-Host ""
    Write-Host "AVISO IMPORTANTE: el proyecto esta dentro de OneDrive." -ForegroundColor Yellow
    Write-Host "  1) El archivo .env con contrasenas se SINCRONIZARA a la nube." -ForegroundColor Yellow
    Write-Host "     Para un proyecto bancario eso no deberia pasar." -ForegroundColor Yellow
    Write-Host "  2) OneDrive puede bloquear archivos mientras Docker escribe," -ForegroundColor Yellow
    Write-Host "     provocando errores raros de I/O en los bind mounts." -ForegroundColor Yellow
    Write-Host "  Recomendacion: mueve el proyecto a C:\dev\airflow-spark" -ForegroundColor Yellow
    Write-Host ""
}

# --- 5. Crear estructura de carpetas (MEJORADO: Incluye etl, analytics, transformations) ---
$dirs = @(
    "airflow\dags\production",
    "airflow\dags\examples",
    "airflow\dags\templates",
    "airflow\plugins",
    "airflow\config",
    "spark\jobs\etl",              # ← NUEVO
    "spark\jobs\analytics",        # ← NUEVO
    "spark\jobs\transformations",  # ← NUEVO
    "spark\config",
    "spark\libs"                   # ← NUEVO
)
foreach ($d in $dirs) {
    if (-not (Test-Path $d)) {
        New-Item -ItemType Directory -Path $d -Force | Out-Null
        Write-Host "[ok] creado $d"
    }
}

# --- 6. Generar .env --------------------------------------------------------
if (Test-Path ".env") {
    Write-Host "[skip] .env ya existe, no lo toco" -ForegroundColor Yellow
    $adminUser = (Select-String -Path ".env" -Pattern "^AIRFLOW_ADMIN_USER=(.*)$").Matches.Groups[1].Value
    $adminPass = (Select-String -Path ".env" -Pattern "^AIRFLOW_ADMIN_PASSWORD=(.*)$").Matches.Groups[1].Value
} else {
    $fernet        = New-FernetKey
    $webSecret     = New-HexKey 32
    $pgPass        = New-Password 24
    $redisPass     = New-Password 24
    $sparkAuthPass = New-Password 32  # ← NUEVO: Spark authentication secret

    $adminUser  = "admin"
    $adminPass  = New-Password 16

    $envContent = @"
# ============================================================================
# CONFIGURACION DEL STACK AIRFLOW + SPARK  ::  WINDOWS
# ============================================================================
# Generado por setup.ps1 el $(Get-Date -Format "yyyy-MM-dd HH:mm")
# Los secretos se generaron localmente en esta maquina.
# NUNCA subas este archivo a git (ya esta en .gitignore).
# ============================================================================

COMPOSE_PROJECT_NAME=airflow-spark

# --- Versiones de imagen ----------------------------------------------------
AIRFLOW_IMAGE_TAG=2.11.2-python3.11
# Tras construir tu imagen propia (docker build -t airflow-bsg:2.11.2 .),
# descomenta la linea siguiente. Es lo que trae SQL Server, DB2 y spark-submit.
# AIRFLOW_IMAGE=airflow-bsg:2.11.2
POSTGRES_IMAGE_TAG=16-alpine
REDIS_IMAGE_TAG=7-alpine
SPARK_IMAGE_TAG=3.5.3

# --- Autenticacion del cluster de Spark (DESACTIVADA POR DEFECTO) -----------
# Para activarla, descomenta SPARK_MASTER_OPTS y SPARK_WORKER_OPTS en ambas
# lineas, asegurate de que SPARK_AUTH_SECRET este presente, y anade en el DAG:
#   "spark.authenticate": "true"
#   "spark.authenticate.secret": "<el mismo valor>"
# AVISO: el secreto pasado por -D es visible con 'ps' dentro del contenedor.
# En produccion usa spark-defaults.conf con permisos 600.
SPARK_MASTER_OPTS=
SPARK_WORKER_OPTS=
SPARK_AUTH_SECRET=$sparkAuthPass

# --- Claves de cifrado ------------------------------------------------------
AIRFLOW_FERNET_KEY=$fernet
AIRFLOW_WEBSERVER_SECRET_KEY=$webSecret

# --- PostgreSQL (metastore) -------------------------------------------------
POSTGRES_USER=airflow
POSTGRES_PASSWORD=$pgPass
POSTGRES_DB=airflow
POSTGRES_PORT=5432

# --- Redis (broker de Celery) -----------------------------------------------
REDIS_PASSWORD=$redisPass

# --- Usuario admin inicial de la UI -----------------------------------------
AIRFLOW_ADMIN_USER=$adminUser
AIRFLOW_ADMIN_PASSWORD=$adminPass
AIRFLOW_ADMIN_EMAIL=admin@example.com

# --- Puertos publicados en el host ------------------------------------------
AIRFLOW_WEB_PORT=8080
FLOWER_PORT=5555
SPARK_MASTER_UI_PORT=8082
SPARK_MASTER_PORT=7077

# --- Escalado ---------------------------------------------------------------
WORKER_CONCURRENCY=8
WORKER_QUEUES=default

AIRFLOW_WORKER_REPLICAS=2
SPARK_WORKER_REPLICAS=1

SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2

# --- Auto-discovery de DAGs -------------------------------------------------
DAG_DIR_LIST_INTERVAL=30

# --- Providers extra (opcional) ---------------------------------------------
PIP_ADDITIONAL_REQUIREMENTS=

# --- Especifico de Windows --------------------------------------------------
AIRFLOW_UID=50000
"@

    # UTF8 sin BOM: Docker Compose no tolera el BOM al leer el .env
    [System.IO.File]::WriteAllText(
        (Join-Path $PWD ".env"),
        $envContent,
        (New-Object System.Text.UTF8Encoding $false)
    )
    Write-Host "[ok] .env generado con secretos aleatorios (incluye SPARK_AUTH_SECRET)" -ForegroundColor Green
}

# --- 7. Resumen -------------------------------------------------------------
Write-Host ""
Write-Host "=== Listo. Para arrancar: ===" -ForegroundColor Cyan
Write-Host "  docker compose -f docker-compose.windows.yml up -d" -ForegroundColor White
Write-Host ""
Write-Host "La primera vez tarda 5-10 min (descarga ~3 GB de imagenes)."
Write-Host ""
Write-Host "Ver progreso:"
Write-Host "  docker compose -f docker-compose.windows.yml ps" -ForegroundColor White
Write-Host "  docker compose -f docker-compose.windows.yml logs -f airflow-init" -ForegroundColor White
Write-Host ""
Write-Host "Cuando airflow-webserver este 'healthy':"
Write-Host "  UI Airflow : http://localhost:8080" -ForegroundColor Green
Write-Host "  Flower     : http://localhost:5555" -ForegroundColor Green
Write-Host "  Spark      : http://localhost:8082" -ForegroundColor Green
Write-Host ""
Write-Host "  Usuario: $adminUser"
Write-Host "  Clave  : $adminPass"
Write-Host ""
Write-Host "Escalar workers sin parar nada:"
Write-Host "  docker compose -f docker-compose.windows.yml up -d --scale airflow-worker=5" -ForegroundColor White
Write-Host ""
Write-Host "MEJORAS en esta versión:"
Write-Host "  ✓ Estructura completa de spark/jobs/ (etl, analytics, transformations)"
Write-Host "  ✓ Directorio spark/libs/ para librerías externas"
Write-Host "  ✓ SPARK_AUTH_SECRET generado en .env"
Write-Host "  ✓ Comentarios mejorados sobre autenticacion de Spark"
Write-Host ""
