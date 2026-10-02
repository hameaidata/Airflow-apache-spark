# ============================================================================
# setup.ps1  ::  Preparacion del stack en WINDOWS (Docker Desktop + WSL2)
# ----------------------------------------------------------------------------
# Uso:  Abre PowerShell EN LA CARPETA DEL PROYECTO y ejecuta:
#
#     Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#     .\setup.ps1
#
# Genera el archivo .env con secretos aleatorios creados EN TU MAQUINA.
# Ningun secreto viaja por la red ni pasa por herramientas remotas.
# ============================================================================

$ErrorActionPreference = "Stop"

Write-Host "=== Preparando stack Airflow + Spark (Windows) ===" -ForegroundColor Cyan

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

# --- 5. Crear estructura de carpetas ---------------------------------------
$dirs = @(
    "airflow\dags\production",
    "airflow\dags\examples",
    "airflow\dags\templates",
    "airflow\plugins",
    "airflow\config",
    "spark\jobs",
    "spark\config"
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
    $fernet     = New-FernetKey
    $webSecret  = New-HexKey 32
    $pgPass     = New-Password 24
    $redisPass  = New-Password 24
    $sparkPass  = New-Password 32
    $adminUser  = "admin"
    $adminPass  = New-Password 16

    $envContent = @"
# ============================================================================
# CONFIGURACION DEL STACK AIRFLOW + SPARK  ::  WINDOWS
# ----------------------------------------------------------------------------
# Generado por setup.ps1 el $(Get-Date -Format "yyyy-MM-dd HH:mm")
# Los secretos se generaron localmente en esta maquina.
# NUNCA subas este archivo a git (ya esta en .gitignore).
# ============================================================================

COMPOSE_PROJECT_NAME=airflow-spark

# --- Versiones de imagen ----------------------------------------------------
# LAS DOS IMAGENES PROPIAS SON OBLIGATORIAS, no opcionales.
#
# Antes venian comentadas, con un "descomenta si construiste tu imagen". Ya no:
# el stack DEPENDE de ellas, y dejarlas comentadas es como se llega a un
# "No suitable driver" que manda a buscar el problema donde no esta.
#
#   airflow-bsg  drivers JDBC (jt400 para Bantotal, mssql-jdbc, singlestore,
#                postgres, mysql, db2), el ODBC de IBM i, Java 8 junto a
#                Java 17, y spark-submit.
#   spark-bsg    Python -que la imagen oficial de Apache NO trae- y los mismos
#                drivers en `$SPARK_HOME/jars, que es la unica ruta que entra
#                en el classpath del driver Y de los executors.
#
# Se construyen con:
#   docker build -t airflow-bsg:2.11.2 .
#   docker build -t spark-bsg:3.5.3 -f spark/Dockerfile .
AIRFLOW_IMAGE_TAG=2.11.2-python3.11
AIRFLOW_IMAGE=airflow-bsg:2.11.2
SPARK_IMAGE_TAG=3.5.3
SPARK_IMAGE=spark-bsg:3.5.3
POSTGRES_IMAGE_TAG=16-alpine
REDIS_IMAGE_TAG=7-alpine

# --- Autenticacion del cluster de Spark -------------------------------------
# DESACTIVADA por defecto para que el cluster arranque sin friccion.
# Para activarla hay que ponerla en LOS TRES SITIOS o las tareas fallaran:
#   1) SPARK_MASTER_OPTS y SPARK_WORKER_OPTS aqui abajo
#   2) el conf del SparkSubmitOperator en el DAG:
#        "spark.authenticate": "true"
#        "spark.authenticate.secret": "<el mismo valor>"
# Aviso: el secreto pasado por -D es visible con `ps` dentro del contenedor.
# Lo correcto en produccion es un spark-defaults.conf con permisos 600.
#   SPARK_MASTER_OPTS=-Dspark.authenticate=true -Dspark.authenticate.secret=EL_SECRETO
#   SPARK_WORKER_OPTS=-Dspark.authenticate=true -Dspark.authenticate.secret=EL_SECRETO
SPARK_MASTER_OPTS=
SPARK_WORKER_OPTS=

# --- Claves de cifrado ------------------------------------------------------
# AIRFLOW_FERNET_KEY cifra Connections y Variables en la base de datos.
# Si la pierdes o la cambias, TODAS las credenciales guardadas quedan ilegibles.
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
# History Server: donde se ven los jobs Spark YA TERMINADOS. Sin el, cuando un
# job falla su interfaz muere con el job y no queda nada que mirar.
SPARK_HISTORY_PORT=18080

# ============================================================================
# CARPETAS DE PARQUET
# ----------------------------------------------------------------------------
# Cada pipeline escribe en la suya. El lado IZQUIERDO es la ruta en esta
# maquina; el DERECHO es la ruta dentro del contenedor y NO se debe cambiar:
# es la que guardan las tablas de control y la que abre despues la carga.
#
# Si cambia el lado derecho, tiene que cambiar tambien output_dir en la
# Variable de Airflow correspondiente, o la extraccion escribira en una ruta
# que no esta montada. Eso NO da error: Docker crea la carpeta DENTRO del
# contenedor, la corrida sale verde, y los archivos desaparecen al reiniciar.
# ============================================================================


#  LAS DOS VERSIONES ESTAN ABAJO, Y SOLO UNA PUEDE ESTAR ACTIVA.
#
#  ANTES de 'docker compose up', descomente el bloque del sistema donde va a
#  levantar y COMENTE el otro. No es cosmetico: si las dos quedan sin comentar,
#  docker compose NO avisa -se queda con la ULTIMA que lee- y usted creera que
#  esta escribiendo en una ruta cuando escribe en la otra.
#
#  El lado CONTENEDOR (/data/...) es el mismo en los dos sistemas y no se toca:
#  es la ruta que guardan las tablas de control, la que lleva output_dir en las
#  Variables de Airflow, y la que abre despues la tarea de carga.
#
#  El lado HOST es el unico que cambia, y es donde se quedan los archivos de
#  verdad: fuera del contenedor, para que el disco del contenedor no crezca y
#  para que los parquet sobrevivan a un 'docker compose down'.
#
#  En Windows la ruta por defecto es C:/datahub y NO ./data, a proposito: el
#  repositorio vive dentro de OneDrive, y OneDrive no lee el .gitignore. Unos
#  parquet de prueba dentro de OneDrive se empiezan a subir a la nube solos.

# Lado contenedor: IGUAL en Windows y en Red Hat. No lo cambie.
PARQUET_CONTAINER_DIR=/data/parquet
S2SQL_PARQUET_CONTAINER_DIR=/data/s2sql
BT2SQL_PARQUET_CONTAINER_DIR=/data/bt2sql

# --- WINDOWS -- ACTIVO ------------------------------------------------------
PARQUET_HOST_DIR=C:/datahub/parquet
S2SQL_PARQUET_HOST_DIR=C:/datahub/s2sql
BT2SQL_PARQUET_HOST_DIR=C:/datahub/bt2sql

# --- RED HAT -- comentado. Descomente estas tres y comente las tres de arriba.
#PARQUET_HOST_DIR=/datos/datahub/parquet
#S2SQL_PARQUET_HOST_DIR=/datos/datahub/s2sql
#BT2SQL_PARQUET_HOST_DIR=/datos/datahub/bt2sql

# --- Pools de Airflow -------------------------------------------------------
# Limitan cuantas tareas golpean cada motor a la vez. Por defecto default_pool
# para que todo funcione recien instalado.
#
# Cuando cree los pools en Admin > Pools, ponga aqui sus nombres. El compose ya
# los pasa a los contenedores; antes no lo hacia y cambiarlos aqui no tenia
# ningun efecto, sin dar tampoco ningun error.
AIRFLOW_POOL_SINGLESTORE=default_pool
AIRFLOW_POOL_SQLSERVER=default_pool

# --- Escalado ---------------------------------------------------------------
# Capacidad total = replicas x WORKER_CONCURRENCY
WORKER_CONCURRENCY=8
WORKER_QUEUES=default

# Replicas al arrancar. En caliente: up -d --scale airflow-worker=N
AIRFLOW_WORKER_REPLICAS=2
SPARK_WORKER_REPLICAS=1

SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2
SPARK_RPC_SECRET=$sparkPass

# --- Auto-discovery de DAGs -------------------------------------------------
# Segundos entre escaneos de dags/. Bajalo a 10 solo en desarrollo.
DAG_DIR_LIST_INTERVAL=30

# --- Providers extra (opcional) ---------------------------------------------
# Se instalan con pip AL ARRANCAR cada contenedor: lento y fragil.
# Para produccion, construye una imagen propia.
# PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-apache-spark==4.11.3
PIP_ADDITIONAL_REQUIREMENTS=

# --- Especifico de Windows --------------------------------------------------
AIRFLOW_UID=50000

# ============================================================================
# TLS CON NGINX  (opcional, docker-compose.tls.yml)
# ----------------------------------------------------------------------------
# Descomente el bloque entero si va a levantar el proxy TLS por delante.
# nginx 1.27 y no una version anterior: "http2 on" necesita >= 1.25.1.
# ============================================================================
# NGINX_IMAGE=nginx:1.27-alpine
# HTTP_PORT=80
# HTTPS_PORT=443
# DOMINIO_AIRFLOW=airflow.suempresa.local
# DOMINIO_FLOWER=flower.suempresa.local
# DOMINIO_SPARK=spark.suempresa.local
"@

    # UTF8 sin BOM: Docker Compose no tolera el BOM al leer el .env
    [System.IO.File]::WriteAllText(
        (Join-Path $PWD ".env"),
        $envContent,
        (New-Object System.Text.UTF8Encoding $false)
    )
    Write-Host "[ok] .env generado con secretos aleatorios" -ForegroundColor Green
}

# --- 6b. Las carpetas de parquet deben existir ANTES de levantar ------------
# Si no existen, Docker Desktop las crea al montar el bind mount, pero conviene
# crearlas aqui: asi quedan con el propietario del usuario y no con el que
# invente el motor, y ademas se ve en el momento si la ruta del .env apunta a
# donde uno cree.
# Las carpetas van FUERA del repositorio: C:\datahub. Si estuvieran dentro,
# en esta maquina caerian dentro de OneDrive, que sincroniza a la nube todo lo
# que encuentre sin mirar el .gitignore.
foreach ($d in @("C:\datahub\parquet", "C:\datahub\s2sql", "C:\datahub\bt2sql")) {
    if (-not (Test-Path $d)) {
        New-Item -ItemType Directory -Force -Path $d | Out-Null
        Write-Host "[ok] carpeta creada: $d"
    }
}

if ($PSScriptRoot -match 'OneDrive') {
    Write-Host ""
    Write-Host "[aviso] Este repositorio esta dentro de OneDrive." -ForegroundColor Yellow
    Write-Host "  Por eso los parquet se configuran en C:\datahub y no en .\data." -ForegroundColor Yellow
    Write-Host "  Si alguna vez apunta PARQUET_HOST_DIR dentro del repositorio," -ForegroundColor Yellow
    Write-Host "  OneDrive empezara a subir esos archivos a la nube: .gitignore" -ForegroundColor Yellow
    Write-Host "  no lo impide, porque OneDrive no lo lee." -ForegroundColor Yellow
}

# --- 6c. Comprobar que las imagenes propias existen -------------------------
# Esta comprobacion existe porque el fallo contrario es silencioso y caro: el
# .env nombra airflow-bsg y spark-bsg, y si no estan construidas el compose
# falla con "image not found" sin decir que hay que construirlas.
# Se usa 'docker images -q' y NO 'docker image inspect'.
#
# inspect sobre una imagen ausente escribe por stderr y sale con codigo 1.
# Redirigirlo con 2>&1 mete ese texto en el flujo de exito, PowerShell lo
# convierte en NativeCommandError y, con $ErrorActionPreference = "Stop",
# aborta el script entero. El sintoma era:
#     docker : Error response from daemon: No such image: airflow-bsg:2.11.2
#     ... NotSpecified: (...) [], RemoteException
# es decir, el script moria justo en la comprobacion que existe para avisar
# con calma de que falta construir la imagen.
#
# 'docker images -q' no escribe nada por stderr cuando la imagen no esta:
# sale con codigo 0 y devuelve una cadena vacia.
$faltan = @()
foreach ($img in @("airflow-bsg:2.11.2", "spark-bsg:3.5.3")) {
    $id = (docker images -q $img)
    if ([string]::IsNullOrWhiteSpace($id)) { $faltan += $img }
}
if ($faltan.Count -gt 0) {
    Write-Host "[aviso] Faltan imagenes que el .env ya referencia: $($faltan -join ', ')" -ForegroundColor Yellow
    Write-Host "  Construyalas antes de levantar el stack:" -ForegroundColor Yellow
    Write-Host "    docker build -t airflow-bsg:2.11.2 ."
    Write-Host "    docker build -t spark-bsg:3.5.3 -f spark/Dockerfile ."
    Write-Host ""
    Write-Host "  La de Spark es la que mas se olvida, y su ausencia no se nota" -ForegroundColor DarkGray
    Write-Host "  al arrancar sino dentro de un job, como 'No suitable driver'." -ForegroundColor DarkGray
} else {
    Write-Host "[ok] las dos imagenes propias estan construidas" -ForegroundColor Green
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
