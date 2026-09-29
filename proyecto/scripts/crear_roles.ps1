# ============================================================================
# crear_roles.ps1 - Aplica la matriz de roles de Airflow (Windows)
# ----------------------------------------------------------------------------
# Uso (desde cualquier carpeta):
#   .\scripts\crear_roles.ps1              # SIMULA, no cambia nada
#   .\scripts\crear_roles.ps1 -Aplicar     # ejecuta los cambios
#   .\scripts\crear_roles.ps1 -Verificar   # compara con la matriz
#
# NOTA PARA QUIEN EDITE ESTE ARCHIVO:
#   - Solo caracteres ASCII. PowerShell 5.1 lee los .ps1 como ANSI cuando no
#     hay marca BOM, y un acento o una raya larga se convierte en basura.
#   - El texto literal va en comillas SIMPLES. Los signos < > dentro de
#     comillas dobles rompen el analizador de PowerShell.
# ============================================================================

param(
    [switch]$Aplicar,
    [switch]$Verificar
)

# NO se usa 'Stop'.
#
# Con $ErrorActionPreference = 'Stop', PowerShell convierte CUALQUIER salida por
# stderr de un comando nativo en un error terminante. La imagen de Airflow
# imprime un RequestsDependencyWarning inofensivo por stderr, y eso bastaba para
# abortar el script antes de hacer nada.
#
# Con comandos nativos la senal fiable es el codigo de salida ($LASTEXITCODE),
# no lo que aparezca por stderr. Se comprueba explicitamente donde importa.
$ErrorActionPreference = 'Continue'

# La raiz del proyecto es la carpeta padre de scripts\, sin importar desde
# donde se invoque el script.
$Raiz = Split-Path -Parent $PSScriptRoot
Set-Location $Raiz

$Compose = 'docker-compose.windows.yml'

if (-not (Test-Path $Compose)) {
    Write-Host 'ERROR: no encuentro docker-compose.windows.yml' -ForegroundColor Red
    Write-Host "  Buscado en: $Raiz" -ForegroundColor Yellow
    exit 1
}

if (-not (Test-Path 'airflow\config\aplicar_roles.py')) {
    Write-Host 'ERROR: falta airflow\config\aplicar_roles.py' -ForegroundColor Red
    exit 1
}

# Comprobar que el webserver responde.
#
# NO se analiza la salida de 'docker compose ps'. Dos motivos:
#   1. El campo .State del formato no existe en todas las versiones de Compose.
#   2. En PowerShell, -match y -notmatch sobre un ARRAY actuan como filtro y
#      devuelven las lineas, no un booleano. Como siempre hay otros servicios,
#      '$lineas -notmatch X' devuelve algo y el if siempre se cumple.
#
# En su lugar se prueba directamente lo que el script necesita hacer: entrar al
# contenedor y ejecutar airflow. Si eso funciona, todo lo demas funciona.
$prueba = docker compose -f $Compose exec -T airflow-webserver airflow version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host 'ERROR: no puedo ejecutar airflow dentro de airflow-webserver.' -ForegroundColor Red
    Write-Host ''
    Write-Host 'Revise el estado de los contenedores:' -ForegroundColor Yellow
    Write-Host "  docker compose -f $Compose ps" -ForegroundColor White
    Write-Host ''
    Write-Host 'Si el webserver aparece como Created o Restarting, aun esta' -ForegroundColor Yellow
    Write-Host 'arrancando. La primera vez tarda 1-2 minutos. Reintente luego.' -ForegroundColor Yellow
    Write-Host ''
    Write-Host 'Salida del intento:' -ForegroundColor Gray
    $prueba | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
    exit 1
}

if     ($Verificar) { $modo = '--verificar' }
elseif ($Aplicar)   { $modo = '--aplicar' }
else                { $modo = '--simular' }

if ($modo -eq '--simular') {
    Write-Host ''
    Write-Host 'MODO SIMULACION - no se cambiara nada.' -ForegroundColor Yellow
    Write-Host 'Para aplicar de verdad:  .\scripts\crear_roles.ps1 -Aplicar' -ForegroundColor Yellow
    Write-Host ''
}

# 2>&1 une stderr a stdout: los avisos que imprime la imagen de Airflow salen
# como texto normal, en orden, y no como bloques de error de PowerShell.
docker compose -f $Compose exec -T airflow-webserver python /opt/airflow/config/aplicar_roles.py $modo 2>&1
$codigo = $LASTEXITCODE

if ($codigo -eq 0 -and $Aplicar) {
    Write-Host ''
    Write-Host 'Roles creados. Verifique en la interfaz:' -ForegroundColor Green
    Write-Host '  http://localhost:8080  ->  Security  ->  List Roles' -ForegroundColor White
    Write-Host ''
    Write-Host 'Para asignar un rol a una persona:' -ForegroundColor Cyan
    Write-Host '  docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users add-role -u USUARIO -r BSG_IngenieroDatos' -ForegroundColor White
    Write-Host ''
    Write-Host 'Reemplace USUARIO por el nombre real de la cuenta.' -ForegroundColor Gray
}

exit $codigo
