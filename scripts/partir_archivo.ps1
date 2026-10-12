<#
.SYNOPSIS
    Parte un archivo grande en trozos, con suma de verificacion.

.DESCRIPTION
    Para mover un .tar de imagen de Docker cuando no cabe de una pieza: el
    limite de 100 MB por archivo de un repositorio de GitHub, los 2 GB de un
    asset de Release, o un USB.

    POR QUE POWERSHELL PURO Y NO 7-Zip NI split
    -------------------------------------------
    No asume nada instalado. 7-Zip no viene con Windows, y 'split' solo existe
    si hay Git Bash. Esto corre en un Windows recien instalado.

    Y NO usa Compress-Archive, que en PowerShell 5.1 va por
    System.IO.Compression.ZipFile y falla por encima de los 2 GB. Un .tar de
    'docker save' de 2,1 GB entra justo en ese caso.

    LA SUMA DE VERIFICACION NO ES OPCIONAL
    --------------------------------------
    Es el motivo de que este script exista en vez de una linea de comando. Un
    trozo truncado -por un USB lleno, una subida cortada, una copia a medias-
    NO da error al unirlo: 'cat' concatena lo que haya. El fallo aparece
    despues, como un 'docker load' que dice que el archivo esta corrupto, o
    peor, como una imagen que carga y se comporta raro.

    Se escribe un archivo .sha256 junto a los trozos, con la suma del ORIGINAL
    y la de cada trozo. unir_archivo.sh las comprueba antes de dar el archivo
    por bueno.

.PARAMETER Archivo
    El archivo a partir.

.PARAMETER TamanoMB
    Tamano de cada trozo en MB. Por defecto 90, que cabe bajo el limite de
    100 MB por archivo de un repositorio de GitHub. Use 900 para assets de
    Release (limite 2 GB) o para un USB.

.PARAMETER Destino
    Carpeta donde dejar los trozos. Por defecto, la misma del archivo.

.EXAMPLE
    # LO RECOMENDADO: comprimir primero. Un .tar de 'docker save' baja mucho,
    # y con frecuencia el .gz ya cabe entero y no hay que partir nada.
    .\scripts\partir_archivo.ps1 -Archivo .\bundle-offline\imagenes\airflow-bsg_2.11.2.tar -Comprimir -TamanoMB 1800

.EXAMPLE
    .\scripts\partir_archivo.ps1 -Archivo .\bundle-offline\imagenes\airflow-bsg_2.11.2.tar -TamanoMB 900

.EXAMPLE
    # Para meterlos en un repositorio de GitHub (limite 100 MB por archivo)
    .\scripts\partir_archivo.ps1 -Archivo .\imagen.tar.gz -TamanoMB 90
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Archivo,

    [int]$TamanoMB = 90,

    [string]$Destino = '',

    # Comprime antes de partir. Un .tar de 'docker save' baja mucho -son
    # capas con mucho texto y muchos ceros- y con frecuencia el .gz entero
    # cabe donde el .tar no cabia, asi que no hay que partir nada.
    #
    # 'docker load -i imagen.tar.gz' lee el comprimido directamente: no hay
    # que descomprimirlo en el servidor.
    [switch]$Comprimir
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $Archivo -PathType Leaf)) {
    throw "No existe el archivo: $Archivo"
}
if ($TamanoMB -lt 1) { throw "TamanoMB tiene que ser 1 o mas." }

$origen = Get-Item -LiteralPath $Archivo
if ($Destino -eq '') { $Destino = $origen.DirectoryName }

# ---------------------------------------------------------------------------
# COMPRIMIR, SI SE PIDIO
# ---------------------------------------------------------------------------
# Se usa GZipStream de .NET y no Compress-Archive a proposito: Compress-Archive
# va por System.IO.Compression.ZipFile y falla por encima de 2 GB, que es justo
# el caso de un .tar de imagen de Airflow. Y GZip produce un .gz que
# 'docker load' entiende sin descomprimir, mientras que un .zip no.
#
# Se escribe por bloques de 4 MB: leer 2 GB en memoria no es una opcion.
if ($Comprimir) {
    $gz = Join-Path $Destino ($origen.Name + '.gz')
    if (Test-Path -LiteralPath $gz) {
        throw "Ya existe $gz. Borrelo o use -Destino para otra carpeta."
    }

    Write-Host ''
    Write-Host "  comprimiendo $($origen.Name) ..."
    $antes = $origen.Length

    $entrada = [System.IO.File]::OpenRead($origen.FullName)
    $salida  = [System.IO.File]::Create($gz)
    try {
        $gzip = New-Object System.IO.Compression.GZipStream(
            $salida, [System.IO.Compression.CompressionLevel]::Optimal)
        try {
            $buf = New-Object byte[] 4MB
            $hechos = [int64]0
            while (($n = $entrada.Read($buf, 0, $buf.Length)) -gt 0) {
                $gzip.Write($buf, 0, $n)
                $hechos += $n
                Write-Progress -Activity 'Comprimiendo' `
                    -Status ("{0:N2} de {1:N2} GB" -f ($hechos / 1GB), ($antes / 1GB)) `
                    -PercentComplete ([int](100 * $hechos / $antes))
            }
        } finally {
            $gzip.Dispose()
        }
    } finally {
        $entrada.Dispose()
        $salida.Dispose()
    }
    Write-Progress -Activity 'Comprimiendo' -Completed

    $origen  = Get-Item -LiteralPath $gz
    $despues = $origen.Length
    Write-Host ("  {0:N2} GB  ->  {1:N2} GB   ({2:N0}% del original)" -f `
        ($antes / 1GB), ($despues / 1GB), (100 * $despues / $antes)) -ForegroundColor Green

    # Si ya cabe de una pieza, decirlo: partir y rearmar son dos pasos mas
    # donde algo puede salir mal, y si no hacen falta es mejor saltarselos.
    $trozoPedido = [int64]$TamanoMB * 1MB
    if ($despues -le $trozoPedido) {
        Write-Host ''
        Write-Host '  Ya cabe en un solo archivo del tamano pedido: no hace' -ForegroundColor Cyan
        Write-Host '  falta partirlo. Suba o copie directamente' -ForegroundColor Cyan
        Write-Host "      $($origen.Name)" -ForegroundColor Cyan
        Write-Host '  y en el servidor:' -ForegroundColor Cyan
        Write-Host "      docker load -i $($origen.Name)" -ForegroundColor Cyan
        Write-Host ''
        Write-Host ("  SHA256: {0}" -f (Get-FileHash -LiteralPath $origen.FullName -Algorithm SHA256).Hash) -ForegroundColor DarkGray
        return
    }
    Write-Host ''
}
if (-not (Test-Path -LiteralPath $Destino)) {
    New-Item -ItemType Directory -Path $Destino -Force | Out-Null
}

$nombre   = $origen.Name
$trozoMax = [int64]$TamanoMB * 1MB
$total    = $origen.Length
$cuantos  = [math]::Ceiling($total / $trozoMax)

Write-Host ''
Write-Host "  archivo : $nombre"
Write-Host ("  tamano  : {0:N2} GB  ({1:N0} bytes)" -f ($total / 1GB), $total)
Write-Host "  trozos  : $cuantos de $TamanoMB MB"
Write-Host "  destino : $Destino"
Write-Host ''

# Avisos sobre los limites de GitHub, aqui y no en la documentacion: es donde
# se va a leer.
if ($TamanoMB -gt 100) {
    Write-Host '  AVISO: trozos de mas de 100 MB NO se pueden meter en un' -ForegroundColor Yellow
    Write-Host '         repositorio de GitHub. Sirven para un asset de Release' -ForegroundColor Yellow
    Write-Host '         (limite 2 GB) o para un USB.' -ForegroundColor Yellow
    Write-Host ''
}
if ($cuantos -gt 15) {
    Write-Host "  AVISO: van a salir $cuantos archivos. Si el destino es un" -ForegroundColor Yellow
    Write-Host '         repositorio de git, recuerde que git los guarda PARA' -ForegroundColor Yellow
    Write-Host '         SIEMPRE: reconstruir la imagen vuelve a sumar su peso.' -ForegroundColor Yellow
    Write-Host '         Un registro de contenedores (ghcr.io) evita esto.' -ForegroundColor Yellow
    Write-Host ''
}

# La suma del original, ANTES de partir. Es la unica que prueba que lo que se
# reconstruye al otro lado es exactamente esto.
Write-Host '  calculando SHA256 del original (tarda, es 1 pasada sobre el disco)...'
$sumaOriginal = (Get-FileHash -LiteralPath $origen.FullName -Algorithm SHA256).Hash
Write-Host "  $sumaOriginal"
Write-Host ''

# Se lee y escribe por bloques de 4 MB. Leer el archivo entero en memoria con
# [IO.File]::ReadAllBytes() revienta con 2 GB; esto usa 4 MB pase lo que pase.
$bufferTam = 4MB
$buffer    = New-Object byte[] $bufferTam
$lineas    = New-Object System.Collections.Generic.List[string]
$lineas.Add("# Generado por scripts/partir_archivo.ps1")
$lineas.Add("# $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
$lineas.Add("ORIGINAL  $sumaOriginal  $nombre")
$lineas.Add("TROZOS    $cuantos")
$lineas.Add("BYTES     $total")

$lector = [System.IO.File]::OpenRead($origen.FullName)
try {
    for ($i = 1; $i -le $cuantos; $i++) {
        $nombreTrozo = "{0}.part{1:D3}" -f $nombre, $i
        $rutaTrozo   = Join-Path $Destino $nombreTrozo
        $escritos    = [int64]0

        $escritor = [System.IO.File]::Create($rutaTrozo)
        try {
            while ($escritos -lt $trozoMax) {
                $pedir = [math]::Min($bufferTam, $trozoMax - $escritos)
                $leido = $lector.Read($buffer, 0, $pedir)
                if ($leido -le 0) { break }
                $escritor.Write($buffer, 0, $leido)
                $escritos += $leido
            }
        } finally {
            $escritor.Dispose()
        }

        $suma = (Get-FileHash -LiteralPath $rutaTrozo -Algorithm SHA256).Hash
        $lineas.Add("PARTE     $suma  $nombreTrozo")
        Write-Host ("  [{0}/{1}]  {2}  {3:N1} MB" -f $i, $cuantos, $nombreTrozo, ($escritos / 1MB))
    }
} finally {
    $lector.Dispose()
}

$manifiesto = Join-Path $Destino "$nombre.sha256"
# UTF8 sin BOM: el .sh del otro lado lee esto con read, y un BOM delante del
# primer campo rompe la comparacion de forma desconcertante.
[System.IO.File]::WriteAllLines($manifiesto, $lineas,
    (New-Object System.Text.UTF8Encoding $false))

Write-Host ''
Write-Host "  manifiesto: $nombre.sha256" -ForegroundColor Green
Write-Host ''
Write-Host '  Para rearmarlo en el servidor, copie los trozos Y el .sha256, y:' -ForegroundColor Cyan
Write-Host "      ./unir_archivo.sh $nombre" -ForegroundColor Cyan
Write-Host ''
Write-Host '  Sin el .sha256 se puede unir igual con' -ForegroundColor DarkGray
Write-Host "      cat $nombre.part* > $nombre" -ForegroundColor DarkGray
Write-Host '  pero entonces nadie comprueba que no falte un trozo.' -ForegroundColor DarkGray
