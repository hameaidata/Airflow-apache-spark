# =============================================================================
# GENERAR CLAVE PRIVADA Y CSR PARA QUE LO FIRME LA CA DEL BANCO
# -----------------------------------------------------------------------------
# Equivalente exacto de scripts/generar_csr.sh (Ubuntu / RHEL).
#
# Uso:
#     .\scripts\generar_csr.ps1 -Dominio bsg.banco.local
#     .\scripts\generar_csr.ps1 -Dominio bsg.banco.local -Org "Banco XYZ" -Pais PE
#     .\scripts\generar_csr.ps1 -Ayuda
#
# Produce en tls\:
#     plataforma.key   LA CLAVE PRIVADA. Nunca sale de este equipo.
#     plataforma.csr   Esto es lo que se le envia al banco para firmar.
#     plataforma.cnf   La configuracion usada, para poder repetirlo igual.
#
# UN SOLO CERTIFICADO PARA LAS TRES INTERFACES: un certificado con varios SAN
# cubre airflow.<dominio>, spark.<dominio> y flower.<dominio>. Un tramite con
# la CA en vez de tres, y una sola caducidad que vigilar.
#
# EL CN YA NO BASTA: Chrome ignora el Common Name desde la version 58. Un
# certificado sin la extension SAN da ERR_CERT_COMMON_NAME_INVALID aunque el
# nombre coincida. Por eso este script SIEMPRE escribe subjectAltName.
# =============================================================================

param(
    [string]$Dominio,
    [string]$Org    = 'Banco',
    [string]$Unidad = 'Tecnologia',
    [string]$Pais   = 'PE',
    [string]$Estado = 'Lima',
    [string]$Ciudad = 'Lima',
    [int]$Bits      = 2048,
    [switch]$Ayuda
)

# 'Continue' y NO 'Stop': openssl escribe avisos informativos por stderr y con
# 'Stop' PowerShell los convierte en errores terminantes. Ya nos ocurrio antes.
$ErrorActionPreference = 'Continue'

function Escribir-Ok    { param($m) Write-Host "[ok] $m"    -ForegroundColor Green }
function Escribir-Aviso { param($m) Write-Host "[aviso] $m" -ForegroundColor Yellow }
function Escribir-Fallo { param($m) Write-Host "[error] $m" -ForegroundColor Red }
function Escribir-Info  { param($m) Write-Host "[..] $m"    -ForegroundColor Cyan }

if ($Ayuda) {
    Get-Content $PSCommandPath | Select-Object -First 24 |
        ForEach-Object { $_ -replace '^#\s?', '' }
    exit 0
}

$Raiz = Split-Path -Parent $PSScriptRoot
$Tls  = Join-Path $Raiz 'tls'

# --- Localizar openssl -------------------------------------------------------
# Windows no lo trae. Git para Windows si, y casi todo el mundo lo tiene
# instalado; se busca ahi antes de rendirse.
function Buscar-OpenSSL {
    $c = Get-Command openssl -ErrorAction SilentlyContinue
    if ($c) { return $c.Source }
    foreach ($ruta in @(
        "$env:ProgramFiles\Git\usr\bin\openssl.exe",
        "${env:ProgramFiles(x86)}\Git\usr\bin\openssl.exe",
        "$env:LOCALAPPDATA\Programs\Git\usr\bin\openssl.exe")) {
        if (Test-Path $ruta) { return $ruta }
    }
    return $null
}

$OpenSSL = Buscar-OpenSSL
if (-not $OpenSSL) {
    Escribir-Fallo 'No encuentro openssl.'
    Write-Host ''
    Write-Host '  Viene con Git para Windows, que probablemente ya tenga:'
    Write-Host '    https://git-scm.com/download/win'
    Write-Host ''
    Write-Host '  Alternativa sin instalar nada, usando la imagen del proyecto:'
    Write-Host '    docker run --rm -v "${PWD}\tls:/tls" airflow-bsg:2.11.2 openssl version'
    exit 1
}
Escribir-Ok "openssl: $OpenSSL"

if (-not $Dominio) {
    Escribir-Fallo 'Falta -Dominio'
    Write-Host ''
    Write-Host '  Ejemplo:  .\scripts\generar_csr.ps1 -Dominio bsg.banco.local'
    Write-Host ''
    Write-Host '  Es el dominio interno del banco bajo el que colgaran las tres'
    Write-Host '  interfaces. Pidaselo a TI junto con las entradas de DNS.'
    exit 2
}

# 2048 es el minimo que aceptan las CA modernas; por debajo, muchas rechazan.
if ($Bits -lt 2048) {
    Escribir-Fallo "Una clave de $Bits bits sera rechazada. El minimo real es 2048."
    exit 2
}

$HostAirflow = "airflow.$Dominio"
$HostSpark   = "spark.$Dominio"
$HostFlower  = "flower.$Dominio"

New-Item -ItemType Directory -Force -Path $Tls | Out-Null

Write-Host ''
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host '  GENERAR CSR PARA LA CA DEL BANCO'                             -ForegroundColor Cyan
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host "  Dominio base : $Dominio"
Write-Host "  Nombres SAN  : $HostAirflow"
Write-Host "                 $HostSpark"
Write-Host "                 $HostFlower"
Write-Host "  Clave        : RSA $Bits bits"
Write-Host "  Organizacion : $Org / $Unidad"
Write-Host ''

# --- Aviso si ya existe una clave --------------------------------------------
# Regenerar la clave invalida cualquier certificado ya firmado con ella. Si eso
# pasa despues de un tramite de dos semanas con la CA, hay que repetirlo.
$rutaKey = Join-Path $Tls 'plataforma.key'
if (Test-Path $rutaKey) {
    Escribir-Aviso "Ya existe $rutaKey"
    Write-Host ''
    Write-Host '  Si la regenera, CUALQUIER certificado ya firmado con ella deja de'
    Write-Host '  servir y habra que repetir el tramite con la CA.'
    Write-Host ''
    $respuesta = Read-Host '  Escriba REGENERAR para continuar'
    if ($respuesta -ne 'REGENERAR') {
        Write-Host '  Cancelado. No se toco nada.'
        exit 0
    }
    Copy-Item $rutaKey "$rutaKey.anterior" -Force
    Escribir-Aviso 'Copia de la clave anterior en plataforma.key.anterior'
}

# --- Archivo de configuracion de openssl -------------------------------------
# Se deja escrito en disco a proposito: si dentro de un ano hay que renovar, se
# repite exactamente el mismo CSR sin recordar que parametros se usaron.
$cnf = @"
# Generado por scripts\generar_csr.ps1 para el dominio $Dominio
[ req ]
default_bits        = $Bits
prompt              = no
default_md          = sha256
distinguished_name  = dn
req_extensions      = req_ext

[ dn ]
C  = $Pais
ST = $Estado
L  = $Ciudad
O  = $Org
OU = $Unidad
CN = $HostAirflow

[ req_ext ]
# LA EXTENSION QUE DE VERDAD IMPORTA.
# Los navegadores validan el nombre contra esta lista, no contra el CN.
subjectAltName   = @alt_names
keyUsage         = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth

[ alt_names ]
DNS.1 = $HostAirflow
DNS.2 = $HostSpark
DNS.3 = $HostFlower
"@

$rutaCnf = Join-Path $Tls 'plataforma.cnf'
# ASCII y saltos LF: openssl en Windows tolera CRLF, pero el mismo archivo se
# usa desde Linux al renovar y alli un CRLF en una linea de valor da problemas.
[System.IO.File]::WriteAllText($rutaCnf, ($cnf -replace "`r`n", "`n"))
Escribir-Ok 'plataforma.cnf escrito'

# --- Clave privada -----------------------------------------------------------
# SIN contrasena, a proposito. Una clave con passphrase obliga a teclearla en
# cada arranque de nginx, lo que impide que el servidor levante solo tras un
# reinicio. La proteccion aqui son los permisos del archivo.
& $OpenSSL genrsa -out $rutaKey $Bits 2>&1 | Out-Null
if (-not (Test-Path $rutaKey)) {
    Escribir-Fallo 'No se pudo generar la clave'
    exit 1
}

# Equivalente de chmod 600: solo el propietario. Se rompe la herencia y se deja
# una unica ACL para el usuario actual.
$acl = Get-Acl $rutaKey
$acl.SetAccessRuleProtection($true, $false)
$acl.Access | ForEach-Object { $acl.RemoveAccessRule($_) | Out-Null }
$regla = New-Object System.Security.AccessControl.FileSystemAccessRule(
    "$env:USERDOMAIN\$env:USERNAME", 'FullControl', 'Allow')
$acl.SetAccessRule($regla)
Set-Acl -Path $rutaKey -AclObject $acl
Escribir-Ok 'plataforma.key generada (acceso solo para su usuario)'

# --- CSR ---------------------------------------------------------------------
$rutaCsr = Join-Path $Tls 'plataforma.csr'
& $OpenSSL req -new -key $rutaKey -out $rutaCsr -config $rutaCnf 2>&1 | Out-Null
if (-not (Test-Path $rutaCsr)) {
    Escribir-Fallo 'No se pudo generar el CSR'
    exit 1
}
Escribir-Ok 'plataforma.csr generado'

# --- Comprobar que el SAN quedo dentro ---------------------------------------
# Un CSR sin SAN se ve perfectamente normal y produce un certificado
# inservible. Mejor descubrirlo ahora que tras dos semanas de tramite.
Write-Host ''
Escribir-Info 'Comprobando que el CSR lleva la extension SAN'
$texto = & $OpenSSL req -in $rutaCsr -noout -text 2>&1 | Out-String
if ($texto -match 'Subject Alternative Name') {
    $i = ($texto -split "`n" | Select-String 'Subject Alternative Name').LineNumber
    $linea = ($texto -split "`n")[$i]
    Escribir-Ok "SAN presente:"
    Write-Host "       $($linea.Trim())"
} else {
    Escribir-Fallo 'El CSR NO lleva SAN. No lo envie: no serviria.'
    exit 1
}

# --- spark-defaults.conf -----------------------------------------------------
# Spark necesita saber por que URL lo van a ver, o genera enlaces internos que
# el navegador no puede seguir. Se escribe como archivo y no como variable de
# entorno porque spark-defaults.conf lo leen SIEMPRE el master y los workers.
$dirSpark = Join-Path $Raiz 'spark\conf'
New-Item -ItemType Directory -Force -Path $dirSpark | Out-Null
$sparkConf = @"
# Generado por scripts\generar_csr.ps1
#
# Spark detras de un proxy inverso. Estas dos opciones DEBEN estar puestas
# igual en el master y en TODOS los workers, o los enlaces de la interfaz
# apuntan a nombres internos que el navegador no resuelve.
spark.ui.reverseProxy      true
spark.ui.reverseProxyUrl   https://$HostSpark

# Con reverseProxy activo, las interfaces de los workers dejan de ser
# accesibles directamente y solo se llega a ellas A TRAVES del master. Es
# justo lo que queremos: una sola puerta de entrada.
"@
[System.IO.File]::WriteAllText((Join-Path $dirSpark 'spark-defaults.conf'),
                               ($sparkConf -replace "`r`n", "`n"))
Escribir-Ok 'spark\conf\spark-defaults.conf generado'

# --- Que sigue ---------------------------------------------------------------
Write-Host ''
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host '  CSR LISTO PARA ENVIAR'                                        -ForegroundColor Green
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host ''
Write-Host '  1. ENVIE a la CA del banco unicamente este archivo:'
Write-Host "       $rutaCsr"
Write-Host ''
Write-Host '     NUNCA envie plataforma.key. Esa clave no sale de este equipo.' -ForegroundColor Red
Write-Host '     Si alguien se la pide, esta pidiendo algo que no necesita: una CA'
Write-Host '     firma un CSR sin ver jamas la clave privada.'
Write-Host ''
Write-Host '  2. PIDA EXPRESAMENTE que el certificado firmado conserve los tres SAN.'
Write-Host '     Algunas plantillas de AD CS descartan las extensiones del CSR y'
Write-Host '     generan las suyas. Si vuelve sin SAN, no sirve y hay que repetirlo.'
Write-Host ''
Write-Host '  3. PIDA TAMBIEN la cadena de la CA (raiz e intermedias) en formato PEM.'
Write-Host '     Sin ella, los navegadores del banco desconfiaran del certificado'
Write-Host '     aunque sea correcto.'
Write-Host ''
Write-Host '  4. Cuando lo devuelvan:'
Write-Host '       .\scripts\instalar_certificado.ps1 -Cert <archivo> -Cadena <archivo>'
Write-Host ''
Write-Host '  5. Y pida a TI las tres entradas de DNS apuntando a este servidor:'
Write-Host "       $HostAirflow"
Write-Host "       $HostSpark"
Write-Host "       $HostFlower"
Write-Host ''
