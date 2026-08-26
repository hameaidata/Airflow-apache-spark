# =============================================================================
# INSTALAR EL CERTIFICADO QUE DEVOLVIO LA CA DEL BANCO
# -----------------------------------------------------------------------------
# Equivalente exacto de scripts/instalar_certificado.sh (Ubuntu / RHEL).
#
# Uso:
#     .\scripts\instalar_certificado.ps1 -Cert firmado.cer
#     .\scripts\instalar_certificado.ps1 -Cert firmado.cer -Cadena ca.pem
#     .\scripts\instalar_certificado.ps1 -Ayuda
#
# LAS CUATRO COMPROBACIONES, Y POR QUE CADA UNA
#
#   1. FORMATO. Una CA puede devolver PEM, DER o PKCS#7. Nginx solo entiende
#      PEM. Los otros dos se convierten aqui automaticamente.
#
#   2. QUE EL CERTIFICADO CORRESPONDA A LA CLAVE. Es el fallo mas caro: si
#      alguien regenero la clave despues de enviar el CSR, el certificado que
#      vuelve es inutil y hay que repetir el tramite entero.
#
#   3. QUE CONSERVE LOS TRES SAN. Algunas plantillas de AD CS descartan las
#      extensiones del CSR y generan las suyas.
#
#   4. QUE NO ESTE CADUCADO. Una CA interna puede firmar con fechas heredadas
#      de una plantilla vieja.
# =============================================================================

param(
    [string]$Cert,
    [string]$Cadena,
    [switch]$Ayuda
)

$ErrorActionPreference = 'Continue'

function Escribir-Ok    { param($m) Write-Host "[ok] $m"    -ForegroundColor Green }
function Escribir-Aviso { param($m) Write-Host "[aviso] $m" -ForegroundColor Yellow }
function Escribir-Fallo { param($m) Write-Host "[error] $m" -ForegroundColor Red }
function Escribir-Info  { param($m) Write-Host "[..] $m"    -ForegroundColor Cyan }

if ($Ayuda) {
    Get-Content $PSCommandPath | Select-Object -First 25 |
        ForEach-Object { $_ -replace '^#\s?', '' }
    exit 0
}

$Raiz = Split-Path -Parent $PSScriptRoot
$Tls  = Join-Path $Raiz 'tls'

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
    Escribir-Fallo 'No encuentro openssl. Viene con Git para Windows.'
    exit 1
}

if (-not $Cert)            { Escribir-Fallo 'Falta -Cert'; exit 2 }
if (-not (Test-Path $Cert)) { Escribir-Fallo "No existe: $Cert"; exit 1 }

$rutaKey = Join-Path $Tls 'plataforma.key'
if (-not (Test-Path $rutaKey)) {
    Escribir-Fallo "No existe $rutaKey"
    Write-Host '  Genere primero la clave y el CSR:'
    Write-Host '      .\scripts\generar_csr.ps1 -Dominio bsg.banco.local'
    exit 1
}

Write-Host ''
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host '  INSTALAR CERTIFICADO FIRMADO'                                 -ForegroundColor Cyan
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host "  Certificado: $Cert"
Write-Host "  Cadena:      $(if ($Cadena) { $Cadena } else { '(no indicada)' })"
Write-Host ''

$Tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("tls_" + [System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Force -Path $Tmp | Out-Null
try {

$certPem = Join-Path $Tmp 'cert.pem'

# --- 1. Normalizar a PEM -----------------------------------------------------
Escribir-Info 'Detectando formato'
& $OpenSSL x509 -in $Cert -noout 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Copy-Item $Cert $certPem -Force
    Escribir-Ok 'Formato PEM'
} else {
    & $OpenSSL x509 -inform DER -in $Cert -out $certPem 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Escribir-Ok 'Formato DER, convertido a PEM'
    } else {
        # PKCS#7 (.p7b) es lo que suele devolver Microsoft AD CS. Trae el
        # certificado del servidor Y la cadena en un solo archivo.
        $p7 = Join-Path $Tmp 'p7.pem'
        & $OpenSSL pkcs7 -inform DER -in $Cert -print_certs -out $p7 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            & $OpenSSL pkcs7 -in $Cert -print_certs -out $p7 2>&1 | Out-Null
        }
        if ($LASTEXITCODE -eq 0 -and (Test-Path $p7)) {
            Escribir-Ok 'Formato PKCS#7 (tipico de AD CS)'
            $lineas = Get-Content $p7
            $bloques = @(); $actual = @(); $dentro = $false
            foreach ($l in $lineas) {
                if ($l -match 'BEGIN CERTIFICATE') { $dentro = $true; $actual = @() }
                if ($dentro) { $actual += $l }
                if ($l -match 'END CERTIFICATE') { $dentro = $false; $bloques += ,($actual -join "`n") }
            }
            if ($bloques.Count -ge 1) {
                [System.IO.File]::WriteAllText($certPem, $bloques[0] + "`n")
            }
            if ($bloques.Count -gt 1 -and -not $Cadena) {
                $cadenaP7 = Join-Path $Tmp 'cadena.pem'
                [System.IO.File]::WriteAllText($cadenaP7, (($bloques[1..($bloques.Count-1)]) -join "`n") + "`n")
                $Cadena = $cadenaP7
                Escribir-Info 'La cadena venia dentro del PKCS#7; se usara'
            }
        } else {
            Escribir-Fallo "No reconozco el formato de $Cert"
            Write-Host '  Se admiten PEM, DER y PKCS#7. Pida a la CA que lo entregue en PEM.'
            exit 1
        }
    }
}

# --- 2. El certificado tiene que corresponder a la clave ---------------------
# La prueba definitiva: el modulo de la clave publica del certificado y el de
# la clave privada tienen que ser el mismo numero.
Escribir-Info 'Comprobando que el certificado corresponda a la clave privada'
$modCert = (& $OpenSSL x509 -in $certPem -noout -modulus 2>&1 | Out-String).Trim()
$modKey  = (& $OpenSSL rsa  -in $rutaKey -noout -modulus 2>&1 | Out-String).Trim()

if (-not $modCert -or $modCert -ne $modKey) {
    Escribir-Fallo 'EL CERTIFICADO NO CORRESPONDE A ESTA CLAVE PRIVADA.'
    Write-Host ''
    Write-Host '  Casi siempre significa una de dos cosas:'
    Write-Host '    - se regenero la clave despues de enviar el CSR, o'
    Write-Host '    - la CA firmo un CSR distinto al que usted envio.'
    Write-Host ''
    Write-Host '  En ambos casos hay que rehacer el CSR y repetir el tramite. No'
    Write-Host '  hay forma de arreglarlo desde aqui.'
    exit 1
}
Escribir-Ok 'Certificado y clave corresponden'

# --- 3. Los tres SAN ---------------------------------------------------------
Escribir-Info 'Comprobando los nombres alternativos (SAN)'
$sanTexto = (& $OpenSSL x509 -in $certPem -noout -ext subjectAltName 2>&1 | Out-String)
if ($sanTexto -notmatch 'DNS:') {
    Escribir-Fallo 'El certificado NO tiene extension SAN.'
    Write-Host ''
    Write-Host '  Los navegadores modernos validan el nombre contra el SAN, no contra'
    Write-Host '  el CN. Sin SAN, Chrome da ERR_CERT_COMMON_NAME_INVALID aunque el'
    Write-Host '  nombre coincida.'
    Write-Host ''
    Write-Host '  La plantilla de la CA descarto las extensiones del CSR. Hay que'
    Write-Host '  pedir que lo firmen con una plantilla que las conserve.'
    exit 1
}
$sanLimpio = (($sanTexto -split "`n" | Where-Object { $_ -match 'DNS:' }) -join ' ').Trim()
Escribir-Ok "SAN presente: $sanLimpio"

# Comparar contra los nombres que pedimos en el CSR
$rutaCnf = Join-Path $Tls 'plataforma.cnf'
$esperados = @()
if (Test-Path $rutaCnf) {
    $esperados = Get-Content $rutaCnf |
        Where-Object { $_ -match '^DNS\.\d+\s*=\s*(.+)$' } |
        ForEach-Object { $Matches[1].Trim() }
}
$faltan = $esperados | Where-Object { $sanLimpio -notmatch [regex]::Escape("DNS:$_") }
if ($faltan) {
    Escribir-Aviso "Estos nombres del CSR NO estan en el certificado: $($faltan -join ', ')"
    Escribir-Aviso 'Las interfaces de esos nombres daran aviso de certificado.'
} elseif ($esperados) {
    Escribir-Ok 'Los tres nombres del CSR estan en el certificado'
}

# --- 4. Vigencia -------------------------------------------------------------
Escribir-Info 'Comprobando fechas'
& $OpenSSL x509 -in $certPem -noout -checkend 0 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Escribir-Fallo 'EL CERTIFICADO YA ESTA CADUCADO.'
    & $OpenSSL x509 -in $certPem -noout -dates
    exit 1
}
$hasta = ((& $OpenSSL x509 -in $certPem -noout -enddate 2>&1 | Out-String) -split '=')[1].Trim()
$desde = ((& $OpenSSL x509 -in $certPem -noout -startdate 2>&1 | Out-String) -split '=')[1].Trim()
Escribir-Ok "Vigente: $desde  ->  $hasta"
& $OpenSSL x509 -in $certPem -noout -checkend 2592000 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Escribir-Aviso 'Caduca en menos de 30 dias. Empiece ya la renovacion.'
}

# --- 5. Armar el archivo final ----------------------------------------------
# Nginx quiere UN archivo con el certificado del servidor primero y la cadena
# despues, en ese orden. Al reves, los clientes no validan.
$final = Join-Path $Tmp 'final.pem'
if ($Cadena -and (Test-Path $Cadena)) {
    $contenido = (Get-Content $certPem -Raw) + "`n" + (Get-Content $Cadena -Raw)
    [System.IO.File]::WriteAllText($final, ($contenido -replace "`r`n", "`n"))
    $n = ([regex]::Matches((Get-Content $final -Raw), 'BEGIN CERTIFICATE')).Count
    Escribir-Ok "Certificado + cadena: $n certificados en el archivo"
} else {
    [System.IO.File]::WriteAllText($final, ((Get-Content $certPem -Raw) -replace "`r`n", "`n"))
    Escribir-Aviso 'Sin cadena. Los navegadores del banco pueden desconfiar aunque el'
    Escribir-Aviso 'certificado sea correcto. Pida a la CA la raiz y las intermedias.'
}

$rutaCrt = Join-Path $Tls 'plataforma.crt'
if (Test-Path $rutaCrt) {
    Copy-Item $rutaCrt "$rutaCrt.anterior" -Force
    Escribir-Info 'El certificado anterior quedo en plataforma.crt.anterior'
}
Copy-Item $final $rutaCrt -Force
Escribir-Ok "Instalado en $rutaCrt"

Write-Host ''
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host '  CERTIFICADO INSTALADO'                                        -ForegroundColor Green
Write-Host '==============================================================' -ForegroundColor Cyan
Write-Host ''
Write-Host '  Levantar con TLS:'
Write-Host '    docker compose -f docker-compose.windows.yml -f docker-compose.tls.yml up -d'
Write-Host ''
Write-Host '  Comprobar que nginx acepto la configuracion:'
Write-Host '    docker compose -f docker-compose.tls.yml exec nginx nginx -t'
Write-Host ''
foreach ($h in $esperados) { Write-Host "    https://$h" }
Write-Host ''
Write-Host "  Anote la fecha de caducidad: $hasta" -ForegroundColor DarkGray
Write-Host ''

} finally {
    Remove-Item $Tmp -Recurse -Force -ErrorAction SilentlyContinue
}
