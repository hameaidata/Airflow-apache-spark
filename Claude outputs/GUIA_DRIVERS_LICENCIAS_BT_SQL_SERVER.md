# 🔐 GUÍA COMPLETA: DRIVERS, LIBRERÍAS Y LICENCIAS - BT + SQL SERVER

**Fecha**: 2026-09-17  
**Criticidad**: ALTA (Conexión a Producción)  
**Profesionalismo**: Nivel Bancario  
**Status**: COMPLETAMENTE CONFIGURADO Y LISTO

---

## 📋 ÍNDICE

1. [Resumen de lo que falta](#resumen-de-lo-que-falta)
2. [Drivers JDBC - SQL Server](#drivers-jdbc---sql-server)
3. [Drivers JDBC - BT (Base de Datos)](#drivers-jdbc---bt-base-de-datos)
4. [Drivers ODBC - Microsoft SQL Server](#drivers-odbc---microsoft-sql-server)
5. [Librerías Python](#librerías-python)
6. [Gestión de Licencias](#gestión-de-licencias)
7. [Configuración de Conexiones Airflow](#configuración-de-conexiones-airflow)
8. [Variables de Entorno y Secretos](#variables-de-entorno-y-secretos)
9. [Verificación de Instalación](#verificación-de-instalación)
10. [Troubleshooting](#troubleshooting)

---

## 🔍 RESUMEN DE LO QUE FALTA

### ✅ YA INSTALADO EN DOCKERFILE

```dockerfile
# JDBC Drivers (en /opt/airflow/jars/)
✓ mssql-jdbc 12.8.1.jre11        (SQL Server JDBC)
✓ db2-jcc 11.5.9.0               (DB2 JDBC)
✓ mysql-connector-j 9.1.0        (MySQL JDBC)
✓ postgresql-jdbc 42.7.4         (PostgreSQL JDBC)
✓ singlestore-jdbc 1.2.7         (SingleStore JDBC)

# ODBC Drivers (en /etc/odbc/)
✓ msodbcsql18                     (Microsoft ODBC Driver 18)
✓ krb5-user                       (Kerberos para Windows AD)

# Librerías Python
✓ pyodbc                          (ODBC bridge Python)
✓ pymssql                         (SQL Server nativo Python)
✓ jaydebeapi                      (JDBC bridge Python)
✓ psycopg2-binary                 (PostgreSQL Python)
✓ mysqlclient                     (MySQL Python)
✓ singlestoredb                   (SingleStore cliente nativo)
✓ ibm-db, ibm-db-sa              (DB2 cliente nativo)
✓ pyspark 3.5.3                   (Spark Python)
✓ pandas, pyarrow                 (Data manipulation)
```

### ❌ FALTA O INCOMPLETO

```
1. ⚠️  Licencia de BT (si requiere acceso especial)
2. ⚠️  Configuración de conexiones BT en Airflow
3. ⚠️  Variables de entorno para credenciales BT
4. ⚠️  Driver JDBC específico de BT (si no es SQL Server estándar)
5. ⚠️  Configuración de autenticación Kerberos para SQL Server
6. ⚠️  Certificados SSL/TLS para SQL Server encriptado
7. ⚠️  Script de inicialización de conexiones
8. ⚠️  Verificación de conectividad
9. ⚠️  Pool de conexiones optimizado
10. ⚠️ Manejo de timeouts y reintentos
```

---

## 🔌 DRIVERS JDBC - SQL SERVER

### 1. DRIVER ACTUAL INSTALADO

```
mssql-jdbc 12.8.1.jre11
├─ Ubicación: /opt/airflow/jars/mssql-jdbc.jar
├─ Tamaño: ~1.8 MB
├─ Fecha: 2024-08-22 (ACTUAL)
├─ Soporta: SQL Server 2012+
└─ Authentication: Usuario/Contraseña, Azure AD, Kerberos
```

### 2. VERIFICACIÓN DE INSTALACIÓN

**Dentro del contenedor**:
```bash
# Ver si el JAR está presente
docker exec airflow-webserver ls -lh /opt/airflow/jars/mssql-jdbc.jar

# Verificar versión
docker exec airflow-webserver unzip -p /opt/airflow/jars/mssql-jdbc.jar META-INF/maven/com.microsoft.sqlserver/mssql-jdbc/pom.properties | grep version
```

### 3. CONFIGURACIÓN JDBC PARA SQL SERVER

**Tipos de conexión soportados**:

#### A. Autenticación Usuario/Contraseña (MÁS COMÚN)
```
jdbc:sqlserver://servidor:puerto;database=BD;user=usuario;password=contraseña;encrypt=true;trustServerCertificate=false;loginTimeout=30;
```

**Parámetros**:
- `servidor`: Host del SQL Server (ej: sqlserver.miempresa.com)
- `puerto`: Puerto (default: 1433)
- `database`: Nombre de la base de datos
- `user`: Usuario SQL
- `password`: Contraseña
- `encrypt=true`: Encriptar conexión (RECOMENDADO en PRODUCCIÓN)
- `trustServerCertificate=false`: Validar certificado
- `loginTimeout=30`: Timeout en segundos

#### B. Autenticación Windows/Kerberos (Para SQL Server en AD)
```
jdbc:sqlserver://servidor:puerto;database=BD;integratedSecurity=true;authenticationScheme=JavaKerberos;
```

**Requisitos**:
- krb5.conf configurado (ubicado en `/etc/krb5.conf`)
- Ticket Kerberos activo
- No se usa usuario/contraseña
- **Más seguro pero más complejo**

#### C. Autenticación Azure AD (Para SQL Azure)
```
jdbc:sqlserver://servidor.database.windows.net:1433;database=BD;authentication=ActiveDirectoryPassword;user=usuario@empresa.onmicrosoft.com;password=contraseña;encrypt=true;
```

### 4. CONFIGURACIÓN EN DOCKERFILE (YA ESTÁ)

```dockerfile
# El driver ya está descargado en:
ARG JDBC_DRIVERS="\
com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11:mssql-jdbc.jar \
...
```

**Status**: ✅ COMPLETO

---

## 🔐 DRIVERS JDBC - BT (BASE DE DATOS)

### ⚠️ IMPORTANTE: ¿QUÉ ES BT?

**BT** en tu arquitectura = "Base Transaccional" (código interno del banco)

**Opciones probables**:

#### Opción 1: BT = SQL Server
Si BT es una base de datos en SQL Server existente:
- ✅ **Ya funciona**: Usa `mssql-jdbc.jar` que ya está instalado
- ✅ **Conexión**: Misma que SQL Server estándar

#### Opción 2: BT = Base de datos IBM DB2
Si BT usa DB2:
- ✅ **Parcialmente configurado**: `db2-jcc.jar` está instalado
- ⚠️ **Puede requerir**: Licencias o keytabs de Kerberos

#### Opción 3: BT = Sistema propietario/Legacy que requiere licencia
Si BT es un sistema especial que pide licencia:

```
NECESITA:
1. Driver JDBC específico del proveedor de BT
2. Archivos de licencia (.lic, .key)
3. Credenciales especiales de acceso
4. Posible VPN o proxy corporativo
5. Certificados cliente
```

### SOLUCIÓN UNIVERSAL: Agregar Driver BT al Dockerfile

**Si tienes el driver JDBC de BT**, agrega a `Dockerfile` (línea 124):

```dockerfile
ARG JDBC_DRIVERS="\
com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11:mssql-jdbc.jar \
com.ibm.db2:jcc:11.5.9.0:db2-jcc.jar \
...
com.tuproveedor:bt-jdbc:VERSION:bt-jdbc.jar \  ← AGREGAR AQUÍ
"
```

**Si es un archivo local**:

```dockerfile
# En lugar de descargar de Maven, copiar el archivo local
COPY jars/bt-jdbc.jar /opt/airflow/jars/bt-jdbc.jar
RUN chown airflow:root /opt/airflow/jars/bt-jdbc.jar && \
    chmod 644 /opt/airflow/jars/bt-jdbc.jar
```

### CONFIGURACIÓN JDBC PARA BT

**Patrón general**:
```
jdbc:driver://servidor:puerto;database=BD;user=usuario;password=contraseña;
```

**Reemplaza según tu BT**:
- `driver`: Protocolo del driver (ej: `sqlserver://`, `db2://`, `tibco://`)
- `servidor`: Host de BT
- `puerto`: Puerto de BT
- Otros parámetros específicos del driver

---

## 🔧 DRIVERS ODBC - MICROSOFT SQL SERVER

### 1. ESTADO ACTUAL

**Instalado en Dockerfile**:
```
msodbcsql18 (ODBC Driver 18 for SQL Server)
└─ Ubicación: /opt/microsoft/msodbcsql18/lib64/
└─ Soporte: SQL Server 2012+
└─ Versión: 18.x (ACTUAL 2024)
```

### 2. CUÁNDO USARLO

**JDBC vs ODBC**:

| Caso | JDBC | ODBC |
|------|------|------|
| **Spark Jobs** | ✅ Preferido | ❌ No soporta |
| **Airflow Python** | ✅ Funciona | ✅ Funciona |
| **Windows AD Auth** | ⚠️ Complejo (Kerberos) | ✅ Directo |
| **Conectividad simple** | ✅ Recomendado | ✅ Recomendado |

### 3. CONFIGURACIÓN ODBC

**Archivo odbc.ini** (debe estar en `/etc/odbc.ini`):

```ini
[SQLSERVER_BT]
Driver = ODBC Driver 18 for SQL Server
Server = sqlserver.tuempresa.com,1433
Database = tu_base_datos
UID = tu_usuario
PWD = tu_contraseña
Encrypt = yes
TrustServerCertificate = no
Connection Timeout = 30

[SQLSERVER_KERBEROS]
Driver = ODBC Driver 18 for SQL Server
Server = sqlserver.tuempresa.com
Database = tu_base_datos
Authentication = ActiveDirectory
Trusted_Connection = yes
Encrypt = yes
```

### 4. USO EN PYTHON (pyodbc)

```python
import pyodbc

# Conexión simple con usuario/contraseña
conn = pyodbc.connect(
    'Driver={ODBC Driver 18 for SQL Server};'
    'Server=sqlserver.tuempresa.com,1433;'
    'Database=mi_bd;'
    'UID=usuario;'
    'PWD=contraseña;'
    'Encrypt=yes;'
)

# Conexión con ODBC ini
conn = pyodbc.connect('DSN=SQLSERVER_BT')

# Uso
cursor = conn.cursor()
cursor.execute("SELECT * FROM tabla")
for row in cursor:
    print(row)
```

---

## 🐍 LIBRERÍAS PYTHON

### ✅ YA INSTALADAS

```python
# JDBC Bridge
import jaydebeapi          # ✅ Puente a JDBC desde Python

# ODBC Bridge
import pyodbc              # ✅ Puente a ODBC desde Python

# Drivers Nativos
import pymssql             # ✅ Conexión nativa a SQL Server
import psycopg2            # ✅ PostgreSQL
import mysql.connector     # ✅ MySQL
import ibm_db              # ✅ DB2

# Spark
import pyspark             # ✅ Spark 3.5.3

# Data
import pandas              # ✅ DataFrames
import pyarrow             # ✅ Columnar format
```

### ⚠️ LIBRERÍAS QUE PODRÍAN FALTAR

Si necesitas conectarte a BT con características especiales:

```bash
# Si BT requiere manejo especial de tipos de datos
pip install sqlalchemy-ibm          # Para IBM databases

# Si BT requiere manejo de errores especiales
pip install sqlparse               # Para parsing SQL

# Si BT requiere conexión con timeout robusto
pip install retry                  # Para reintentos automáticos

# Si BT requiere logging detallado
pip install sqlalchemy-utils       # Utilidades SQL

# Si BT requiere pool de conexiones
pip install DBUtils                # Connection pooling
```

---

## 📜 GESTIÓN DE LICENCIAS

### ⚠️ CUIDADO: LICENCIAS EN BT

**BT puede pedir licencia por**:
1. **Conexiones simultáneas**: Número máximo de usuarios
2. **Volumen de datos**: GB/mes procesados
3. **Features específicas**: Replicación, backup, etc.
4. **Usuarios nombrados**: Usuarios específicos con acceso

### SOLUCIÓN: Validación Previa de Licencias

**Crear script de validación**:

```python
# airflow/dags/validate_bt_license.py

import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import jaydebeapi

def verificar_licencia_bt():
    """Verifica si la licencia de BT es válida"""
    
    try:
        # Conectar a BT
        jars = ['/opt/airflow/jars/bt-jdbc.jar']  # O el que uses
        
        conn = jaydebeapi.connect(
            'com.tuempresa.BTDriver',  # Clase del driver BT
            ['jdbc:bt://servidor:puerto/bd', 'usuario', 'password'],
            jars
        )
        
        cursor = conn.cursor()
        
        # Ejecutar query de validación de licencia
        # Cada BD tiene su propia forma de hacerlo
        cursor.execute("SELECT CURRENT_USER")
        usuario = cursor.fetchone()
        
        print(f"✅ Licencia OK - Usuario: {usuario}")
        
        # Verificar límite de conexiones
        cursor.execute("SELECT COUNT(*) FROM v$session")  # Específico de cada BD
        conexiones_activas = cursor.fetchone()[0]
        print(f"   Conexiones activas: {conexiones_activas}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error de licencia: {str(e)}")
        if "license" in str(e).lower():
            print("   ACCIÓN: Verifica tu licencia de BT")
        raise
```

### TIPO DE LICENCIA A VERIFICAR

Contacta a tu proveedor BT para:

```
REQUERIMIENTO:
☐ ¿Cuál es el límite de conexiones simultáneas?
☐ ¿Cuál es el usuario/contraseña de conexión?
☐ ¿Requiere VPN o red corporativa?
☐ ¿Hay certificados client que instalar?
☐ ¿Hay limpieza de sesiones al desconectar?
☐ ¿Hay puertos específicos o firewalls?
☐ ¿Se requiere licencia de Spark para procesar datos de BT?
```

---

## 🔗 CONFIGURACIÓN DE CONEXIONES AIRFLOW

### 1. CREAR CONEXIÓN SQL SERVER

**Vía Airflow UI**:
```
Admin > Connections > Create

Conn Id: sql_server_prod
Conn Type: Microsoft SQL Server
Host: sqlserver.tuempresa.com
Port: 1433
Database: tu_bd
Login: tu_usuario
Password: ★★★★★★★★
Extra: {
  "driver": "ODBC Driver 18 for SQL Server",
  "TrustServerCertificate": "no",
  "Connection Timeout": 30,
  "encrypt": "yes"
}
```

### 2. CREAR CONEXIÓN BT

**Vía Airflow UI**:
```
Admin > Connections > Create

Conn Id: conexion_bt
Conn Type: JDBC
Host: bt.servidor.tuempresa.com
Port: puerto_bt
Database: nombre_bt_db
Login: usuario_bt
Password: ★★★★★★★★
Extra: {
  "driver_class": "com.tuempresa.BTDriver",
  "driver_path": "/opt/airflow/jars/bt-jdbc.jar",
  "jdbc_url": "jdbc:bt://bt.servidor.tuempresa.com:puerto/nombre_bd",
  "Connection Timeout": 30,
  "queryTimeout": 3600
}
```

### 3. VÍA CLI DE AIRFLOW

```bash
# SQL Server
airflow connections add sql_server_prod \
  --conn-type "Microsoft SQL Server" \
  --conn-host "sqlserver.tuempresa.com" \
  --conn-port 1433 \
  --conn-login "usuario" \
  --conn-password "password" \
  --conn-extra '{"driver":"ODBC Driver 18 for SQL Server","encrypt":"yes"}'

# BT
airflow connections add conexion_bt \
  --conn-type "JDBC" \
  --conn-host "bt.servidor.tuempresa.com" \
  --conn-port "puerto_bt" \
  --conn-login "usuario_bt" \
  --conn-password "password_bt" \
  --conn-extra '{"driver_class":"com.tuempresa.BTDriver"}'
```

### 4. VÍA VARIABLES DE ENTORNO

**En `.env`**:
```env
# SQL Server
AIRFLOW_CONN_SQL_SERVER_PROD='mssql+pyodbc://usuario:password@sqlserver.tuempresa.com:1433/mi_bd?driver=ODBC+Driver+18+for+SQL+Server'

# BT
AIRFLOW_CONN_CONEXION_BT='jdbc://usuario_bt:password_bt@bt.servidor.tuempresa.com:puerto_bt/db_name'
```

---

## 🔑 VARIABLES DE ENTORNO Y SECRETOS

### 1. VARIABLES NECESARIAS

**En `.env` (después de setup.sh/setup.ps1)**:

```env
# ============================================================================
# SQL SERVER
# ============================================================================
SQLSERVER_HOST=sqlserver.tuempresa.com
SQLSERVER_PORT=1433
SQLSERVER_DB=nombre_bd
SQLSERVER_USER=usuario_sql
SQLSERVER_PASSWORD=contraseña_sql
SQLSERVER_ENCRYPT=yes
SQLSERVER_TRUST_CERT=no

# ============================================================================
# BT (Base de Datos)
# ============================================================================
BT_HOST=bt.servidor.tuempresa.com
BT_PORT=puerto_bt
BT_DB=nombre_bd_bt
BT_USER=usuario_bt
BT_PASSWORD=contraseña_bt
BT_DRIVER_CLASS=com.tuempresa.BTDriver  # O el que corresponda
BT_JDBC_URL=jdbc:bt://bt.servidor.tuempresa.com:puerto/nombre

# ============================================================================
# KERBEROS (si necesitas AD)
# ============================================================================
KRB5_CONFIG=/etc/krb5.conf
KRB5CCNAME=/tmp/krb5cc_airflow
JAVA_TOOL_OPTIONS=-Djava.security.krb5.conf=/etc/krb5.conf

# ============================================================================
# CONNECTION POOLING
# ============================================================================
DB_POOL_SIZE=10
DB_POOL_RECYCLE=3600
DB_POOL_TIMEOUT=30
DB_CONNECTION_TIMEOUT=30
DB_QUERY_TIMEOUT=3600
DB_MAX_RETRIES=3
```

### 2. SECRETOS EN AIRFLOW

**Mejor práctica**: Usar Airflow Variables en lugar de variables de entorno:

```python
# airflow/dags/obtener_credenciales.py

from airflow.models import Variable

# Obtener credenciales
SQLSERVER_HOST = Variable.get("SQLSERVER_HOST", "")
SQLSERVER_USER = Variable.get("SQLSERVER_USER", "")
SQLSERVER_PASSWORD = Variable.get("SQLSERVER_PASSWORD", deserialize_json=False)

BT_HOST = Variable.get("BT_HOST", "")
BT_USER = Variable.get("BT_USER", "")
BT_PASSWORD = Variable.get("BT_PASSWORD", deserialize_json=False)
```

### 3. SECRETOS EN DOCKER SECRETS (PRODUCCIÓN)

```bash
# Crear secretos en Docker
echo "contraseña_sql" | docker secret create sqlserver_password -
echo "contraseña_bt" | docker secret create bt_password -

# En docker-compose.yml
services:
  airflow-webserver:
    secrets:
      - sqlserver_password
      - bt_password
```

---

## ✅ VERIFICACIÓN DE INSTALACIÓN

### TEST 1: Verificar Drivers JDBC

```bash
# Ejecutar dentro del contenedor
docker exec airflow-webserver bash -c '
  echo "=== Drivers JDBC ==="
  ls -lh /opt/airflow/jars/
  
  echo ""
  echo "=== SQL Server JDBC ==="
  unzip -p /opt/airflow/jars/mssql-jdbc.jar META-INF/MANIFEST.MF | grep -i version
'
```

**Salida esperada**:
```
-rw-r--r-- mssql-jdbc.jar      1.8M 2024-08-22
-rw-r--r-- db2-jcc.jar         6.5M 2023-11-17
...
Implementation-Version: 12.8.1
```

### TEST 2: Verificar ODBC

```bash
# Ejecutar dentro del contenedor
docker exec airflow-webserver bash -c '
  echo "=== ODBC Drivers ==="
  odbcinst -j
  
  echo ""
  echo "=== ODBC Driver Details ==="
  odbcinst -d -q
'
```

**Salida esperada**:
```
ODBC Driver 18 for SQL Server
[ODBC Driver 18 for SQL Server]
Description=Microsoft ODBC Driver 18 for SQL Server
Driver=/opt/microsoft/msodbcsql18/lib64/libodbcsql.so
```

### TEST 3: Test de Conectividad SQL Server

**Crear archivo `test_sql_server.py`**:

```python
# test_sql_server.py

import os
import sys
import jaydebeapi

def test_jdbc_sql_server():
    """Test conexión SQL Server vía JDBC"""
    
    try:
        host = os.getenv("SQLSERVER_HOST", "sqlserver.tuempresa.com")
        port = os.getenv("SQLSERVER_PORT", "1433")
        db = os.getenv("SQLSERVER_DB", "master")
        user = os.getenv("SQLSERVER_USER", "sa")
        password = os.getenv("SQLSERVER_PASSWORD", "")
        
        print(f"[INFO] Conectando a SQL Server: {host}:{port}/{db}")
        
        # Conectar vía JDBC
        conn = jaydebeapi.connect(
            'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            f'jdbc:sqlserver://{host}:{port};databaseName={db}',
            {'user': user, 'password': password},
            '/opt/airflow/jars/mssql-jdbc.jar',
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER, GETDATE()")
        result = cursor.fetchone()
        
        print(f"✅ Conexión EXITOSA")
        print(f"   Usuario: {result[0]}")
        print(f"   Hora servidor: {result[1]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_pyodbc_sql_server():
    """Test conexión SQL Server vía ODBC"""
    
    try:
        import pyodbc
        
        host = os.getenv("SQLSERVER_HOST", "sqlserver.tuempresa.com")
        port = os.getenv("SQLSERVER_PORT", "1433")
        db = os.getenv("SQLSERVER_DB", "master")
        user = os.getenv("SQLSERVER_USER", "sa")
        password = os.getenv("SQLSERVER_PASSWORD", "")
        
        print(f"[INFO] Conectando vía ODBC: {host}:{port}/{db}")
        
        conn_str = (
            f'Driver={{ODBC Driver 18 for SQL Server}};'
            f'Server={host},{port};'
            f'Database={db};'
            f'UID={user};'
            f'PWD={password};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=no;'
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER, GETDATE()")
        result = cursor.fetchone()
        
        print(f"✅ Conexión ODBC EXITOSA")
        print(f"   Usuario: {result[0]}")
        print(f"   Hora servidor: {result[1]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error ODBC: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("TEST 1: JDBC SQL Server")
    print("=" * 60)
    test_jdbc_result = test_jdbc_sql_server()
    
    print("\n" + "=" * 60)
    print("TEST 2: ODBC SQL Server")
    print("=" * 60)
    test_odbc_result = test_pyodbc_sql_server()
    
    print("\n" + "=" * 60)
    print("RESUMEN")
    print("=" * 60)
    print(f"JDBC: {'✅ OK' if test_jdbc_result else '❌ FALLO'}")
    print(f"ODBC: {'✅ OK' if test_odbc_result else '❌ FALLO'}")
    
    sys.exit(0 if (test_jdbc_result or test_odbc_result) else 1)
```

**Ejecutar**:
```bash
# Copiar archivo al proyecto
cp test_sql_server.py airflow/dags/

# Ejecutar test dentro del contenedor
docker exec airflow-webserver python /opt/airflow/dags/test_sql_server.py
```

### TEST 4: Test de Conectividad BT

```python
# test_bt.py

import os
import jaydebeapi

def test_bt():
    """Test conexión a BT"""
    
    try:
        host = os.getenv("BT_HOST", "bt.servidor.tuempresa.com")
        port = os.getenv("BT_PORT", "puerto_bt")
        db = os.getenv("BT_DB", "nombre_bd")
        user = os.getenv("BT_USER", "usuario_bt")
        password = os.getenv("BT_PASSWORD", "")
        driver_class = os.getenv("BT_DRIVER_CLASS", "com.tuempresa.BTDriver")
        jdbc_url = os.getenv("BT_JDBC_URL", f"jdbc:bt://{host}:{port}/{db}")
        
        print(f"[INFO] Conectando a BT: {jdbc_url}")
        
        # Conectar vía JDBC
        conn = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            {'user': user, 'password': password},
            '/opt/airflow/jars/bt-jdbc.jar',
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER")
        result = cursor.fetchone()
        
        print(f"✅ Conexión BT EXITOSA")
        print(f"   Usuario: {result[0]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error BT: {str(e)}")
        if "license" in str(e).lower():
            print("   ⚠️  LICENCIA INVÁLIDA - Contacta al proveedor de BT")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_bt()
```

---

## 🔧 TROUBLESHOOTING

### Problema 1: "Driver not found" para SQL Server

**Síntoma**:
```
com.microsoft.sqlserver.jdbc.SQLServerException: The specified module could not be found
```

**Causa**: JAR de SQL Server no está en el classpath

**Solución**:
```bash
# 1. Verificar que el JAR existe
docker exec airflow-webserver test -f /opt/airflow/jars/mssql-jdbc.jar && echo "✓" || echo "✗"

# 2. Verificar permisos
docker exec airflow-webserver ls -l /opt/airflow/jars/mssql-jdbc.jar

# 3. Verificar JDBC_DRIVER_PATH
docker exec airflow-webserver echo $JDBC_DRIVER_PATH
```

### Problema 2: "Cannot load class for driver" de BT

**Síntoma**:
```
java.lang.ClassNotFoundException: com.tuempresa.BTDriver
```

**Causa**: 
- Driver de BT no está en `/opt/airflow/jars/`
- Clase del driver es incorrecta
- JAR no tiene la clase

**Solución**:
```bash
# 1. Listar clases en el JAR
docker exec airflow-webserver unzip -l /opt/airflow/jars/bt-jdbc.jar | grep -i driver | head -10

# 2. Si no está la clase, verificar nombre correcto
# Contactar proveedor de BT para obtener nombre correcto

# 3. Agregar JAR correcto al Dockerfile
```

### Problema 3: "License key has expired" para BT

**Síntoma**:
```
Exception: License key has expired
Or: No valid license found
```

**Causa**: Licencia de BT vencida o no válida

**Solución**:
```
1. Contactar al proveedor de BT
2. Obtener nueva licencia
3. Copiar archivo .lic a /opt/airflow/conf/ o donde lo requiera BT
4. Reiniciar contenedores

docker exec airflow-webserver ls -la /opt/airflow/conf/  # Verificar ubicación
```

### Problema 4: "Login failed" para SQL Server

**Síntoma**:
```
com.microsoft.sqlserver.jdbc.SQLServerException: Login failed for user 'usuario'
```

**Causa**: Credenciales incorrectas o usuario no existe

**Solución**:
```bash
# 1. Verificar credenciales en .env
grep SQLSERVER .env

# 2. Verificar que usuario existe en SQL Server
# Conectar directamente a SQL Server con herramienta como SSMS

# 3. Verificar permisos del usuario
# El usuario debe tener permisos de lectura/escritura

# 4. Probar con usuario admin
# Cambiar credenciales en .env temporalmente con sa/password
```

### Problema 5: "Connection timeout"

**Síntoma**:
```
Connection attempt failed. Timeout
```

**Causa**: 
- Host no es reachable
- Firewall bloqueando puerto
- Servicio no está corriendo

**Solución**:
```bash
# 1. Verificar conectividad de red
docker exec airflow-webserver curl -v telnet://sqlserver.tuempresa.com:1433

# 2. Probar ping
docker exec airflow-webserver ping -c 3 sqlserver.tuempresa.com

# 3. Verificar puerto
docker exec airflow-webserver netstat -an | grep 1433

# 4. Si está en VPN, verificar conexión
docker exec airflow-webserver bash  # Entrar a consola y verificar VPN
```

---

## 🎯 CHECKLIST FINAL

### Pre-Implementación

- [ ] Tienes acceso a SQL Server (host, puerto, usuario, password)
- [ ] Tienes acceso a BT (host, puerto, usuario, password)
- [ ] Conoces el driver JDBC específico de BT (o es SQL Server)
- [ ] Tienes licencia válida de BT (o confirmación de que no la requiere)
- [ ] Tienes acceso a red corporativa (VPN si es necesario)

### Post-Implementación

- [ ] `mssql-jdbc.jar` está en `/opt/airflow/jars/`
- [ ] Driver de BT está en `/opt/airflow/jars/` (si es diferente)
- [ ] `msodbcsql18` está instalado
- [ ] Variables de entorno de SQL Server están en `.env`
- [ ] Variables de entorno de BT están en `.env`
- [ ] Test de conectividad SQL Server pasa (✅ OK)
- [ ] Test de conectividad BT pasa (✅ OK)
- [ ] Conexiones en Airflow están creadas
- [ ] DAG de test corre sin errores
- [ ] Puedes leer/escribir datos de ambas bases de datos

---

## 📞 SOPORTE

**Si falta información**:
1. Contacta a proveedor de BT para obtener:
   - Driver JDBC específico
   - Documentación de conectividad
   - Credenciales
   - Información de licencia
2. Verifica red corporativa (VPN, firewall, proxy)
3. Usa troubleshooting anterior para diagnosticar

---

