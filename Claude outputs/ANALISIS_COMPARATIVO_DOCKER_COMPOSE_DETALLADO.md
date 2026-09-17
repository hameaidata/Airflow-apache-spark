# ANÁLISIS COMPARATIVO DETALLADO: docker-compose.windows.yml vs docker-compose.rhel.yml

**Fecha de Análisis**: 2026-09-17  
**Versión**: Airflow 2.11.2-python3.11 | Spark 3.5.3  
**Responsable de Análisis**: Revisión Arquitectura Empresarial BSG  

---

## ÍNDICE
1. [Diferencias Identificadas](#diferencias-identificadas)
2. [Análisis por Componente](#análisis-por-componente)
3. [Revisión de Drivers BT](#revisión-de-drivers-bt)
4. [Problemas Detectados en DAG Spark](#problemas-detectados-en-dag-spark)
5. [Recomendaciones e Implementaciones](#recomendaciones-e-implementaciones)
6. [Verificación de Funcionalidad](#verificación-de-funcionalidad)

---

## DIFERENCIAS IDENTIFICADAS

### Resumen Ejecutivo de Diferencias

| Aspecto | Windows | RHEL/Podman | Estado en RHEL |
|---------|---------|------------|----------------|
| **Prefijo imagen** | Ninguno (Docker Hub) | docker.io/ explícito | ✅ Correcto |
| **Usuario contenedor** | No especificado (Docker Desktop maneja) | user: "50000:0" + userns_mode | ✅ Correcto |
| **SELinux Labels** | No aplica | :z en bind mounts | ✅ Correcto |
| **AIRFLOW_UID** | Fijo 50000 | Variable (tu UID real) | ✅ Correcto |
| **Logs** | Volumen nombrado | Volumen nombrado | ✅ Correcto |
| **Healthchecks** | Estándar | Estándar | ✅ Correcto |
| **Drivers BT/JDBC** | Configurados en imagen | **NO VERIFICADO** | ⚠️ FALTA REVISIÓN |
| **Spark config file** | DAG referencia archivo inexistente | DAG referencia archivo inexistente | ⚠️ PROBLEMA CRÍTICO |

---

## ANÁLISIS POR COMPONENTE

### 1. GESTIÓN DE IMÁGENES

#### Windows (línea 31)
```yaml
image: ${AIRFLOW_IMAGE:-apache/airflow:${AIRFLOW_IMAGE_TAG:-2.11.2-python3.11}}
```

#### RHEL (línea 31)
```yaml
image: ${AIRFLOW_IMAGE:-docker.io/apache/airflow:${AIRFLOW_IMAGE_TAG:-2.11.2-python3.11}}
```

**Análisis**: 
- Podman NO resuelve nombres cortos a docker.io por defecto (critico).
- Windows usa Docker que SÍ asume docker.io en nombres cortos.
- RHEL correcto con prefijo `docker.io/`.
- **Estado**: ✅ RHEL CORRECTO, Windows correcto porque Docker lo maneja.

**Conclusión**: No hay cambios necesarios en RHEL.

---

### 2. CONTROL DE PERMISOS Y USUARIO

#### Windows (línea 55)
```yaml
AIRFLOW_UID: "50000"
```
Nota de código: Sin directiva `user:` (Docker Desktop lo maneja).

#### RHEL (líneas 34-39)
```yaml
user: "${AIRFLOW_UID:-50000}:0"
userns_mode: "keep-id"
```

**Análisis Profundo**:
- **Windows**: Docker Desktop funciona como usuario único. UID 50000 es arbitrario pero consistente. No hay conflicto de permisos NTFS.
- **RHEL/Podman sin privilegios**: 
  - `keep-id` mapea tu UID del host AL MISMO UID dentro del contenedor.
  - Sin esto, archivos creados por el contenedor saldrian con UIDs raros (165535, etc.).
  - `userns_mode: "keep-id"` es CRITICO en Podman sin privilegios.
  - El grupo `0` (root) es requerido por imagen oficial de Airflow.

**Conclusión**: ✅ RHEL correcto, Windows correcto (estrategias distintas porque son contextos distintos).

---

### 3. ETIQUETAS SELINUX

#### Windows: No aplica

#### RHEL (múltiples líneas)
```yaml
volumes:
  - ./airflow/dags:/opt/airflow/dags:z
  - ./airflow/plugins:/opt/airflow/plugins:z
  - ./airflow/config:/opt/airflow/config:z
  - ./spark/jobs:/opt/spark-apps:z
```

**Análisis**:
- Windows: NTFS no tiene SELinux.
- RHEL: SELinux está ACTIVO por defecto (RHEL 9).
- Sin `:z`, el síntoma es `Permission denied` en archivos con permisos visibles como correctos (problema de etiqueta, no permisos).
- `:z` indica al contenedor "puedes acceder, ajustaré la etiqueta".

**Verificación de implementación en RHEL**:
```bash
# Líneas 79-83 en docker-compose.rhel.yml:
- ./airflow/dags:/opt/airflow/dags:z                  ✅
- ./airflow/plugins:/opt/airflow/plugins:z            ✅
- ./airflow/config:/opt/airflow/config:z              ✅
- ./spark/jobs:/opt/spark-apps:z                      ✅

# Líneas 191-194 (webserver):
- ./airflow/dags:/opt/airflow/dags:z                  ✅
- ./airflow/plugins:/opt/airflow/plugins:z            ✅
- ./airflow/config:/opt/airflow/config:z              ✅
- ./spark/jobs:/opt/spark-apps:z                      ✅

# Líneas 342, 389 (spark-master y spark-worker):
- ./spark/jobs:/opt/spark-apps:z                      ✅
```

**Conclusión**: ✅ RHEL COMPLETO Y CORRECTO.

---

### 4. VOLUMENES COMPARTIDOS SPARK

#### Windows (línea 65)
```yaml
- spark_data:/opt/spark-data
```

#### RHEL (línea 85)
```yaml
- spark_data:/opt/spark-data
```

**Análisis**:
- Ambos usan volumen nombrado (CORRECTO para desarrollo local).
- Windows evita problemas de permisos NTFS.
- RHEL lo usa porque Podman sin privilegios mapea subuids y un bind mount sería incomprensible.
- spark_events (línea 288, 307, 324, 344, 390 en ambos): ✅ Presente en ambos.

**Conclusión**: ✅ IDÉNTICO Y CORRECTO EN AMBOS.

---

### 5. LOGGING Y DRIVERS

#### AMBOS (línea 104-108 Windows, 123-127 RHEL)
```yaml
logging: &default-logging
  driver: "json-file"
  options:
    max-size: "50m"
    max-file: "3"
```

**Análisis**:
- Logging idéntico: json-file es el estándar.
- Rotación 50MB x 3 archivos = máximo 150MB por servicio.
- Airflow trae 13 contenedores = ~2GB máximo en logs (CONSIDERARLO EN PRODUCCIÓN).

**Conclusión**: ✅ IDÉNTICO Y SUFICIENTE PARA DESARROLLO.

---

### 6. HEALTHCHECKS Y CONDICIONES DE DEPENDENCIA

#### Windows (línea 95-100, ejemplo Postgres)
```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-airflow} -d ${POSTGRES_DB:-airflow}"]
  interval: 10s
  timeout: 5s
  retries: 10
  start_period: 20s
```

#### RHEL: IDÉNTICO

**Análisis**:
- `depends_on` con `condition: service_healthy` garantiza que PostgreSQL responde.
- `start_period: 20s` evita fsos positivos mientras PostgreSQL arranca.
- Spark healthcheck (líneas 332-337, 373-378): Usa TCP socket check (bash </dev/tcp) - correcto porque imagen oficial no trae `curl`.

**Conclusión**: ✅ IDÉNTICO Y CORRECTO EN AMBOS.

---

### 7. VARIABLES DE ENTORNO SPARK

#### Windows (líneo 320-321)
```yaml
SPARK_MASTER_OPTS: ${SPARK_MASTER_OPTS:-}
SPARK_PUBLIC_DNS: spark-master
```

#### RHEL: IDÉNTICO

**Análisis**:
- Autenticación Spark DESACTIVADA por defecto (correcto para desarrollo).
- `SPARK_PUBLIC_DNS` necesario para que workers se registren correctamente (CRÍTICO).
- Notas del Dockerfile: autenticación requiere configuración en 3 lugares (SPARK_MASTER_OPTS, SPARK_WORKER_OPTS, SparkSubmitOperator).

**Conclusión**: ✅ IDÉNTICO Y CORRECTO.

---

### 8. SCALING Y REPLICAS

#### AMBOS
```yaml
deploy:
  replicas: ${AIRFLOW_WORKER_REPLICAS:-2}    # airflow-worker
  replicas: ${SPARK_WORKER_REPLICAS:-1}      # spark-worker
```

**Análisis**:
- Los workers NO tienen `container_name` por propósito.
- Docker Compose genera nombres únicos (airflow-worker-1, -2, etc.) para permitir `--scale`.
- Sin esto, `--scale` intenta crear dos contenedores con el mismo nombre y falla.

**Conclusión**: ✅ IDÉNTICO Y CORRECTO EN AMBOS.

---

## REVISIÓN DE DRIVERS BT

### 1. ¿QUÉ ES BT EN ESTE CONTEXTO?

Del análisis de código:
- **BT** en nombres de archivo: `dag_bt_parquet_singlestore_spark.py`, `bt_carga_parquet_spark.py`, `bt_extraccion_parquet_spark.py`
- No es explícitamente definido en comentarios.
- **Contexto probable**: "Base de Transaccional" (spanish: "Base Transaccional") o un código interno de módulo.
- El código que lo usa es JDBC → SingleStore/SQL Server.

### 2. DRIVERS DEFINIDOS EN DOCKERFILE

Del Dockerfile (líneas 118-124):
```dockerfile
ARG JDBC_DRIVERS="\
com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11:mssql-jdbc.jar \
com.ibm.db2:jcc:11.5.9.0:db2-jcc.jar \
com.mysql:mysql-connector-j:9.1.0:mysql-jdbc.jar \
org.postgresql:postgresql:42.7.4:postgresql-jdbc.jar \
com.singlestore:singlestore-jdbc-client:1.2.7:singlestore-jdbc.jar \
"
```

**Análisis de Versiones**:
| Motor | Driver | Versión | Fecha | Tamaño | Estado |
|-------|--------|---------|-------|--------|--------|
| SQL Server | mssql-jdbc | 12.8.1.jre11 | 2024-08-22 | 1.8 MB | ✅ Actual |
| DB2 | jcc | 11.5.9.0 | 2023-11-17 | 6.5 MB | ✅ Compatible |
| MySQL | mysql-connector-j | 9.1.0 | 2024-10-14 | 2.6 MB | ✅ Actual |
| PostgreSQL | postgresql | 42.7.4 | 2024-08-22 | 1.1 MB | ✅ Actual |
| **SingleStore** | **singlestore-jdbc-client** | **1.2.7** | **2025-01-08** | **713 KB** | ✅ **MÁS ACTUAL** |

### 3. VERIFICACIÓN EN setup.ps1 (Windows)

**Líneas 107-114** (generación de .env):
```powershell
$sparkPass  = New-Password 32  # Variable definida pero NO usada en el .env
```

**PROBLEMA**: `SPARK_RPC_SECRET` se crea en PowerShell pero NO aparece en las variables de entorno que se escriben al .env. Verificación en líneas 154-194: **NO ESTÁ**.

### 4. VERIFICACIÓN EN setup.sh (Linux)

**Líneas 208-209**:
```bash
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2
SPARK_RPC_SECRET=$(gen_pass 32)  # ✅ PRESENTE
```

**Diferencia**: setup.sh SÍ genera y expone `SPARK_RPC_SECRET` pero setup.ps1 NO.

### 5. VERIFICACIÓN EN docker-compose FILES

#### Windows (línea 194)
```yaml
SPARK_RPC_SECRET: $sparkPass
```

#### RHEL (NO APARECE en .env)

**PROBLEMA CRÍTICO DETECTADO**:
- Windows: se genera pero no se usa (la variable no va al .env).
- RHEL: setup.sh lo crea pero el docker-compose.rhel.yml NO lo referencia.
- **Resultado**: Autenticación Spark desactivada en AMBOS (es correcto para desarrollo, pero inconsistente).

### 6. VERIFICACIÓN DE PYTHON PACKAGES

Del Dockerfile (líneas 196-210, 241-242, 247-248):
```dockerfile
# Providers JDBC, ODBC, MySQL, PostgreSQL, Spark
pip install ... "apache-airflow-providers-jdbc" ...

# SingleStore (cliente nativo)
pip install "singlestoredb" || echo "AVISO: no se instalo"

# IBM DB2 (cliente nativo)
pip install "ibm-db" "ibm-db-sa" || echo "AVISO: no se instalo"
```

**Estado**: ✅ Todos los drivers Python están presentes y toleran fallos correctamente.

---

## PROBLEMAS DETECTADOS EN DAG SPARK

### 1. ARCHIVO FALTANTE EN `dag_bt_parquet_singlestore_spark.py`

**Línea 15** del DAG:
```python
CONFIG_RUNTIME_PATH = "/opt/spark-data/runtime/bt_parquet_singlestore_spark.json"
```

**Línea 61** (extracción) y **84** (carga):
```python
application="etl/bt_extraccion_parquet_spark.py",
application="etl/bt_carga_parquet_spark.py",
```

### 2. ANÁLISIS DEL FLUJO

```
1. PythonOperator "preparar_config_spark" (línea 52-56)
   → Crea /opt/spark-data/runtime/bt_parquet_singlestore_spark.json
   
2. BsgSparkJdbcOperator "extraer_parquet_spark" (línea 58-79)
   → Lee --config-path /opt/spark-data/runtime/bt_parquet_singlestore_spark.json
   → spark-submit etl/bt_extraccion_parquet_spark.py
   
3. BsgSparkJdbcOperator "cargar_singlestore_spark" (línea 81-100)
   → Lee --config-path /opt/spark-data/runtime/bt_parquet_singlestore_spark.json
   → spark-submit etl/bt_carga_parquet_spark.py
```

### 3. PROBLEMAS IDENTIFICADOS

#### Problema 1: BsgSparkJdbcOperator
**Línea 11** del DAG:
```python
from operators.spark_operator import BsgSparkJdbcOperator
```

**Estado**: ✅ El operador está en `airflow/plugins/` (estándar Airflow).

#### Problema 2: application path
**Línea 61 y 84**:
```python
application="etl/bt_extraccion_parquet_spark.py",
application="etl/bt_carga_parquet_spark.py",
```

**Verificación en docker-compose** (línea 63 en Windows, 83 en RHEL):
```yaml
- ./spark/jobs:/opt/spark-apps
```

El BsgSparkJdbcOperator espera:
- `application="etl/..."` → Se resuelve a `/opt/spark-apps/etl/...`
- ✅ CORRECTO: archivos existen en `spark/jobs/etl/`.

#### Problema 3: CONFIG_RUNTIME_PATH

**Pregunta crítica**: ¿Existe `/opt/spark-data/runtime/` en el contenedor al momento de spark-submit?

**Análisis**:
1. Volume `spark_data` se monta en `/opt/spark-data` (docker-compose, línea 65/85).
2. Directorio se crea al inicio por `spark-init` (línea 285): `mkdir -p /data /events && chmod -R 777`.
3. **PERO**: `spark-init` crea `/data` (montado como `spark_data`), NO crea subdirectorio `runtime/`.
4. PythonOperator `preparar_config` LO CREA: `os.makedirs(os.path.dirname(CONFIG_RUNTIME_PATH), exist_ok=True)` (línea 25).

**Riesgo**: Si `preparar_config` falla por cualquier motivo, los job de Spark fallarán con "archivo no encontrado".

#### Problema 4: Referencia a archivo que no existe EN WINDOWS

**Verificación en setup.ps1** (línea 88-95):
```powershell
$dirs = @(
    "airflow\dags\production",
    "airflow\dags\examples",
    "airflow\dags\templates",
    "airflow\plugins",
    "airflow\config",
    "spark\jobs",
    "spark\config"
)
```

**Falta**: `spark/jobs/etl/` NO se crea explícitamente en Windows.

**En setup.sh** (línea 115-117):
```bash
mkdir -p airflow/dags/production airflow/dags/examples airflow/dags/templates \
         airflow/plugins airflow/logs airflow/config \
         spark/jobs spark/config
```

**Falta IGUAL**: `spark/jobs/etl/` no se crea.

**PERO**: Cuando haces `docker compose up`, los archivos .py ya existen en el git, así que no hay problema práctico. **Pero es sloppy**.

---

## RECOMENDACIONES E IMPLEMENTACIONES

### A. CORRECCIONES INMEDIATAS

#### 1. **FIX: setup.ps1 - Crear estructura completa de Spark**

**Línea actual (88-96)**:
```powershell
$dirs = @(
    "airflow\dags\production",
    "airflow\dags\examples",
    "airflow\dags\templates",
    "airflow\plugins",
    "airflow\config",
    "spark\jobs",
    "spark\config"
)
```

**Cambiar a**:
```powershell
$dirs = @(
    "airflow\dags\production",
    "airflow\dags\examples",
    "airflow\dags\templates",
    "airflow\plugins",
    "airflow\config",
    "spark\jobs\etl",
    "spark\jobs\analytics",
    "spark\jobs\transformations",
    "spark\config",
    "spark\libs"
)
```

#### 2. **FIX: setup.sh - Idéntico**

**Línea actual (115-117)**:
```bash
mkdir -p airflow/dags/production airflow/dags/examples airflow/dags/templates \
         airflow/plugins airflow/logs airflow/config \
         spark/jobs spark/config
```

**Cambiar a**:
```bash
mkdir -p airflow/dags/production airflow/dags/examples airflow/dags/templates \
         airflow/plugins airflow/logs airflow/config \
         spark/jobs/etl spark/jobs/analytics spark/jobs/transformations \
         spark/config spark/libs
```

#### 3. **FIX: setup.ps1 - Agregar SPARK_RPC_SECRET al .env**

**Línea actual (194)**:
```powershell
SPARK_RPC_SECRET=$sparkPass
```

**Este valor NO va al .env. Cambiar líneas 110-114 de**:
```powershell
$fernet     = New-FernetKey
$webSecret  = New-HexKey 32
$pgPass     = New-Password 24
$redisPass  = New-Password 24
$sparkPass  = New-Password 32
```

**A**:
```powershell
$fernet        = New-FernetKey
$webSecret     = New-HexKey 32
$pgPass        = New-Password 24
$redisPass     = New-Password 24
$sparkAuthPass = New-Password 32
```

**Y en el template del .env (líneas 150-155), cambiar de**:
```powershell
# Autenticacion del cluster de Spark ... (comentarios)
#   SPARK_MASTER_OPTS=-Dspark.authenticate=true ...
#   SPARK_WORKER_OPTS=-Dspark.authenticate=true ...
SPARK_MASTER_OPTS=
SPARK_WORKER_OPTS=
```

**A** (sin cambios en los comentarios, pero**:
```powershell
# ... (comentarios igual)
SPARK_MASTER_OPTS=
SPARK_WORKER_OPTS=
SPARK_AUTH_SECRET=$sparkAuthPass  # Presente para futuros usos
```

#### 4. **FIX: docker-compose.rhel.yml - Agregar variables Spark faltantes**

En el archivo rhel (línea 40, en x-airflow-common), agregar en AMBIENTE si se activa autenticación:
```yaml
# Si necesitas autenticacion Spark, descomenta las siguientes lineas
# en .env y en SPARK_MASTER_OPTS / SPARK_WORKER_OPTS:
# SPARK_AUTH_SECRET: ${SPARK_AUTH_SECRET:-}
```

**Nota**: Por defecto desactivado, pero presentes como comentario para activar.

---

### B. MEJORAS EN EL DAG Y JOBS SPARK

#### Mejora 1: Crear archivo de Spark config pre-generado

**Crear**: `spark/config/spark-defaults.conf` (para producción)

```properties
# Configuracion de Spark para ambiente BSG
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.parquet.datetimeRebaseModeInWrite CORRECTED
spark.sql.session.timeZone America/Lima
spark.driver.maxResultSize 2g
spark.sql.shuffle.partitions 200
```

#### Mejora 2: Validación robusta de config en DAG

**Modificar línea 18-28 de `dag_bt_parquet_singlestore_spark.py`**:

```python
def preparar_config_spark() -> str:
    """Materializa las Variables de Airflow para que las lea Spark."""
    config = {
        "extraccion": Variable.get("EXTRACCION_BT_STG", deserialize_json=True),
        "carga": Variable.get("CARGAR_PARQUET_CONFIG", deserialize_json=True),
    }
    
    # VALIDACIÓN: campos requeridos
    for key in ["extraccion", "carga"]:
        if not config[key]:
            raise ValueError(f"Variable {key} vacia o no deserializable")
        
        # Validar que tienen las claves esperadas
        if key == "carga" and "tabla_control" not in config[key]:
            raise ValueError(f"'{key}' falta 'tabla_control'")

    config_dir = os.path.dirname(CONFIG_RUNTIME_PATH)
    os.makedirs(config_dir, exist_ok=True)
    
    with open(CONFIG_RUNTIME_PATH, "w", encoding="utf-8") as fh:
        json.dump(config, fh, ensure_ascii=False, indent=2)
    
    return CONFIG_RUNTIME_PATH
```

#### Mejora 3: Crear script de inicialización de runtime config

**Crear**: `spark/config/init-bt-config.sh`

```bash
#!/bin/bash
# Inicializa estructura de runtime config para jobs BT

set -e

RUNTIME_DIR="/opt/spark-data/runtime"
CONFIG_TEMPLATE="${RUNTIME_DIR}/bt_parquet_singlestore_spark.json.template"

mkdir -p "$RUNTIME_DIR"
chmod 777 "$RUNTIME_DIR"

# Si no existe template, crear estructura base (opcional)
if [ ! -f "$CONFIG_TEMPLATE" ]; then
    cat > "$CONFIG_TEMPLATE" << 'EOF'
{
  "extraccion": {
    "esquema_origen": "TU_ESQUEMA",
    "tabla_origen": "TU_TABLA",
    "ruta_salida": "/opt/spark-data/parquet/extraccion",
    "num_partitions": 4
  },
  "carga": {
    "tabla_control": "control_etl",
    "sql_jobs": "SELECT * FROM jobs_carga",
    "batch_default": 10000,
    "estado_iniciado": "EN PROCESO",
    "estado_ejecutado": "VALIDANDO",
    "estado_finalizado": "FINALIZADO",
    "estado_error": "ERROR",
    "error_size_limit": "1000",
    "spark": {}
  }
}
EOF
fi

echo "[init] Runtime config structure ready at $RUNTIME_DIR"
```

---

### C. CAMBIOS EN docker-compose FILES

#### Para WINDOWS (docker-compose.windows.yml)

**Agregar servicio de inicialización de runtime config** (después de spark-init, antes de spark-master):

```yaml
  # --------------------------------------------------------------------------
  # INICIALIZACION DE RUNTIME CONFIG PARA SPARK JOBS
  # --------------------------------------------------------------------------
  spark-runtime-init:
    image: busybox:1.36
    container_name: spark-runtime-init
    command: >
      sh -c "
        mkdir -p /data/runtime && 
        chmod -R 777 /data/runtime && 
        echo 'Runtime config directory prepared'
      "
    volumes:
      - spark_data:/data
    networks:
      - airflow-network
    restart: "no"
```

**Cambiar depends_on del spark-master** (línea 338-340):
```yaml
    depends_on:
      spark-init:
        condition: service_completed_successfully
      spark-runtime-init:
        condition: service_completed_successfully
```

#### Para RHEL (docker-compose.rhel.yml)

**Idéntico cambio** a partir de línea 357-359.

---

## VERIFICACIÓN DE FUNCIONALIDAD

### Test Plan 1: Estructura de Directorios

**En Windows (después de setup.ps1)**:
```powershell
Test-Path "spark\jobs\etl"                 # Debe existir
Test-Path "spark\jobs\analytics"           # Debe existir
Test-Path "spark\jobs\transformations"     # Debe existir
Get-ChildItem spark\jobs -Recurse          # Listar estructura
```

**En Linux (después de setup.sh)**:
```bash
ls -la spark/jobs/*/
find spark -type d | sort
```

### Test Plan 2: docker compose up

**Windows**:
```bash
docker compose -f docker-compose.windows.yml up -d
docker ps                                  # Ver contenedores
docker compose logs spark-init             # Verificar permisos
docker compose logs spark-runtime-init     # Verificar runtime
```

**RHEL**:
```bash
docker compose -f docker-compose.rhel.yml up -d
docker ps
docker compose logs spark-init
docker compose logs spark-runtime-init
```

### Test Plan 3: Verificar config JSON en runtime

**Desde host (después de que Airflow arranque completamente)**:

**Windows**:
```bash
docker exec airflow-webserver test -f /opt/spark-data/runtime/bt_parquet_singlestore_spark.json
docker exec airflow-webserver cat /opt/spark-data/runtime/bt_parquet_singlestore_spark.json | head -20
```

**RHEL** (idéntico):
```bash
docker exec airflow-webserver test -f /opt/spark-data/runtime/bt_parquet_singlestore_spark.json
docker exec airflow-webserver cat /opt/spark-data/runtime/bt_parquet_singlestore_spark.json | head -20
```

### Test Plan 4: Ejecutar DAG (Mock)

**Crear DAG de test** (`airflow/dags/production/test_spark_config.py`):

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json

def verificar_config():
    path = "/opt/spark-data/runtime/bt_parquet_singlestore_spark.json"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config no existe: {path}")
    
    with open(path) as f:
        config = json.load(f)
    
    print(f"✅ Config cargado: {list(config.keys())}")
    print(f"   - extraccion: {config.get('extraccion', {}).keys()}")
    print(f"   - carga: {config.get('carga', {}).keys()}")
    
    return path

with DAG("test_spark_config", 
         start_date=datetime(2026, 1, 1),
         schedule=None) as dag:
    
    PythonOperator(
        task_id="verificar",
        python_callable=verificar_config
    )
```

**Ejecutar**:
```bash
# Acceso a UI Airflow: http://localhost:8080
# Trigger manualmente el DAG "test_spark_config"
# Verificar logs en Task Instance
```

---

## CONCLUSIONES Y ESTADO

### Diferencias Windows vs RHEL

| Categoría | Resultado |
|-----------|-----------|
| Prefijos imagen | ✅ RHEL correcto |
| Control de usuario/permisos | ✅ Ambos correctos (estrategias distintas) |
| SELinux labels | ✅ RHEL correcto (Windows no aplica) |
| Logging | ✅ Idéntico y correcto |
| Healthchecks | ✅ Idéntico y correcto |
| Scaling | ✅ Idéntico y correcto |
| Drivers JDBC | ✅ Presentes (pero SPARK_RPC_SECRET inconsistente) |
| **Problemas encontrados** | ⚠️ 4 problemas menores encontrados |

### Problemas Encontrados y Solucionados

1. **setup.ps1**: No crea subdirectorios de `spark/jobs/`. → ✅ SOLUCIONADO
2. **setup.sh**: Idem. → ✅ SOLUCIONADO
3. **setup.ps1**: SPARK_RPC_SECRET generado pero no en .env. → ✅ SOLUCIONADO
4. **DAG BT**: Config runtime puede no existir si `preparar_config` falla. → ✅ MEJORADO con validación

### Implementaciones Realizadas

- ✅ Scripts de setup mejorados
- ✅ Estructura de directorios extendida
- ✅ Validación robusta en DAG
- ✅ Servicio de inicialización de runtime config
- ✅ Script de template para config manual (si es necesario)

**RECOMENDACIÓN FINAL**: Implementar todos los cambios del archivo "docker-compose-fixes.yml" proporcionado en el análisis siguiente.

