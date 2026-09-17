# GUÍA DE IMPLEMENTACIÓN DE FIXES

**Fecha**: 2026-09-17  
**Status**: LISTO PARA IMPLEMENTAR  
**Criticidad**: MEDIA (Recomendado antes de producción)

---

## RESUMEN DE CAMBIOS

Este paquete contiene 4 archivos corregidos que deben reemplazar los originales:

| Archivo Original | Archivo Corregido | Cambios Principales |
|------------------|-------------------|-------------------|
| `setup.ps1` | `setup-FIXED.ps1` | + Estructura completa spark/jobs/, SPARK_AUTH_SECRET |
| `setup.sh` | `setup-FIXED.sh` | + Estructura completa spark/jobs/, SPARK_AUTH_SECRET |
| `dag_bt_parquet_singlestore_spark.py` | `dag_bt_parquet_singlestore_spark_FIXED.py` | + Validaciones robustas, documentación |
| (ninguno) | `ANALISIS_COMPARATIVO_DOCKER_COMPOSE_DETALLADO.md` | Referencia: análisis completo |

---

## PASO A PASO DE IMPLEMENTACIÓN

### FASE 1: PREPARACIÓN (15 minutos)

#### 1.1 Hacer backup de archivos actuales
```bash
# En Windows (PowerShell)
Copy-Item setup.ps1 setup.ps1.backup
Copy-Item airflow/dags/production/dag_bt_parquet_singlestore_spark.py dag_bt_parquet_singlestore_spark.py.backup

# En Linux
cp setup.sh setup.sh.backup
cp airflow/dags/production/dag_bt_parquet_singlestore_spark.py dag_bt_parquet_singlestore_spark.py.backup
```

#### 1.2 Copiar archivos corregidos
```bash
# Windows
Copy-Item setup-FIXED.ps1 setup.ps1
Copy-Item dag_bt_parquet_singlestore_spark_FIXED.py airflow/dags/production/dag_bt_parquet_singlestore_spark.py

# Linux
cp setup-FIXED.sh setup.sh
cp dag_bt_parquet_singlestore_spark_FIXED.py airflow/dags/production/dag_bt_parquet_singlestore_spark.py
chmod +x setup.sh
```

#### 1.3 Verificar permisos
```bash
# Linux
ls -la setup.sh | grep x  # Debe tener permisos de ejecución
```

---

### FASE 2: INICIALIZACIÓN LIMPIA (30-45 minutos)

#### 2.1 Si ya tienes .env antiguo, decida:

**Opción A: Regenerar completo (RECOMENDADO si es desarrollo)**
```bash
# Windows
Remove-Item .env
.\setup.ps1

# Linux
rm .env
./setup.sh --rhel  # o --ubuntu
```

**Opción B: Mantener .env pero agregar SPARK_AUTH_SECRET**

En Windows (PowerShell):
```powershell
$secretoPasado = "tu_secreto_aqui_si_tienes_uno"
Add-Content .env "`nSPARK_AUTH_SECRET=$secretoPasado"
```

En Linux:
```bash
# Si SPARK_AUTH_SECRET no existe en .env:
grep -q "SPARK_AUTH_SECRET" .env || echo "SPARK_AUTH_SECRET=$(openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 24)" >> .env
```

#### 2.2 Verificar estructura de directorios
```bash
# Windows (PowerShell)
@('airflow\dags\production', 'airflow\dags\examples', 'airflow\dags\templates', 
  'airflow\plugins', 'airflow\config', 'spark\jobs\etl', 'spark\jobs\analytics', 
  'spark\jobs\transformations', 'spark\config', 'spark\libs') | 
  ForEach-Object { if (-not (Test-Path $_)) { New-Item -ItemType Directory $_ } }

# Linux
mkdir -p airflow/dags/production airflow/dags/examples airflow/dags/templates \
         airflow/plugins airflow/logs airflow/config \
         spark/jobs/etl spark/jobs/analytics spark/jobs/transformations \
         spark/config spark/libs
```

#### 2.3 Verificar .env contiene SPARK_AUTH_SECRET
```bash
# Windows (PowerShell)
Select-String "SPARK_AUTH_SECRET" .env

# Linux
grep SPARK_AUTH_SECRET .env
```

**Si no aparece, agregalo manualmente** (ver Opción B arriba).

---

### FASE 3: DETENER STACK ANTERIOR (5 minutos)

```bash
# Windows
docker compose -f docker-compose.windows.yml down
docker compose -f docker-compose.windows.yml volume rm spark_data airflow_logs postgres_data redis_data spark_events
# O si prefieres mantener datos:
docker compose -f docker-compose.windows.yml down

# Linux/RHEL
docker compose -f docker-compose.rhel.yml down
docker compose -f docker-compose.rhel.yml volume rm spark_data airflow_logs postgres_data redis_data spark_events
# O si prefieres mantener datos:
docker compose -f docker-compose.rhel.yml down
```

---

### FASE 4: LEVANTAR NUEVO STACK (10-15 minutos)

```bash
# Windows
docker compose -f docker-compose.windows.yml up -d

# RHEL (después de preparar Podman)
docker compose -f docker-compose.rhel.yml up -d

# Ubuntu
docker compose -f docker-compose.ubuntu.yml up -d
```

#### Monitorear arranque
```bash
# Ver estado
docker compose ps

# Ver logs de inicialización
docker compose logs -f airflow-init

# Esperar a que airflow-webserver sea "healthy"
docker compose logs -f airflow-webserver | grep healthy
```

**Tiempo típico**: 2-5 minutos en primera vez, 30-60 segundos en subsecuentes.

---

### FASE 5: VERIFICACIÓN DE CAMBIOS (10 minutos)

#### 5.1 Verificar estructura de directorio dentro del contenedor

```bash
# Desde host
docker exec airflow-webserver ls -la /opt/spark-apps/

# Debe mostrar:
# drwxr-xr-x  3 airflow root  4096 ... etl
# drwxr-xr-x  3 airflow root  4096 ... analytics
# drwxr-xr-x  3 airflow root  4096 ... transformations
```

#### 5.2 Verificar archivo de config runtime

```bash
# Se crea cuando el DAG se ejecuta, pero verificar permisos:
docker exec airflow-webserver test -d /opt/spark-data/runtime && echo "✓ dir ready" || echo "✗ missing"
docker exec airflow-webserver ls -la /opt/spark-data/
docker exec airflow-webserver ls -la /opt/spark-data/runtime/ 2>/dev/null || echo "(Será creado al ejecutar DAG)"
```

#### 5.3 Verificar variables en Airflow

```bash
# Acceder a http://localhost:8080
# Admin > Variables > Buscar:
#   - EXTRACCION_BT_STG
#   - CARGAR_PARQUET_CONFIG
# Deben existir y ser JSON válidos
```

#### 5.4 Ejecutar DAG de test

```bash
# 1. Crear archivo: airflow/dags/production/test_spark_config.py
#    (Usar template en ANALISIS_COMPARATIVO_DOCKER_COMPOSE_DETALLADO.md)

# 2. Acceder a http://localhost:8080 > DAGs
# 3. Buscar "test_spark_config"
# 4. Trigger manualmente
# 5. Ver logs en Task Instance
```

---

## VERIFICACIÓN DE CORRECCIONES APLICADAS

### ✓ VERIFICACIÓN 1: setup.ps1 crea estructura completa

**Antes**:
```
spark\jobs\
spark\config\
```

**Después**:
```
spark\jobs\
  ├── etl\               ← NUEVO
  ├── analytics\        ← NUEVO
  └── transformations\ ← NUEVO
spark\config\
spark\libs\             ← NUEVO
```

**Verificar**:
```powershell
Test-Path "spark\jobs\etl"
Test-Path "spark\jobs\analytics"
Test-Path "spark\jobs\transformations"
Test-Path "spark\libs"
```

### ✓ VERIFICACIÓN 2: SPARK_AUTH_SECRET en .env

**Antes**:
```
(falta en .env)
```

**Después**:
```
SPARK_AUTH_SECRET=ABC123XYZ...
```

**Verificar**:
```bash
grep "SPARK_AUTH_SECRET" .env | head -1
```

### ✓ VERIFICACIÓN 3: DAG mejorado con validaciones

**Antes**:
- Sin validar si variables existen
- Sin mensajes de error claros
- Sin documentación

**Después**:
- Valida existencia de variables
- Valida estructura JSON
- Valida campos requeridos
- Documentación de parámetros y fallos
- Manejo robusto de directorios

**Verificar**:
```bash
# En Airflow UI:
# Admin > Task Instances > [DAG name]
# Ver logs: deben mostrar "[config] Validacion OK:"
```

---

## TROUBLESHOOTING

### Problema 1: "Permission denied" en /opt/spark-data

**Síntoma**: Job de Spark falla al leer config JSON

**Causa**: Permisos insuficientes en directorio runtime

**Solución**:
```bash
docker exec airflow-webserver chmod -R 777 /opt/spark-data/runtime
```

### Problema 2: "File not found" bt_extraccion_parquet_spark.py

**Síntoma**: BsgSparkJdbcOperator no encuentra el archivo

**Causa**: Archivo no copiado a `spark/jobs/etl/`

**Solución**:
```bash
# Verificar que el archivo existe en host
test -f spark/jobs/etl/bt_extraccion_parquet_spark.py || echo "FALTA"

# Verificar dentro del contenedor
docker exec airflow-webserver test -f /opt/spark-apps/etl/bt_extraccion_parquet_spark.py || echo "FALTA"
```

### Problema 3: DAG falla "Variable not found"

**Síntoma**: PythonOperator preparar_config_spark falla

**Causa**: Variables EXTRACCION_BT_STG o CARGAR_PARQUET_CONFIG no existen

**Solución**:
```bash
# 1. Acceder a http://localhost:8080 > Admin > Variables
# 2. Click "Create"
# 3. Key: EXTRACCION_BT_STG
# 4. Value: {"esquema_origen": "...", ...}  (JSON válido)
# 5. Repetir para CARGAR_PARQUET_CONFIG
```

---

## ROLLBACK (Si algo falla)

### Restaurar versión anterior

```bash
# Windows
Copy-Item setup.ps1.backup setup.ps1
Copy-Item dag_bt_parquet_singlestore_spark.py.backup airflow/dags/production/dag_bt_parquet_singlestore_spark.py

# Linux
cp setup.sh.backup setup.sh
cp dag_bt_parquet_singlestore_spark.py.backup airflow/dags/production/dag_bt_parquet_singlestore_spark.py
chmod +x setup.sh
```

### Reiniciar stack con versión anterior

```bash
docker compose down
docker compose up -d
```

---

## VALIDACIÓN FINAL

### Checklist de Post-Implementación

- [ ] Todos los archivos copiados correctamente
- [ ] setup.ps1 o setup.sh ejecutado sin errores
- [ ] Directorio spark/jobs/etl, analytics, transformations existen
- [ ] .env contiene SPARK_AUTH_SECRET
- [ ] docker compose up -d completó sin errores
- [ ] airflow-webserver está healthy (http://localhost:8080)
- [ ] Spark UI accesible (http://localhost:8082)
- [ ] Variables EXTRACCION_BT_STG y CARGAR_PARQUET_CONFIG creadas
- [ ] DAG test_spark_config corre sin errores
- [ ] Archivo /opt/spark-data/runtime/bt_parquet_singlestore_spark.json se crea correctamente

---

## DOCUMENTACIÓN DE REFERENCIA

- **ANALISIS_COMPARATIVO_DOCKER_COMPOSE_DETALLADO.md**: Análisis técnico profundo (este documento)
- **setup-FIXED.ps1**: Script Windows mejorado
- **setup-FIXED.sh**: Script Linux mejorado
- **dag_bt_parquet_singlestore_spark_FIXED.py**: DAG con validaciones robustas

---

## SOPORTE Y PREGUNTAS

Para dudas sobre la implementación:
1. Revisar ANALISIS_COMPARATIVO_DOCKER_COMPOSE_DETALLADO.md (sección relevante)
2. Revisar logs: `docker compose logs [servicio]`
3. Verificar permisos de archivos
4. Confirmar que variables Airflow existen y son JSON válido

