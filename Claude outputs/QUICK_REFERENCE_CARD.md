# ⚡ QUICK REFERENCE CARD - Implementación Rápida

**Imprime esto o ábrelo en otra ventana durante la implementación**

---

## 🎯 3 CAMBIOS PRINCIPALES

### 1️⃣ Scripts Setup
```
setup.ps1  ───> setup-FIXED.ps1
setup.sh   ───> setup-FIXED.sh
```
**Qué agrega**: Estructura `spark/jobs/{etl,analytics,transformations}` + `SPARK_AUTH_SECRET`

### 2️⃣ DAG Spark
```
dag_bt_parquet_singlestore_spark.py  ───> dag_bt_parquet_singlestore_spark_FIXED.py
```
**Qué agrega**: Validaciones robustas + documentación

### 3️⃣ Docker-Compose
```
❌ NO CAMBIA - Ya están correctos
```

---

## ⏱️ TIMELINE

| Paso | Comando | Tiempo |
|------|---------|--------|
| Backup | `cp setup.ps1 setup.ps1.backup` | 1 min |
| Copiar fixes | `cp *-FIXED.* <destino>` | 2 min |
| Generar config | `./setup.ps1` o `./setup.sh` | 5 min |
| Detener stack | `docker compose down` | 2 min |
| Levantar stack | `docker compose up -d` | 5 min |
| Esperar arranque | Ver logs | 5 min |
| Verificación | Script test | 3 min |
| **TOTAL** | | **~23 min** |

---

## 📋 VERIFICACIÓN RÁPIDA

```bash
# Windows
Test-Path "spark\jobs\etl"                           # ✓ Debe existir
Test-Path "spark\jobs\analytics"                     # ✓ Debe existir
Test-Path "spark\jobs\transformations"              # ✓ Debe existir
Select-String "SPARK_AUTH_SECRET" .env               # ✓ Debe estar
docker exec airflow-webserver ls /opt/spark-apps/etl # ✓ Debe tener archivos

# Linux
test -d spark/jobs/etl && echo "✓"                   # ✓ Debe existir
test -d spark/jobs/analytics && echo "✓"            # ✓ Debe existir
test -d spark/jobs/transformations && echo "✓"     # ✓ Debe existir
grep SPARK_AUTH_SECRET .env                          # ✓ Debe estar
docker exec airflow-webserver ls /opt/spark-apps/etl # ✓ Debe tener archivos
```

---

## 🔧 COMANDOS PRINCIPALES

### Backup & Restore
```bash
# BACKUP (antes)
cp setup.ps1 setup.ps1.backup
cp dag_bt_parquet_singlestore_spark.py dag.py.backup

# RESTORE (si algo falla)
cp setup.ps1.backup setup.ps1
cp dag.py.backup airflow/dags/production/dag_bt_parquet_singlestore_spark.py
docker compose down && docker compose up -d
```

### Copiar Archivos
```bash
# Windows
Copy-Item setup-FIXED.ps1 setup.ps1
Copy-Item dag_bt_parquet_singlestore_spark_FIXED.py airflow/dags/production/dag_bt_parquet_singlestore_spark.py

# Linux
cp setup-FIXED.sh setup.sh
cp dag_bt_parquet_singlestore_spark_FIXED.py airflow/dags/production/dag_bt_parquet_singlestore_spark.py
chmod +x setup.sh
```

### Setup & Stack
```bash
# Generar .env (regenerar si algo cambió)
rm .env && .\setup.ps1         # Windows
rm .env && ./setup.sh --rhel   # Linux

# Reiniciar Docker
docker compose down
docker compose up -d

# Monitorear
docker compose ps
docker compose logs -f airflow-webserver
```

### Verificar Config Runtime
```bash
# Después de ejecutar DAG
docker exec airflow-webserver test -f /opt/spark-data/runtime/bt_parquet_singlestore_spark.json && echo "✓ OK" || echo "✗ FALTA"

# Ver contenido
docker exec airflow-webserver cat /opt/spark-data/runtime/bt_parquet_singlestore_spark.json | head -20
```

### Ver Logs
```bash
# Servicio específico
docker compose logs airflow-webserver
docker compose logs spark-master

# Con follow
docker compose logs -f airflow-init

# N últimas líneas
docker compose logs --tail=50 airflow-scheduler
```

---

## ⚠️ ERRORES COMUNES & SOLUCIONES

| Error | Causa | Solución |
|-------|-------|----------|
| `Permission denied /opt/spark-data` | Permisos | `docker exec airflow-webserver chmod -R 777 /opt/spark-data` |
| `File not found bt_extraccion_parquet_spark.py` | Archivo no copiado | Verificar `spark/jobs/etl/` en host y contenedor |
| `Variable not found EXTRACCION_BT_STG` | Variable no existe | Crear en Airflow > Admin > Variables |
| `Config file not found` | DAG no ejecutado | Ejecutar DAG `preparar_config_spark` primero |
| `Setup.ps1 no crea directorios` | Script antiguo | Copiar `setup-FIXED.ps1` |
| `SPARK_AUTH_SECRET no existe` | Setup antiguo | Agregar manual en .env o regenerar |

---

## 📊 ARCHIVO DE CONFIGURACIÓN

**Ubicación**: `/opt/spark-data/runtime/bt_parquet_singlestore_spark.json`

**Creado por**: `preparar_config_spark` (PythonOperator en DAG)

**Contenido esperado**:
```json
{
  "extraccion": {
    "esquema_origen": "...",
    "tabla_origen": "..."
  },
  "carga": {
    "tabla_control": "control_etl",
    "sql_jobs": "SELECT ...",
    "estado_iniciado": "EN PROCESO"
  }
}
```

---

## 🚀 GOTCHAS (Cosas que no son obvias)

1. **setup.ps1 no agrega SPARK_AUTH_SECRET al .env** → Fix: usar script FIXED
2. **spark/jobs/ no tiene estructura etl/analytics** → Fix: usar scripts FIXED
3. **DAG sin validar variables** → Fix: usar DAG FIXED
4. **Archivo config se crea solo cuando DAG ejecuta** → OK, esperar a primera ejecución
5. **Permisos de /opt/spark-data pueden fallar** → OK, chmod -R 777 si falla

---

## ✅ DESPUÉS DE IMPLEMENTAR

```
Checklist Post-Implementación:
□ spark/jobs/etl, analytics, transformations existen
□ .env contiene SPARK_AUTH_SECRET
□ docker compose up -d completó sin errores
□ airflow-webserver es "healthy"
□ Variables EXTRACCION_BT_STG y CARGAR_PARQUET_CONFIG existen
□ DAG aparece en UI
□ Test DAG corre sin errores
□ /opt/spark-data/runtime/ tiene archivo JSON
□ Permisos de archivos son correctos (644)
□ Puedes leer logs sin problemas
```

---

## 📖 REFERENCIAS RÁPIDAS

```
¿Cómo implementar?      → GUIA_IMPLEMENTACION_FIXES.md
¿Qué cambió?            → RESUMEN_EJECUTIVO_FIXES.md  
¿Análisis profundo?     → ANALISIS_COMPARATIVO_DOCKER_COMPOSE_DETALLADO.md
¿Scripts nuevos?        → setup-FIXED.ps1, setup-FIXED.sh
¿DAG mejorado?          → dag_bt_parquet_singlestore_spark_FIXED.py
¿Navegación?            → INDICE_DOCUMENTACION.md
```

---

## 🎯 ÉXITO CUANDO

- ✅ Todos los directorios existen
- ✅ .env tiene SPARK_AUTH_SECRET
- ✅ docker compose ps muestra todos healthy
- ✅ DAG ejecuta sin errores
- ✅ Archivo de config JSON se crea en /opt/spark-data/runtime/

---

**Tiempo de esta referencia**: 2-3 minutos  
**Tiempo de implementación**: 20-30 minutos  
**Tiempo de verificación**: 5-10 minutos

**Total: ~30-45 minutos**

