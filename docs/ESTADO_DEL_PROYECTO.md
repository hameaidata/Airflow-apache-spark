# Estado del proyecto

Corte: 21 de agosto de 2026, 12:40

Este análisis se hizo **inspeccionando los archivos en disco**, no de memoria.
Cada afirmación indica cómo se comprobó.

---

## Semáforo

| Componente | Estado | Comprobado con |
|---|---|---|
| Compose (Windows y Ubuntu) | **Funciona** | Restaurado del commit y validado con `docker compose config` |
| Spark sobre imagen Apache | **Funciona** | Usted lo levantó; entrypoint y healthcheck verificados |
| Auto-discovery de DAGs | **Funciona** | El scheduler generó `__pycache__` de los 7 DAGs |
| Sintaxis de todo el código | **Limpia** | `py_compile` sobre 9 archivos, sin errores |
| DAG 20 y canario | **Deberían correr** | Solo usan el núcleo de Airflow |
| **DAGs 21, 22, 23** | **No cargan** | Necesitan providers que la imagen actual no trae |
| DAG 24 | **Carga, falla al correr** | Le falta una conexión configurada |
| Módulos de `plugins/` | **Vacíos** | 8 carpetas con solo `__init__.py` |
| Imagen propia | **Sin construir** | `AIRFLOW_IMAGE` comentado en el `.env` |
| Conexión a SQL Server / DB2 | **Sin probar** | No hay conexiones creadas |

---

## El hallazgo principal

En su `.env`, esta línea está comentada:

```ini
# AIRFLOW_IMAGE=airflow-bsg:2.11.2
```

Eso significa que el stack corre con la **imagen oficial** de Airflow, que no
incluye los providers de SQL Server, JDBC ni Spark. Son justamente los que
añade el `Dockerfile` de este proyecto.

Consecuencia, DAG por DAG:

| DAG | Qué necesita | Estado esperado |
|---|---|---|
| `20_credenciales_variables_conexiones` | Solo el núcleo | Carga bien |
| `canary_auto_discovery` | Solo el núcleo | Carga bien |
| `21_sqlserver_stored_procedures` | `microsoft.mssql` | **Error de importación** |
| `22_db2_ibm_jdbc` | `jdbc` | **Error de importación** |
| `23_spark_jdbc_parquet` | `apache.spark`, `microsoft.mssql` | **Error de importación** |
| `24_auditoria_logs_a_base_datos` | `postgres` | Carga, pero falla al ejecutar |

**Confírmelo con un comando** (yo no puedo ejecutarlo desde aquí):

```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow dags list-import-errors
```

Si aparecen los tres, es esto. Se resuelve construyendo la imagen:

```powershell
docker build -t airflow-bsg:2.11.2 .
# quitar el # de la línea AIRFLOW_IMAGE en el .env
docker compose -f docker-compose.windows.yml up -d
```

> **Aviso sobre ese `docker build`:** nunca se ha ejecutado. Descarga de siete
> orígenes distintos (Debian, Microsoft, Apache, Maven, PyPI). Es probable que
> algo falle en el primer intento — una versión de jar que cambió, una descarga
> bloqueada. Reserve tiempo para esa primera construcción; no la deje para el
> día que la necesite funcionando.

---

## Lo segundo: no hay conexiones creadas

Aunque construya la imagen, los DAGs 21 a 24 seguirán fallando al ejecutarse,
porque las conexiones que referencian no existen:

| Conexión | La usa | Estado |
|---|---|---|
| `sqlserver_core` | DAGs 21 y 23 | **No creada** |
| `db2_core` | DAG 22 | **No creada** |
| `postgres_auditoria` | DAG 24 | **No creada** |
| `spark_default` | DAG 23 | Creada por el arranque |

Las tres primeras dependen de decisiones que aún no están tomadas (abajo).

Verificar cuáles existen:

```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow connections list
```

---

## Inventario verificado

### Código — sintaxis validada, sin errores

| Archivo | Tamaño |
|---|---|
| `docker-compose.windows.yml` | 15.397 bytes |
| `docker-compose.ubuntu.yml` | 15.934 bytes |
| `Dockerfile` | 6.697 bytes |
| `Dockerfile.offline` | 6.032 bytes |
| `setup.ps1` / `setup.sh` | 10.277 / 9.192 bytes |
| `spark/Dockerfile` | 1.568 bytes |
| 6 DAGs de ejemplo + canario | 44.577 bytes |
| `spark/jobs/etl/jdbc_a_parquet.py` | 6.485 bytes |
| `airflow/plugins/security/auth_manager.py` | 12.366 bytes |
| `airflow/config/webserver_config.py` | 5.755 bytes |

### Módulos: siguen vacíos

Ocho carpetas bajo `airflow/plugins/` contienen únicamente `__init__.py` de
0 bytes: `governance`, `hooks`, `logging`, `monitoring`, `operators`,
`resilience`, `secrets`, `utils`.

El único con contenido real es `security/auth_manager.py`, reescrito para
verificar credenciales contra la tabla de usuarios de Airflow. Sustituyó a un
archivo de 1 KB cuyo `authenticate_user()` hacía `return True` sin comprobar
nada.

**Los DAGs de ejemplo no dependen de estos módulos** — usan `PythonOperator`
directamente, a propósito. Que estén vacíos no bloquea nada hoy.

### Documentación: 12 archivos

| Archivo | Para qué |
|---|---|
| `README.md` | Puertos, paneles, seguridad, Parquet, preguntas frecuentes |
| `README-TI.md` | Solicitud de infraestructura — 10–100 GB/día |
| `README-TI-REDUCIDO.md` | La misma, versión de 10 GB con Docker sobre Linux |
| `README-REQUERIMIENTOS-FUNCIONAMIENTO.md` | Operar sin Internet |
| `README_DOCKER.md` | Diferencias Windows / Linux |
| `docs/GUIA_CONEXIONES_DAGS.md` | Credenciales, SQL Server, DB2, SPs, Parquet |
| `docs/CREDENCIALES_Y_HASHING.md` | Para el área de seguridad |
| `docs/ESCALAR_SPARK_WORKERS.md` | Añadir capacidad |
| `docs/DIAGNOSTICO_SPARK.md` | Si Spark no levanta |
| `docs/ESTADO_DEL_PROYECTO.md` | Este archivo |
| `INICIO_RAPIDO.md` | **Obsoleto** |
| `README_MODULES.md` | **Obsoleto** |

Los dos últimos son de la primera versión y describen un montaje que ya no
existe: mencionan `.env.example`, `LocalExecutor` y Bitnami, todos eliminados.
**Conviene borrarlos** antes de que alguien los siga.

---

## Cambios que introduje sin que se me pidiera

Por transparencia, y para que sepa qué hay en el proyecto que no salió de un
pedido suyo:

| Qué | Estado | Nota |
|---|---|---|
| Puertos enlazados a `127.0.0.1` | **Revertido** | Lo hice al escribir documentación. Los compose están idénticos al commit |
| `airflow/config/webserver_config.py` + su montaje | **Activo** | Salió del pedido de arreglar `authenticate_user`. Configura el login de la interfaz |

Sobre el segundo: el compose monta ese archivo en el webserver. **Si el archivo
desapareciera, Docker crearía una carpeta con ese nombre y el webserver no
arrancaría.** Existe y está bien (5.755 bytes, verificado), pero téngalo
presente si clona el repositorio en otra máquina.

---

## Decisiones abiertas

Siguen siendo las mismas tres, y bloquean todo lo que viene:

**1. Autenticación a SQL Server.** Usted indicó que es integrada de Windows/AD.
Con eso, `pymssql` —la librería del provider— no sirve: solo soporta usuario y
contraseña de SQL. Las salidas son una cuenta de servicio con autenticación SQL
(esfuerzo cero) o Kerberos con keytab de AD (días de trabajo, y depende del
equipo de AD, no de nosotros).

**2. Variante de DB2.** ¿LUW, AS/400 o mainframe? Determina el driver. Si es
AS/400, el `jcc.jar` que trae el Dockerfile no sirve y hay que reescribir el
DAG 22 para JTOpen.

**3. Base de datos de destino.** Sin definir el motor ni el servidor no se puede
tramitar la regla de firewall ni la cuenta.

---

## Pendientes técnicos

Ordenados por lo que desbloquea más:

1. **Construir la imagen propia** — sin esto, tres DAGs no cargan
2. **Crear las conexiones** — depende de las decisiones de arriba
3. **TLS** — hoy todo va en claro
4. **Logs remotos** — al reducir workers, sus logs desaparecen con el contenedor
5. **Limpieza del Parquet** — nada lo borra; el volumen crece indefinidamente
6. **`airflow db clean` periódico**, con archivado previo
7. Borrar `INICIO_RAPIDO.md` y `README_MODULES.md`
8. Borrar la carpeta `_to_delete/`
9. Reconstruir los módulos de `plugins/` (no bloquea nada hoy)

---

## Lo que sigue sin verificarse

Para que quede claro qué es hecho y qué es supuesto:

- El `docker build` del `Dockerfile` — nunca ejecutado
- Las URLs de los jars en Maven Central — no alcanzables desde donde trabajo
- La conexión real a SQL Server, DB2 o la base de destino — sin acceso
- El comportamiento en tiempo de ejecución de los DAGs 21 a 24
- El script `preparar-bundle-offline.sh` — sintaxis validada, nunca ejecutado

---

## Resumen en tres líneas

La infraestructura funciona y está estable. El código está sintácticamente
limpio y validado. Lo que falta para procesar datos de verdad no es técnico:
son las tres decisiones sobre autenticación, variante de DB2 y base de destino,
más construir la imagen una vez.
