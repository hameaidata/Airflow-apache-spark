# Docker Compose — Windows y Ubuntu

Dos archivos, uno por sistema operativo. El resto del proyecto (DAGs, plugins,
configuración de Spark) es idéntico en ambos.

| Archivo | Para |
|---|---|
| `docker-compose.windows.yml` + `setup.ps1` | Windows con Docker Desktop (WSL2) |
| `docker-compose.ubuntu.yml` + `setup.sh` | Ubuntu / Debian / cualquier Linux |

El archivo `.env` no viene incluido: lo **genera el script de setup** en tu
máquina, con secretos aleatorios creados localmente (`RandomNumberGenerator` en
Windows, `openssl rand` en Linux). Así ninguna contraseña viaja por la red ni
queda escrita en un archivo de plantilla.

---

## Antes que nada: el compose anterior no arrancaba

El `docker-compose.yml` que teníamos en el proyecto tiene fallos que impiden
que el stack levante. Los listo porque conviene entender por qué cambió tanto:

| # | Problema | Consecuencia |
|---|---|---|
| 1 | `EXECUTOR: LocalExecutor` pero el servicio worker corría `airflow celery worker` | Contradicción directa. LocalExecutor no usa Celery; el worker no arrancaba, y sin CeleryExecutor **no hay escalado horizontal** — que era el requisito central |
| 2 | `spark-master` y `adminer` ambos publicaban el puerto host `8081` | `docker compose up` aborta con "port is already allocated" |
| 3 | Bind mounts a archivos inexistentes (`prometheus.yml`, `init.sql`, `airflow.cfg`, `webserver_config.py`, `vault-config.hcl`, certificados SSL) | Docker crea un **directorio vacío** donde se esperaba un archivo. Airflow arranca con config corrupta o el contenedor muere |
| 4 | `AIRFLOW__WEBSERVER__AUTHENTICATE`, `__RBAC`, `__AUTH_BACKEND` | No son opciones válidas en Airflow 2.x. RBAC ya viene siempre activo y la autenticación se configura en `webserver_config.py`, no por variables de entorno |
| 5 | `auth_backend: airflow.providers.ldap.auth_manager.LdapAuthManager` | Ese módulo no existe. No hay provider `apache-airflow-providers-ldap` con esa ruta |
| 6 | Ningún servicio ejecutaba `db migrate` ni creaba un usuario | Aunque levantara, **no podrías entrar a la UI**: no hay usuario |
| 7 | `POSTGRES_INITDB_ARGS: "-c ssl=on ..."` | `initdb` no acepta parámetros de servidor con `-c`. Los certificados montados nunca se usaban: el "SSL a base de datos" no estaba activo |
| 8 | `container_name` fijo en el worker | Bloquea `--scale`: Docker no puede crear dos contenedores con el mismo nombre |
| 9 | Imagen `vault:latest` | Obsoleta desde 2023; ahora es `hashicorp/vault` |

Los dos archivos nuevos corrigen todo eso. Los validé con `docker compose config`
(sintaxis e interpolación de variables correctas). **No pude ejecutarlos** — el
entorno donde trabajo no tiene daemon de Docker — así que la primera ejecución
real la harás tú. Si algo falla, la sección de diagnóstico del final cubre los
casos típicos.

---

## Diferencias entre las dos versiones

Solo hay tres, pero importan:

### 1. UID de usuario

**Linux**: la imagen de Airflow corre como UID 50000. Si tus carpetas del host
pertenecen a tu usuario (típicamente 1000), el contenedor no puede escribir en
ellas. Por eso el compose de Ubuntu lleva:

```yaml
user: "${AIRFLOW_UID:-50000}:0"
```

y `setup.sh` rellena `AIRFLOW_UID` con tu `id -u` real. Sin esto, los logs se
crean como root y ni siquiera puedes borrarlos sin `sudo`.

**Windows**: Docker Desktop traduce los permisos automáticamente; no existe un
UID POSIX que mapear. Se deja el 50000 por defecto y no se usa la directiva
`user:`.

### 2. Dónde viven los logs

**Windows**: volumen nombrado (`airflow_logs:/opt/airflow/logs`). Un bind mount
a NTFS da problemas de permisos y es lento. Los logs los lees desde la UI de
Airflow o con `docker compose logs`.

**Linux**: bind mount (`./airflow/logs:/opt/airflow/logs`). Funciona bien y
puedes hacer `tail -f airflow/logs/...` directamente desde el host.

En ambos casos `dags/` y `plugins/` sí van bind-montados: necesitas editarlos
desde tu editor y que el contenedor lo vea al instante.

### 3. Finales de línea

Windows convierte a CRLF. Un `.sh` con CRLF ejecutado dentro de un contenedor
Linux falla con `bad interpreter: /bin/bash^M`. El `.gitattributes` incluido
fuerza LF en todo lo que se ejecuta dentro de contenedores y permite CRLF solo
en `.ps1` / `.bat`.

---

## Arranque

### Windows

```powershell
# En PowerShell, dentro de la carpeta del proyecto
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\setup.ps1

docker compose -f docker-compose.windows.yml up -d
```

### Ubuntu

```bash
chmod +x setup.sh
./setup.sh

docker compose -f docker-compose.ubuntu.yml up -d
```

La primera vez descarga unos 3 GB de imágenes: cuenta 5–10 minutos. El servicio
`airflow-init` corre primero (migra la base y crea el usuario admin), termina, y
recién entonces arrancan webserver, scheduler y workers.

Seguir el progreso:

```bash
docker compose -f docker-compose.<so>.yml ps
docker compose -f docker-compose.<so>.yml logs -f airflow-init
```

Cuando `airflow-webserver` aparezca como `healthy`:

| Servicio | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| Flower (workers Celery) | http://localhost:5555 |
| Spark Master | http://localhost:8082 |

El usuario y la contraseña los imprime el script de setup al terminar, y quedan
en tu `.env` (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`).

> **Si el proyecto está dentro de OneDrive**, el `.env` con las contraseñas se
> sincroniza a la nube. Para un proyecto bancario conviene moverlo a una ruta
> local (`C:\dev\airflow-spark`). Además OneDrive a veces bloquea archivos
> mientras Docker escribe, lo que produce errores de I/O difíciles de rastrear
> en los bind mounts. El `setup.ps1` te avisa si detecta esa ruta.

---

## Escalar workers sin parar nada

Esto es lo que el compose anterior no permitía. Ahora:

```bash
docker compose -f docker-compose.ubuntu.yml up -d --scale airflow-worker=5
```

Docker crea `airflow-worker-3`, `-4`, `-5`. Los que ya corrían **no se tocan**:
Compose solo añade los que faltan. Cada worker nuevo se conecta a Redis por su
cuenta y empieza a tomar tareas de la cola.

Capacidad total = `réplicas × WORKER_CONCURRENCY` (8 por defecto). Con 5 réplicas
son 40 tareas simultáneas.

Para reducir:

```bash
docker compose -f docker-compose.ubuntu.yml up -d --scale airflow-worker=2
```

El `stop_grace_period: 60s` da un minuto a las tareas en vuelo para terminar
antes de que el contenedor muera. Aun así, evita reducir réplicas en mitad de
una ventana de carga: las tareas que no alcancen a terminar se reintentan.

Spark escala igual:

```bash
docker compose -f docker-compose.ubuntu.yml up -d --scale spark-worker=4
```

Verificar:

```bash
docker compose -f docker-compose.ubuntu.yml ps airflow-worker
curl -s http://localhost:5555/api/workers | python3 -m json.tool
```

---

## Verificar el auto-discovery de DAGs

Incluí `airflow/dags/examples/canary_auto_discovery.py`. No depende de Spark ni
de providers externos, así que si ese DAG falla el problema es del stack, no de
tu código.

```
1. El archivo ya está en airflow/dags/examples/
2. Espera ≤ 30 s  (DAG_DIR_LIST_INTERVAL en el .env)
3. Aparece en la UI como "canary_auto_discovery"
4. Despáusalo y dale Trigger
5. Si las 3 tareas quedan en verde, el stack funciona
```

La tarea `quien_me_ejecuta` imprime el hostname del worker que la corrió. Escala
a 5 workers, dispara el DAG varias veces y verás hostnames distintos: esa es la
prueba real de que Celery reparte el trabajo.

**Nota sobre los 30 segundos**: `DAG_DIR_LIST_INTERVAL` controla cada cuánto el
scheduler busca *archivos nuevos*. Que el DAG aparezca en la UI puede tardar algo
más, porque después de detectar el archivo hay que parsearlo y serializarlo. En
la práctica cuenta entre 30 y 60 segundos, no exactamente 30.

Los DAGs nuevos aparecen **pausados** (`DAGS_ARE_PAUSED_AT_CREATION=true`). Es
deliberado: en un entorno regulado no quieres que un archivo recién copiado
empiece a ejecutarse solo. Despáusalo a mano o cambia esa variable.

---

## Lo que estos archivos NO incluyen

Para que no haya sorpresas más adelante:

- **Sin LDAP.** La autenticación es la de base de Airflow (usuario y contraseña
  en la propia base de datos). LDAP se configura en `webserver_config.py`, no en
  el compose, y necesita un servidor LDAP real contra el cual probar.
- **Sin TLS.** Todo va por HTTP plano y Postgres sin SSL. Para banca esto no
  sirve tal cual: hace falta terminación TLS delante (nginx/Traefik) y
  certificados reales. Que la documentación previa mostrara `ssl=on` no
  significaba que estuviera activo.
- **Sin Vault, Prometheus, Grafana ni ELK.** Los quité del core a propósito:
  metían ~4 GB de RAM extra y hacían que el stack no levantara en máquinas
  normales. Se añaden después, como overlay, cuando el core ya funcione.
- **Sin Kerberos para Spark.** Está activada la autenticación RPC por secreto
  compartido (`spark.authenticate`), que es bastante más simple que Kerberos.

Ninguna de estas piezas es difícil de añadir, pero cada una necesita
infraestructura real detrás. Conviene levantar primero el core, comprobar que el
canario pasa, y recién entonces ir sumando capas.

---

## Diagnóstico rápido

**`airflow-init` falla con error de conexión a Postgres**
El healthcheck de Postgres tarda; el `start_period: 20s` debería cubrirlo.
Si persiste: `docker compose logs postgres` — normalmente es que el volumen
tiene datos de un intento anterior con otra contraseña. Solución:
`docker compose down -v` (borra los datos) y volver a levantar.

**Los workers no toman tareas**
Revisa Flower (http://localhost:5555). Si no aparece ninguno, casi siempre es la
contraseña de Redis: `REDIS_PASSWORD` debe ser idéntica en el broker y en el
servidor. Si la cambiaste en `.env` después del primer arranque, hace falta
`docker compose down && docker compose up -d`.

**El DAG no aparece tras varios minutos**
```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
```
Ese comando muestra los errores de parseo, que es la causa en la mayoría de los
casos. Un `import` que falla hace que Airflow descarte el archivo entero en
silencio.

**Permisos en Linux**
Si ves `Permission denied` sobre `/opt/airflow/logs`, `AIRFLOW_UID` no coincide
con tu usuario:
```bash
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
sudo chown -R "$(id -u):0" airflow/logs
docker compose -f docker-compose.ubuntu.yml up -d
```

**`bad interpreter: /bin/bash^M` en Windows**
Un archivo `.sh` quedó con CRLF. Con el `.gitattributes` incluido no debería
pasar; si ya ocurrió:
```powershell
docker run --rm -v "${PWD}:/w" -w /w alpine sh -c "apk add -q dos2unix && dos2unix *.sh"
```

**La imagen de Spark no se descarga**
Bitnami reorganizó sus tags públicos durante 2025 y algunos dejaron de estar
disponibles. Si `bitnami/spark:3.5.3` falla, cambia `SPARK_IMAGE_TAG` en el
`.env` o usa la imagen oficial `apache/spark:3.5.3` (las variables de entorno
`SPARK_MODE` son de Bitnami, así que con la oficial hay que ajustar el `command`).

---

## Nota sobre la versión de Airflow

Puse `2.10.5` por defecto en vez del `2.7.3` que traía el proyecto. 2.7.3 es de
noviembre de 2023 y desde entonces hubo varias releases con parches de
seguridad — para un entorno bancario, arrancar sobre una versión con casi dos
años de parches pendientes es difícil de justificar ante una auditoría.

Si tus plugins dependen de APIs de 2.7, cambia `AIRFLOW_IMAGE_TAG` en el `.env`
y revisa la guía de actualización de Airflow antes de subir de versión. Verifica
cuál es la última 2.x estable en el momento en que leas esto.
