# Por qué no levantaba Spark

Cambiaste `bitnami/spark` por `apache/spark` y todo lo demás arrancó menos
Spark. La causa no fue la imagen: fue que **toda la configuración que teníamos
era específica de Bitnami**, y la imagen oficial no la entiende.

Esto es culpa mía: escribí ese bloque para Bitnami y no dejé anotado qué pasaba
al cambiar de imagen.

---

## Las cuatro cosas que fallaban

### 1. Las variables `SPARK_*` no existen en Spark

```yaml
environment:
  SPARK_MODE: master              # <- no existe
  SPARK_MASTER_URL: spark://...   # <- no existe
  SPARK_WORKER_MEMORY: 2G         # <- no existe (así)
  SPARK_RPC_AUTHENTICATION_ENABLED: "yes"   # <- no existe
```

Ninguna de esas es de Apache Spark. Son invento del *entrypoint* de Bitnami: un
script suyo las lee y las traduce a la invocación real. La imagen oficial no
tiene ese script, así que **las ignora en silencio**. Sin `SPARK_MODE`, el
contenedor no sabe si tiene que ser master o worker, y termina.

Con la imagen oficial hay que invocar la clase Java directamente — que es lo que
Bitnami hacía por debajo:

```yaml
entrypoint: ["/opt/spark/bin/spark-class"]
command:
  - org.apache.spark.deploy.master.Master
  - --host
  - spark-master
  - --port
  - "7077"
```

### 2. El healthcheck usaba `curl`, y la imagen oficial no lo trae

```yaml
healthcheck:
  test: ["CMD", "curl", "--fail", "http://localhost:8080/"]
```

`curl` no está instalado en `apache/spark`. El healthcheck fallaba siempre, así
que el master **nunca llegaba a `healthy`**.

Y aquí está el efecto dominó que explica que no arrancara *nada* de Spark:

```yaml
spark-worker:
  depends_on:
    spark-master:
      condition: service_healthy    # <- nunca se cumplía
```

El worker se quedaba esperando indefinidamente por un master que nunca iba a
reportarse sano. Aunque hubieras arreglado solo el punto 1, el worker habría
seguido sin arrancar.

Ahora el healthcheck usa solo bash, sin binarios extra:

```yaml
test: ["CMD-SHELL", "timeout 3 bash -c '</dev/tcp/localhost/8080' || exit 1"]
```

### 3. Las rutas cambian de sitio

| | Bitnami | Apache |
|---|---|---|
| `SPARK_HOME` | `/opt/bitnami/spark` | `/opt/spark` |
| usuario | uid 1001 | uid 185 (`spark`) |

Cualquier ruta absoluta escrita para Bitnami apunta a un directorio inexistente
en la oficial.

### 4. Los volúmenes eran de root, y nadie podía escribir

Un volumen nombrado recién creado pertenece a `root`. Spark corre como uid 185
y Airflow como uid 50000: **ninguno de los dos puede escribir** en
`/opt/spark-data` ni en `/opt/spark-events`.

Este no te habría dado la cara al arrancar — habría aparecido más tarde, al
escribir el primer Parquet, como un `Permission denied` desde el executor.

Añadí un contenedor `spark-init` que corre una vez, ajusta permisos y termina:

```yaml
spark-init:
  image: busybox:1.36
  command: sh -c "mkdir -p /data /events && chmod -R 777 /data /events"
```

El `777` es de desarrollo, y lo dejé anotado en el compose: es un área
compartida entre dos usuarios distintos. En producción eso vive en
almacenamiento externo (S3, HDFS, NFS) con permisos de verdad.

---

## Lo que cambié

- `spark-init` nuevo: prepara permisos antes de que arranque nada
- `spark-master` y `spark-worker`: `entrypoint` + `command` explícitos
- Healthchecks sin `curl`
- `--cores` y `--memory` como argumentos explícitos, no variables de entorno
- Autenticación RPC → `SPARK_MASTER_OPTS` / `SPARK_WORKER_OPTS`, **desactivada
  por defecto** (ver abajo)
- `SPARK_IMAGE` en el `.env` para cambiar de imagen sin tocar el compose

Validado con `docker compose config` en las dos versiones. Como siempre: sin
daemon de Docker aquí, la ejecución real la haces tú.

---

## Cómo levantarlo

```powershell
docker compose -f docker-compose.windows.yml down
docker compose -f docker-compose.windows.yml up -d

# el init corre y termina — eso es lo esperado
docker compose -f docker-compose.windows.yml logs spark-init

# el master debe quedar en (healthy) en ~30 s
docker compose -f docker-compose.windows.yml ps spark-master spark-worker
```

Deberías ver:

```
NAME            STATUS
spark-master    Up 40 seconds (healthy)
spark-worker-1  Up 20 seconds (healthy)
```

Y en http://localhost:8082 → `Alive Workers: 1`.

Confirma en los logs del master que el worker se registró:

```powershell
docker logs spark-master 2>&1 | Select-String "Registering worker"
```

---

## Sobre la autenticación, que ahora está apagada

La dejé desactivada a propósito, y quiero ser claro sobre el porqué: con
`spark.authenticate=true`, el secreto tiene que estar en **tres sitios** o los
jobs fallan con errores de red poco claros:

1. `SPARK_MASTER_OPTS` en el `.env`
2. `SPARK_WORKER_OPTS` en el `.env`
3. El `conf={}` del `SparkSubmitOperator` en cada DAG

Si falta el tercero, el cluster levanta bien y las tareas fallan — el peor
escenario para depurar. Prefiero que primero veas el cluster arriba y verde.

Para activarla, en el `.env`:

```ini
SPARK_MASTER_OPTS=-Dspark.authenticate=true -Dspark.authenticate.secret=EL_SECRETO
SPARK_WORKER_OPTS=-Dspark.authenticate=true -Dspark.authenticate.secret=EL_SECRETO
```

Y en el DAG (ejemplo 23), dentro de `conf={}`:

```python
"spark.authenticate": "true",
"spark.authenticate.secret": "EL_SECRETO",
```

Puedes reutilizar el `SPARK_RPC_SECRET` que ya generó el setup.

**Y una advertencia que importa para tu contexto:** un secreto pasado por `-D`
es visible con `ps` desde dentro del contenedor. Para banca eso no pasa una
auditoría. Lo correcto es un `spark-defaults.conf` con permisos 600 montado en
cada contenedor, o un secrets backend. El `-D` sirve para validar que funciona,
no para producción.

---

## Si necesitas Python en los executors

`apache/spark:3.5.3` no incluye Python. Para el job `jdbc_a_parquet.py` **no
hace falta**: usa solo la API de DataFrames, y esas operaciones se ejecutan
enteras en la JVM. El driver (que sí corre Python) vive en el worker de Airflow,
donde el `Dockerfile` ya instala PySpark.

Ahora bien, en el momento en que uses una UDF de Python, `rdd.map()` o
`applyInPandas`, los executors necesitan un intérprete de Python o el job falla
con `Cannot run program "python3"`.

Dos salidas:

- usar un tag de `apache/spark` que incluya `python3` (mira los tags
  disponibles en Docker Hub y ponlo en `SPARK_IMAGE`), o
- una imagen propia de tres líneas:

```dockerfile
FROM apache/spark:3.5.3
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && ln -sf /usr/bin/python3 /usr/bin/python && rm -rf /var/lib/apt/lists/*
USER spark
```

```powershell
docker build -t spark-bsg:3.5.3 -f spark/Dockerfile .
# .env:  SPARK_IMAGE=spark-bsg:3.5.3
```

---

## Nota sobre Bitnami

Si el `bitnami/spark:3.5.3` te falló al descargar, encaja con la reorganización
que Bitnami hizo de sus repositorios públicos durante 2025: varios tags dejaron
de estar disponibles en las rutas de siempre. No pude comprobarlo desde aquí
(sin acceso a Docker Hub), pero da igual: la ruta con `apache/spark` no depende
de eso, y es la imagen del propio proyecto Apache.

---

## Diagnóstico rápido

**`spark-master` reinicia en bucle**
```powershell
docker logs spark-master --tail 50
```
Si dice `Could not find or load main class`, la ruta del entrypoint no coincide
con la imagen — comprueba `SPARK_HOME` con
`docker run --rm apache/spark:3.5.3 env | Select-String SPARK_HOME`.

**El master queda `Up` pero nunca `healthy`**
Su UI no responde. Entra y compruébalo desde dentro:
```powershell
docker exec spark-master bash -c '</dev/tcp/localhost/8080 && echo abierto'
```

**El worker arranca pero no aparece en la UI del master**
Casi siempre es autenticación desparejada, o que el worker no resuelve
`spark-master`:
```powershell
docker exec <worker> getent hosts spark-master
docker logs <worker> 2>&1 | Select-String -Pattern "Failed|Retrying|Authentication"
```

**El worker aparece pero los jobs se quedan en `WAITING`**
`executor_memory` es mayor que lo que el worker anuncia. Baja el del DAG o sube
`SPARK_WORKER_MEMORY`, dejando ~1 GB de margen.
