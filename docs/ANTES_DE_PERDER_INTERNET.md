# Antes de perder Internet

Todo lo que hay que hacer **mientras todavía hay red**, en orden. Cada paso
dice qué pasa si se salta, porque varios de estos fallos no se notan hasta que
ya es tarde para arreglarlos.

La regla de fondo: **nada de lo que necesita descargarse se puede descargar
después.** Las dos imágenes propias bajan drivers de Maven y paquetes del
sistema al construirse. En el servidor aislado no hay forma de obtenerlas.

---

## 1. Construir las dos imágenes

```powershell
.\scripts\construir_imagen.ps1
```

Construye **`airflow-bsg:2.11.2` y `spark-bsg:3.5.3`**. Antes solo construía
la primera; ahora hace las dos, porque construir una sola y descubrirlo en el
destino es exactamente cómo se llega a un servidor sin poder levantar Spark.

Verificar antes de seguir:

```powershell
docker images | Select-String "airflow-bsg|spark-bsg"
```

Tienen que salir **las dos**.

> **Por qué `spark-bsg` y no la oficial de Apache.** La oficial no trae
> intérprete de Python: cualquier UDF muere en el executor con
> `Cannot run program "python3"`. Y no trae los drivers JDBC en
> `$SPARK_HOME/jars`, que es la única ruta que entra en el classpath del
> driver **y** de los executors. Con la oficial, los jobs contra Bantotal y
> SQL Server fallan con `No suitable driver` — y ese error aparece dentro del
> job, no al arrancar, así que el stack parece estar bien.

---

## 2. Comprobar que los drivers quedaron dentro

```powershell
docker run --rm airflow-bsg:2.11.2 python /opt/airflow/verificar_drivers.py --forzar
```

Revisa los siete drivers JDBC, las dos versiones de Java, los drivers ODBC
registrados y los jars duplicados. Si algo falta, **este es el momento de
verlo**: reconstruir necesita red.

Y para Spark, que el verificador no alcanza desde ahí:

```powershell
docker run --rm spark-bsg:3.5.3 ls /opt/spark/jars/ | Select-String "jt400|jdbc|jcc"
```

Deben aparecer `jt400.jar`, `mssql-jdbc.jar` y el resto.

---

## 3. Armar el paquete

```powershell
.\scripts\preparar-bundle-offline.ps1
```

Exporta **siete** imágenes:

| Imagen | Para qué |
|---|---|
| `airflow-bsg:2.11.2` | webserver, scheduler, worker, flower, init |
| `spark-bsg:3.5.3` | spark-master, spark-worker, spark-history |
| `postgres:16-alpine` | metastore |
| `redis:7-alpine` | broker de Celery |
| `busybox:1.36` | contenedor de permisos — **sin él no arranca nada** |
| `nginx:1.27-alpine` | proxy TLS |
| `registry:2` | registro privado dentro de la red aislada |

Antes exportaba `apache/spark:3.5.3` en lugar de `spark-bsg`, y `nginx` estaba
comentada. El paquete parecía completo y dejaba el destino sin Spark.

El script ahora **se niega a continuar** si falta alguna de las dos propias, en
vez de armar un paquete incompleto en silencio.

El script **borra `proyecto/` antes de copiar**. Sin eso, un archivo
renombrado en el repositorio sobrevive en un paquete reutilizado: al pasar
`dag_bt2sql_stg.py` a `dag_stg_bt2sql_carga.py`, el paquete llevaría los dos
y el servidor aislado registraría `BT2SQL_STG` y `STG_BT2SQL_CARGA` a la vez,
dos DAGs escribiendo en las mismas tablas STG. Y comprueba, antes de darse
por bueno, que `.airflowignore` y otros cinco archivos críticos llegaron:
`.airflowignore` empieza por punto y su ausencia no se notaría hasta tener
cuatro DAGs de más en la interfaz, sin red para corregirlo.

También copia ahora `sql/` (el DDL de las tablas de control y los stored
procedures), `requirements.txt`, `infrastructure/`, `rhel/` y
`Dockerfile.offline`, que antes se quedaban fuera. Sin `sql/`, el destino no
tiene dónde escribir: las tablas de control no existirían.

---

## 4. Verificar el paquete antes de moverlo

```powershell
Get-Content .\bundle-offline\MANIFIESTO.txt
Get-ChildItem .\bundle-offline\imagenes\
```

Siete `.tar` y sus SHA256. **Contarlos**: el traslado suele ser por USB o por un
recinto de transferencia, y un archivo truncado ahí produce, días después, un
`docker load` que falla sin explicar por qué. Las sumas convierten eso en una
comprobación de treinta segundos.

Tamaño esperado, unos **3 GB**:

```
airflow-bsg_2.11.2.tar      ~2.1 GB
spark-bsg_3.5.3.tar         ~600-700 MB
postgres_16-alpine.tar      ~111 MB
redis_7-alpine.tar          ~16 MB
nginx_1.27-alpine.tar       ~20 MB
busybox_1.36.tar            ~2 MB
registry_2.tar              ~25 MB
```

Si ve un `apache_spark_3.5.3.tar` de 512 MB en vez de `spark-bsg`, el paquete
es de la versión vieja del script: vuelva a correr el paso 3.

Y borre cualquier `.tmp-*.tar` que encuentre: son restos de un `docker save`
interrumpido y no sirven para nada.

---

## 5. Cosas que también necesitan red y se olvidan

**Los jars de `airflow/jars/`.** Van dentro de la imagen, así que ya viajan.
Pero si piensa agregar alguno más tarde, tiene que estar ahí **antes** del
paso 1.

**Los drivers ODBC de IBM i.** Los instala el Dockerfile desde el repositorio
público de IBM. Ya están dentro de la imagen; solo se pierden si reconstruye
sin red.

**`pip install` en el arranque.** Si `PIP_ADDITIONAL_REQUIREMENTS` tiene algo
en el `.env`, cada contenedor intentará descargarlo al arrancar y fallará sin
red. Déjelo **vacío** y ponga esas dependencias en el Dockerfile.

**Los stored procedures y el DDL.** Ya viajan con `sql/`. Confirme que estén:

```powershell
Get-ChildItem .\bundle-offline\proyecto\sql -Recurse -Filter *.sql | Measure-Object
```

---

## 6. En el servidor aislado

```bash
cd bundle-offline
./cargar_bundle.sh          # docker load de los siete tar
cd proyecto
./setup.sh                  # genera el .env con secretos NUEVOS
```

Confirme en el `.env` recién generado:

```
AIRFLOW_IMAGE=airflow-bsg:2.11.2
SPARK_IMAGE=spark-bsg:3.5.3
```

`setup.sh` ya las escribe sin comentar y además comprueba que las imágenes
existan, avisando si falta alguna. Antes venían comentadas y había que
acordarse de descomentarlas.

Luego:

```bash
docker compose -f docker-compose.ubuntu.yml up -d
```

**Nunca `docker compose pull`**: intentaría salir a Internet y fallaría.

---

## 7. Lo que queda por hacer allá, y no necesita red

- Correr el DDL de `sql/bt2sql/` y `sql/s2sql/` en SQL Server
- Crear las Connections `CONEXION_BANTOTAL` y `CONEXION_SQLSERVER`
- `python scripts/sync_variables.py` para publicar las Variables
- Crear los pools en Admin → Pools, y ponerlos en el `.env`
  (`AIRFLOW_POOL_SINGLESTORE`, `AIRFLOW_POOL_SQLSERVER`)

---

## Comprobación final, en una línea

Antes de cortar la red, esto tiene que dar **7**:

```powershell
(Get-ChildItem .\bundle-offline\imagenes\*.tar).Count
```

Y esto tiene que imprimir las dos:

```powershell
docker images --format "{{.Repository}}:{{.Tag}}" | Select-String "bsg"
```

---

## 8. El registro privado interno

`registry:2` viaja en el paquete desde el 2026-09-30. Son 25 MB, y es la única
forma de tener un registro de imágenes **dentro** de la red aislada: Docker Hub,
GHCR y GitLab no se alcanzan desde allá, ni en su plan de pago.

No hace falta usarlo el primer día —`docker load` de los `.tar` alcanza para
levantar el stack—, pero cuando haya un segundo nodo, copiar 2,8 GB por USB a
cada máquina deja de ser razonable:

```bash
docker run -d --restart=always -p 5000:5000 \
  -v /datos/registry:/var/lib/registry --name registry registry:2

docker tag airflow-bsg:2.11.2 registro.interno:5000/airflow-bsg:2.11.2
docker push registro.interno:5000/airflow-bsg:2.11.2
```

Después `AIRFLOW_IMAGE` y `SPARK_IMAGE` apuntan ahí y cualquier nodo nuevo
levanta sin USB. Necesita TLS, o declarar el registro como inseguro en
`/etc/docker/daemon.json` de cada demonio.

Si no viaja ahora, esta opción desaparece para siempre.
