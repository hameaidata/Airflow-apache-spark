# Añadir workers de Spark — paso a paso

## Por qué ves solo uno

El servicio `spark-worker` arranca con 1 réplica por defecto. No es un error del
stack, es el valor inicial. Para verlo:

```powershell
docker compose -f docker-compose.windows.yml ps spark-worker
```

---

## Paso 1 — Mira cuánto tienes antes de pedir más

Esto es lo que más se salta la gente, y es lo que decide si añadir workers sirve
de algo. Abre el Spark Master en http://localhost:8082 y fíjate en la cabecera:

```
Alive Workers:   1
Cores in use:    2 Total, 0 Used
Memory in use:   2.0 GiB Total, 0.0 B Used
```

Ahora compara con lo que Docker tiene disponible en total:

```powershell
docker info --format "Cores: {{.NCPU}}  RAM: {{.MemTotal}}"
```

En Windows ese número **no** es el de tu PC: es lo que Docker Desktop reservó
para la VM de WSL2. Suele ser bastante menos de lo que crees.

---

## Paso 2 — Haz la cuenta

Cada worker consume `SPARK_WORKER_CORES` y `SPARK_WORKER_MEMORY` de tu `.env`
(2 cores y 2 GB por defecto). Y no están solos en la VM: el resto del stack ya
ocupa lo suyo.

| Componente | RAM aprox. |
|---|---|
| Postgres + Redis | ~0.5 GB |
| Airflow webserver | ~1 GB |
| Airflow scheduler | ~1 GB |
| Airflow worker × 2 | ~1.5 GB |
| Spark master | ~0.5 GB |
| **Subtotal sin workers de Spark** | **~4.5 GB** |
| Cada worker de Spark | ~2 GB |

Con 8 GB en Docker Desktop te quedan unos 3.5 GB libres, o sea **1 worker de
Spark cómodo, 2 apretados**. Si pides 4, la VM de WSL2 empieza a hacer swap y
todo el stack se vuelve lentísimo — o el kernel mata procesos y te encuentras
contenedores caídos sin explicación aparente.

Para subir el techo, crea `C:\Users\goran\.wslconfig`:

```ini
[wsl2]
memory=12GB
processors=6
```

Y aplícalo:

```powershell
wsl --shutdown
# reinicia Docker Desktop
```

---

## Paso 3 — Escala

Un solo comando, sin parar nada de lo que ya corre:

```powershell
docker compose -f docker-compose.windows.yml up -d --scale spark-worker=3
```

Verás algo así:

```
[+] Running 3/3
 ✔ Container airflow-spark-spark-worker-1  Running       0.0s
 ✔ Container airflow-spark-spark-worker-2  Started       1.2s
 ✔ Container airflow-spark-spark-worker-3  Started       1.3s
```

Fíjate en la diferencia: el worker 1 dice **Running** (no se tocó), los otros dos
**Started** (recién creados). Compose solo añade lo que falta. El Spark Master no
se reinicia; los workers nuevos se registran solos por el puerto 7077.

---

## Paso 4 — Verifica

Recarga http://localhost:8082. La cabecera debe decir ahora:

```
Alive Workers:   3
Cores in use:    6 Total, 0 Used
Memory in use:   6.0 GiB Total, 0.0 B Used
```

Desde la línea de comandos:

```powershell
docker compose -f docker-compose.windows.yml ps spark-worker
```

Y en los logs del master ves el registro de cada uno:

```powershell
docker logs spark-master --tail 20 | Select-String "Registering worker"
```

---

## Paso 5 — Déjalo fijo

El `--scale` no sobrevive a un `docker compose down`. Para que 3 sea el valor
por defecto, edita tu `.env`:

```ini
SPARK_WORKER_REPLICAS=3
```

Y levanta normal:

```powershell
docker compose -f docker-compose.windows.yml up -d
```

El compose ya lee esa variable (`deploy.replicas: ${SPARK_WORKER_REPLICAS:-1}`).
El flag `--scale` sigue funcionando y tiene prioridad cuando lo usas.

---

## Para reducir

```powershell
docker compose -f docker-compose.windows.yml up -d --scale spark-worker=1
```

Docker elimina los de numeración más alta. Si había un job corriendo en ellos,
Spark reprograma esas tareas en los workers que quedan — se pierde el trabajo
parcial de esas particiones, pero el job no falla. Aun así, evita reducir en
mitad de una ejecución larga.

---

## El detalle que importa: más workers ≠ más capacidad

En una sola máquina, repartir 6 cores entre 3 workers de 2 cores **no te da más
CPU** que 1 worker de 6 cores. Es la misma máquina partida en tres.

Entonces, ¿para qué sirve tener varios workers en local?

- **Para probar el comportamiento distribuido.** Ver que las particiones se
  reparten, que un worker que muere no tumba el job, que el shuffle funciona
  entre procesos separados.
- **Para aislar fallos.** Un executor que se queda sin memoria mata a su worker,
  no a todos.

Lo que **no** hace es acelerar tus jobs en local. De hecho suele ralentizarlos:
más shuffle por la red y más overhead de JVMs.

**El escalado horizontal de verdad es añadir máquinas**, no contenedores en la
misma máquina. Ese paso está más abajo.

---

## Por qué tu job puede seguir usando un solo worker

Escalaste a 3, el master dice "3 Alive Workers", pero el job sigue tardando lo
mismo. Causas habituales, en orden de frecuencia:

**1. Tienes menos particiones que cores.**
Es la causa número uno. Si tu DataFrame tiene 2 particiones y hay 6 cores, 4
quedan parados. Comprueba y corrige:

```python
print(df.rdd.getNumPartitions())
df = df.repartition(6)   # >= total de cores del cluster
```

**2. `spark.cores.max` está limitado.**
Si lo fijaste en 2, tu aplicación no tomará más de 2 cores aunque haya 6 libres.
Déjalo sin definir para que use todo el cluster, o súbelo.

**3. El executor no cabe en el worker.**
Si pides `spark.executor.memory=4g` pero cada worker anuncia 2 GB, el master no
puede colocar ningún executor ahí. La aplicación se queda en estado `WAITING`
para siempre. En la UI del master lo ves como una app aceptada pero sin
executors asignados. Regla práctica: `executor.memory` ≤ `SPARK_WORKER_MEMORY`
menos ~1 GB de overhead.

**4. El dataset es demasiado pequeño.**
Con unos pocos MB, el coste de repartir y volver a juntar supera al de procesar.
Spark empieza a rendir con volúmenes donde una sola máquina sufre.

---

## Escalado real: workers en otra máquina

Aquí sí ganas capacidad. En la máquina nueva, con Docker instalado:

```bash
docker run -d \
  --name spark-worker-remoto-1 \
  --restart unless-stopped \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=spark://IP_DEL_MASTER:7077 \
  -e SPARK_WORKER_MEMORY=4G \
  -e SPARK_WORKER_CORES=4 \
  -e SPARK_RPC_AUTHENTICATION_ENABLED=yes \
  -e SPARK_RPC_AUTHENTICATION_SECRET=<el SPARK_RPC_SECRET de tu .env> \
  -e SPARK_RPC_ENCRYPTION_ENABLED=yes \
  --network host \
  bitnami/spark:3.5.3
```

Tres cosas que te van a morder si no las miras:

- **El secreto RPC debe ser idéntico.** Cópialo tal cual del `.env` de la máquina
  del master. Si no coincide, el worker intenta registrarse, falla la
  autenticación y reintenta en bucle. En los logs del worker sale
  `Authentication failed`.
- **El firewall de Windows bloquea el 7077 por defecto.** Ábrelo en la máquina
  del master:
  ```powershell
  New-NetFirewallRule -DisplayName "Spark Master" -Direction Inbound -LocalPort 7077 -Protocol TCP -Action Allow
  ```
- **El driver tiene que ser alcanzable de vuelta.** Esto sorprende a mucha gente:
  los executors abren conexión *hacia* el driver, que en tu caso corre dentro del
  contenedor del worker de Airflow. Con `--network host` en el worker remoto y
  `spark.driver.host` apuntando a la IP real de la máquina de Airflow suele
  bastar, pero es la parte más frágil del montaje. Si los executors arrancan y
  mueren enseguida sin mensaje claro, empieza por aquí.

Si vas a llegar a este punto de forma estable, merece la pena mirar Spark on
Kubernetes en vez de standalone: resuelve el descubrimiento y el ciclo de vida
de los executors por ti.

---

## Limitar recursos de verdad

`SPARK_WORKER_MEMORY=2G` le dice al worker **cuánta memoria anunciar al master**.
No es un límite del contenedor: si un executor se pasa, el contenedor se lleva
toda la RAM que pueda de la VM de WSL2 y arrastra al resto del stack.

Para poner un límite real, añade en el servicio `spark-worker`:

```yaml
    deploy:
      replicas: ${SPARK_WORKER_REPLICAS:-1}
      resources:
        limits:
          cpus: '2'
          memory: 3G
```

Deja el límite del contenedor ~1 GB por encima de `SPARK_WORKER_MEMORY`: la JVM
necesita espacio para heap off-heap, metaspace y buffers de red. Si los igualas,
el OOM killer mata el contenedor antes de que Spark pueda quejarse.

---

## Resumen

```powershell
# 1. Ver estado
docker compose -f docker-compose.windows.yml ps spark-worker

# 2. Escalar en caliente
docker compose -f docker-compose.windows.yml up -d --scale spark-worker=3

# 3. Verificar
#    http://localhost:8082  ->  "Alive Workers: 3"

# 4. Dejarlo fijo
#    .env  ->  SPARK_WORKER_REPLICAS=3
```

Y antes de escalar, la pregunta que ahorra tiempo: **¿tengo particiones
suficientes para llenar los cores que voy a añadir?** Si la respuesta es no,
añadir workers no cambia nada.
