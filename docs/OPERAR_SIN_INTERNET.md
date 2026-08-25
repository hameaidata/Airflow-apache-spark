# Levantar y operar sin Internet

Guía completa: desde guardar las imágenes en la máquina que todavía tiene
Internet, hasta tener la plataforma corriendo en la red aislada del banco.

> **Esta guía viaja dentro del paquete.** En el servidor aislado no habrá forma
> de consultarla en otro sitio. Está en `proyecto/docs/OPERAR_SIN_INTERNET.md`.

---

## Lo primero: lo que hay que hacer ANTES del corte

Hay cosas que solo se pueden hacer con Internet. Si el corte llega antes,
quedan bloqueadas hasta que alguien consiga una máquina con salida.

| # | Tarea | Por qué no se puede después |
|---|---|---|
| 1 | **Construir `airflow-bsg:2.11.2`** | Descarga Spark, Java, 5 jars y ~40 paquetes de pip |
| 2 | **Descargar las 4 imágenes base** | Vienen de Docker Hub |
| 3 | **Generar el paquete y verificarlo** | Necesita las imágenes del paso 1 y 2 |
| 4 | **Probar el paquete en una máquina limpia** | Es la única prueba real de que sirve |

El paso 4 es el que se salta todo el mundo y el que más caro sale. Cargar el
paquete en una máquina que nunca vio este proyecto es la única forma de
descubrir que faltaba algo **mientras todavía se puede arreglar**.

---

## Parte 1 — En la máquina CON Internet

### 1.1 Construir la imagen

```bash
./scripts/construir_imagen.sh          # Linux
.\scripts\construir_imagen.ps1         # Windows
```

Tarda de 15 a 25 minutos. Al terminar debe existir:

```bash
docker images airflow-bsg:2.11.2
```

**Si el build falla en el paso del ODBC de Microsoft** (`packages.microsoft.com`
bloqueado), reintente con `--sin-odbc` / `-SinOdbc`. Solo pierde la
autenticación integrada de AD contra SQL Server; usuario y contraseña siguen
funcionando. Es preferible un paquete sin ODBC que ningún paquete.

### 1.2 Comprobar los drivers ANTES de empaquetar

No empaquete una imagen sin verificarla. Levántela aquí y compruebe:

```bash
docker compose -f docker-compose.ubuntu.yml up -d
docker compose -f docker-compose.ubuntu.yml exec airflow-webserver \
    python /opt/airflow/verificar_drivers.py
```

Las cinco filas deben salir en verde. Si algo falla aquí, falla también allá,
pero aquí se puede arreglar.

### 1.3 Generar el paquete

```bash
./scripts/preparar-bundle-offline.sh /ruta/al/traslado
.\scripts\preparar-bundle-offline.ps1 -Destino D:\traslado\bundle
```

Produce esta estructura:

```
bundle-offline/
├── imagenes/
│   ├── airflow-bsg.tar.gz          la imagen propia, la más pesada
│   ├── postgres_16-alpine.tar.gz
│   ├── redis_7-alpine.tar.gz
│   ├── apache_spark_3.5.3.tar.gz
│   └── busybox_1.36.tar.gz         imprescindible; sin ella no arranca
├── proyecto/                        DAGs, plugins, scripts, compose, docs
├── cargar_bundle.sh                 se ejecuta en el destino
├── verificar_bundle.sh              comprueba el traslado
└── MANIFIESTO.txt                   sumas SHA-256
```

Mida el tamaño real antes de decidir el medio de traslado:

```bash
du -sh bundle-offline/
```

Ronda los 2 a 3 GB comprimido. Un USB basta; un correo, no.

### 1.4 La prueba que no se debe saltar

En **otra máquina**, o al menos con las imágenes borradas:

```bash
docker rmi airflow-bsg:2.11.2 postgres:16-alpine redis:7-alpine \
           apache/spark:3.5.3 busybox:1.36
cd /ruta/al/traslado && ./cargar_bundle.sh
cd proyecto && ./setup.sh
docker compose -f docker-compose.ubuntu.yml up -d
```

Si esto funciona con las imágenes borradas, funcionará sin Internet.

---

## Parte 2 — El traslado

Antes de copiar, y **otra vez después de copiar**:

```bash
./verificar_bundle.sh
```

Compara las sumas SHA-256 del manifiesto contra los archivos reales. Un
archivo truncado en un USB produce, días después, un `docker load` que falla
sin explicar por qué. Treinta segundos aquí ahorran esa tarde.

> **Los secretos no viajan.** El paquete excluye `.env` a propósito. El
> servidor aislado genera los suyos con `setup.sh`. Eso es lo correcto: las
> claves de desarrollo no deben existir en producción.

---

## Parte 3 — En el servidor aislado

### 3.1 Cargar las imágenes

```bash
./verificar_bundle.sh      # primero, comprobar integridad
./cargar_bundle.sh         # después, cargar
```

El cargador hace tres cosas: descomprime y carga cada imagen, **normaliza los
nombres** (ver abajo) y lista lo cargado.

> **Por qué se normalizan los nombres.** Docker asume Docker Hub cuando ve
> `postgres:16-alpine`. Podman no: puede dejar la imagen como
> `localhost/postgres:16-alpine` o `docker.io/library/postgres:16-alpine`. El
> compose dice `postgres:16-alpine` a secas, así que si el nombre no coincide
> el arranque falla con `short-name resolution failed`. El cargador vuelve a
> etiquetar, que es inofensivo en Docker y necesario en Podman.

Compruebe que aparecen las cinco:

```bash
docker images | grep -E "airflow-bsg|postgres|redis|spark|busybox"
```

### 3.2 Generar la configuración

```bash
cd proyecto
./setup.sh
```

`setup.sh` genera un `.env` con **secretos nuevos**: clave Fernet, contraseña
de PostgreSQL, clave de Redis, `AIRFLOW_UID` igual a su usuario.

> **Nunca copie `.env.ubuntu` a `.env` directamente.** Ese archivo es una
> plantilla y trae una clave Fernet fija, escrita en el repositorio. Usarla en
> producción significa cifrar las credenciales del banco con una clave que
> cualquiera que vea el repositorio conoce.

Confirme dos líneas antes de seguir:

```bash
grep -E "^AIRFLOW_IMAGE=|^AIRFLOW_UID=" .env
# AIRFLOW_IMAGE=airflow-bsg:2.11.2      <- la imagen propia, no la oficial
# AIRFLOW_UID=1000                       <- debe ser igual a  id -u
```

Y confirme que esta línea está **vacía**:

```bash
grep PIP_ADDITIONAL .env
# PIP_ADDITIONAL_REQUIREMENTS=
```

Si alguien la rellena, los contenedores intentarán instalar de PyPI en cada
arranque y fallarán en bucle. Sin Internet, ese bucle no tiene salida.

### 3.3 Levantar

```bash
docker compose -f docker-compose.ubuntu.yml up -d
```

**Nunca ejecute `docker compose pull`.** Intentaría salir a Internet y
fallaría. `up -d` usa las imágenes ya cargadas.

### 3.4 Verificar

```bash
# 1. Todo arriba y sano
docker compose -f docker-compose.ubuntu.yml ps

# 2. Los drivers, dentro del contenedor
docker compose -f docker-compose.ubuntu.yml exec airflow-webserver \
    python /opt/airflow/verificar_drivers.py

# 3. Spark ve a sus workers
curl -s http://localhost:8080 | grep -o "Alive Workers.*" | head -1
```

Las cinco filas de drivers en verde y los workers vivos: eso es estar arriba.

### 3.5 Aplicar los roles

```bash
./scripts/crear_roles.sh --simular    # revisar primero
./scripts/crear_roles.sh --aplicar
```

---

## Parte 4 — Lo que la red aislada SÍ necesita

Aislado no es desconectado. Estas cuatro cosas tienen que funcionar dentro de
la red del banco, y conviene confirmarlas con TI **antes** de levantar:

| Necesidad | Comprobación | Si falta |
|---|---|---|
| **DNS interno** | `getent hosts servidor-db2` | Las Connections fallan con *name or service not known* |
| **NTP** | `timedatectl status` | DAGs que no disparan, o disparan dos veces |
| **Puertos a las BD** | `nc -zv servidor-db2 50000` | Sin origen ni destino, la plataforma no hace nada |
| **SMTP interno** | `nc -zv smtp.banco.local 25` | Nadie se entera de un fallo a las 3 de la mañana |

El SMTP merece atención aparte: **hoy no está configurado**. Sin él, Airflow
no puede avisar de un DAG fallido y alguien tiene que mirar la pantalla. Pida
a TI el relé SMTP interno y añádalo al `.env`.

---

## Parte 5 — Lo que NO va a funcionar, y está bien

Conviene saberlo de antemano para no perder tiempo diagnosticando lo que no
está roto.

| Qué se ve | Por qué | ¿Problema? |
|---|---|---|
| La página de error de Airflow sin estilos | `traceback.html` carga Bootstrap de un CDN | **No.** Solo cosmético, y solo en la página de excepción |
| `docker compose pull` falla | No hay Internet | **No.** No se debe usar |
| No se puede reconstruir la imagen | El build necesita apt, pip y Maven | **No.** Se reconstruye fuera y se vuelve a trasladar |
| `pip install` dentro del contenedor falla | No hay PyPI | **No.** Todo lo necesario ya está en la imagen |

**Airflow 2.11.2 no llama a casa.** Se revisó el código fuente: no hay
telemetría, no hay recolección de uso. El analytics de Segment existe en las
plantillas pero su valor por defecto es nulo y solo se carga si alguien
configura `AIRFLOW__WEBSERVER__ANALYTICS_TOOL`. No lo configure. Es una
respuesta que sirve tal cual ante una auditoría de seguridad.

---

## Parte 6 — Operación continua

### Actualizar algo

Todo cambio de imagen sigue el mismo ciclo, y **empieza fuera**:

```
maquina CON Internet          traslado          servidor aislado
──────────────────────        ────────          ─────────────────
1. editar Dockerfile
2. construir_imagen.sh
3. verificar drivers
4. preparar-bundle-offline    ──USB──>          5. verificar_bundle.sh
                                                6. cargar_bundle.sh
                                                7. up -d --force-recreate
```

Cambiar solo un **DAG** o un **script de Spark** no necesita nada de esto: son
archivos montados desde el disco, y basta copiarlos a `airflow/dags/` o
`spark/jobs/`. Airflow los recoge solo en menos de un minuto.

Esa distinción importa: **el 90% de los cambios del día a día son DAGs**, y
esos no requieren traslado de imágenes.

### Respaldo

Lo único irreemplazable es la base de metadatos: Connections, Variables,
usuarios, roles e historial.

```bash
docker compose -f docker-compose.ubuntu.yml exec -T postgres \
    pg_dump -U airflow airflow | gzip > respaldo_$(date +%F).sql.gz
```

> **Guarde el `.env` junto al respaldo, en el mismo lugar seguro.** Las
> Connections están cifradas con la clave Fernet del `.env`. Un respaldo sin
> esa clave es un archivo de credenciales que nadie puede descifrar — incluido
> usted.

### Limpiar

Con el límite de 10 GB acordado con TI, dos tareas periódicas:

```bash
# Historial de Airflow anterior a 90 dias
docker compose -f docker-compose.ubuntu.yml exec airflow-webserver \
    airflow db clean --clean-before-timestamp "$(date -d '90 days ago' +%F)" --yes

# Parquet procesados, con mas de 30 dias
docker compose -f docker-compose.ubuntu.yml exec airflow-worker \
    find /opt/spark-data/parquet -type f -mtime +30 -delete
```

> **Ojo con la ruta.** Los parquet **no** están en una carpeta del host: viven
> en el volumen nombrado `spark_data`, montado en `/opt/spark-data` dentro de
> los contenedores. Un `find ./spark/data/...` desde el host no encuentra nada
> y da la falsa impresión de que ya está limpio. Para ver cuánto ocupa:
>
> ```bash
> docker compose -f docker-compose.ubuntu.yml exec airflow-worker \
>     du -sh /opt/spark-data/parquet
> ```

---

## Apéndice — Diagnóstico rápido

| Mensaje | Causa real | Solución |
|---|---|---|
| `short-name resolution failed` | Podman no resuelve nombres cortos | Vuelva a ejecutar `cargar_bundle.sh`, que re-etiqueta |
| `manifest unknown` / `pull access denied` | Algo intentó descargar | Una imagen no se cargó: `docker images` y compare con las 5 |
| `bash: $'\r': command not found` | El `.sh` viajó con saltos de línea de Windows | `dos2unix scripts/*.sh` |
| `Permission denied` en logs | `AIRFLOW_UID` no coincide con `id -u` | Corríjalo en `.env` y `up -d --force-recreate` |
| `Permission denied` en un volumen, con permisos correctos | Etiqueta de SELinux ausente | Añada `:Z` (o `:z` si es compartido) al montaje |
| Los contenedores reinician en bucle | `PIP_ADDITIONAL_REQUIREMENTS` con valor | Vacíela y recree |
| Spark queda en `WAITING` para siempre | `executor_memory` mayor que la memoria del worker | Bájela; el master nunca coloca el executor y no lo dice |
| Las Connections no descifran | La clave Fernet cambió | Restaure el `.env` original, o vuelva a crear las Connections |
| Un DAG no aparece | Error de importación | `docker compose ... exec airflow-scheduler airflow dags list-import-errors` |

---

## Lista final de comprobación

Antes de dar por terminada la instalación:

```
[ ] Las 5 imagenes cargadas (incluida busybox)
[ ] .env generado por setup.sh, NO copiado de .env.ubuntu
[ ] AIRFLOW_IMAGE=airflow-bsg:2.11.2
[ ] AIRFLOW_UID igual a  id -u
[ ] PIP_ADDITIONAL_REQUIREMENTS vacia
[ ] verificar_drivers.py: 5 filas en verde
[ ] Spark con sus workers vivos
[ ] Los 7 roles aplicados
[ ] Connections creadas y probadas contra las BD reales
[ ] DNS resuelve los servidores de base de datos
[ ] NTP sincronizado
[ ] SMTP interno configurado para las alertas
[ ] Respaldo de PostgreSQL probado (y restaurado una vez, de prueba)
[ ] El .env guardado junto al respaldo, en lugar seguro
[ ] Tarea periodica de  airflow db clean  programada
```

El punto del respaldo dice "y restaurado una vez, de prueba" a propósito. Un
respaldo que nunca se restauró no es un respaldo: es un archivo del que se
supone algo.
