# Plataforma Airflow + Spark

Orquestación de procesos ETL: lee de SQL Server y DB2, procesa con Spark, y
escribe el resultado en la base de datos de destino.

**Este archivo responde las preguntas de todos los días.** Si no está aquí, el
índice del final dice en qué documento buscar.

---


## Arrancar

```powershell
.\setup.ps1                                          # solo la primera vez
docker compose -f docker-compose.windows.yml up -d
```

En Linux: `./setup.sh` y `docker-compose.ubuntu.yml`.

Usuario y contraseña los imprime el setup y quedan en el `.env`.

```powershell
docker compose -f docker-compose.windows.yml ps      # ¿está todo arriba?
docker compose -f docker-compose.windows.yml down    # apagar
```

---

## Los paneles: dónde veo qué

| Panel | Dirección | Para qué lo abres |
|---|---|---|
| **Airflow** | http://localhost:8080 | El principal. DAGs, ejecuciones, logs, credenciales, auditoría |
| **Flower** | http://localhost:5555 | Ver los workers de Airflow y qué tarea corre cada uno |
| **Spark Master** | http://localhost:8082 | Ver cuántos workers de Spark hay y cuántos núcleos libres |

### Dentro de Airflow, dónde está cada cosa

| Necesito… | Dónde |
|---|---|
| Ver por qué falló una tarea | Clic en el DAG → clic en el cuadro rojo → **Logs** |
| Ejecutar un DAG a mano | Clic en el DAG → botón **Trigger** |
| Reintentar una tarea fallida | Clic en el cuadro → **Clear** (vuelve a correr esa y las siguientes) |
| Pausar un proceso | Interruptor a la izquierda del nombre del DAG |
| Guardar una credencial | **Admin → Connections** |
| Guardar un parámetro | **Admin → Variables** |
| Ver quién hizo qué | **Browse → Audit Logs** |
| Ver si un DAG tiene errores | **Browse → DAG Import Errors** |
| Gestionar usuarios y permisos | **Security → List Users / List Roles** |

### Lo que hoy NO se puede ver

**El detalle de un trabajo de Spark en ejecución** (etapas, tareas, plan de
consulta). El panel de Spark Master muestra la aplicación corriendo, pero el
enlace a su detalle apunta a un puerto dentro del contenedor de Airflow que no
está publicado.

Para tenerlo hay dos caminos, ninguno hecho todavía:

- Publicar el puerto 4040 del worker de Airflow — sirve solo mientras el trabajo
  corre
- Levantar un History Server de Spark — conserva el histórico, requiere activar
  el registro de eventos

Con volúmenes de 10 GB rara vez hace falta. Cuando un trabajo empiece a tardar
más de lo razonable, ahí sí conviene montarlo.

---

## Puertos: cuáles y por qué

### Los que abres en el navegador

| Puerto | Servicio | Por qué existe |
|---|---|---|
| 8080 | Airflow | Es la interfaz de trabajo |
| 5555 | Flower | Ver los workers de Celery |
| 8082 | Spark Master | Ver el estado del cluster de Spark |

### Los que no se publican

| Puerto | Servicio | Nota |
|---|---|---|
| 6379 | Redis | Cola interna. Solo entre contenedores |
| 8974 | Salud del planificador | Uso interno |
| 8081 | Interfaz del worker de Spark | Uso interno |

### Los que están abiertos y conviene revisar

Estos dos se publican en `0.0.0.0`, es decir, quedan accesibles desde cualquier
equipo que alcance al servidor:

| Puerto | Servicio | Qué expone |
|---|---|---|
| 5432 | PostgreSQL | La base de metadatos: credenciales cifradas e historial completo |
| 7077 | Envío de trabajos a Spark | Permite enviar trabajos al cluster |

**No es un problema en desarrollo local.** Sí conviene decidirlo antes de
producción, sobre todo el 5432.

Ninguno de los dos hace falta desde la red para que la plataforma funcione: la
base la usan los contenedores entre sí, y el envío a Spark también. Se publican
por comodidad —conectar un cliente de base de datos, hacer un `spark-submit`
manual— y esa comodidad se conserva enlazándolos a `127.0.0.1`:

```yaml
- "127.0.0.1:${POSTGRES_PORT:-5432}:5432"
- "127.0.0.1:${SPARK_MASTER_PORT:-7077}:7077"
```

Con eso siguen accesibles desde el propio servidor, y desde tu equipo por túnel:

```bash
ssh -L 5432:127.0.0.1:5432 usuario@servidor
```

> **Este cambio NO está aplicado.** Queda anotado como recomendación para que lo
> decidas tú. El compose actual publica ambos puertos en `0.0.0.0`, tal como lo
> levantaste.

### Los que hay que pedirle a TI — salida

| Puerto | Hacia dónde | Por qué |
|---|---|---|
| 1433 | SQL Server | Leer los datos de origen |
| *por definir* | DB2 | Leer los datos de origen — depende de la variante |
| *por definir* | Base de destino | Escribir los resultados |
| 53 | DNS interno | Resolver nombres. Sin esto no conecta a nada |
| **123** | **NTP interno** | **Sincronizar reloj. Con más de 5 min de desfase, Kerberos rechaza todo** |
| 389 / 636 | Directorio Activo | Login de usuarios, si se usa LDAP |
| 88 / 464 | Kerberos | Tickets, si se usa autenticación integrada |
| 25 / 587 | Correo interno | Alertas cuando algo falla |

Detalle completo en `README-TI.md` (sección 6).

---

## Seguridad: qué está protegido y qué no

### Lo que ya está

| Qué | Cómo |
|---|---|
| Contraseñas de usuarios de la interfaz | Hasheadas con scrypt y salt aleatorio. Lo hace Airflow solo |
| Credenciales de las bases de datos | Cifradas en la base de metadatos (AES-128-CBC + HMAC-SHA256) |
| Secretos en los logs | Enmascarados como `***` |
| Permisos por usuario | RBAC con roles: Admin, Op, User, Viewer |
| Creación automática de cuentas | Desactivada — nadie entra por su cuenta |
| Cookies de sesión | `HttpOnly` y `SameSite=Lax` |
| Redis y puertos de uso interno | Sin publicar al host |

### Lo que falta

| Qué | Impacto | Prioridad |
|---|---|---|
| **TLS** | La interfaz y la base van en claro por la red | **Alta** |
| **Logs remotos** | Si se reduce el número de workers, sus logs desaparecen con el contenedor | **Alta** |
| Gestor de secretos externo | Las credenciales viven en la base de metadatos | Media |
| LDAP / Directorio Activo | Usuarios locales en vez de cuentas corporativas | Media |
| PostgreSQL publicado en `0.0.0.0` | Alcanzable desde la red — ver sección de puertos | Media |

> **Sobre el TLS.** Es lo primero que va a observar una revisión de seguridad.
> Una credencial perfectamente cifrada en reposo que después viaja en claro por
> la red no está protegida.

> **Sobre los logs remotos.** Los workers son efímeros. Si escalas hacia abajo,
> el contenedor se va y sus logs con él, y la interfaz mostrará "log file not
> found" para tareas que sí se ejecutaron. En un entorno regulado eso es un
> hallazgo de auditoría.

### Si Seguridad pregunta si las contraseñas se pueden hashear

La respuesta corta: **las de conexión a bases de datos no, y no es una decisión
que se pueda revisar.** Un hash es irreversible; Airflow necesita presentar la
contraseña al servidor para autenticarse. Hashear sirve para *verificar*, no
para *presentar*.

`docs/CREDENCIALES_Y_HASHING.md` tiene la explicación completa, las alternativas
que sí elevan el control, y un párrafo redactado para pasarles.

---

## La data en Parquet: qué es y qué se hace con ella

**Parquet es un formato de archivo**, no una base de datos. Guarda los datos por
columnas y comprimidos: un CSV de 10 GB queda en unos 3 GB, y leer dos columnas
no obliga a leer el archivo entero.

### En este proyecto es un paso intermedio, no el destino

```
SQL Server / DB2  →  Parquet (temporal)  →  Base de datos de destino
    origen              en el servidor           destino final
```

El resultado que consume el negocio vive en la **base de datos de destino**. El
Parquet existe mientras el proceso corre y un tiempo después.

### Preguntas concretas

| Pregunta | Respuesta |
|---|---|
| **¿Dónde está?** | En el volumen `spark_data`, montado como `/opt/spark-data` dentro de los contenedores |
| **¿Cuánto se conserva?** | Lo que definas. Propuesto: 30 días, para poder reprocesar sin volver a golpear el origen |
| **¿Se respalda?** | **No hace falta.** Es regenerable: si se pierde, se vuelve a extraer del origen |
| **¿Se puede consultar?** | Sí, con Spark o con pandas. **No con SQL directamente** |
| **¿Lo puedo abrir en Excel?** | No directamente. Hay que convertirlo a CSV primero |
| **¿Quién lo borra?** | Nadie todavía — **falta programar la limpieza** (ver pendientes) |

### Verlo desde tu equipo

```powershell
# Listar lo que hay
docker compose -f docker-compose.windows.yml exec airflow-worker `
  find /opt/spark-data -name "*.parquet" | head -20

# Ver el contenido de un archivo
docker compose -f docker-compose.windows.yml exec airflow-worker python -c `
  "import pandas as pd; df = pd.read_parquet('/opt/spark-data/parquet/movimientos'); print(df.head()); print(df.shape)"
```

### Por qué Parquet y no CSV

Con 10 GB la diferencia ya se nota: ocupa un tercio, conserva los tipos de datos
—un CSV no distingue una fecha de un texto— y al filtrar por año o mes, Spark
salta las carpetas que no aplican en vez de leerlo todo.

Detalle en `docs/GUIA_CONEXIONES_DAGS.md`.

---

## Preguntas rápidas

**¿Cómo agrego un DAG nuevo?**
Copias el `.py` en `airflow/dags/production/` y esperas 30 segundos. Aparece
solo. Llega pausado a propósito: despáusalo con el interruptor.

**Mi DAG no aparece.**
```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow dags list-import-errors
```
Casi siempre es un error de importación. Airflow descarta el archivo entero en
silencio.

**¿Dónde guardo la contraseña de una base de datos?**
En **Admin → Connections**, nunca en Variables ni en el código. Las Variables se
ven en texto plano en la interfaz.

**¿Cómo pruebo que una conexión funciona?**
```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow connections test <id>
```

**¿Cómo agrego capacidad?**
```powershell
docker compose -f docker-compose.windows.yml up -d --scale airflow-worker=4
```
Sin parar nada. Detalle en `docs/ESCALAR_SPARK_WORKERS.md`.

**¿Cómo sé si el sistema está sano?**
`docker compose ps` — todos deben decir `healthy`. Y el DAG
`canary_auto_discovery` en verde confirma que la orquestación funciona.

**¿Dónde están los logs?**
Los de tarea, en la interfaz. Los de un contenedor:
`docker compose logs -f airflow-scheduler`

**Cambié un archivo y no pasa nada.**
Los DAGs se recargan solos. El `.env`, el compose y los plugins requieren
`docker compose up -d`.

---

## Índice de documentos

### Para pedir infraestructura
| Archivo | Contenido |
|---|---|
| `README-TI.md` | Solicitud de servidores. Volumen 10–100 GB/día, RHEL con Podman |
| `README-TI-REDUCIDO.md` | La misma solicitud con volumen de 10 GB y Linux con Docker |
| `README-REQUERIMIENTOS-FUNCIONAMIENTO.md` | Qué hace falta al cortar Internet |

### Para trabajar
| Archivo | Contenido |
|---|---|
| `docs/GUIA_CONEXIONES_DAGS.md` | Credenciales, SQL Server, DB2, procedimientos almacenados, Parquet |
| `airflow/dags/examples/` | Cinco DAGs de ejemplo comentados |
| `README_DOCKER.md` | Diferencias entre las versiones de Windows y Linux |

### Para operar
| Archivo | Contenido |
|---|---|
| `docs/ESCALAR_SPARK_WORKERS.md` | Añadir capacidad, y por qué a veces no acelera |
| `docs/DIAGNOSTICO_SPARK.md` | Qué hacer si Spark no levanta |
| `docs/ESTADO_DEL_PROYECTO.md` | Qué está verificado y qué no |

### Para Seguridad
| Archivo | Contenido |
|---|---|
| `docs/CREDENCIALES_Y_HASHING.md` | Por qué las contraseñas de conexión no se hashean, y qué ofrecer en su lugar |

> `INICIO_RAPIDO.md` y `README_MODULES.md` quedaron de una versión anterior y
> **describen un montaje que ya no existe** (mencionan un `.env.example` que se
> eliminó). Conviene borrarlos para que nadie los siga.

---

## Estado y pendientes

**Funciona:** el stack levanta, los DAGs se descubren solos, los workers escalan
sin downtime, los secretos están fuera de git.

**Sin verificar:** la construcción de la imagen propia, y la conexión real a
SQL Server y DB2.

**Decisiones abiertas** — bloquean lo demás:

1. **SQL Server**: ¿cuenta de servicio con autenticación SQL, o Kerberos? Con
   Kerberos, los ejemplos actuales no conectan: `pymssql` no soporta
   autenticación integrada
2. **DB2**: ¿LUW, AS/400 o mainframe? Cambia el controlador por completo
3. **Base de destino**: falta definir motor y servidor

**Pendientes técnicos**, por orden:

1. TLS
2. Logs remotos
3. Programar la limpieza del Parquet — hoy nada lo borra
4. `airflow db clean` periódico, con archivado previo
5. Reconstruir los módulos de `airflow/plugins/` (están vacíos)
6. Borrar `INICIO_RAPIDO.md` y `README_MODULES.md`
