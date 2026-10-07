# Documentación del DataHub

Veintisiete documentos, ordenados por **cuándo los va a necesitar**, no por
tema. Si busca algo concreto, empiece por la sección que describe el momento en
que está.

Este índice existe porque hasta el 2026-09-30 diez de estos documentos no
estaban enlazados desde ningún sitio. Existían, estaban al día, y no había
forma de llegar a ellos salvo listando la carpeta. Cada documento nuevo que se
agregue aquí se anota en la tabla que le corresponda, o vuelve a perderse.

---

## Empezando

| Documento | Léalo cuando |
|---|---|
| [ESTADO_DEL_PROYECTO.md](ESTADO_DEL_PROYECTO.md) | Se incorpora al proyecto y necesita saber qué está hecho y qué no |
| [REVISION_ARQUITECTURA.md](REVISION_ARQUITECTURA.md) | Quiere entender por qué la plataforma está armada así |
| [Diagrama_Pipeline_Airflow_Spark.md](Diagrama_Pipeline_Airflow_Spark.md) | Necesita el documento técnico formal, el que se presenta |
| [CONVENCIONES.md](CONVENCIONES.md) | **Antes de escribir su primer DAG o su primera Connection** |

`CONVENCIONES.md` no es opcional: define los prefijos `AF_` y `SPK_` de las
conexiones, los nombres de los `dag_id`, las etiquetas de cadencia y los
argumentos obligatorios. Hay una suite de tests que lo hace cumplir
(`airflow/tests/unit/test_convenciones.py`), así que saltárselo se nota en CI,
no en producción.

---

## Construir algo

| Documento | Léalo cuando |
|---|---|
| [CREAR_UN_DAG.md](CREAR_UN_DAG.md) | Va a escribir un DAG nuevo |
| [GUIA_CONEXIONES_DAGS.md](GUIA_CONEXIONES_DAGS.md) | No sabe cómo pedirle credenciales a Airflow sin escribirlas en el archivo |
| [PLUGIN_SPARK.md](PLUGIN_SPARK.md) | Su DAG tiene que lanzar un job de Spark |
- [CONFIG_RUNTIME_SPARK.md](CONFIG_RUNTIME_SPARK.md) - el pipeline Spark: la Variable BT2SQL_SPARK, las conexiones Generic y el control de publicacion del JSON puente. Lealo si cambio una Variable y el job siguio usando la anterior.
- [VALIDAR_PIPELINE_SPARK.md](VALIDAR_PIPELINE_SPARK.md) - los comandos para validar el pipeline Spark en orden, de `compose config` a comparar contra el pipeline de pandas.
- [CAPAS_ODS_BDS.md](CAPAS_ODS_BDS.md) - que pasa cuando una tabla de ODS o BDS falla: por que la capa ODS termina igual, como BDS continua solo con lo que paso, y por que la corrida acaba en rojo de todas formas.
| [PIPELINE_BT2SQL.md](PIPELINE_BT2SQL.md) | Trabaja sobre Bantotal → Parquet → STG en SQL Server |
| [TABLAS_DE_CONTROL_Y_LOGS.md](TABLAS_DE_CONTROL_Y_LOGS.md) | Necesita saber qué tabla de control escribe cada proceso |
| [MODELO_CONTROL_UNIFICADO.md](MODELO_CONTROL_UNIFICADO.md) | Va a tocar el modelo de parámetros y logs. **Tiene cinco decisiones abiertas al final** |

---

## Conectarse a los orígenes

| Documento | Léalo cuando |
|---|---|
| [CONEXION_BANTOTAL_AS400_JDBC.md](CONEXION_BANTOTAL_AS400_JDBC.md) | Una consulta a Bantotal falla y no entiende el error |
| [CREDENCIALES_Y_HASHING.md](CREDENCIALES_Y_HASHING.md) | Alguien propone «guardar las contraseñas hasheadas» |

El `-204` de DB2 for i suele ser un desajuste entre `naming=sql` y
`naming=system`, no una tabla que falta. Está explicado en el primero.

---

## Operar

| Documento | Léalo cuando |
|---|---|
| [GESTION_USUARIOS.md](GESTION_USUARIOS.md) | Hay que dar de alta a alguien |
| [DISENO_ROLES.md](DISENO_ROLES.md) | Quiere entender qué puede hacer cada rol y por qué |
| [ESCALAR_SPARK_WORKERS.md](ESCALAR_SPARK_WORKERS.md) | Los jobs se encolan y hace falta más capacidad |
| [DIAGNOSTICO_SPARK.md](DIAGNOSTICO_SPARK.md) | Spark no levanta, un job muere con `No suitable driver`, o un DAG sale roto con un `ImportError` de algo que sí está en el archivo |
| [COMANDOS_PODMAN_DOCKER.md](COMANDOS_PODMAN_DOCKER.md) | Está en un servidor con Podman y tiene los dedos acostumbrados a Docker |

---

## Desplegar

| Documento | Léalo cuando |
|---|---|
| [README_DOCKER.md](README_DOCKER.md) | Necesita saber en qué se diferencian los compose de Windows y Linux |
| [RHEL_SIN_DOCKER.md](RHEL_SIN_DOCKER.md) | El servidor es RHEL 9 y no le dejan instalar Docker |
| [README-PODMAN-DOCKER.md](README-PODMAN-DOCKER.md) | Tiene que argumentar Podman frente a Docker |
| [README-NGINX.md](README-NGINX.md) | Va a poner el proxy inverso delante de las tres interfaces |
| [TLS_CERTIFICADOS.md](TLS_CERTIFICADOS.md) | El banco le entregó un certificado y hay que instalarlo |

---

## La red aislada

Tres documentos y **uno de ellos tiene fecha de caducidad**.

| Documento | Léalo cuando |
|---|---|
| [ANTES_DE_PERDER_INTERNET.md](ANTES_DE_PERDER_INTERNET.md) | **Todavía tiene Internet.** Después ya no sirve de nada |
| [OPERAR_SIN_INTERNET.md](OPERAR_SIN_INTERNET.md) | Ya está del otro lado y tiene que levantar el stack |
| [README-REQUERIMIENTOS-FUNCIONAMIENTO.md](README-REQUERIMIENTOS-FUNCIONAMIENTO.md) | Tiene que explicarle a otra área qué hace falta para operar aislado |

El primero es una lista de pasos que sólo se pueden dar con red: construir las
dos imágenes propias, verificar que los drivers quedaron dentro y armar el
paquete de siete `.tar`. Ninguno de esos pasos tiene equivalente en el servidor
aislado — las imágenes bajan drivers de Maven al construirse.

---

## Para pedir infraestructura

| Documento | Léalo cuando |
|---|---|
| [README-TI.md](README-TI.md) | Va a solicitar servidores para 10–100 GB/día, RHEL con Podman |
| [README-TI-REDUCIDO.md](README-TI-REDUCIDO.md) | La misma solicitud, pero con volumen de 10 GB y Docker sobre Linux |

Son alternativas, no versiones: elija una según el volumen que vaya a defender.

---

## La propuesta de ingeniería de datos

[`propuesta-ingenieria-datos/`](propuesta-ingenieria-datos/) es un documento
aparte, de doce capítulos más plantillas de CI/CD. Describe cómo *debería*
trabajar el área, no cómo está montada esta plataforma. Empiece por su propio
[README](propuesta-ingenieria-datos/README.md).

Hasta el 2026-09-30 había una copia completa de estos capítulos dentro de
`sql/`, byte por byte. Está en `_archivo/sql-duplicados/` por si acaso, pero no
hay nada ahí que no esté aquí.

---

## Lo que no está en esta carpeta

El **DDL y los stored procedures** viven en [`sql/`](../sql/):
`sql/bt2sql/` y `sql/s2sql/` para las tablas de control, `sql/sql_sp/` para los
procedimientos originales del DataHub y `sql/sp_reescritos/` para las versiones
reescritas en los dos dialectos, con su script de comparación.

Los **tests** viven en `airflow/tests/unit/`. No son sólo pruebas: varios hacen
cumplir decisiones que de otro modo se olvidan — que el `.env` cubra lo que los
compose piden, que el paquete offline lleve las dos imágenes propias, que
ningún DAG traiga credenciales en texto plano.

El **README de la raíz** es la puerta de entrada al repositorio completo.
