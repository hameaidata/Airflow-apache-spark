# Requerimientos de funcionamiento en red aislada

**Ambiente:** Desarrollo (aplicable también a Producción)
**Escenario:** el servidor pierde la salida a Internet y opera solo en la red
interna del banco

Este documento responde a una pregunta concreta: **qué deja de funcionar cuando
se corta Internet, y qué hace falta para que todo siga operando.**

---

## 1. Lo primero: la plataforma en marcha no necesita Internet

Conviene separarlo desde el inicio, porque suele generar alarma innecesaria:

| Momento | ¿Necesita Internet? |
|---|---|
| **Ejecutar los procesos ETL** | **No.** Todo ocurre contra la red interna |
| **Interfaz web de Airflow** | **No.** Se sirve desde el propio servidor |
| **Conectarse a SQL Server / DB2** | **No.** Son servidores internos |
| **Instalar la plataforma la primera vez** | **Sí**, salvo que se prepare por adelantado |
| **Actualizar versiones o añadir librerías** | **Sí**, salvo que haya repositorios internos |

Es decir: el corte de Internet **no afecta la operación diaria**. Afecta la
instalación y el mantenimiento. Ambos se resuelven, y este documento explica
cómo.

---

## 2. Qué se rompe exactamente al cortar Internet

Auditamos el proyecto y encontramos **siete puntos** que descargan de fuera.
Los listamos con nombre y apellido porque cada uno necesita una solución:

| # | Qué descarga | De dónde | Cuándo ocurre |
|---|---|---|---|
| 1 | Imágenes base de contenedor | Docker Hub | Al levantar por primera vez |
| 2 | Paquetes del sistema operativo | Repositorios de Debian | Al construir la imagen |
| 3 | Driver ODBC de Microsoft | `packages.microsoft.com` | Al construir la imagen |
| 4 | Distribución de Apache Spark | `dlcdn.apache.org` | Al construir la imagen |
| 5 | Controladores JDBC | `repo1.maven.org` | Al construir la imagen |
| 6 | Archivo de restricciones de Airflow | `raw.githubusercontent.com` | Al construir la imagen |
| 7 | Paquetes de Python | PyPI | Al construir la imagen |

**Seis de los siete ocurren al construir la imagen.** Esa es la buena noticia:
si la imagen se construye antes del corte, o fuera del banco, seis de los siete
problemas desaparecen de golpe.

### Cosas que también pueden fallar y no son obvias

**Validación de certificados TLS.** Al conectarse por HTTPS o LDAPS, el sistema
puede intentar consultar la lista de revocación del certificado (CRL/OCSP) en
una URL pública. Si esa consulta se queda esperando en lugar de fallar rápido,
las conexiones tardan **hasta 30 segundos cada una** antes de continuar. Se
resuelve configurando la CA interna correctamente, o desactivando la
comprobación de revocación cuando la CA es interna. Vale la pena tenerlo
anotado: cuando aparece, nadie sospecha del certificado.

**Enlaces externos en la interfaz de Airflow.** La documentación y algunos
iconos apuntan a sitios externos. No cargarán. Es puramente cosmético y no
afecta ninguna función.

**DAGs que consulten APIs externas.** Si algún proceso futuro necesita una API
de Internet, no funcionará. Con los orígenes y destinos actuales (todos
internos) no aplica.

---

## 3. La solución: preparar el paquete antes

Preparamos dos herramientas, ya incluidas en el proyecto:

### `scripts/preparar-bundle-offline.sh`

Se ejecuta **en una máquina con Internet** (tu equipo, o un servidor de
staging). Descarga y empaqueta absolutamente todo:

- Construye la imagen propia de Airflow y la exporta ya armada
- Descarga las cuatro imágenes base
- Descarga los tres controladores JDBC
- Descarga la distribución de Spark
- Descarga el archivo de restricciones
- Descarga los paquetes de Python **dentro de un contenedor de la misma imagen
  base**, para garantizar que sean compatibles
- Calcula sumas SHA-256 de todo
- Genera un `LEEME-INSTALACION.txt` con los pasos del otro lado

Produce un solo archivo `bundle-airflow-2.11.2-<fecha>.tar.gz` de unos **5–6 GB**.

```bash
# En la máquina CON Internet
./scripts/preparar-bundle-offline.sh
```

### `Dockerfile.offline`

Reconstruye la imagen dentro del banco usando el paquete, si la política exige
que las imágenes se construyan internamente. **En la mayoría de los casos no
hace falta**: la imagen ya viene construida en el paquete.

### El camino corto

```
Máquina con Internet          Canal autorizado          Servidor del banco
─────────────────────         ────────────────          ──────────────────
preparar-bundle-offline.sh
        │
        ▼
bundle-....tar.gz  ────────►  antivirus + traslado ──►  sha256sum -c
   (~5-6 GB)                                                  │
                                                              ▼
                                                       docker load
                                                              │
                                                              ▼
                                                    docker compose up -d
```

Sin descargar nada. Sin ningún repositorio interno. Es lo que recomendamos para
la primera puesta en marcha.

---

## 4. Lo que hay que solicitar a TI

### 4.1 Canal de traslado de archivos

Necesitamos poder introducir un archivo de **5–6 GB** al servidor.

| Ítem | Detalle |
|---|---|
| Mecanismo | El que el banco tenga establecido (repositorio de intercambio, comparte controlado, proceso de ingreso de software) |
| Tamaño | 5–6 GB por paquete |
| Frecuencia | Inicial, y luego con cada actualización (estimado: trimestral) |
| Análisis antivirus | Sí, previo al ingreso — solicitamos conocer el procedimiento |
| Verificación | Entregamos SHA-256 de cada archivo |

**Solicitamos que nos indiquen el procedimiento formal.** Es lo que más suele
demorar un despliegue aislado, y conviene resolverlo antes de necesitarlo.

### 4.2 Acceso al código fuente — indispensable en desarrollo

En desarrollo se modifican DAGs a diario. Sin acceso al repositorio, cada cambio
requeriría un traslado manual de archivos, lo que hace inviable el trabajo.

| Requisito | Detalle |
|---|---|
| **Servidor Git interno** | GitLab, Bitbucket, Azure DevOps — el que use el banco |
| Puerto | 443/TCP (HTTPS) o 22/TCP (SSH) desde el servidor de desarrollo |
| Cuenta | De servicio, con permiso de lectura sobre el repositorio del proyecto |
| Acceso de los desarrolladores | Al mismo repositorio, desde sus estaciones |

> Si el banco no dispone de Git interno, la alternativa es que los
> desarrolladores editen mediante una carpeta compartida montada en el
> servidor. Funciona, pero se pierde el historial de cambios y el control de
> versiones — algo que una auditoría de gestión de cambios va a observar.
> **Recomendamos resolver el acceso a Git.**

### 4.3 Repositorios internos — para el mantenimiento

El paquete resuelve la instalación. Para **mantener** la plataforma a lo largo
del tiempo sin depender de un traslado manual cada vez, conviene disponer de:

| Repositorio | Para qué | Prioridad |
|---|---|---|
| **Registro de imágenes** (Harbor, Quay, Artifactory, Nexus) | Alojar las imágenes; permite actualizar sin trasladar archivos | **Alta** |
| **Espejo de PyPI** (Artifactory, Nexus, devpi) | Añadir o actualizar librerías de Python | **Alta** |
| Espejo de Maven | Actualizar controladores JDBC | Media |
| Espejo de paquetes del SO | Parches del sistema operativo | Alta (probablemente ya existe) |

**Cuál importa más y por qué.** El espejo de PyPI es el que más se va a usar en
el día a día: cada vez que un proceso necesite una librería nueva —una de
cálculo, un cliente de algún sistema, un formato de archivo— sin espejo hay que
preparar un paquete y tramitar un traslado. Con espejo es un comando.

Si hay que priorizar una sola cosa, es esa.

---

## 5. Puertos y conexiones del ambiente de desarrollo

### 5.1 Entrantes

| Origen | Puerto | Protocolo | Uso |
|---|---|---|---|
| Estaciones del equipo de datos | 8080 | TCP | Interfaz web de Airflow |
| Estaciones del equipo de datos | 5555 | TCP | Panel de la cola de tareas |
| Estaciones del equipo de datos | 8082 | TCP | Consola de Spark |
| Estaciones del equipo de datos | 22 | TCP | Acceso SSH/SFTP |

> En desarrollo proponemos exponer las tres interfaces web directamente, sin
> proxy inverso ni TLS, para simplificar. Si la política del banco exige TLS
> incluso en desarrollo, se publica solo el 443 y el resto queda interno.

### 5.2 Salientes — dentro de la red interna

| Destino | Puerto | Protocolo | Uso | Criticidad |
|---|---|---|---|---|
| **Servidor Git interno** | 443 o 22 | TCP | Descarga del código de los DAGs | **Bloqueante** |
| **SQL Server (origen)** — ambiente de pruebas | 1433 | TCP | Lectura de datos | Bloqueante |
| **DB2 (origen)** — ambiente de pruebas | *por confirmar* | TCP | Lectura de datos | Bloqueante |
| **Base de datos de destino** — ambiente de pruebas | *por confirmar* | TCP | Escritura de resultados | Bloqueante |
| Servidores DNS internos | 53 | TCP + UDP | Resolución de nombres | **Bloqueante** |
| **Servidores NTP internos** | 123 | UDP | Sincronización horaria | **Bloqueante** |
| Controladores de dominio | 389, 636 | TCP | LDAP / LDAPS | Si aplica |
| KDC Kerberos | 88, 464 | TCP + UDP | Tickets | Si aplica |
| Registro interno de imágenes | 443 | TCP | Descarga de imágenes | Alta |
| Espejo de PyPI | 443 | TCP | Librerías de Python | Alta |
| Relay SMTP interno | 25 o 587 | TCP | Alertas | Media |
| Repositorio de parches del SO | 443 | TCP | Actualizaciones | Alta |

### 5.3 Salientes a Internet

**Ninguna.** Ese es el objetivo del aislamiento y no requiere excepciones.

> **Un punto a confirmar con Seguridad.** Si el servidor valida certificados
> TLS contra listas de revocación publicadas en Internet, y esas consultas
> quedan bloqueadas sin respuesta, cada conexión puede demorarse decenas de
> segundos. Hay dos salidas: que la CA interna publique su lista de revocación
> internamente, o desactivar la comprobación para certificados de CA interna.
> **Preguntar antes** ahorra un diagnóstico largo y confuso.

### 5.4 Diferencias respecto a producción

| Aspecto | Desarrollo | Producción |
|---|---|---|
| Acceso a Git | **Sí** — indispensable | Solo despliegues controlados |
| Interfaces web expuestas | Tres (Airflow, cola, Spark) | Solo Airflow, por HTTPS |
| TLS | Opcional | Obligatorio |
| Acceso SSH | El equipo de datos | Solo bastión |
| Bases de datos | Ambientes de pruebas | Producción |

> **Importante:** el ambiente de desarrollo debe apuntar a **bases de datos de
> pruebas**, nunca a producción. Un error en un DAG en desarrollo no puede
> tener consecuencias sobre datos reales. Si no existen ambientes de prueba de
> SQL Server y DB2, hay que solicitarlos — es un requisito de control de
> cambios antes que técnico.

---

## 6. Mantenimiento sin conexión

### 6.1 Añadir una librería de Python

Es la operación más frecuente del día a día.

**Con espejo de PyPI** (recomendado):
```bash
# Añadir al Dockerfile y reconstruir
docker build -t airflow-bsg:2.11.2 .
docker compose up -d
```

**Sin espejo:**
```bash
# En la máquina CON Internet, descargar dentro de la imagen base
docker run --rm -v "$PWD/wheels:/out" apache/airflow:2.11.2-python3.11 \
  pip download -d /out <paquete>

# Trasladar wheels/ y luego, en el servidor:
docker build -f Dockerfile.offline -t airflow-bsg:2.11.2 .
```

> **Descargar las ruedas dentro de un contenedor de la misma imagen base no es
> un capricho.** Si se descargan desde otro sistema operativo o versión de
> Python, algunas ruedas compiladas no serán compatibles, y el error aparece
> recién al instalar dentro del banco, cuando ya no hay Internet para
> corregirlo.

### 6.2 Actualizar la versión de Airflow

1. En la máquina con Internet: cambiar la versión en el `Dockerfile` y en
   `preparar-bundle-offline.sh`
2. Ejecutar el script para generar el paquete nuevo
3. Trasladar y cargar
4. **Respaldar la base de metadatos antes de actualizar**
5. `docker compose up -d` — Airflow ejecuta las migraciones necesarias

> Las migraciones de esquema de Airflow **no son reversibles**. Si algo sale
> mal, la vuelta atrás es restaurar el respaldo. Por eso el paso 4 no es
> opcional.

### 6.3 Parches del sistema operativo

Por el mecanismo estándar del banco, con su repositorio interno. No depende de
la plataforma.

Tras aplicar parches que afecten al motor de contenedores, verificar que los
servicios levantaron:
```bash
docker compose ps
```

### 6.4 Mantenimiento de la base de metadatos

Crece de forma continua con el historial de ejecuciones. Sin limpieza periódica
degrada el planificador.

```bash
# Simular primero — SIEMPRE
docker compose exec airflow-scheduler \
  airflow db clean --clean-before-timestamp '2026-01-01' --dry-run

# Ejecutar
docker compose exec airflow-scheduler \
  airflow db clean --clean-before-timestamp '2026-01-01'
```

> **Antes de limpiar, archivar.** En banca hay obligación de conservar el rastro
> de auditoría varios años. `airflow db clean` admite `--export-archived` para
> exportar antes de borrar. Coordinar la retención con Cumplimiento.

### 6.5 Rotación de certificados

Cuando venza el certificado TLS del servidor, reemplazar los archivos y
reiniciar el proxy inverso. Conviene dejar recordatorio: un certificado vencido
deja la interfaz inaccesible sin ningún mensaje que apunte a la causa.

### 6.6 Espacio en disco

Los tres consumos que crecen solos:

| Qué | Dónde | Control |
|---|---|---|
| Imágenes antiguas | `/var/lib/docker` | `docker image prune -a` periódico |
| Bitácoras de tareas | Volumen de logs | Retención configurada en Airflow |
| Área temporal de Spark | `/datos` | Se limpia al terminar cada proceso; verificar que no queden restos de procesos fallidos |

---

## 7. Lista de verificación

**Antes del corte de Internet**
- [ ] Ejecutar `preparar-bundle-offline.sh` y guardar el paquete
- [ ] Verificar que la imagen propia se construyó correctamente
- [ ] Guardar copia del paquete en un lugar accesible
- [ ] Confirmar que el `.env` apunta a `AIRFLOW_IMAGE=airflow-bsg:2.11.2`
- [ ] Levantar la plataforma completa y verificar que funciona

**Solicitar a TI**
- [ ] Procedimiento formal de traslado de archivos (5–6 GB)
- [ ] Acceso al servidor Git interno desde el servidor de desarrollo
- [ ] Registro interno de imágenes
- [ ] Espejo de PyPI *(la prioridad más alta de los repositorios)*
- [ ] Espejo de paquetes del SO
- [ ] Reglas de firewall de la sección 5
- [ ] Ambientes de prueba de SQL Server y DB2
- [ ] Confirmar comportamiento de las listas de revocación de certificados

**Verificación posterior al corte**
- [ ] `docker compose ps` — todos los servicios arriba
- [ ] Interfaz de Airflow accesible
- [ ] DAG `canary_auto_discovery` en verde
- [ ] Conexión a las bases de origen: `airflow connections test <id>`
- [ ] Escritura en la base de destino
- [ ] Correo de alerta recibido correctamente
- [ ] Sincronización horaria: `timedatectl status`

---

## 8. Prueba recomendada antes del corte definitivo

Vale la pena simular el aislamiento mientras todavía se puede revertir:

```bash
# Cortar la salida del contenedor a Internet, dejando la red interna
sudo iptables -I DOCKER-USER -d 0.0.0.0/0 -j DROP
sudo iptables -I DOCKER-USER -d 10.0.0.0/8 -j ACCEPT
sudo iptables -I DOCKER-USER -d 172.16.0.0/12 -j ACCEPT
sudo iptables -I DOCKER-USER -d 192.168.0.0/16 -j ACCEPT
```

(Ajustar los rangos a los del banco.) Con eso puesto, ejecutar la lista de
verificación de la sección 7. Lo que falle, falla ahora, cuando aún hay Internet
para corregirlo.

Para revertir:
```bash
sudo iptables -F DOCKER-USER
```

> Coordinar con TI antes de tocar reglas de firewall, incluso en desarrollo.

---

## 9. Resumen para el correo

> El corte de Internet **no afecta la operación diaria** de la plataforma: los
> procesos leen y escriben contra bases de datos internas, y la interfaz se
> sirve desde el propio servidor.
>
> Lo que sí requiere previsión es la instalación y el mantenimiento. Para
> resolverlo preparamos un paquete que contiene todos los componentes
> necesarios —imágenes de contenedor, controladores y librerías— y que se
> traslada una sola vez por el canal autorizado.
>
> Para el trabajo continuo solicitamos tres accesos dentro de la red interna:
> el servidor Git donde reside el código, un registro interno de imágenes, y un
> espejo de PyPI. Este último es el de mayor impacto operativo: sin él, cada
> librería nueva requiere un traslado manual de archivos.
