# Solicitud de infraestructura — Plataforma de orquestación de datos

**Solicitante:** Hamer Jara Ocas
**Destinatario:** Área de Tecnología / Infraestructura
**Fecha:** _(completar)_
**Ambientes solicitados:** Desarrollo y Producción

---

## 1. Resumen ejecutivo

Se solicita infraestructura para una plataforma de orquestación y procesamiento
de datos basada en **Apache Airflow** y **Apache Spark**, que ejecutará procesos
ETL diarios: **lee** de las bases de datos corporativas de origen (SQL Server y
DB2), procesa la información, y **escribe** el resultado en la base de datos
corporativa de destino.

**El servidor solicitado no almacena datos de negocio de forma permanente.** Ni
las bases de origen ni la de destino residen en él: son servidores corporativos
que ya existen. El servidor de esta solicitud ejecuta el procesamiento y usa
almacenamiento local únicamente como área de trabajo temporal.

| Concepto | Valor |
|---|---|
| Volumen estimado a procesar | 10 – 100 GB/día |
| Sistema operativo | Red Hat Enterprise Linux 9.x (mínimo 8.8) |
| Tecnología de despliegue | Contenedores (ver sección 3) |
| Servidor de producción | 1 (24 vCPU / 96 GB / ~2 TB) |
| Servidor de desarrollo | 1 (8 vCPU / 32 GB / 600 GB) |
| Base de datos de metadatos | Dentro del mismo servidor (uso interno de la plataforma) |
| Bases de datos de origen | **Servidores corporativos existentes** — SQL Server, DB2 |
| Base de datos de destino | **Servidor corporativo existente** — motor por confirmar |

---

## 2. Alcance de disponibilidad

**Se ha decidido que la plataforma opere sobre un único servidor por ambiente,
sin esquema de alta disponibilidad.** Es una decisión tomada de forma
consciente, y se documenta aquí para que quede constancia del alcance acordado.

Todos los componentes —orquestador, cola de tareas, base de datos de metadatos y
motor de procesamiento— conviven en la misma máquina. Si el servidor falla, la
plataforma queda fuera de servicio hasta que se restaure.

### Perfil de disponibilidad esperado

| Escenario | Impacto | Recuperación estimada |
|---|---|---|
| Falla un contenedor | Reinicio automático, sin intervención | Segundos |
| Falla el sistema operativo | Plataforma detenida | 15 – 30 min |
| Falla el hardware del anfitrión | Plataforma detenida | Según el clúster de virtualización |
| Se corrompe la base de metadatos | Pérdida del historial de ejecuciones | Según respaldo (ver 9.3) |

Los procesos ETL son **diarios y reprocesables**: una interrupción no implica
pérdida de información, solo retraso. Los datos permanecen en los sistemas de
origen y el proceso puede volver a ejecutarse para la fecha afectada.

### Lo que sí sostiene el servicio

Sin redundancia de aplicación, la continuidad descansa en tres medidas de
infraestructura:

1. **Respaldo diario de la base de metadatos** (sección 9.3) — es lo único que
   no se puede regenerar desde los sistemas de origen.
2. **Instantánea diaria de la VM**, para restaurar rápido ante fallo del SO.
3. **Reinicio automático de los contenedores**, gestionado por systemd.

> **Consulta a TI, solo informativa:** ¿el clúster de virtualización reinicia
> automáticamente las VMs ante fallo de un anfitrión? No lo solicitamos como
> requisito; nos serviría para documentar con precisión el tiempo de
> recuperación esperado ante ese escenario.

### Nota sobre crecimiento

La solución está construida en contenedores independientes, de modo que si en el
futuro el negocio requiere redundancia, se puede distribuir en varios servidores
sin rehacerla. No forma parte de esta solicitud.

---

## 3. Plataforma de contenedores — decisión requerida de TI

**Este es el punto que necesitamos resolver primero con ustedes.**

RHEL 8 y 9 **no incluyen Docker**. Red Hat lo retiró de sus repositorios y lo
sustituyó por **Podman**. La solución está construida sobre archivos
`docker-compose`, por lo que hay que decidir cómo se despliega.

Presentamos las cuatro alternativas con su evaluación honesta:

| Opción | ¿Soportada por Red Hat? | Esfuerzo de adaptación | Riesgo | Recomendación |
|---|---|---|---|---|
| **A. Podman + Quadlet/systemd** | Sí — incluido en RHEL | Medio | Bajo | **Recomendada para producción** |
| **B. Podman con socket compatible + `docker compose`** | Podman sí; `compose` es externo | Bajo | Bajo-medio | **Recomendada para desarrollo** |
| **C. Docker CE del repositorio de Docker** | **No** | Nulo | **Alto** | No recomendada |
| **D. OpenShift** | Sí — producto Red Hat | Alto | Bajo | Ver nota |

### Detalle de cada opción

**A. Podman + Quadlet/systemd** *(recomendada para producción)*
Cada contenedor se declara como una unidad de systemd mediante archivos
`.container` (Quadlet, disponible desde RHEL 9.3). El área de operaciones los
gestiona con `systemctl`, igual que cualquier otro servicio del banco: arranque
automático, reinicio ante fallo, integración con journald.

- A favor: totalmente soportado por Red Hat, sin dependencias externas, operable
  con las herramientas que el área ya usa.
- En contra: hay que traducir los `docker-compose` a unidades Quadlet. Es
  trabajo nuestro, no de TI.

**B. Podman con socket compatible + `docker compose`** *(recomendada para desarrollo)*
Podman expone un socket compatible con la API de Docker (`podman.socket`),
contra el que `docker compose` v2 funciona casi sin cambios.

- A favor: los archivos existentes se aprovechan tal cual; puesta en marcha
  inmediata.
- En contra: el binario `docker compose` no forma parte de RHEL. Aceptable en
  desarrollo; en producción preferimos la opción A.

**C. Docker CE desde el repositorio de Docker** *(no recomendada)*
Funcionaría sin ninguna adaptación, pero:

- Red Hat **no da soporte** a un RHEL con Docker CE instalado. Ante un
  incidente, el fabricante puede declinar el caso.
- Puede tener implicaciones sobre el contrato de soporte del banco.

Lo dejamos documentado por transparencia, pero **no lo proponemos**. Si TI lo
descarta por política, estamos de acuerdo con esa decisión.

**D. OpenShift**
Si el banco ya opera OpenShift, técnicamente es una plataforma superior. Sin
embargo, con un despliegue de un solo nodo su ventaja principal —programación y
recuperación entre nodos— no se aprovecha, y añade complejidad de conversión a
manifiestos de Kubernetes.

Nuestra lectura: **si existe OpenShift, conviene evaluarlo directamente como
destino final**, en lugar de montar un servidor único que después habría que
migrar. Agradecemos que nos indiquen si está disponible.

---

## 4. Ambiente de DESARROLLO

| Recurso | Especificación | Justificación |
|---|---|---|
| Servidores | 1 | — |
| vCPU | 8 | 4 para Spark, 2 para Airflow, 2 para SO y base de datos |
| Memoria RAM | 32 GB | Airflow ~6 GB, Spark ~14 GB, PostgreSQL/Redis ~4 GB, SO y margen ~8 GB |
| Disco sistema operativo | 100 GB | RHEL, paquetes, journald |
| Disco de datos | 500 GB | Imágenes de contenedor (~20 GB), datos de prueba, bitácoras |
| Tipo de disco | SSD / ≥3.000 IOPS | El almacén de metadatos hace muchas escrituras pequeñas |
| Sistema operativo | RHEL 9.x | — |
| Nombre sugerido | `srvdesetl01` | Según nomenclatura del banco |

Las pruebas se harán con subconjuntos de datos, no con el volumen completo de
producción.

---

## 5. Ambiente de PRODUCCIÓN

Un único servidor con todos los componentes.

### 5.1 Cómputo

| Recurso | Solicitado | Mínimo aceptable |
|---|---|---|
| vCPU | **24** | 16 |
| Memoria RAM | **96 GB** | 64 GB |
| Sistema operativo | RHEL 9.x | RHEL 8.8 |
| Nombre sugerido | `srvproetl01` | — |

**Desglose de la memoria** (lo que justifica los 96 GB):

| Componente | RAM |
|---|---|
| Ejecutores de Spark (3 × 16 GB) | 48 GB |
| Coordinador de Spark | 4 GB |
| Ejecutores de Airflow (3 × 4 GB) | 12 GB |
| Planificador e interfaz web de Airflow | 6 GB |
| PostgreSQL (metadatos) | 8 GB |
| Redis (cola de tareas) | 2 GB |
| Sistema operativo y margen operativo | 16 GB |
| **Total** | **96 GB** |

Con 64 GB la plataforma funciona, pero obliga a reducir los ejecutores de Spark
a 8 GB cada uno, lo que limita el tamaño del mayor conjunto de datos procesable
en una sola pasada.

**Desglose de vCPU:**

| Componente | vCPU |
|---|---|
| Ejecutores de Spark (3 × 4) | 12 |
| Ejecutores de Airflow | 6 |
| PostgreSQL y Redis | 3 |
| Sistema operativo | 3 |
| **Total** | **24** |

### 5.2 Almacenamiento

Solicitamos **volúmenes separados**, no un único disco grande. La separación
importa: el área temporal de Spark genera escrituras intensivas que, si comparten
volumen con la base de metadatos, degradan al planificador.

| Volumen | Tamaño | Tipo | Punto de montaje | Uso |
|---|---|---|---|---|
| Sistema | 100 GB | SSD | `/` | RHEL, paquetes, journald |
| Contenedores | 200 GB | SSD ≥3.000 IOPS | `/var/lib/containers` | Imágenes y capas |
| Base de datos | 200 GB | SSD ≥5.000 IOPS | `/var/lib/pgsql` | Metadatos — muchas escrituras pequeñas |
| Área temporal Spark | 500 GB | **SSD ≥10.000 IOPS** | `/datos/scratch` | Intermedios de procesamiento |
| Área de trabajo | 1 TB | Capacidad | `/datos/staging` | Extracciones y resultados en tránsito |
| **Total** | **~2 TB** | | | |

Todos los volúmenes sobre **LVM**, para poder ampliarlos en caliente sin
detener el servicio.

**Justificación del área de trabajo (1 TB):** el resultado final se escribe en
la base de datos de destino, no se conserva en este servidor. El almacenamiento
local guarda las extracciones y resultados en tránsito mientras el proceso
corre, más una ventana de retención corta para poder reprocesar sin volver a
extraer del origen.

Cálculo: 100 GB/día en origen → aproximadamente 30 GB/día en formato Parquet
comprimido → 30 días de ventana ≈ 900 GB.

> **Si el proyecto evoluciona a conservar una capa histórica de datos** en este
> servidor (un repositorio analítico en Parquet, además de la carga a la base de
> destino), este volumen debe subir a **3 TB** para 90 días de retención. Es una
> decisión de arquitectura de datos que aún no está tomada; el volumen es
> ampliable en caliente al estar sobre LVM.

**Por qué el área temporal necesita ser rápida:** en operaciones de agrupación y
unión, Spark escribe resultados intermedios a disco. Con almacenamiento lento
ese paso domina el tiempo total del proceso. Es el volumen donde el rendimiento
más se nota.

### 5.3 Crecimiento previsto

| Horizonte | Necesidad estimada |
|---|---|
| 12 meses | +3 TB en el volumen de datos |
| Si el volumen diario se duplica | +32 GB de RAM, +8 vCPU |
| Si se clasifica como crítico | Migración a esquema distribuido (8–13 servidores) |

Solicitamos que la VM se cree con capacidad de ampliar CPU y memoria sin
reinstalar.

---

## 6. Red — puertos y reglas de firewall

Al concentrar todo en un servidor, **la comunicación interna entre componentes
no atraviesa la red**: ocurre dentro del anfitrión. Esto simplifica mucho las
reglas, que se reducen a las de entrada y salida.

### 6.1 Entrantes

| Origen | Puerto | Protocolo | Uso |
|---|---|---|---|
| Red de usuarios del área de datos | 443 | TCP | Interfaz web (HTTPS) |
| Bastión de administración | 22 | TCP | Administración |

No se requiere ningún otro puerto entrante. Los puertos internos de la
plataforma (base de datos, cola de tareas, coordinador de Spark) quedarán
enlazados a `127.0.0.1` y **no serán accesibles desde la red**.

> **Nota para cuando se amplíe.** Si en el futuro se añade un segundo servidor,
> aparecerá un requisito adicional: Spark asigna **puertos aleatorios** a la
> comunicación entre el proceso coordinador y los ejecutores, lo que provoca
> fallos intermitentes difíciles de diagnosticar cuando hay firewall de por
> medio. Se resuelve fijando el rango en la configuración
> (`spark.driver.port`, `spark.blockManager.port`) y abriendo **20000–20100/TCP**
> entre los nodos. Con un solo servidor no aplica; lo dejamos anotado para no
> tropezar después.

### 6.2 Salientes — hacia servicios corporativos

| Destino | Puerto | Protocolo | Uso | Criticidad |
|---|---|---|---|---|
| **SQL Server (origen)** | 1433 | TCP | **Lectura** de datos | Bloqueante |
| **DB2 (origen)** — *ver nota* | *ver nota* | TCP | **Lectura** de datos | Bloqueante |
| **Base de datos de destino** — *ver nota* | *por confirmar* | TCP | **Escritura** de resultados | Bloqueante |
| Controladores de dominio | 389, 636 | TCP | LDAP / LDAPS | Autenticación |
| Catálogo global de AD | 3268, 3269 | TCP | LDAP | Autenticación |
| KDC Kerberos | 88 | TCP + UDP | Emisión de tickets | Bloqueante* |
| Cambio de contraseña Kerberos | 464 | TCP + UDP | — | Recomendado* |
| Servidores DNS | 53 | TCP + UDP | Resolución de nombres | Bloqueante |
| **Servidores NTP** | **123** | **UDP** | **Sincronización horaria** | **Bloqueante** |
| Relay SMTP | 25 o 587 | TCP | Alertas de fallo | Alta |
| Registro interno de imágenes | 443 | TCP | Descarga de imágenes | Bloqueante |
| Repositorio interno de paquetes | 443 | TCP | Bibliotecas y controladores | Bloqueante |
| Red Hat Satellite | 443 | TCP | Parches del sistema operativo | Alta |
| HashiCorp Vault *(si aplica)* | 8200 | TCP | Gestión de secretos | Media |

\* Solo si se opta por autenticación integrada (ver 7.2).

> **Por qué NTP es bloqueante:** Kerberos rechaza toda autenticación con un
> desfase horario superior a 5 minutos. Si el servidor no sincroniza reloj, la
> conexión a SQL Server con autenticación integrada falla con errores que en
> ningún momento mencionan la hora, y el diagnóstico se vuelve muy largo. Se
> solicita `chronyd` apuntando a los servidores NTP corporativos.

> **Base de datos de destino — pendiente de definir.** Aún no está confirmado
> el motor ni el servidor donde se escribirán los resultados. Según el motor:
>
> | Motor | Puerto habitual |
> |---|---|
> | SQL Server | 1433 |
> | PostgreSQL | 5432 |
> | Oracle | 1521 |
> | Db2 LUW | 50000 (50001 con TLS) |
>
> Se completará antes de tramitar la regla. Si el destino resultara ser el mismo
> servidor SQL Server que actúa como origen, no se requiere una regla adicional
> —pero sí una cuenta distinta, con permisos de escritura (ver 7.2).

> **Puerto de DB2 (origen) — pendiente de confirmar la variante.** El puerto depende de
> sobre qué plataforma corre DB2, dato que aún debemos confirmar con el DBA:
>
> | Variante | Puerto habitual |
> |---|---|
> | Db2 LUW (Linux/UNIX/Windows) | 50000 (50001 con TLS) |
> | Db2 for z/OS (mainframe) | 446, o el configurado en DDF |
> | Db2 for i (AS/400) | 8471 (9471 con TLS), más 449 del asignador de puertos |
>
> Se completará antes de tramitar la regla.

### 6.3 Nombre DNS y certificado

| Parámetro | Valor |
|---|---|
| Registro DNS tipo A | `airflow.banco.com.pe` *(a definir)* → IP del servidor |
| Certificado TLS | Emitido por la CA interna para ese nombre |

La terminación TLS se hará en el propio servidor mediante un proxy inverso. Si
el banco prefiere terminarla en un balanceador corporativo, se solicita una VIP
en 443 que apunte al puerto 8080 del servidor.

### 6.4 Sin acceso a Internet

Damos por supuesto que **el servidor no tendrá salida a Internet**, como
corresponde a un entorno productivo bancario. Eso implica los requisitos de la
sección 8.

Si existiera un proxy corporativo con lista blanca, sería una alternativa; en
tal caso solicitamos sus datos de configuración.

---

## 7. Cuentas y accesos requeridos

### 7.1 Sistema operativo

| Ítem | Detalle |
|---|---|
| Cuenta de servicio | `svc_airflow` — sin shell interactivo, sin contraseña |
| Rangos subuid/subgid | Requeridos para ejecutar Podman sin privilegios |
| `sudo` | Únicamente `systemctl` sobre las unidades de la plataforma. **No se requiere root general** |
| Cuentas nominales | Para el equipo de datos, con `sudo` acotado a las mismas unidades |

### 7.2 Bases de datos del pipeline

Se requieren **dos cuentas distintas**: una de solo lectura para los orígenes y
otra con permisos de escritura para el destino. Separarlas no es formalismo: es
lo que impide que un error de programación escriba sobre un sistema de origen, y
es lo primero que revisa una auditoría.

#### Origen — SQL Server *(pendiente de definición, ver nota)*

| Ítem | Detalle |
|---|---|
| Tipo de autenticación | A confirmar con el DBA y con seguridad |
| Permisos | `SELECT` sobre las tablas de origen; `EXECUTE` sobre los procedimientos acordados |
| Alcance | Únicamente las bases de datos del proyecto |
| Escritura | **Ninguna.** La cuenta de origen no debe poder modificar nada |

> **Punto abierto que requiere decisión.** Si SQL Server usa **autenticación
> integrada de Windows/AD**, la biblioteca estándar de Airflow no la soporta, y
> se requiere configuración adicional de Kerberos con *keytab* emitido por el
> equipo de Active Directory (ver 7.3). Añade varios días de trabajo y una
> dependencia externa.
>
> La alternativa, considerablemente más simple, es una **cuenta de servicio con
> autenticación SQL**, de permisos mínimos, acotada y auditable. Solicitamos la
> opinión del área de seguridad sobre cuál de las dos aplicar.

#### Origen — DB2

Usuario de solo lectura sobre los esquemas del proyecto, y `EXECUTE` sobre los
procedimientos que se acuerden. Sin permisos de escritura. Modo de autenticación
a confirmar según la variante.

#### Destino — base de datos de resultados

| Ítem | Detalle |
|---|---|
| Motor y servidor | **Por confirmar** |
| Permisos | `SELECT`, `INSERT`, `UPDATE`, `DELETE` sobre las tablas de destino |
| Creación de tablas | Solo si el diseño lo requiere — indicar si se concede `CREATE` |
| Alcance | Únicamente el esquema de destino del proyecto |
| Carga masiva | Ver nota |

> **Sobre la carga masiva.** Insertar fila por fila varios millones de registros
> tarda horas; una carga masiva tarda minutos. Solicitamos que la cuenta pueda
> usar el mecanismo de carga masiva del motor:
>
> | Motor | Permiso / mecanismo |
> |---|---|
> | SQL Server | `ADMINISTER BULK OPERATIONS`, o `INSERT` con `TABLOCK` |
> | PostgreSQL | Comando `COPY` (no requiere permiso especial si es desde el cliente) |
> | Oracle | Acceso a `SQL*Loader` o inserción directa |
> | Db2 | Utilitario `LOAD` |
>
> Si la política no permite conceder ese permiso, es viable igualmente, pero la
> ventana de proceso será considerablemente más larga. Conviene saberlo antes de
> comprometer horarios de disponibilidad de la información.

### 7.3 Active Directory / Kerberos

Requerido si se opta por autenticación integrada:

| Ítem | Detalle |
|---|---|
| Cuenta de servicio en AD | `svc_airflow_etl` |
| SPN | A definir con el equipo de AD |
| Archivo *keytab* | Emitido por el equipo de AD — **no podemos generarlo nosotros** |
| Realm y KDCs | Datos para construir `/etc/krb5.conf` |
| Grupos para autorización | Para mapear a roles de la plataforma: administradores, ingenieros, analistas, auditoría |

### 7.4 Certificados TLS

| Ítem | Detalle |
|---|---|
| Certificado de servidor | Para el nombre DNS de la plataforma, emitido por la CA interna |
| Cadena de CA interna | Para validar LDAPS y el certificado de SQL Server |
| Renovación | Procedimiento y responsable |

> Solicitamos certificados emitidos por la CA interna. Operar con la validación
> de certificado desactivada es una desviación que preferimos no introducir,
> aunque técnicamente funcione.

---

## 8. Repositorios internos

Sin salida a Internet, se requiere:

| Recurso | Necesidad |
|---|---|
| **Registro de imágenes** (Quay, Artifactory, Harbor) | Para alojar las imágenes de la plataforma |
| **Repositorio de paquetes Python** (espejo de PyPI) | Bibliotecas de la aplicación |
| **Repositorio Maven** (espejo) | Controladores JDBC |
| **Red Hat Satellite** | Parches del sistema operativo |

### Imágenes que se alojarán

| Imagen | Origen | Tamaño aprox. |
|---|---|---|
| `apache/airflow:2.11.2-python3.11` | Docker Hub | ~1,5 GB |
| `apache/spark:3.5.3` | Docker Hub | ~1 GB |
| `postgres:16-alpine` | Docker Hub | ~250 MB |
| `redis:7-alpine` | Docker Hub | ~40 MB |
| Imagen propia de Airflow | Construida internamente | ~3 GB |

**Solicitamos autorización para replicar estas imágenes al registro interno**,
mediante el procedimiento que TI tenga establecido.

### Controladores JDBC

| Controlador | Notas |
|---|---|
| `mssql-jdbc` | Microsoft, licencia MIT |
| `jcc` (DB2) | IBM. **Sujeto a los términos de licencia de IBM** — solicitamos confirmación del área legal o del DBA sobre la cobertura para este uso |
| `postgresql` | Licencia BSD |

> Si DB2 corre sobre **z/OS**, conectarse desde un cliente externo puede requerir
> licenciamiento de **DB2 Connect**. Es una cuestión contractual previa al
> desarrollo; solicitamos verificarlo antes de avanzar.

---

## 9. Seguridad y operación

### 9.1 SELinux

Se mantendrá en modo **enforcing**. La plataforma etiquetará correctamente sus
volúmenes. No solicitamos desactivarlo ni ponerlo en permisivo.

### 9.2 Agentes corporativos

Solicitamos la instalación de los agentes estándar del banco: antivirus,
monitoreo, gestión de configuración y recolección de bitácoras.

**Exclusiones de antivirus solicitadas** — el análisis en tiempo real sobre
estas rutas degrada gravemente el rendimiento y puede corromper capas de imagen:

```
/var/lib/containers/
/var/lib/pgsql/
/datos/scratch/
/datos/parquet/
```

### 9.3 Respaldo

Con un solo servidor, el respaldo es la única red de seguridad. Cobra más
importancia que en un esquema redundante.

| Elemento | Frecuencia | Retención | Observación |
|---|---|---|---|
| Instantánea de la VM | Diaria | 7 días | Recuperación rápida ante fallo del SO |
| Base de datos de metadatos | **Diaria, respaldo lógico** | Según política | Independiente de la instantánea |
| Configuración y código | Versionado en Git | Permanente | Ya implementado |
| Datos procesados (Parquet) | Semanal | 90 días | Regenerables desde origen |
| Claves de cifrado | Al crearse y al rotarse | Permanente | **Fuera del mismo respaldo que los datos** |

> **Dos notas que importan:**
>
> La instantánea de VM no sustituye al respaldo lógico de la base de datos. Una
> instantánea tomada con la base en escritura puede restaurarse en estado
> inconsistente. Se requieren ambos.
>
> La clave de cifrado no debe respaldarse junto a la base de datos que protege.
> Si ambas se guardan en el mismo lugar, el cifrado deja de aportar protección
> frente a quien acceda al respaldo.

### 9.4 Bitácoras y auditoría

- Bitácoras remitidas al SIEM corporativo
- Registro de auditoría de accesos y de ejecuciones
- Retención según normativa aplicable (SBS, SOX)

### 9.5 Ventanas de mantenimiento

Solicitamos una ventana mensual para aplicación de parches. **Con un solo
servidor, el mantenimiento implica interrupción del servicio**, por lo que
conviene coordinarla fuera de la ventana de procesamiento nocturno.

---

## 10. Lista de verificación para la solicitud

**Infraestructura**
- [ ] 1 servidor de desarrollo (8 vCPU / 32 GB / 600 GB)
- [ ] 1 servidor de producción (24 vCPU / 96 GB / ~2 TB en volúmenes separados)
- [ ] Volúmenes sobre LVM, ampliables en caliente
- [ ] Registros DNS de ambos servidores

**Sistema operativo**
- [ ] RHEL 9.x con suscripción activa
- [ ] Repositorios BaseOS, AppStream y container-tools
- [ ] `chronyd` apuntando a NTP corporativo
- [ ] SELinux en enforcing

**Red**
- [ ] Reglas entrantes: 443 desde red de usuarios, 22 desde bastión
- [ ] Reglas salientes (sección 6.2)
- [ ] Confirmación del puerto de DB2 (origen) según variante
- [ ] **Confirmación del motor, servidor y puerto de la base de destino**
- [ ] Certificado TLS de la CA interna

**Cuentas**
- [ ] Cuenta de servicio del SO con subuid/subgid
- [ ] Cuenta de **solo lectura** en SQL Server y DB2 (origen)
- [ ] Cuenta con **permisos de escritura** en la base de destino
- [ ] Definir si la cuenta de destino puede usar carga masiva
- [ ] Cuenta de servicio en AD y *keytab* (si aplica Kerberos)

**Repositorios**
- [ ] Registro interno de imágenes y autorización de réplica
- [ ] Espejos de PyPI y Maven
- [ ] Confirmación de licenciamiento del controlador de DB2

**Respaldo**
- [ ] Instantánea diaria de la VM
- [ ] Respaldo lógico diario de la base de metadatos
- [ ] Custodia de claves de cifrado separada

---

## 11. Puntos que requieren decisión

Estos cuatro condicionan el resto:

1. **Plataforma de contenedores.** ¿El banco dispone de OpenShift? Si no,
   proponemos Podman con Quadlet para producción.

2. **Autenticación a SQL Server.** ¿Cuenta de servicio con autenticación SQL, o
   autenticación integrada con Kerberos?

3. **Variante de DB2 (origen).** ¿LUW, z/OS o for i? Determina el controlador,
   el puerto y, en el caso de z/OS, un posible requisito de licenciamiento.

4. **Base de datos de destino.** Falta definir el motor y el servidor donde se
   escribirán los resultados. Determina el puerto de firewall, el controlador
   necesario y si la cuenta podrá usar carga masiva —lo que marca la diferencia
   entre una ventana de proceso de minutos o de horas.

---

## 12. Borrador de correo

> **Asunto:** Solicitud de infraestructura — Plataforma de orquestación de datos (DEV y PROD)
>
> Estimados,
>
> Escribo para solicitar la infraestructura necesaria para implementar una
> plataforma de orquestación y procesamiento de datos basada en Apache Airflow y
> Apache Spark. La plataforma leerá de las bases corporativas de origen (SQL
> Server y DB2), procesará la información y escribirá el resultado en la base de
> datos de destino, con un volumen estimado de 10 a 100 GB diarios.
>
> Cabe precisar que **el servidor solicitado no aloja bases de datos de
> negocio**: tanto los orígenes como el destino residen en servidores
> corporativos existentes. El servidor ejecuta el procesamiento y usa
> almacenamiento local solo como área de trabajo temporal.
>
> Adjunto el documento de especificaciones técnicas. En resumen, se solicitan
> dos servidores RHEL:
>
> - **Desarrollo:** 8 vCPU, 32 GB de RAM, 600 GB de disco.
> - **Producción:** 24 vCPU, 96 GB de RAM, ~2 TB en volúmenes separados.
>
> La plataforma operará sobre un único servidor por ambiente, sin esquema de
> alta disponibilidad. Los procesos son diarios y reprocesables, por lo que una
> interrupción implica retraso pero no pérdida de información.
>
> Quisiera resolver con ustedes cuatro puntos antes de tramitar la solicitud:
>
> 1. **Plataforma de contenedores.** RHEL no incluye Docker. ¿El banco dispone
>    de OpenShift? De ser así preferiríamos evaluarlo como destino. En caso
>    contrario proponemos Podman con unidades systemd, que es la alternativa
>    soportada por Red Hat.
>
> 2. **Autenticación a SQL Server.** Necesitamos definir con el área de
>    seguridad si se utilizará una cuenta de servicio con autenticación SQL o
>    autenticación integrada mediante Kerberos, que requeriría un keytab
>    emitido por el equipo de Active Directory.
>
> 3. **Base de datos de destino.** Necesitamos definir en qué motor y servidor
>    se escribirán los resultados, para tramitar la regla de firewall y la
>    cuenta correspondiente. Solicitamos también que esa cuenta pueda utilizar
>    el mecanismo de carga masiva del motor: la diferencia frente a la inserción
>    fila por fila es de minutos contra horas de ventana de proceso.
>
> 4. **Acceso a repositorios internos.** Al no haber salida a Internet,
>    requerimos replicar cuatro imágenes de contenedor al registro interno y
>    disponer de espejos de PyPI y Maven.
>
> Quedo atento a sus comentarios y con disposición para una reunión técnica si
> lo consideran conveniente.
>
> Saludos cordiales,
> Hamer Jara Ocas

---

## Anexo — Resumen de recursos

| | Desarrollo | Producción |
|---|---|---|
| Servidores | 1 | 1 |
| vCPU | 8 | 24 (mínimo 16) |
| Memoria RAM | 32 GB | 96 GB (mínimo 64) |
| Disco total | 600 GB | ~2 TB |
| Volúmenes separados | No | Sí (5 volúmenes) |
| Bases de origen y destino | Servidores corporativos existentes | Servidores corporativos existentes |
| Respaldo | Semanal | Diario |
