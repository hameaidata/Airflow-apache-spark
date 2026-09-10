# Solicitud de infraestructura — Versión reducida

**Solicitante:** Hamer Jara Ocas
**Destinatario:** Área de Tecnología / Infraestructura
**Fecha:** _(completar)_
**Ambientes solicitados:** Desarrollo y Producción

> **Sobre este documento.** Es una variante de `README-TI.md` con dos diferencias:
>
> 1. **Volumen acotado a 10 GB por ejecución** (el otro documento contempla
>    10–100 GB/día), lo que reduce el requerimiento de forma considerable.
> 2. **Se solicita un Linux con Docker soportado**, en lugar de asumir RHEL con
>    Podman.
>
> Ambas versiones son válidas. Elegir cuál enviar depende de qué volumen se
> comprometa y de qué tan flexible sea TI con el sistema operativo.
>
> | | `README-TI.md` | Este documento |
> |---|---|---|
> | Volumen | 10–100 GB/día | 10 GB por ejecución |
> | Producción | 24 vCPU / 96 GB / 2 TB | **8 vCPU / 16 GB / 250 GB** |
> | Desarrollo | 8 vCPU / 32 GB / 600 GB | **4 vCPU / 8 GB / 250 GB** |
> | Sistema operativo | RHEL + Podman | **RED HAT con PODMANS** |

---

## 1. Resumen ejecutivo

Se solicita infraestructura para una plataforma de orquestación y procesamiento
de datos basada en **Apache Airflow** y **Apache Spark**. La plataforma **lee**
de las bases de datos corporativas de origen (SQL Server y DB2), procesa la
información, y **escribe** el resultado en la base de datos corporativa de
destino.

**El servidor solicitado no almacena datos de negocio de forma permanente.** Ni
las bases de origen ni la de destino residen en él: son servidores corporativos
que ya existen. El servidor de esta solicitud ejecuta el procesamiento y usa
almacenamiento local únicamente como área de trabajo temporal.

| Concepto | Valor |
|---|---|
| Volumen máximo a procesar | **10 GB por ejecución** |
| Sistema operativo | **Linux con soporte para Docker Engine** (ver sección 3) |
| Servidor de producción | 1 (8 vCPU / 16 GB / ~250 GB) |
| Servidor de desarrollo | 1 (4 vCPU / 8 GB / ~250 GB) |
| Base de datos de metadatos | Dentro del mismo servidor (uso interno de la plataforma) |
| Bases de datos de origen | **Servidores corporativos existentes** — SQL Server, DB2 |
| Base de datos de destino | **Servidor corporativo existente** — motor por confirmar |

---

## 2. Alcance de disponibilidad

**La plataforma opera sobre un único servidor por ambiente, sin esquema de alta
disponibilidad.** Es una decisión tomada de forma consciente y se documenta aquí
para que quede constancia del alcance acordado.

| Escenario | Impacto | Recuperación estimada |
|---|---|---|
| Falla un contenedor | Reinicio automático, sin intervención | Segundos |
| Falla el sistema operativo | Plataforma detenida | 15 – 30 min |
| Falla el hardware del anfitrión | Plataforma detenida | Según el clúster de virtualización |
| Se corrompe la base de metadatos | Pérdida del historial de ejecuciones | Según respaldo (ver 9.3) |

Los procesos ETL son **diarios y reprocesables**: una interrupción implica
retraso, no pérdida de información. Los datos permanecen en los sistemas de
origen y el proceso puede volver a ejecutarse para la fecha afectada.

Sin redundancia de aplicación, la continuidad descansa en tres medidas:

1. **Respaldo diario de la base de metadatos** (sección 9.3) — es lo único que
   no se puede regenerar desde los sistemas de origen.
2. **Instantánea diaria de la VM**, para restaurar rápido ante fallo del SO.
3. **Reinicio automático de los contenedores**, gestionado por el sistema.

> **Consulta informativa a TI:** ¿el clúster de virtualización reinicia
> automáticamente las VMs ante fallo de un anfitrión? No lo solicitamos como
> requisito; nos serviría para documentar el tiempo de recuperación esperado.

---

## 3. Sistema operativo y motor de contenedores

### 3.1 El requisito

> **Solicitamos un sistema operativo Linux sobre el cual Docker Engine esté
> soportado por su fabricante.**

La solución está construida y probada sobre Docker con archivos
`docker-compose`. Contar con Docker en el servidor evita una capa de traducción
y hace que lo que se prueba en desarrollo sea idéntico a lo que corre en
producción.

**Nota sobre licenciamiento, por si surge en la evaluación:** lo que se instala
en un servidor es **Docker Engine**, software libre bajo licencia Apache 2.0,
que **no requiere licencia comercial**. Lo que sí la requiere para empresas
grandes es **Docker Desktop**, un producto distinto, de escritorio, que no
interviene aquí. Es una confusión frecuente en revisiones de cumplimiento y
preferimos anticiparla.

### 3.2 El problema con RHEL

Entendemos que el estándar del banco es Red Hat. Conviene señalar que **RHEL 8 y
9 no incluyen Docker**: Red Hat lo retiró de sus repositorios y lo sustituyó por
Podman. Docker CE puede instalarse desde el repositorio de Docker, pero **Red
Hat no da soporte a un RHEL con Docker instalado**, lo que puede afectar al
contrato de soporte del banco.

Por eso planteamos alternativas en lugar de asumir que RHEL es la única opción.

### 3.3 Opciones, en orden de preferencia

| Sistema operativo | Docker Engine | Soporte del fabricante | Compatible con el estándar RHEL | Recomendación |
|---|---|---|---|---|
| **RHEL 9 + Podman** | No aplica — no es Docker | Sí, incluido | Sí | Si  |

#### RHEL 9 — por qué las proponemos primero

Son distribuciones **compatibles binariamente con RHEL 9**: mismo gestor de
paquetes, mismas rutas, mismo comportamiento de SELinux, mismo `systemd`. Para
el área de operaciones esto significa que:

- Sus procedimientos, guiones y líneas base de endurecimiento aplican sin cambios
- Las guías de CIS para RHEL 9 son válidas
- No hay curva de aprendizaje para el equipo

Y a diferencia de RHEL, Docker Engine se instala desde el repositorio oficial de
Docker sin comprometer ningún contrato de soporte.

**Los dos puntos en contra, para que la decisión sea informada:**

1. **No traen contrato de soporte del fabricante por defecto.** Si la política
   del banco exige soporte comercial sobre el sistema operativo, existen
   proveedores que lo ofrecen para estas distribuciones (CIQ para Rocky,
   TuxCare). Habría que evaluarlo con Compras.
2. **No se gestionan con Red Hat Satellite.** Los parches vendrían de los
   repositorios propios de la distribución, que habría que replicar en el
   repositorio interno. Es trabajo adicional para operaciones y conviene
   contemplarlo desde ahora, no descubrirlo después.

#### Ubuntu Server LTS

Es la plataforma sobre la que Docker se desarrolla y prueba primero, con soporte
comercial disponible de Canonical. La desventaja es que introduce una segunda
familia de sistema operativo en el parque, con sus propios procedimientos de
endurecimiento y parcheo.

### 3.4 Si TI descarta Docker

Si la política es RHEL exclusivamente y sin software no soportado, la
alternativa es **Podman**, incluido en RHEL. Es funcionalmente equivalente y
está plenamente soportado por Red Hat.

Implica trabajo de adaptación por nuestra parte —traducir los archivos
`docker-compose` a unidades de systemd— pero es perfectamente viable y **no
supone un obstáculo para el proyecto**. Lo planteamos así para que TI decida con
todos los elementos: nuestra preferencia por Docker es de comodidad de
desarrollo, no una limitación técnica.

**Lo que necesitamos de ustedes es la decisión**, en cualquiera de los cuatro
sentidos. Nos adaptamos al que corresponda.

---

## 4. Ambiente de DESARROLLO

| Recurso | Especificación | Justificación |
|---|---|---|
| Servidores | 1 | — |
| vCPU | 4 | 2 para Spark, 1 para Airflow, 1 para SO y base de datos |
| Memoria RAM | 8 GB | Airflow ~2 GB, Spark ~2 GB, base de datos ~2 GB, SO ~2 GB |
| Disco total | 250 GB | Imágenes de contenedor (~20 GB), datos de prueba, bitácoras |
| Tipo de disco | SSD | — |
| Sistema operativo | Según sección 3 | Idéntico al de producción |
| Nombre sugerido | `srvdesetl01` | Según nomenclatura del banco |

Las pruebas se harán con subconjuntos de datos. No requiere volúmenes separados.

---

## 5. Ambiente de PRODUCCIÓN

Un único servidor con todos los componentes de la plataforma.

### 5.1 Cómputo

| Recurso | Solicitado | Mínimo aceptable |
|---|---|---|
| vCPU | **8** | 6 |
| Memoria RAM | **16 GB** | 16 GB |
| Sistema operativo | Según sección 3 | — |
| Nombre sugerido | `srvproetl01` | — |

**Desglose de la memoria** (lo que justifica los 32 GB):

| Componente | RAM |
|---|---|
| Ejecutores de Spark (2 × 3 GB) | 6 GB |
| Coordinador de Spark | 1 GB |
| Ejecutores de Airflow (2 × 1.5 GB) | 3 GB |
| Planificador e interfaz web de Airflow | 2 GB |
| PostgreSQL (metadatos de la plataforma) | 1 GB |
| Redis (cola de tareas) | 1 GB |
| Sistema operativo y margen operativo | 2 GB |
| **Total** | **16 GB** |

**Por qué 12 GB para Spark con un volumen de 10 GB.** Spark necesita espacio
para el conjunto de datos más las estructuras intermedias que genera al ordenar,
agrupar y unir. La regla práctica es entre 1,5 y 2 veces el tamaño del dato. Con
menos memoria el proceso no falla, pero empieza a escribir a disco y se vuelve
notablemente más lento.

**Desglose de vCPU:**

| Componente | vCPU |
|---|---|
| Ejecutores de Spark (2 × 2) | 4 |
| Ejecutores de Airflow | 2 |
| PostgreSQL y Redis | 1 |
| Sistema operativo | 1 |
| **Total** | **8** |

> **Observación honesta sobre Spark.** Con 10 GB por ejecución, Spark no aporta
> ventaja de rendimiento: un proceso convencional en un solo servidor lo
> resolvería igual de bien. Se mantiene por dos razones: estandarizar la forma
> de trabajo del equipo, y no tener que rehacer los procesos si el volumen
> crece. Si TI prefiere reducir el alcance de esta solicitud, **eliminar Spark
> bajaría el requerimiento a 4 vCPU y 20 GB de RAM**. Es una decisión que
> podemos conversar.

### 5.2 Almacenamiento

| Volumen | Tamaño | Tipo | Punto de montaje | Uso |
|---|---|---|---|---|
| Sistema | 50 GB | SSD | `/` | Sistema operativo, paquetes, bitácoras |
| Contenedores | 75 GB | SSD | `/var/lib/docker` | Imágenes y capas |
| Base de datos | 50 GB | SSD  | `/var/lib/pgsql` | Metadatos — muchas escrituras pequeñas |
| Datos de trabajo | 75 GB | SSD | `/datos` | Área temporal e intercambio |
| **Total** | **~250 GB** | | | |

Todos los volúmenes sobre **LVM**, para poder ampliarlos en caliente sin detener
el servicio.

**Justificación del área de trabajo (75 GB):** el resultado final se escribe en
la base de datos de destino, no se conserva aquí. El disco local guarda las
extracciones y resultados en tránsito mientras el proceso corre, más una ventana
corta de retención para poder reprocesar sin volver a extraer del origen.

Cálculo: 10 GB por ejecución → aproximadamente 3 GB en formato comprimido → 30
días de ventana ≈ 90 GB, más el área temporal de procesamiento y margen.

**Por qué la base de metadatos va en su propio volumen:** el planificador de
Airflow escribe en ella de forma constante, con operaciones pequeñas y muy
frecuentes. Compartir volumen con el área de trabajo de Spark —que hace
escrituras secuenciales grandes— degrada a ambos.

### 5.3 Crecimiento previsto

| Horizonte | Necesidad estimada |
|---|---|
| Si el volumen sube a 50 GB por ejecución | +16 GB de RAM, +4 vCPU |
| Si se conserva histórico en el servidor | +500 GB en `/datos` |
| Si se clasifica como crítico | Migración a esquema distribuido |

Solicitamos que la VM se cree con capacidad de ampliar CPU y memoria sin
reinstalar.

---

## 6. Red — puertos y reglas de firewall

Al concentrar todo en un servidor, **la comunicación entre componentes de la
plataforma no atraviesa la red**: ocurre dentro del anfitrión. Las reglas se
reducen a entrada y salida.

### 6.1 Entrantes

| Origen | Puerto | Protocolo | Uso |
|---|---|---|---|
| Red de usuarios del área de datos | 443 | TCP | Interfaz web (HTTPS) |
| Bastión de administración | 22 | TCP | Administración |

No se requiere ningún otro puerto entrante. Los puertos internos de la
plataforma quedarán enlazados a `127.0.0.1` y **no serán accesibles desde la
red**.

### 6.2 Salientes — hacia servicios corporativos

| Destino | Puerto | Protocolo | Uso | Criticidad |
|---|---|---|---|---|
| **SQL Server (origen)** | 1433 | TCP | **Lectura** de datos | Bloqueante |
| **DB2 (origen)** — *ver nota* | *ver nota* | TCP | **Lectura** de datos | Bloqueante |
| **Base de datos de destino** — *ver nota* | *por confirmar* | TCP | **Escritura** de resultados | Bloqueante |
| Servidores DNS | 53 | TCP + UDP | Resolución de nombres | Bloqueante |
| **Servidores NTP** | **123** | **UDP** | **Sincronización horaria** | **Bloqueante** |
| Relay SMTP | 25 o 587 | TCP | Alertas de fallo | Alta |
| Registro interno de imágenes | 443 | TCP | Descarga de imágenes | Bloqueante |
| Repositorio interno de paquetes | 443 | TCP | Bibliotecas y controladores | Bloqueante |
| Repositorio de parches del SO | 443 | TCP | Actualizaciones | Alta |

\* Solo si se opta por autenticación integrada (ver 7.2).

> **Base de datos de destino — pendiente de definir.** Aún no está confirmado el
> motor ni el servidor donde se escribirán los resultados. Según el motor:
>
> | Motor | Puerto habitual |
> |---|---|
> | SQL Server | 1433 |
> | PostgreSQL | 5432 |
> | Oracle | 1521 |
> | Db2 LUW | 50000 (50001 con TLS) |
>
> Si el destino resultara ser el mismo servidor SQL Server que actúa como
> origen, no se requiere una regla adicional —pero sí una cuenta distinta, con
> permisos de escritura (ver 7.2).

> **Puerto de DB2 (origen) — pendiente de confirmar la variante.** El puerto
> depende de sobre qué plataforma corre DB2, dato que debemos confirmar con el
> DBA:
>
> | Variante | Puerto habitual |
> |---|---|
> | Db2 LUW (Linux/UNIX/Windows) | 50000 (50001 con TLS) |
> | Db2 for z/OS (mainframe) | 446, o el configurado en DDF |
> | Db2 for i (AS/400) | 8471 (9471 con TLS), más 449 del asignador de puertos |

> **Por qué NTP es bloqueante:** Kerberos rechaza toda autenticación con un
> desfase horario superior a 5 minutos. Si el servidor no sincroniza reloj, la
> conexión con autenticación integrada falla con errores que en ningún momento
> mencionan la hora, y el diagnóstico se vuelve muy largo. Se solicita el
> servicio de sincronización apuntando a los servidores NTP corporativos.

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
| Pertenencia a grupo | Grupo `docker` (o equivalente según el motor elegido) |
| `sudo` | Únicamente sobre los servicios de la plataforma. **No se requiere root general** |
| Cuentas nominales | Para el equipo de datos, con `sudo` acotado |

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

> **Sobre la carga masiva.** Con volúmenes de 10 GB la inserción convencional es
> viable, pero la carga masiva reduce la ventana de proceso de forma
> significativa. Solicitamos evaluar si la cuenta puede usar el mecanismo del
> motor:
>
> | Motor | Permiso / mecanismo |
> |---|---|
> | SQL Server | `ADMINISTER BULK OPERATIONS`, o `INSERT` con `TABLOCK` |
> | PostgreSQL | Comando `COPY` |
> | Oracle | `SQL*Loader` o inserción directa |
> | Db2 | Utilitario `LOAD` |
>
> Si la política no permite concederlo, el proyecto es viable igualmente.

### 7.3 Active Directory / Kerberos

Requerido únicamente si se opta por autenticación integrada:

| Ítem | Detalle |
|---|---|
| Cuenta de servicio en AD | `svc_airflow_etl` |
| SPN | A definir con el equipo de AD |
| Archivo *keytab* | Emitido por el equipo de AD — **no podemos generarlo nosotros** |
| Realm y KDCs | Datos para construir la configuración de Kerberos |
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
| **Repositorio de paquetes del SO** | Parches del sistema operativo y del motor de contenedores |

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

### 9.1 SELinux / AppArmor

Se mantendrá el control de acceso obligatorio del sistema operativo en modo
**activo**. La plataforma etiquetará correctamente sus volúmenes. No solicitamos
desactivarlo ni ponerlo en modo permisivo.

### 9.2 Agentes corporativos

Solicitamos la instalación de los agentes estándar del banco: antivirus,
monitoreo, gestión de configuración y recolección de bitácoras.

**Exclusiones de antivirus solicitadas** — el análisis en tiempo real sobre
estas rutas degrada gravemente el rendimiento y puede corromper capas de imagen:

```
/var/lib/docker/
/var/lib/pgsql/
/datos/
```

### 9.3 Respaldo

Con un solo servidor, el respaldo es la única red de seguridad.

| Elemento | Frecuencia | Retención | Observación |
|---|---|---|---|
| Instantánea de la VM | Diaria | 7 días | Recuperación rápida ante fallo del SO |
| Base de datos de metadatos | **Diaria, respaldo lógico** | Según política | Independiente de la instantánea |
| Configuración y código | Versionado en Git | Permanente | Ya implementado |
| Área de trabajo (`/datos`) | No requiere | — | Regenerable desde el origen |
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
- [ ] 1 servidor de desarrollo (4 vCPU / 8 GB / 250 GB)
- [ ] 1 servidor de producción (8 vCPU / 16 GB / 500 GB en volúmenes separados)
- [ ] Volúmenes sobre LVM, ampliables en caliente
- [ ] Registros DNS de ambos servidores

**Sistema operativo**
- [ ] **Decisión sobre el sistema operativo** (sección 3)
- [ ] Motor de contenedores instalado y habilitado al arranque
- [ ] Sincronización horaria apuntando a NTP corporativo
- [ ] Control de acceso obligatorio activo

**Red**
- [ ] Reglas entrantes: 443 desde red de usuarios, 22 desde bastión
- [ ] Reglas salientes (sección 6.2)
- [ ] Confirmación del puerto de DB2 (origen) según variante
- [ ] **Confirmación del motor, servidor y puerto de la base de destino**
- [ ] Certificado TLS de la CA interna

**Cuentas**
- [ ] Cuenta de servicio del SO, con acceso al motor de contenedores
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

1. **Sistema operativo y motor de contenedores.** Solicitamos un Linux con
   Docker soportado; nuestra primera opción es Rocky Linux o AlmaLinux 9, por
   ser compatibles con el estándar RHEL del banco. Si TI prefiere RHEL con
   Podman, nos adaptamos.

2. **Autenticación a SQL Server.** ¿Cuenta de servicio con autenticación SQL, o
   autenticación integrada con Kerberos?

3. **Variante de DB2 (origen).** ¿LUW, z/OS o for i? Determina el controlador,
   el puerto y, en el caso de z/OS, un posible requisito de licenciamiento.

4. **Base de datos de destino.** Falta definir el motor y el servidor donde se
   escribirán los resultados.

---


## Anexo — Resumen de recursos

| | Desarrollo | Producción |
|---|---|---|
| Servidores | 1 | 1 |
| vCPU | 4 | 8 (mínimo 6) |s
| Memoria RAM | 8 GB | 16 GB|
| Disco total | 250 GB | ~250 GB |
| Volúmenes separados | No | Sí (4 volúmenes) |
| Bases de origen y destino | Servidores corporativos existentes | Servidores corporativos existentes |
| Alta disponibilidad | No | No |
| Respaldo | Semanal | Diario |
