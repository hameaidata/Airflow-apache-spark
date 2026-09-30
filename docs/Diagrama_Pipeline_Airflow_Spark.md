

# DOCUMENTACIÓN TÉCNICA

## Plataforma de Orquestación y Procesamiento de Datos — Apache Airflow + Apache Spark

**Versión:** 1.0
**Ambientes:** Desarrollo y Producción
**Sistema operativo:** RHEL 9.x
**Arquitectura:** Plataforma contenerizada sobre servidor único por ambiente
**Volumen estimado:** 10–100 GB diarios

---

# 1. Objetivo

El objetivo de la solución es implementar una plataforma centralizada para la **orquestación, extracción, procesamiento y carga de datos** utilizando Apache Airflow y Apache Spark.

La plataforma permitirá:

1. Orquestar procesos ETL mediante DAGs.
2. Extraer información desde bases de datos corporativas.
3. Procesar grandes volúmenes de información utilizando Spark.
4. Utilizar Parquet como almacenamiento temporal/intermedio.
5. Cargar los resultados procesados hacia una base de datos corporativa de destino.
6. Administrar ejecuciones, errores, reintentos y dependencias.
7. Registrar logs y auditoría de los procesos.
8. Escalar horizontalmente los workers de Airflow y Spark.

El flujo principal es:

```text
SQL Server ───────┐
                  │
                  ▼
              Apache Airflow
                  │
                  ▼
             Apache Spark
                  │
                  ▼
          Parquet temporal
                  │
                  ▼
        Base de datos destino
```

La arquitectura contempla ambientes independientes de **Desarrollo** y **Producción**. El servidor de procesamiento no almacena bases de datos de negocio; los datos de origen y destino permanecen en servidores corporativos. 

---

# 2. Arquitectura general

La solución se divide conceptualmente en las siguientes capas:

```text
┌──────────────────────────────────────────────────────────────┐
│                    USUARIOS CORPORATIVOS                     │
│                                                              │
│ Developers | Operadores | Administradores | Auditores        │
└────────────────────────────┬─────────────────────────────────┘
                             │ HTTPS
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                     CAPA DE ACCESO                           │
│                                                              │
│                  Firewall / Reverse Proxy                    │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│              SERVIDOR DE PROCESAMIENTO                       │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                    APACHE AIRFLOW                      │  │
│  │                                                        │  │
│  │ Webserver │ Scheduler │ Celery Workers │ Triggerer     │  │
│  └──────────────────────────┬─────────────────────────────┘  │
│                             │                                │
│                        Redis / Celery                        │
│                             │                                │
│  ┌──────────────────────────▼─────────────────────────────┐  │
│  │                    APACHE SPARK                        │  │
│  │                                                        │  │
│  │ Spark Master │ Spark Workers │ Spark Executors         │  │
│  └──────────────────────────┬─────────────────────────────┘  │
│                             │                                │
│                    Parquet / Staging                         │
│                             │                                │
│  PostgreSQL │ Vault │ Logs │ Configuración │ Métricas        │
└─────────────────────────────┬────────────────────────────────┘
                              │
                 ┌────────────┴─────────────┐
                 │                          │
                 ▼                          ▼
          SQL Server / DB2           BD DESTINO
             ORIGEN                   CORPORATIVA
```

La característica principal de esta arquitectura es que los componentes internos se encuentran dentro del mismo servidor, por lo que la comunicación entre ellos no requiere atravesar la red corporativa. Esto simplifica considerablemente las reglas de firewall. 

---

# 3. Componentes de la plataforma

## 3.1 Apache Airflow

Airflow es el componente encargado de la **orquestación**.

Sus responsabilidades son:

* Definir DAGs.
* Programar ejecuciones.
* Administrar dependencias entre tareas.
* Ejecutar tareas.
* Controlar reintentos.
* Gestionar errores.
* Registrar ejecuciones.
* Administrar conexiones.
* Administrar variables.
* Proporcionar auditoría.
* Exponer la interfaz web.

Dentro de Airflow se consideran principalmente:

### Airflow Webserver

Proporciona la interfaz gráfica de administración.

Permite:

* Consultar DAGs.
* Ejecutar DAGs manualmente.
* Revisar logs.
* Consultar ejecuciones.
* Administrar conexiones.
* Administrar variables.
* Revisar auditoría.
* Administrar usuarios y roles.

La interfaz se encuentra actualmente en el puerto `8080` en el entorno de desarrollo. 

En producción, la propuesta de red es exponerla mediante HTTPS en el puerto `443`. 

---

## 3.2 Airflow Scheduler

El Scheduler es responsable de:

1. Detectar los DAGs.
2. Evaluar cuándo corresponde ejecutar un DAG.
3. Resolver dependencias.
4. Crear las instancias de tareas.
5. Enviar las tareas a la cola de ejecución.

Por ejemplo:

```text
Scheduler
    │
    ├── Detecta DAG
    │
    ├── Evalúa horario
    │
    ├── Verifica dependencias
    │
    └── Envía tarea
             │
             ▼
           Redis
             │
             ▼
      Airflow Worker
```

---

# 4. Airflow Celery Workers

Los workers son los procesos encargados de **ejecutar las tareas**.

El Scheduler no necesariamente ejecuta directamente el trabajo pesado. En la arquitectura propuesta:

```text
Airflow Scheduler
       │
       ▼
     Redis
       │
       ▼
Celery Workers
       │
       ├── Extracción
       ├── Validaciones
       ├── Spark Submit
       ├── Cargas
       └── Procesos auxiliares
```

Los workers pueden escalar horizontalmente.

Por ejemplo:

```bash
docker compose -f docker-compose.ubuntu.yml up -d --scale airflow-worker=5
```

La capacidad se calcula como:

```text
Número de workers × WORKER_CONCURRENCY
```

Con cinco workers y una concurrencia de ocho:

```text
5 × 8 = 40 tareas simultáneas
```

Los nuevos workers se conectan a Redis y comienzan a consumir tareas de la cola. 

---

# 5. Redis

Redis funciona como **broker de Celery**.

Su función principal es mantener la cola de tareas:

```text
                ┌─────────────┐
                │   Airflow   │
                │  Scheduler  │
                └──────┬──────┘
                       │
                       ▼
                ┌─────────────┐
                │    Redis    │
                │    6379     │
                └──────┬──────┘
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
          Worker 1  Worker 2  Worker N
```

Redis es un componente **interno** y no debe quedar expuesto a la red corporativa. El puerto `6379` está previsto para comunicación interna entre contenedores. 

---

# 6. PostgreSQL

PostgreSQL actúa como **base de datos de metadatos de Airflow**.

No es la base de datos de negocio.

Almacena información como:

* DAGs y ejecuciones.
* Estado de tareas.
* Historial.
* Connections.
* Variables.
* Usuarios.
* Roles.
* Información de auditoría.
* Credenciales cifradas.

Por este motivo debe tratarse como un componente crítico.

Actualmente el puerto `5432` aparece publicado en el entorno existente, pero la recomendación para producción es limitarlo a `127.0.0.1` cuando no exista una necesidad de acceso externo. 

---

# 7. Apache Spark

Spark es el motor utilizado para el **procesamiento distribuido de datos**.

La arquitectura contempla:

```text
                 Spark Master
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Worker 1    Worker 2    Worker N
          │           │           │
          ▼           ▼           ▼
      Executor     Executor     Executor
```

El Master administra los recursos y coordina la ejecución.

Los Workers proporcionan capacidad de procesamiento.

---

# 8. Integración Airflow + Spark

Airflow y Spark tienen responsabilidades diferentes.

### Airflow

Se encarga de:

> **¿Cuándo, en qué orden y bajo qué condiciones se ejecuta el proceso?**

### Spark

Se encarga de:

> **¿Cómo procesamos eficientemente los datos?**

Por tanto:

```text
                    AIRFLOW
                       │
             Orquestación del DAG
                       │
                       ▼
                 Spark Submit
                       │
                       ▼
                SPARK MASTER
                       │
              ┌────────┼────────┐
              ▼        ▼        ▼
           Worker    Worker    Worker
              │        │        │
              └────────┼────────┘
                       ▼
                 Resultado
```

---

# 9. Fuentes de datos

La solución contempla inicialmente dos fuentes corporativas principales:

## SQL Server

SQL Server funciona como fuente de información.

La plataforma requiere conectividad saliente hacia el puerto:

```text
TCP/1433
```

para realizar lectura de datos. 

La cuenta utilizada debe disponer únicamente de permisos de lectura.

---

## DB2

DB2 también funciona como fuente de información.

El puerto depende de la variante instalada:

| Variante        |   Puerto habitual |
| --------------- | ----------------: |
| Db2 LUW         |             50000 |
| Db2 LUW + TLS   |             50001 |
| Db2 z/OS        | 446 o configurado |
| Db2 for i       |              8471 |
| Db2 for i + TLS |              9471 |

La variante exacta todavía debe ser confirmada con el DBA. 

---

# 10. Flujo ETL

El proceso completo será:

```text
             ┌──────────────┐
             │   SQL Server │
             └──────┬───────┘
                    │
                    │ Lectura
                    ▼
             ┌──────────────┐
             │              │
             │   Airflow    │
             │              │
             └──────┬───────┘
                    │
                    │ Ejecuta proceso
                    ▼
             ┌──────────────┐
             │    Spark     │
             └──────┬───────┘
                    │
                    │ Transformación
                    ▼
             ┌──────────────┐
             │   Parquet    │
             │   temporal   │
             └──────┬───────┘
                    │
                    │ Resultado
                    ▼
          ┌─────────────────────┐
          │ Base de datos       │
          │ corporativa destino │
          └─────────────────────┘
```

El flujo definido por la documentación es:

```text
SQL Server / DB2
       ↓
Parquet temporal
       ↓
Base de datos destino
```

Parquet **no es el destino final**; funciona como almacenamiento intermedio. 

---

# 11. Uso de Parquet

Parquet es un formato columnar y comprimido.

En esta arquitectura se utiliza como área temporal para:

* Resultados intermedios.
* Extracciones.
* Datos procesados.
* Reprocesamiento.
* Intercambio entre etapas del proceso.

El almacenamiento está previsto en el volumen de datos de Spark.

La propuesta considera una retención aproximada de **30 días**, aunque la limpieza automática todavía debe ser implementada. 

### Importante

El resultado definitivo vive en la base de datos destino.

```text
Parquet
   │
   └── temporal/regenerable

BD destino
   │
   └── información definitiva para consumo
```

---

# 12. Ejemplo de DAG

Un DAG típico podría conceptualizarse así:

```text
START
  │
  ▼
Validar conexiones
  │
  ▼
Extraer SQL Server
  │
  ▼
Extraer DB2
  │
  ▼
Validar estructura
  │
  ▼
Procesar con Spark
  │
  ▼
Generar Parquet
  │
  ▼
Validar calidad
  │
  ▼
Cargar BD destino
  │
  ▼
Validar carga
  │
  ▼
Registrar métricas
  │
  ▼
END
```

En caso de error:

```text
                 ┌──────────────┐
                 │    Tarea     │
                 │    falla     │
                 └──────┬───────┘
                        │
                        ▼
                 ¿Tiene retry?
                  /          \
                Sí            No
                │              │
                ▼              ▼
             Reintentar      ERROR
                │              │
                ▼              ▼
             Continúa      Notificación
```

---

# 13. Seguridad

La arquitectura considera varios niveles de seguridad.

## 13.1 Autenticación

Actualmente Airflow contempla usuarios y roles.

Los roles definidos son:

* Admin
* Op
* User
* Viewer

Las contraseñas de usuarios de la interfaz utilizan hashing con `scrypt` y salt aleatorio. 

---

# 14. Credenciales de bases de datos

Las credenciales de conexión a las bases de datos no pueden almacenarse simplemente como hash porque Airflow necesita recuperar el secreto para autenticarse contra el sistema externo.

Actualmente las credenciales se almacenan cifradas en la base de metadatos. 

Como mejora de arquitectura se contempla incorporar un gestor externo de secretos, por ejemplo Vault.

```text
Airflow
   │
   │ solicita secreto
   ▼
Vault
   │
   │ devuelve credencial
   ▼
Airflow
   │
   ▼
SQL Server / DB2
```

El uso de un gestor externo todavía aparece como pendiente. 

---

# 15. TLS / HTTPS

La interfaz debe utilizar HTTPS en producción.

El diseño contempla:

```text
Usuario
   │
   │ HTTPS / 443
   ▼
Firewall / Proxy
   │
   ▼
Airflow
```

Se requiere un certificado emitido por la CA interna para el nombre DNS correspondiente, por ejemplo:

```text
airflow.banco.com.pe
```

El certificado TLS y el registro DNS están pendientes de confirmación/implementación. 

**TLS es una de las prioridades de seguridad antes de producción**, porque el cifrado de credenciales en reposo no protege los datos si las comunicaciones viajan en claro. 

---

# 16. LDAP / Active Directory

Se contempla integración con los servicios corporativos de autenticación.

Puertos considerados:

| Servicio           | Puerto |
| ------------------ | -----: |
| LDAP               |    389 |
| LDAPS              |    636 |
| Global Catalog     |   3268 |
| Global Catalog SSL |   3269 |

Estos servicios permitirían integrar la autenticación de usuarios con el Directorio Activo corporativo. 

---

# 17. Kerberos

Si se utiliza autenticación integrada para SQL Server, será necesario Kerberos.

Puertos:

```text
88   TCP/UDP → emisión de tickets
464  TCP/UDP → cambio de contraseña
```

También será necesario garantizar sincronización horaria.

Esto es particularmente importante porque Kerberos rechaza autenticaciones cuando existe un desfase horario superior a aproximadamente cinco minutos. 

Por ello:

```text
Servidor ETL
     │
     │ NTP / UDP 123
     ▼
Servidor NTP corporativo
```

es una dependencia crítica.

---

# 18. DNS

La plataforma requiere resolución DNS interna.

Puerto:

```text
TCP/UDP 53
```

Se utilizará para resolver:

* SQL Server.
* DB2.
* Base destino.
* Active Directory.
* Kerberos.
* NTP.
* SMTP.
* Otros servicios corporativos.

La ausencia de DNS impediría establecer las conexiones mediante nombres corporativos. 

---

# 19. SMTP

SMTP se utilizará para notificaciones y alertas.

Puertos considerados:

```text
25
587
```

Por ejemplo:

```text
DAG
 │
 ├── OK → continúa
 │
 └── ERROR
       │
       ▼
     SMTP
       │
       ▼
   Equipo soporte
```

El uso de SMTP está clasificado como una dependencia de alta importancia para alertas. 

---

# 20. Puertos de la solución

## Puertos de acceso

| Puerto | Servicio      | Exposición             |
| -----: | ------------- | ---------------------- |
|    443 | Airflow HTTPS | Red usuarios           |
|     22 | SSH           | Bastión                |
|   8080 | Airflow       | Desarrollo/local       |
|   5555 | Flower        | Interno/administración |
|   8082 | Spark Master  | Interno/administración |

## Puertos internos

| Puerto | Servicio         |
| -----: | ---------------- |
|   6379 | Redis            |
|   5432 | PostgreSQL       |
|   7077 | Spark Master     |
|   8081 | Spark Worker     |
|   8974 | Health Scheduler |

En producción, los servicios internos no deberían quedar expuestos innecesariamente a la red. 

---

# 21. Reglas de firewall

## Entrantes

La propuesta contempla únicamente:

```text
Red usuarios ───── TCP/443 ────► Plataforma
Bastión ────────── TCP/22 ──────► Plataforma
```

No se requieren conexiones entrantes adicionales para los componentes internos. 

## Salientes

```text
Plataforma ──1433────► SQL Server
Plataforma ──DB2──────► DB2
Plataforma ──DEST─────► BD destino
Plataforma ──53───────► DNS
Plataforma ──123──────► NTP
Plataforma ──389/636──► AD
Plataforma ──88/464───► Kerberos
Plataforma ──25/587───► SMTP
Plataforma ──443──────► Repositorios
```

Los puertos de DB2 y de la base destino todavía requieren confirmación. 

---

# 22. Almacenamiento

En producción se recomienda separar los volúmenes.

| Volumen       | Tamaño | Uso                       |
| ------------- | -----: | ------------------------- |
| Sistema       | 100 GB | RHEL, paquetes, logs      |
| Contenedores  | 200 GB | Imágenes/capas            |
| PostgreSQL    | 200 GB | Metadatos                 |
| Spark scratch | 500 GB | Intermedios               |
| Staging       |   1 TB | Extracciones y resultados |

Total aproximado:

**2 TB**

Los volúmenes deben utilizar LVM para permitir ampliaciones sin detener el servicio. 

La separación es importante porque Spark genera operaciones intensivas de escritura y no debería competir por I/O con PostgreSQL. 

---

# 23. Dimensionamiento de producción

La configuración solicitada es:

### CPU

**24 vCPU**

Distribución:

| Componente         |   vCPU |
| ------------------ | -----: |
| Spark Executors    |     12 |
| Airflow Workers    |      6 |
| PostgreSQL + Redis |      3 |
| Sistema operativo  |      3 |
| **Total**          | **24** |

### Memoria

**96 GB RAM**

| Componente                  |       RAM |
| --------------------------- | --------: |
| Spark Executors             |     48 GB |
| Spark Coordinator           |      4 GB |
| Airflow Workers             |     12 GB |
| Airflow Scheduler/Webserver |      6 GB |
| PostgreSQL                  |      8 GB |
| Redis                       |      2 GB |
| SO + margen                 |     16 GB |
| **Total**                   | **96 GB** |



---

# 24. Ambiente de desarrollo

El ambiente de desarrollo contempla:

| Recurso    |         Valor |
| ---------- | ------------: |
| Servidores |             1 |
| vCPU       |             8 |
| RAM        |         32 GB |
| Sistema    |        100 GB |
| Datos      |        500 GB |
| OS         |      RHEL 9.x |
| Servidor   | `srvdesetl01` |

Las pruebas deben utilizar subconjuntos de datos y no el volumen completo de producción. 

---

# 25. Ambiente de producción

Producción contempla:

| Recurso    |         Valor |
| ---------- | ------------: |
| Servidores |             1 |
| vCPU       |            24 |
| RAM        |         96 GB |
| Storage    |         ~2 TB |
| OS         |      RHEL 9.x |
| Servidor   | `srvproetl01` |

La arquitectura propuesta actualmente es **single-node por ambiente**, por lo que no debe considerarse una solución HA completa. 

---

# 26. Escalabilidad

Aunque inicialmente existe un único servidor, los servicios contenerizados permiten aumentar la cantidad de workers.

### Airflow

```text
1 Worker
   ↓
2 Workers
   ↓
3 Workers
   ↓
N Workers
```

### Spark

```text
1 Spark Worker
        ↓
2 Spark Workers
        ↓
3 Spark Workers
        ↓
N Spark Workers
```

Por ejemplo:

```bash
docker compose -f docker-compose.ubuntu.yml up -d --scale spark-worker=4
```



---

# 27. Observabilidad

La plataforma debe permitir observar:

### Airflow

* Estado de DAGs.
* Estado de tareas.
* Duración.
* Logs.
* Errores.
* Reintentos.
* Auditoría.

### Flower

Permite observar los workers de Celery y las tareas que están ejecutando. 

### Spark Master

Permite observar:

* Workers.
* Recursos.
* Capacidad disponible.
* Aplicaciones.

---

# 28. Logs

Los logs son críticos para operación y auditoría.

El principal riesgo identificado es que los workers son efímeros.

Si un worker desaparece:

```text
Worker
  │
  └── Logs locales
          │
          ▼
       Contenedor eliminado
          │
          ▼
     Logs desaparecen
```

Por esta razón se requiere implementar **logs remotos/persistentes** antes de considerar la solución lista para producción. 

Una arquitectura futura puede ser:

```text
Airflow Workers
      │
      ▼
 Fluent Bit / Filebeat
      │
      ▼
 Elasticsearch / Logstash
      │
      ▼
     Kibana
```

---

# 29. Monitoreo de Spark

Actualmente se puede visualizar el estado general del cluster mediante Spark Master.

Sin embargo, el detalle de una aplicación Spark en ejecución no está completamente expuesto.

Existen dos alternativas documentadas:

### Alternativa A

Publicar el puerto `4040`.

Ventaja:

* Permite visualizar el job mientras está ejecutándose.

Desventaja:

* El detalle desaparece al terminar el job.

### Alternativa B

Implementar Spark History Server.

Ventaja:

* Permite consultar histórico.

Requiere:

* Activar event logging.
* Implementar almacenamiento de eventos.
* Implementar History Server.



---

# 30. Backups

Se deben considerar como mínimo:

### Backup de VM

Snapshot diario.

### PostgreSQL

Backup lógico diario de la base de metadatos.

### Claves

Las claves de cifrado deben tener una custodia separada.

Estas actividades aparecen como pendientes dentro del checklist de infraestructura. 

---

# 31. Recuperación

La arquitectura actual permite recuperar componentes mediante:

1. Restauración de VM.
2. Restauración de PostgreSQL.
3. Restauración de configuración.
4. Restauración de DAGs.
5. Recreación de workers.
6. Reprocesamiento de datos desde los sistemas origen.

Un punto importante es que **Parquet es regenerable**.

Por lo tanto:

```text
Pérdida de Parquet
       │
       ▼
Volver a extraer
       │
       ▼
Volver a procesar
       │
       ▼
Regenerar Parquet
```

Esto reduce la necesidad de respaldar todo el staging temporal. 

---

# 32. Alta disponibilidad

La arquitectura actual **no implementa alta disponibilidad completa**.

El diseño utiliza:

```text
1 servidor
 ├── Airflow
 ├── Redis
 ├── PostgreSQL
 ├── Spark Master
 └── Spark Workers
```

Por tanto, si el servidor completo falla:

```text
Servidor OFF
     ↓
Airflow OFF
Redis OFF
PostgreSQL OFF
Spark OFF
```

La plataforma deja de procesar hasta recuperar el servidor.

Para una futura arquitectura HA se debería evaluar:

* Múltiples nodos.
* PostgreSQL externo/HA.
* Redis HA.
* Airflow distribuido.
* Spark distribuido entre nodos.
* Storage compartido.
* Balanceador.
* OpenShift/Kubernetes.

La documentación específicamente plantea evaluar OpenShift si ya está disponible en el banco. 

---

# 33. Plataforma de contenedores

Se contemplan diferentes alternativas.

### Desarrollo

Se propone Podman con socket compatible con Docker Compose.

### Producción

La documentación propone evaluar:

**Podman + Quadlet**

como alternativa cuando OpenShift no esté disponible.

Docker CE no se recomienda sobre RHEL porque Red Hat no proporciona soporte para ese escenario. 

Si el banco dispone de OpenShift, se recomienda evaluar directamente su utilización como plataforma final. 

---

# 34. Ciclo de vida de un DAG

El ciclo operativo recomendado es:

```text
                    ┌─────────────┐
                    │ Código DAG  │
                    └──────┬──────┘
                           │
                           ▼
                   Repositorio Git
                           │
                           ▼
                    Servidor Airflow
                           │
                           ▼
                     DAG Scheduler
                           │
                           ▼
                       Redis
                           │
                           ▼
                    Airflow Worker
                           │
                           ▼
                    Spark Processing
                           │
                           ▼
                    Parquet/Staging
                           │
                           ▼
                     BD destino
                           │
                           ▼
                     Validación
                           │
                           ▼
                         FIN
```

Los DAG nuevos están configurados para aparecer pausados, evitando que un archivo recién desplegado se ejecute automáticamente en un entorno regulado. 

---

# 35. Validación de instalación

Para comprobar que la plataforma funciona se contempla un DAG denominado:

```text
canary_auto_discovery
```

El procedimiento es:

1. Colocar el DAG.
2. Esperar la detección.
3. Verificar que aparece en Airflow.
4. Despausarlo.
5. Ejecutarlo.
6. Verificar que las tareas terminan correctamente.

El proceso de descubrimiento puede tomar aproximadamente entre 30 y 60 segundos en la práctica. 

Para comprobar distribución de tareas:

```text
Worker 1 → DAG ejecución 1
Worker 2 → DAG ejecución 2
Worker 3 → DAG ejecución 3
Worker 4 → DAG ejecución 4
```

La tarea `quien_me_ejecuta` permite identificar el hostname del worker que ejecutó la tarea. 

---

# 36. Checklist para pasar a producción

## Infraestructura

* [ ] Servidor RHEL provisionado.
* [ ] CPU y RAM configuradas.
* [ ] Discos separados.
* [ ] LVM configurado.
* [ ] DNS configurado.
* [ ] NTP configurado.
* [ ] Sistema operativo actualizado.

## Red

* [ ] Firewall 443.
* [ ] Firewall SSH desde bastión.
* [ ] Acceso SQL Server.
* [ ] Acceso DB2.
* [ ] Acceso BD destino.
* [ ] DNS.
* [ ] NTP.
* [ ] LDAP/LDAPS.
* [ ] Kerberos si corresponde.
* [ ] SMTP.

## Seguridad

* [ ] HTTPS/TLS.
* [ ] Certificado CA interna.
* [ ] RBAC.
* [ ] Gestión de secretos.
* [ ] Integración AD si corresponde.
* [ ] Restricción de PostgreSQL.
* [ ] Restricción de Redis.
* [ ] Auditoría.

## Datos

* [ ] Cuenta de solo lectura SQL Server.
* [ ] Cuenta de solo lectura DB2.
* [ ] Cuenta de escritura BD destino.
* [ ] Control de carga masiva.
* [ ] Validaciones de calidad.
* [ ] Retención de Parquet.
* [ ] Limpieza automática.

## Operación

* [ ] Logs remotos.
* [ ] Monitoreo.
* [ ] Alertas SMTP.
* [ ] Backup de PostgreSQL.
* [ ] Snapshot de VM.
* [ ] Backup de configuración.
* [ ] Procedimiento de recuperación.

La documentación fuente identifica específicamente como pendientes la confirmación del puerto DB2, la base destino, certificado TLS, cuentas de servicio, repositorios y backups. 

---

# 37. Decisiones pendientes

Hay **cuatro decisiones arquitectónicas principales** que deben cerrarse antes de completar la implementación:

### 1. Plataforma de contenedores

¿El banco dispone de OpenShift?

* Sí → evaluar OpenShift.
* No → evaluar Podman + Quadlet para producción.

### 2. Autenticación SQL Server

Definir:

```text
SQL Authentication
        VS
Kerberos / Integrated Authentication
```

### 3. Variante DB2

Determinar si es:

```text
DB2 LUW
DB2 z/OS
DB2 for i
```

Esto afecta:

* Driver.
* Puerto.
* Configuración.
* Posibles licencias.

### 4. Base de datos destino

Debe definirse:

* Motor.
* Servidor.
* Puerto.
* Driver.
* Cuenta.
* Permisos.
* Capacidad de carga masiva.

Estas decisiones son explícitamente identificadas como condicionantes del resto de la arquitectura. 

---

# 38. Resumen ejecutivo

La arquitectura propuesta puede resumirse de la siguiente manera:

```text
                        USUARIOS
                           │
                         HTTPS
                           │
                           ▼
                    ┌─────────────┐
                    │   AIRFLOW   │
                    │ Webserver   │
                    │ Scheduler   │
                    │ Workers     │
                    └──────┬──────┘
                           │
                         Redis
                           │
                           ▼
                    ┌─────────────┐
                    │    SPARK    │
                    │    Master   │
                    │   Workers   │
                    └──────┬──────┘
                           │
                ┌──────────┴──────────┐
                ▼                     ▼
          SQL Server                 DB2
          ORIGEN                     ORIGEN
                │                     │
                └──────────┬──────────┘
                           ▼
                       PROCESO
                       SPARK
                           │
                           ▼
                      PARQUET
                     TEMPORAL
                           │
                           ▼
                    BASE DESTINO
                           │
                           ▼
                    CONSUMO NEGOCIO
```

La solución está dimensionada para un volumen aproximado de **10 a 100 GB diarios**, con un ambiente de desarrollo de **8 vCPU / 32 GB RAM** y producción de **24 vCPU / 96 GB RAM / ~2 TB**. 

### Estado actual

**La arquitectura es técnicamente viable**, pero antes de considerarla lista para producción deben cerrarse principalmente:

1. **TLS/HTTPS**
2. **Logs remotos**
3. **Gestión externa de secretos**
4. **LDAP/AD**, si corresponde
5. **Restricción de puertos internos**
6. **Puerto/variante de DB2**
7. **Motor y puerto de BD destino**
8. **Backups y recuperación**
9. **Plataforma de contenedores definitiva**
10. **Modelo de autenticación SQL Server**

Estos puntos están reflejados como brechas o decisiones pendientes en la documentación original.  
