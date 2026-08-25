# Revisión de arquitectura

Evaluación crítica del proyecto contra el estándar que aplicaría un arquitecto
de datos con experiencia larga en banca.

Corte: 21 de agosto de 2026

---

## 1. Los servicios que corren

Diez servicios en un solo host. Dos de ellos arrancan, hacen su trabajo y
terminan.

| Servicio | Imagen | Réplicas | Puerto | Qué hace |
|---|---|---|---|---|
| `postgres` | postgres:16-alpine | 1 | 5432 | Base de metadatos: historial, credenciales cifradas, auditoría |
| `redis` | redis:7-alpine | 1 | — | Cola de tareas entre el planificador y los ejecutores |
| `airflow-init` | apache/airflow:2.11.2 | 1 (una vez) | — | Migra la base, crea el admin, crea `spark_default` y los pools |
| `airflow-webserver` | apache/airflow:2.11.2 | 1 | 8080 | Interfaz web |
| `airflow-scheduler` | apache/airflow:2.11.2 | 1 | — | Lee los DAGs, decide qué correr, encola |
| `airflow-worker` | apache/airflow:2.11.2 | **2** | — | Ejecuta las tareas. **Escalable** |
| `airflow-flower` | apache/airflow:2.11.2 | 1 | 5555 | Panel de los ejecutores Celery |
| `spark-init` | busybox:1.36 | 1 (una vez) | — | Ajusta permisos de los volúmenes compartidos |
| `spark-master` | apache/spark:3.5.3 | 1 | 8082, 7077 | Coordinador del clúster Spark |
| `spark-worker` | apache/spark:3.5.3 | **1** | — | Procesamiento. **Escalable** |

**Volúmenes:** `postgres_data`, `redis_data`, `airflow_logs`, `spark_data`,
`spark_events`

**Capacidad actual:** 2 ejecutores de Airflow × 8 tareas simultáneas = 16 tareas
en paralelo. 1 worker de Spark con 2 núcleos y 2 GB.

---

## 2. El veredicto

Usted preguntó si esto cumple el estándar de treinta años de experiencia como
arquitecto de datos. La respuesta honesta:

> **No todavía. Lo construido es un entorno de ejecución correcto, no una
> arquitectura de datos.**

La distinción importa. Lo que existe hoy es **fontanería**: contenedores que
levantan, un orquestador que funciona, credenciales bien guardadas,
documentación por encima del promedio. Todo eso está bien hecho.

Pero los artefactos que definen una arquitectura de datos —el modelo de destino,
las capas, las reglas de calidad, el linaje, el control de cambios— **no
existen**. No están a medias: están vacíos.

Evidencia recogida del disco, no de impresiones:

| Artefacto | Estado |
|---|---|
| Modelo de datos / DDL del destino | **0 archivos `.sql` en todo el proyecto** |
| DAGs de producción | **0** — solo un `.gitkeep` |
| Plantillas o fábricas de DAGs | **0 archivos** |
| Pruebas automatizadas | **0 archivos** en `airflow/tests/` |
| CI/CD | **0 archivos** en `.github/workflows/` |
| Infraestructura como código | **0 archivos** en las 6 carpetas de `infrastructure/` |
| Módulos de plugins | 8 paquetes con `__init__.py` de 0 bytes |

Ocho carpetas vacías bajo `airflow/plugins/` y seis bajo `infrastructure/` no
son arquitectura: son andamiaje que aparenta serlo. Un revisor con experiencia
lo señala en los primeros cinco minutos, y con razón — una estructura de
carpetas sin contenido sugiere que se copió un esquema sin entenderlo.

---

## 3. Lo que un arquitecto senior objetaría

Ordenado por gravedad, no por dificultad.

### 3.1 No hay modelo de datos — crítico

Esta es la objeción principal, y no es un detalle pendiente: **es la
arquitectura**.

No existe respuesta a ninguna de estas preguntas:

- ¿Cuál es el grano de las tablas de destino? ¿Una fila por qué cosa?
- ¿Hay capas —aterrizaje, integración, consumo— o todo va de origen a destino?
- ¿Cómo se manejan los cambios históricos? ¿Se sobrescribe o se versiona?
- ¿Cuál es la convención de nombres? ¿Quién es dueño de cada tabla?
- ¿Qué llaves de negocio identifican un registro entre sistemas distintos?

Sin eso, cada DAG que se escriba será una decisión aislada, y en seis meses habrá
veinte criterios distintos conviviendo. Es el patrón que produce las plataformas
que nadie quiere mantener.

**Un arquitecto empieza aquí, no por los contenedores.**

### 3.2 No hay control de cambios — crítico en banca

Hoy el despliegue consiste en copiar un `.py` a una carpeta. El descubrimiento
automático lo recoge en 30 segundos y lo ejecuta.

Hemos tratado eso como una virtud durante todo el proyecto. **En un banco es una
debilidad de control.** Significa que:

- Cualquiera con acceso a la carpeta puede poner código que se ejecuta contra
  bases productivas
- No hay revisión de pares, ni pruebas, ni aprobación
- No queda rastro de quién desplegó qué y cuándo
- No hay forma de volver atrás salvo recordar qué había antes

Para SOX, eso es un hallazgo. La corrección no es difícil —un repositorio con
revisión obligatoria, una tubería que valide y despliegue, ambientes separados—
pero hoy no existe nada de eso: la carpeta `.github/workflows` está vacía.

### 3.3 Cero pruebas — alto

`airflow/tests/unit`, `integration` y `security` están vacías.

En una plataforma que moverá datos financieros, sin una sola prueba que valide
que una transformación produce lo que debe, la única verificación es que el DAG
termine en verde. Un proceso puede terminar en verde y estar cargando datos
equivocados.

Lo mínimo: pruebas de que los DAGs importan sin error, de que las reglas de
negocio dan el resultado esperado con datos conocidos, y de que los conteos
cuadran entre origen y destino.

### 3.4 La calidad de datos es decorativa — alto

El DAG 24 tiene funciones llamadas `data_quality_check` que devuelven `True`
sin comprobar nada. Es un ejemplo ilustrativo, y como tal está bien, pero **hoy
no hay ningún control real de calidad**.

Falta lo esencial: umbrales definidos, qué se hace con los registros que fallan
—¿se descartan, se aíslan, se detiene la carga?—, y quién se entera.

### 3.5 No hay observabilidad de datos — alto

Se monitorea si las tareas terminan. No se monitorea si los datos tienen
sentido:

- ¿Cuántas filas llegaron hoy comparado con el promedio de la semana?
- ¿La carga terminó dentro de la ventana comprometida?
- ¿Hay tablas que dejaron de actualizarse sin que nadie lo note?

Prometheus y Grafana se quitaron del arranque para aligerar el stack, decisión
razonable en su momento, pero no se repusieron. Hoy la detección de problemas
depende de que alguien mire la interfaz.

### 3.6 Sin linaje — medio

Se documenta como módulo pendiente. Vale señalar que Airflow ya trae integración
con OpenLineage: activarlo cuesta poco y daría trazabilidad automática de qué
proceso alimenta qué tabla. En banca, esa pregunta la hace un auditor tarde o
temprano.

### 3.7 Sin definición de acuerdos de servicio — medio

No hay ninguna afirmación del tipo "la carga diaria debe estar disponible antes
de las 6:00". Sin eso no se puede saber si la plataforma cumple, ni dimensionar
con criterio, ni justificar una ampliación.

### 3.8 Recuperación no probada — medio

Hay respaldos definidos en los documentos. No hay evidencia de que una
restauración se haya probado nunca. Un respaldo que no se ha restaurado es una
hipótesis, no un control.

---

## 4. La objeción más incómoda: ¿por qué Spark?

Un arquitecto con experiencia haría esta pregunta primero, y conviene tener la
respuesta lista porque **hoy no la hay**.

El volumen definido es **10 GB por ejecución**. Spark existe para cuando los
datos no caben en una máquina. 10 GB caben holgadamente en la memoria del
servidor solicitado.

Lo que cuesta mantener Spark en el proyecto:

- 12 GB de RAM de los 32 solicitados — **más de un tercio del servidor**
- Un clúster que operar, con sus modos de fallo propios: colocación de
  ejecutores, rangos de puertos, versiones que deben coincidir entre el cliente
  y el clúster
- Una imagen adicional que mantener y actualizar
- Complejidad de diagnóstico: cuando un proceso falla, hay tres sitios donde
  mirar en vez de uno

Lo que se obtiene a cambio, **con 10 GB**: nada que no diera `pandas`, `polars`
o `duckdb` dentro de un `PythonOperator`. Probablemente más rápido, porque no
hay reparto ni serialización de por medio.

La justificación registrada hasta ahora —estandarizar y prever crecimiento— es
débil frente a ese costo. Prever crecimiento tiene sentido cuando el crecimiento
está proyectado; aquí el volumen se acotó a 10 GB de forma explícita.

**No es una recomendación de quitarlo.** Es señalar que la decisión debe
sostenerse con un argumento, porque le van a preguntar. Argumentos que sí la
sostendrían:

- Hay un proyecto conocido a 12–18 meses que sí requerirá volumen
- El equipo ya sabe Spark y aprender otra herramienta cuesta más
- Existe un estándar corporativo que lo impone

Si ninguno aplica, la arquitectura más defendible es más simple.

---

## 5. Lo que sí está bien hecho

No sería honesto listar solo las carencias.

| Decisión | Por qué está bien |
|---|---|
| CeleryExecutor desde el inicio | Permite escalar sin rehacer. La alternativa habría sido un callejón |
| Credenciales en Connections, no en Variables | Es el error más frecuente y aquí se evitó |
| Procedimiento almacenado → tabla → Spark | El patrón correcto. Se descartó el atajo de "ejecutar el SP desde Spark", que no funciona de forma fiable |
| Lecturas JDBC con columna de partición | Es lo que separa una extracción de minutos de una de horas |
| Idempotencia en el ejemplo 23 | Borra antes de insertar; un reintento no duplica |
| Separación de cuentas origen/destino | Solo lectura en origen. Lo primero que revisa una auditoría |
| Documentación | Por encima del promedio, y explícita sobre lo que no está verificado |
| Puertos y dependencias auditados | Los siete puntos de descarga externa están identificados uno por uno |

El trabajo hecho es sólido **en su alcance**. El problema no es la calidad de lo
construido: es que lo construido es la capa de ejecución, y se ha tratado como si
fuera la arquitectura completa.

---

## 6. Qué haría un arquitecto ahora

En este orden. Los tres primeros no requieren tocar un contenedor.

**1. Definir el modelo de destino** — semanas 1–2
Grano, capas, convención de nombres, propietarios, manejo de histórico. Es el
documento que hoy no existe y del que depende todo lo demás.

**2. Definir los acuerdos de servicio** — semana 1
Ventana de proceso, umbrales de calidad, qué se hace cuando falla, a quién se
avisa. Sin esto no se puede decir si la plataforma funciona.

**3. Cerrar las tres decisiones abiertas** — inmediato
Autenticación a SQL Server, variante de DB2, base de destino. Bloquean todo.

**4. Control de cambios** — semanas 2–3
Repositorio con revisión obligatoria, validación automática de los DAGs,
promoción entre ambientes. Es requisito de cumplimiento, no una mejora.

**5. Un DAG de producción, completo** — semanas 3–4
Uno solo, real, con pruebas, calidad, alertas y documentación. Sirve de patrón
para los siguientes y revela los problemas que los ejemplos no muestran.

**6. Decidir sobre Spark** — antes de pedir el servidor
Si se queda, con argumento escrito. Si se va, el requerimiento de memoria baja
de 32 a 20 GB y desaparece un componente entero que mantener.

**7. Observabilidad** — semanas 4–6
Frescura, volumetría, cumplimiento de ventana. Reponer Prometheus y Grafana.

**8. Linaje con OpenLineage** — cuando haya varios DAGs
Barato de activar, y responde una pregunta que el auditor hará.

**9. Limpiar el andamiaje** — inmediato
Borrar las carpetas vacías o llenarlas. Hoy prometen algo que no está.

---

## 7. Resumen

**Lo construido:** un entorno de ejecución correcto y bien documentado, con
decisiones técnicas acertadas en lo que se decidió.

**Lo que falta:** la arquitectura de datos propiamente dicha. Modelo, capas,
calidad, linaje, control de cambios, pruebas. No están a medias — están vacías.

**El riesgo si se avanza así:** escribir DAGs de producción sin modelo definido
produce veinte criterios distintos conviviendo en seis meses, y ese es el estado
del que ya no se sale sin rehacer.

**Lo bueno:** nada de lo construido se pierde. Es la base sobre la que se apoya
lo que falta. Solo hace falta reconocer que la base no es el edificio.
