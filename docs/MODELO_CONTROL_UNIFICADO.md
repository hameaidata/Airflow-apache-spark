# Modelo de control unificado del DataHub

Propuesta de **una sola tabla de parámetros** para configurar extracción,
carga, STG, ODS y BDS, y **una sola fila de log por proceso y corrida** que se
va actualizando conforme avanza.

Esto es un modelo, no el DDL. Está para discutirlo y corregirlo antes de
escribir una línea de SQL.

---

## El problema que resuelve

Hoy la configuración de una sola tabla vive repartida en cuatro sitios:

```
CTL_PARAMETROS_PARQUET     qué extraer, con qué columnas y filtro
ETL_CONFIG                 a qué tabla STG va
Variable EXTRACCION_BT_STG si entra en la corrida diaria, semanal o mensual
Variable CARGAR_PARQUET…   cómo se carga
```

Y para procesos ODS/BDS, en otros dos: `CTL_CFG_PROCESOS` y
`DAG_ODS_TABLAS` / `DAG_BDS_TABLAS`.

Dar de alta una tabla nueva significa tocar los cuatro y que los nombres
coincidan **exactamente**. Si `NOMBRE_PARQUET` no cuadra con
`nombre_proceso`, la tabla se extrae **cero veces** y nada avisa: la corrida
sale verde. Es el error de configuración más común del sistema y es
estructural, no un descuido de nadie.

Con este modelo, dar de alta una tabla es un `INSERT`.

---

## La decisión de fondo: qué es "un registro"

"Un solo registro" es la parte del requisito que hay que matizar, y conviene
hacerlo ahora y no cuando la tabla esté en producción.

De Bantotal a STG el flujo es **1:1**: una tabla de origen produce un parquet y
se carga en una tabla STG. Ahí una fila cubre todo el recorrido sin perder
nada, y de hecho el pipeline BT2SQL ya lo hizo a medias al meter
`TABLA_DESTINO` dentro del catálogo.

De STG hacia arriba **deja de ser 1:1**:

```
   STG_FSH005  ─┐
   STG_FSH012  ─┼──►  SP_ODS_SALDOS  ──►  ODS_SALDOS  ─┬──►  SP_BDS_CIERRE
   STG_FSH031  ─┘                                      └──►  SP_BDS_OPERATIVOS
```

Un proceso ODS lee varias tablas STG. Una tabla ODS alimenta varios BDS. Eso
es muchos-a-muchos y no cabe en una columna.

**Por eso el modelo es: una fila por objeto y capa.** Para STG, esa fila cubre
extracción y carga juntas, que es lo que se pidió. Para ODS y BDS, una fila
por proceso. Y las dependencias entre ellas van aparte.

> **La única concesión.** Podría meter las dependencias en una columna de texto
> separada por comas y mantener literalmente una sola tabla. No lo propongo por
> una razón concreta: una columna de texto no se puede validar contra nada, así
> que un código mal escrito ahí no da error — la dependencia simplemente se
> ignora y el proceso arranca antes de tiempo. Con una tabla hija y una clave
> foránea, ese mismo error revienta en el `INSERT`.
>
> Si prefiere la columna de texto, se hace; pero quiero que la decisión sea
> consciente.

---

## Las tres tablas

```
   CTL_PROCESO                 qué se hace y cómo          ← configuración
        │
        ├──< CTL_PROCESO_DEPENDENCIA   qué va antes de qué
        │
        └──< CTL_EJECUCION             qué pasó en cada corrida  ← bitácora
```

---

## 1. `CTL_PROCESO` — la tabla de parámetros

Una fila por objeto y capa. Las columnas que no aplican a una capa van en
`NULL`, y hay restricciones que impiden dejar en `NULL` las que sí aplican
(más abajo).

### Identidad

| Columna | Para qué |
|---|---|
| `ID_PROCESO` | Clave técnica. |
| `COD_PROCESO` | **Nombre de negocio, único.** Es el que se usa en las dependencias y el que sale en los logs. Ej: `STG_FSH005`, `ODS_SALDOS`. |
| `CAPA` | `STG`, `ODS`, `BDS`, `CU`. |
| `GRUPO` | Agrupa procesos que corren juntos. Reemplaza a `GRUPO_PROCESO`. |
| `DESCRIPCION` | En castellano, para quien abra la tabla dentro de dos años. |

### Origen

| Columna | Para qué |
|---|---|
| `TIPO_ORIGEN` | `JDBC`, `PARQUET`, `TABLA`, `SP`. Decide qué columnas del resto tienen sentido. |
| `CONN_ID_ORIGEN` | Connection de Airflow. Nunca credenciales aquí. |
| `ESQUEMA_ORIGEN` | Biblioteca del core, esquema o base. |
| `TABLA_ORIGEN` | Tabla o fichero físico. |
| `COLUMNAS` | Lista separada por comas. `NULL` = todas. |
| `FILTRO` | `WHERE` sin la palabra `WHERE`. |
| `TIPOS` | Tipos forzados, cuando el motor infiere mal. |

> **`FILTRO` es SQL libre que se concatena contra el origen.** No se puede
> validar sin un parser. Quien pueda editar esta columna puede consultar
> cualquier cosa del core con los permisos del pipeline. El permiso de
> escritura sobre `CTL_PROCESO` es, en la práctica, permiso de lectura sobre
> Bantotal. Merece su propio rol.

### Destino

| Columna | Para qué |
|---|---|
| `CONN_ID_DESTINO` | Connection de Airflow. |
| `ESQUEMA_DESTINO` | |
| `TABLA_DESTINO` | |
| `MODO_CARGA` | `REEMPLAZO`, `INCREMENTAL`, `MERGE`. |
| `CLAVES_MERGE` | Obligatorio si `MODO_CARGA = 'MERGE'`. |
| `COLUMNA_MARCA` | Obligatorio si `MODO_CARGA = 'INCREMENTAL'`. |
| `USAR_STAGING` | |
| `SUFIJO_STAGING` | |
| `BATCH_SIZE` | Filas por lote. `NULL` = el de la capa. |
| `COMMIT_EVERY` | Lotes por commit. `NULL` = el de la capa. |

### Transformación (ODS, BDS, CU)

| Columna | Para qué |
|---|---|
| `STORED_PROCEDURE` | Nombre del SP. |
| `PARAMETROS_SP` | Cómo se le llama. Ej: `{fecha_proceso}, {num_dias}`. |
| `TIMEOUT_MINUTOS` | Corta el SP si se pasa. Hoy no existe y un SP colgado bloquea el DAG entero. |

### Calendario y prioridad

Esto es lo que hoy vive **solo** en las Variables y causa la doble llave.

| Columna | Para qué |
|---|---|
| `ACTIVO` | Interruptor general. Apaga el proceso en todos los calendarios de golpe. |
| `EJEC_DIARIA` | |
| `EJEC_SEMANAL` | |
| `EJEC_MENSUAL` | |
| `DIA_SEMANA` | Para la semanal: qué día. |
| `DIA_MES` | Para la mensual: qué día, o `-1` para fin de mes. |
| `PRIORIDAD` | Orden dentro del grupo. |
| `PARALELIZABLE` | Si puede correr a la vez que sus hermanos. |

### Operación

| Columna | Para qué |
|---|---|
| `REINTENTOS` | Por proceso, no global. Una extracción del core no se reintenta igual que un SP. |
| `CONTINUAR_SI_FALLA` | Si su fallo tumba al grupo o solo a él. |
| `RETENCION_DIAS` | Para los parquet de este proceso. |
| `AMBIENTE` | `DEV`, `PRE`, `PRD`. |

### Auditoría de la propia fila

| Columna | Para qué |
|---|---|
| `FEC_CREACION`, `USR_CREACION` | |
| `FEC_MODIFICACION`, `USR_MODIFICACION` | |

> Esto no es burocracia. Esta tabla decide qué se extrae del core bancario y
> con qué filtro. Cuando dentro de seis meses alguien pregunte por qué una
> tabla dejó de cargarse en marzo, la respuesta tiene que estar aquí y no en la
> memoria de quien lo cambió. Con `MODO_CARGA` y `FILTRO` de por medio, yo iría
> más allá y le pondría una tabla de historial con disparador, pero eso ya es
> otra conversación.

### Las restricciones son la mitad del valor

Un catálogo sin restricciones mueve el error de configuración de las cuatro
tablas a una sola, pero no lo elimina. Con ellas, la configuración imposible
**no se puede guardar**:

```sql
CHECK (MODO_CARGA <> 'MERGE'       OR CLAVES_MERGE  IS NOT NULL)
CHECK (MODO_CARGA <> 'INCREMENTAL' OR COLUMNA_MARCA IS NOT NULL)
CHECK (CAPA NOT IN ('ODS','BDS','CU') OR STORED_PROCEDURE IS NOT NULL)
CHECK (CAPA <> 'STG' OR (TABLA_ORIGEN IS NOT NULL AND TABLA_DESTINO IS NOT NULL))
CHECK (EJEC_DIARIA + EJEC_SEMANAL + EJEC_MENSUAL > 0 OR ACTIVO = 'N')
UNIQUE (COD_PROCESO, AMBIENTE)
```

La última de las `CHECK` merece explicación: un proceso activo que no está en
ningún calendario nunca se ejecuta, y hoy eso es perfectamente configurable sin
que nada lo advierta. Con esa restricción hay que apagarlo explícitamente.

Es la diferencia entre descubrir un error de configuración al guardarlo y
descubrirlo a las tres de la mañana de un cierre.

---

## 2. `CTL_PROCESO_DEPENDENCIA`

| Columna | Para qué |
|---|---|
| `ID_PROCESO` | El que espera. FK a `CTL_PROCESO`. |
| `ID_PROCESO_REQUERIDO` | El que tiene que terminar antes. FK a `CTL_PROCESO`. |
| `TIPO` | `DURA` (si falla, no arranca) o `BLANDA` (avisa y sigue). |

Dos filas y ya está. Lo que aporta frente a una columna de texto:

- Un código inexistente **revienta el `INSERT`** en vez de ignorarse.
- El grafo se consulta con SQL: qué depende de qué, en los dos sentidos.
- Los ciclos se detectan con una consulta recursiva antes de desplegar, no
  cuando el DAG se queda esperando para siempre.

Detectar el ciclo antes importa: hoy, si alguien declara A → B → A en la
Variable, el DAG se construye igual y Airflow se queda parado sin decir por qué.

---

## 3. `CTL_EJECUCION` — la bitácora de una fila por corrida

Una fila por `(proceso, batch_id)`, que se **actualiza** conforme avanza. Es lo
que se pidió, y funciona, pero tiene dos trampas que hay que resolver en el
diseño.

### Identidad y estado

| Columna | Para qué |
|---|---|
| `ID_EJECUCION` | |
| `ID_PROCESO` | FK. |
| `COD_PROCESO` | Desnormalizado a propósito: la bitácora se consulta a las tres de la mañana y no quiere depender de un `JOIN`. |
| `BATCH_ID` | `yyyyMMddHHmmss`. Une todas las filas de una misma corrida. |
| `FECHA_PROCESO` | Fecha de **negocio**, no del reloj. |
| `ESTADO` | `PENDIENTE`, `EJECUTANDO`, `TERMINADO`, `SIN_DATOS`, `ERROR`, `OMITIDO`, `ABANDONADO`. |
| `ETAPA_ACTUAL` | `EXTRACCION`, `CARGA`, `TRANSFORMACION`, `VALIDACION`. |

### El avance, etapa por etapa

Aquí está la primera trampa. Una sola fila que se actualiza **pierde el detalle
del recorrido**: al terminar solo se ve el estado final, y la pregunta «¿en qué
etapa se fue el tiempo?» deja de tener respuesta.

Se resuelve con columnas por etapa en vez de filas por etapa:

| Columna | |
|---|---|
| `FEC_INI_EXTRACCION`, `FEC_FIN_EXTRACCION`, `FILAS_EXTRAIDAS` | |
| `FEC_INI_CARGA`, `FEC_FIN_CARGA`, `FILAS_CARGADAS` | |
| `FEC_INI_TRANSFORMACION`, `FEC_FIN_TRANSFORMACION`, `FILAS_AFECTADAS` | |
| `ARCHIVO_PARQUET` | Ruta completa dentro del contenedor. |

Así se mantiene una sola fila y no se pierde la línea de tiempo.

### El latido, que es lo que evita el zombi

Y aquí la segunda trampa, que es la seria. Si el worker muere a mitad, la fila
se queda en `EJECUTANDO` **para siempre**, y no hay forma de distinguir «murió
hace seis horas» de «va lento pero sigue».

Hoy esto ya pasa con `ctl_proceso_parquet` y la única forma de saberlo es mirar
si el contenedor sigue vivo.

| Columna | Para qué |
|---|---|
| `FEC_LATIDO` | El proceso la actualiza cada N segundos mientras trabaja. |
| `DAG_ID`, `RUN_ID`, `TASK_ID` | Contexto de Airflow: del log a la tarea en un clic. |
| `INTENTO` | Qué reintento es. |
| `HOST_NAME` | Qué worker. Útil cuando falla solo en uno porque le falta un montaje. |

Con el latido, una consulta responde la pregunta:

```sql
SELECT COD_PROCESO, ETAPA_ACTUAL, FEC_LATIDO,
       DATEDIFF(minute, FEC_LATIDO, SYSDATETIME()) AS MINUTOS_SIN_LATIR
FROM   CTL_EJECUCION
WHERE  ESTADO = 'EJECUTANDO'
  AND  FEC_LATIDO < DATEADD(minute, -15, SYSDATETIME());
```

Eso es lo que hoy no se puede preguntar.

### El error

| Columna | Para qué |
|---|---|
| `ETAPA_ERROR` | En cuál de las etapas se rompió. |
| `MSG_ERROR` | Recortado al ancho de la columna **antes** de insertar. |
| `DETALLE_ERROR` | La traza completa, si cabe. |

> Recortar antes de insertar no es un detalle: si la traza no cabe, el propio
> `INSERT` del log falla, y entonces se pierde el error original — que era el
> que importaba. Ya está resuelto así en el código de BT2SQL y S2SQL.

### La restricción que hace que todo esto funcione

```sql
UNIQUE (ID_PROCESO, BATCH_ID)
```

Es lo que garantiza **una fila por proceso y corrida**. Sin ella, un reintento
inserta una fila nueva en vez de actualizar la que ya estaba, y a las tres
corridas nadie sabe cuál mirar.

---

## Cómo se mapea lo que ya existe

Nada se pierde. Cada columna de hoy tiene su sitio:

| Hoy | En el modelo |
|---|---|
| `CTL_PARAMETROS_PARQUET.ESQUEMA` / `.TABLA` | `ESQUEMA_ORIGEN` / `TABLA_ORIGEN` |
| `.COLUMNAS` / `.FILTRO` / `.TIPOS` | iguales |
| `.ACTIVO` | `ACTIVO` |
| `.NOMBRE_PARQUET` | `COD_PROCESO` ← **el que causaba la doble llave** |
| `.TABLA_DESTINO` / `.BATCH_SIZE` | iguales |
| `ETL_CONFIG.commit_every` / `.orden` | `COMMIT_EVERY` / `PRIORIDAD` |
| `CTL_S2SQL_CATALOGO.modo_carga` / `.claves_merge` | `MODO_CARGA` / `CLAVES_MERGE` |
| `CTL_CFG_PROCESOS.STORED_PROCEDURE` | igual |
| `.CAPA` / `.GRUPO_PROCESO` | `CAPA` / `GRUPO` |
| Variable `procesos[].estado_diario` | `EJEC_DIARIA` |
| Variable `procesos[].prioridad` | `PRIORIDAD` |
| Variable `dependencias` | `CTL_PROCESO_DEPENDENCIA` |
| `ctl_proceso_parquet` | `CTL_EJECUCION`, columnas de extracción |
| `ctl_carga_stg` | `CTL_EJECUCION`, columnas de carga |
| `CONTROL_EJECUCIONES_SP` | `CTL_EJECUCION`, columnas de transformación |
| `ctl_log_proceso` | `CTL_EJECUCION` |
| `CTL_S2SQL_LOTE` | ver más abajo |

**Once tablas se convierten en tres.**

---

## Lo que NO propongo meter en la tabla

Esto es deliberado y conviene discutirlo.

**Las rutas y los recursos de máquina se quedan en `.env`:** `output_dir`,
`max_worker`, `chunk_size`, `PARQUET_HOST_DIR`. La regla que sigo es:

> lo que describe el **dato** va en la tabla; lo que describe la **máquina** va
> en el `.env`.

`COLUMNAS` describe el dato y es igual en desarrollo y en producción.
`output_dir` describe dónde está montado el disco y es distinto en Windows y en
Red Hat. Si lo metemos en la tabla, hace falta una fila por ambiente y volvemos
a tener configuración duplicada — justo lo que este modelo viene a quitar.

**`CTL_S2SQL_LOTE` (el resumen por corrida) no sobrevive como tabla.** Es
derivable: `SELECT batch_id, COUNT(*), SUM(CASE WHEN estado='ERROR'...)` sobre
`CTL_EJECUCION`. Se convierte en una vista. Una tabla de resumen que se puede
calcular es una tabla que puede quedar desincronizada con el detalle.

---

## Lo que hay que decidir antes del DDL

**1. ¿Una tabla por motor o una compartida?** Hoy hay control en SingleStore y
en SQL Server. Lo más limpio es **una sola instancia, en SQL Server**, que sea
la fuente de verdad para los dos pipelines. Pero eso significa que el pipeline
de SingleStore tiene que consultar SQL Server para saber qué hacer, y si SQL
Server está caído no arranca. La alternativa es replicar la tabla, con el coste
de mantenerlas sincronizadas.

**2. ¿Migración en paralelo o corte?** Recomiendo en paralelo: las tablas nuevas
conviven con las viejas, los DAGs nuevos leen de las nuevas, y se apagan las
viejas cuando todo esté migrado. Un corte limpio de once tablas a tres en un
solo despliegue es mucho riesgo para lo que se gana.

**3. ¿`CU` es una capa o un tipo de proceso?** Hay `cu_sp` en el repositorio y
no me quedó claro si es una cuarta capa al nivel de ODS y BDS, o reportes que
se construyen sobre BDS. Cambia el `CHECK` de `CAPA`.

**4. ¿Quién puede escribir en `CTL_PROCESO`?** Por lo de `FILTRO`, debería ser
un rol distinto del que usa el pipeline para leerla. El pipeline necesita solo
`SELECT`.

**5. Las dependencias, ¿tabla hija o columna de texto?** Mi recomendación está
arriba, pero es la única parte donde me aparto de "un solo registro".

---

## Lo que gana el día a día

**Dar de alta una tabla:** de tocar cuatro sitios y rezar para que los nombres
cuadren, a un `INSERT` que además valida lo que insertas.

**Apagar un proceso:** `UPDATE CTL_PROCESO SET ACTIVO='N' WHERE COD_PROCESO='…'`,
y deja de correr en todos los calendarios. Hoy hay que acordarse de los dos
sitios.

**Saber cómo va la corrida:** una consulta a una tabla, no un `JOIN` de cuatro.

**Saber si algo está colgado:** hoy no se puede; con `FEC_LATIDO`, sí.

**Rehacer un día:** `WHERE BATCH_ID = '…'` sobre una tabla te da todo lo que
pasó, en todas las capas.

---

## Siguiente paso

Si el modelo le cuadra, lo siguiente es el DDL en los dos dialectos, más el
script de migración que llena las tablas nuevas desde las once actuales sin
tocarlas, para poder comparar antes de cambiar nada.

Pero primero dígame qué le sobra, qué le falta, y sobre todo las cinco
decisiones de arriba.
