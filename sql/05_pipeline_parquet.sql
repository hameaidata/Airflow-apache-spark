-- ============================================================================
--  05 - PIPELINE PARQUET: la ruta completa vive en la tabla de control
-- ============================================================================
--
--  EL PROBLEMA QUE RESUELVE
--  ------------------------
--  Antes la extraccion escribia en una carpeta con fecha
--
--      /data/parquet/20260923/FSH005.parquet
--
--  y la carga leia la ruta de una columna ESTATICA (ETL_CONFIG.ruta_archivo).
--  Como la carpeta cambia cada dia y la columna no, la carga nunca encontraba
--  el archivo salvo que alguien editara la tabla a mano cada manana.
--
--  Ahora la extraccion guarda la RUTA COMPLETA en CTL_PROCESO_PARQUET, y la
--  carga la lee de ahi. La tabla es el punto de encuentro entre las dos
--  mitades del pipeline.
--
--      extraccion  ->  escribe el archivo
--                  ->  INSERT/UPDATE CTL_PROCESO_PARQUET
--                         archivo_parquet = '/data/parquet/20260923/FSH005.parquet'
--                         batch_id        = '20260923041500'
--                         estado          = 'TERMINADO'
--
--      carga       ->  SELECT de esa tabla (unida a ETL_CONFIG para saber
--                      a que tabla destino va cada archivo)
--                  ->  lee el parquet de esa ruta
--                  ->  INSERT en STG_*
--
--  SOBRE LA RUTA
--  -------------
--  Lo que se guarda es la ruta DENTRO DEL CONTENEDOR (siempre Linux), porque
--  es la unica que los procesos pueden abrir. La carpeta fisica esta fuera
--  del contenedor y se define en .env:
--
--      Windows :  PARQUET_HOST_DIR=D:/datahub/parquet
--      Red Hat :  PARQUET_HOST_DIR=/datos/datahub/parquet
--      ambos ->   PARQUET_CONTAINER_DIR=/data/parquet
--
--  Cambiar de plataforma no cambia nada de este SQL ni de los datos: solo se
--  comenta una linea en .env. La ruta guardada sigue siendo /data/parquet/...
-- ============================================================================

USE DATAHUB;


-- ============================================================================
-- 1. CTL_PROCESO_PARQUET  -  log de extraccion Y catalogo de archivos
-- ============================================================================
-- Si la tabla ya existe con la version anterior, corre estos ALTER en vez del
-- CREATE. Son los tres campos nuevos:
--
--     ALTER TABLE CTL_PROCESO_PARQUET ADD COLUMN batch_id      VARCHAR(14)  DEFAULT NULL;
--     ALTER TABLE CTL_PROCESO_PARQUET ADD COLUMN fecha_proceso DATE         DEFAULT NULL;
--     ALTER TABLE CTL_PROCESO_PARQUET MODIFY COLUMN archivo_parquet VARCHAR(500);
--
CREATE REFERENCE TABLE IF NOT EXISTS CTL_PROCESO_PARQUET (

    id_log              BIGINT          NOT NULL AUTO_INCREMENT,

    -- Constante NOM_PROCESO = 'GENERACION_PARQUET'
    nom_proceso         VARCHAR(100)    NOT NULL,

    -- Tabla de origen. Es la llave del join con ETL_CONFIG.
    tabla_origen        VARCHAR(150)             DEFAULT NULL,

    -- RUTA ABSOLUTA Y COMPLETA dentro del contenedor.
    -- Antes aqui solo se guardaba el nombre del archivo ("FSH005.parquet"),
    -- asi que la tabla no servia para encontrarlo. Ahora guarda
    --     /data/parquet/20260923/FSH005.parquet
    -- que es exactamente lo que la carga le pasa a pyarrow.
    archivo_parquet     VARCHAR(500)             DEFAULT NULL,

    -- NUEVO. Identificador de la corrida: yyyyMMddHHmmss.
    -- Permite a la carga tomar SOLO los archivos de la extraccion que acaba
    -- de correr, en vez de arriesgarse a recargar los de ayer si la de hoy
    -- fallo. Es el mismo valor que va en la columna BATCH_ID del parquet.
    batch_id            VARCHAR(14)              DEFAULT NULL,

    -- NUEVO. Fecha de negocio (la que devuelve sql_fecha), no la del reloj.
    -- Es tambien el nombre de la carpeta: /data/parquet/<yyyyMMdd>/
    fecha_proceso       DATE                     DEFAULT NULL,

    fec_inicio          DATETIME(6)     NOT NULL,
    fec_termino         DATETIME(6)              DEFAULT NULL,

    -- INICIADO | EJECUTANDO | TERMINADO | ERROR
    estado              VARCHAR(20)     NOT NULL,

    -- socket.gethostname() del worker que hizo la extraccion
    host_name           VARCHAR(100)             DEFAULT NULL,

    msg_error           VARCHAR(4000)            DEFAULT NULL,
    filas_procesadas    BIGINT                   DEFAULT NULL,
    duracion_segundos   DECIMAL(12,2)            DEFAULT NULL,

    PRIMARY KEY (id_log),
    KEY idx_parq_batch (batch_id),
    KEY idx_parq_tabla_estado (tabla_origen, estado),
    KEY idx_parq_fecha (fecha_proceso)
);


-- ============================================================================
-- 2. ETL_CONFIG  -  a que tabla STG va cada archivo
-- ============================================================================
-- Ya NO guarda la ruta: esa la pone la extraccion en CTL_PROCESO_PARQUET.
-- Aqui queda solo lo que de verdad es estable, el mapeo origen -> destino.
--
-- Si tu ETL_CONFIG ya existe con ruta_archivo:
--     ALTER TABLE ETL_CONFIG ADD COLUMN tabla_origen VARCHAR(150) DEFAULT NULL;
--     UPDATE ETL_CONFIG SET tabla_origen = <lo que corresponda>;
--     -- ruta_archivo se puede dejar; el nuevo sql_jobs ya no la usa.
--
CREATE REFERENCE TABLE IF NOT EXISTS ETL_CONFIG (

    id_config           INT             NOT NULL AUTO_INCREMENT,

    -- Llave del join con CTL_PROCESO_PARQUET.tabla_origen.
    -- Debe coincidir con CTL_PARAMETROS_PARQUET.TABLA y con el
    -- nombre_proceso de la Variable EXTRACCION_BT_STG.
    tabla_origen        VARCHAR(150)    NOT NULL,

    -- Tabla STG destino. Se TRUNCA antes de cargar.
    tabla_destino       VARCHAR(150)    NOT NULL,

    -- Filas por batch de pyarrow. NULL -> usa batch_default de la Variable.
    batch_size          INT                      DEFAULT NULL,

    -- Cada cuantos batches se hace commit. NULL -> commit_default.
    commit_every        INT                      DEFAULT NULL,

    activo              TINYINT         NOT NULL DEFAULT 0,
    orden               INT             NOT NULL DEFAULT 0,
    descripcion         VARCHAR(500)             DEFAULT NULL,
    fec_creacion        DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    PRIMARY KEY (id_config),
    UNIQUE KEY uk_etl_origen (tabla_origen),
    KEY idx_etl_activo (activo, orden)
);


-- ============================================================================
-- 3. LA CONSULTA QUE UNE LAS DOS MITADES
-- ============================================================================
-- Este es el valor de sql_jobs en la Variable CARGAR_PARQUET_CONFIG.
-- Devuelve EXACTAMENTE 4 columnas y en este orden, porque el codigo las
-- desempaqueta por posicion:
--
--     ruta, tabla_destino, batch_size, commit_every = job
--
-- El token {BATCH_ID} lo reemplaza carga_parquet.py por un parametro
-- enlazado (%s) con el batch que devolvio la extraccion via XCom. Si la
-- carga se ejecuta sola, sin batch, el codigo sustituye esa condicion por
-- la ultima extraccion TERMINADA de cada tabla.
--
--     SELECT  p.archivo_parquet, e.tabla_destino, e.batch_size, e.commit_every
--     FROM    CTL_PROCESO_PARQUET p
--     JOIN    ETL_CONFIG e ON e.tabla_origen = p.tabla_origen AND e.activo = 1
--     WHERE   p.estado = 'TERMINADO' AND p.batch_id = {BATCH_ID}
--     ORDER BY e.orden, p.tabla_origen
--
-- Verificacion manual de que la cadena quedo unida (reemplaza el batch):
/*
SELECT  p.tabla_origen,
        p.archivo_parquet,
        e.tabla_destino,
        p.filas_procesadas,
        p.estado
FROM    CTL_PROCESO_PARQUET p
LEFT JOIN ETL_CONFIG e ON e.tabla_origen = p.tabla_origen AND e.activo = 1
WHERE   p.batch_id = '20260923041500'
ORDER BY p.tabla_origen;
*/
-- Si tabla_destino sale NULL, falta la fila en ETL_CONFIG o esta con activo=0:
-- ese archivo se extrajo pero no se va a cargar.


-- ============================================================================
-- 4. FILAS DE EJEMPLO, alineadas con la Variable EXTRACCION_BT_STG
-- ============================================================================
DELETE FROM ETL_CONFIG WHERE tabla_origen IN
    ('STG_FSH005', 'STG_MSFD008', 'STG_FSH031', 'STG_FSH012');

INSERT INTO ETL_CONFIG
    (tabla_origen, tabla_destino, batch_size, commit_every, activo, orden, descripcion)
VALUES
    ('STG_FSH005' , 'STG_FSH005' , 5000, 10, 1, 1, 'Tipo de cambio'),
    ('STG_MSFD008', 'STG_MSFD008', 5000, 10, 1, 2, 'Maestro FD008'),
    ('STG_FSH031' , 'STG_FSH031' , 5000, 10, 1, 3, 'Saldos diarios consolidados'),
    ('STG_FSH012' , 'STG_FSH012' , 5000, 10, 1, 4, 'Historico saldos contables');


-- ============================================================================
-- 5. LAS TABLAS DESTINO NECESITAN DOS COLUMNAS EXTRA
-- ============================================================================
-- La extraccion inyecta dos columnas que no existen en el origen:
--     FECHA_PROCESO  como PRIMERA columna
--     BATCH_ID       como ULTIMA
-- y la carga arma el INSERT leyendo el esquema del parquet, asi que toda
-- tabla destino tiene que tenerlas con esos nombres exactos o falla con
-- "Unknown column".
--
--     CREATE TABLE STG_FSH005 (
--         FECHA_PROCESO  DATE            DEFAULT NULL,   -- inyectada
--         -- ... columnas de negocio, en el mismo orden que COLUMNAS
--         BATCH_ID       VARCHAR(14)     DEFAULT NULL,   -- inyectada
--         SHARD KEY (),
--         SORT KEY (FECHA_PROCESO)
--     );


-- ============================================================================
-- 6. CONSULTAS DE OPERACION
-- ============================================================================

-- Que se extrajo en la ultima corrida
SELECT batch_id, fecha_proceso, tabla_origen, archivo_parquet,
       filas_procesadas, estado, duracion_segundos
FROM   CTL_PROCESO_PARQUET
WHERE  batch_id = (SELECT MAX(batch_id) FROM CTL_PROCESO_PARQUET)
ORDER  BY tabla_origen;

-- Extracciones con error en los ultimos 7 dias
SELECT fec_inicio, tabla_origen, archivo_parquet, LEFT(msg_error, 200) AS error
FROM   CTL_PROCESO_PARQUET
WHERE  estado = 'ERROR'
  AND  fec_inicio >= NOW() - INTERVAL 7 DAY
ORDER  BY fec_inicio DESC;

-- Archivos extraidos que NO tienen destino configurado (se pierden)
SELECT DISTINCT p.tabla_origen
FROM   CTL_PROCESO_PARQUET p
LEFT   JOIN ETL_CONFIG e ON e.tabla_origen = p.tabla_origen AND e.activo = 1
WHERE  p.estado = 'TERMINADO' AND e.id_config IS NULL;
