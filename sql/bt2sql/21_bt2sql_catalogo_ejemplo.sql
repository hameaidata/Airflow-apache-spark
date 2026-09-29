-- ============================================================================
--  BT2SQL - Catalogo de ejemplo y tablas STG destino
-- ============================================================================
--
--  Las cuatro tablas de aqui son las mismas que ya trae la Variable
--  BT2SQL_EXTRACCION, para que el pipeline se pueda probar de punta a punta
--  sin inventar nada. Ajusta ESQUEMA, COLUMNAS y FILTRO a lo que de verdad
--  tenga tu core antes de la primera corrida real.
--
--  LA DOBLE LLAVE
--  --------------
--  Una tabla entra en una corrida solo si esta en los DOS sitios:
--
--      aqui, con ACTIVO = 'S'
--      en la Variable BT2SQL_EXTRACCION, en "procesos", con estado = 1
--      y el flag del calendario (estado_diario / semanal / mensual) en 1
--
--  El cruce se hace por (NOMBRE_PARQUET, ESQUEMA) contra
--  (nombre_proceso, nombre_esquema). Si no coinciden, la tabla se extrae
--  cero veces y el DAG lanza un error que lo dice con esos nombres.
--
--  Por eso NOMBRE_PARQUET vale 'STG_FSH005' y no 'FSH005': es el nombre
--  LOGICO del proceso, el mismo que usa la Variable. TABLA es el nombre
--  FISICO en el core.
-- ============================================================================

USE GNBPE_DATAHUB;
GO


-- ============================================================================
-- 1. CATALOGO
-- ============================================================================
-- MERGE y no DELETE+INSERT: asi se puede volver a correr este script sin
-- perder los ajustes que alguien haya hecho en FILTRO o COLUMNAS. Solo se
-- actualiza lo que este script define.
--
-- HOLDLOCK no es decorativo. Sin el, dos ejecuciones simultaneas pueden pasar
-- las dos por el "no existe" y las dos hacer INSERT, y la segunda revienta
-- contra UK_CTL_PARAMETROS_PARQUET. Es la forma correcta de escribir un MERGE
-- en SQL Server y casi nunca se ve puesta.
-- ============================================================================
MERGE dbo.CTL_PARAMETROS_PARQUET WITH (HOLDLOCK) AS destino
USING (VALUES
    -- ESQUEMA     TABLA      NOMBRE_PARQUET  TABLA_DESTINO  BATCH  DESCRIPCION
    ('GPPPBTDB', 'FSH005' , 'STG_FSH005' , 'STG_FSH005' , 5000, 'Tipo de cambio'),
    ('GPPPBTDB', 'MSFD008', 'STG_MSFD008', 'STG_MSFD008', 5000, 'Maestro FD008'),
    ('GPPPBTDB', 'FSH031' , 'STG_FSH031' , 'STG_FSH031' , 5000, 'Saldos diarios consolidados'),
    ('GPPPBTDB', 'FSH012' , 'STG_FSH012' , 'STG_FSH012' , 5000, 'Historico de saldos contables')
) AS origen (ESQUEMA, TABLA, NOMBRE_PARQUET, TABLA_DESTINO, BATCH_SIZE, DESCRIPCION)
    ON  destino.ESQUEMA = origen.ESQUEMA
    AND destino.TABLA   = origen.TABLA

WHEN MATCHED THEN UPDATE SET
    destino.NOMBRE_PARQUET = origen.NOMBRE_PARQUET,
    destino.TABLA_DESTINO  = origen.TABLA_DESTINO,
    destino.BATCH_SIZE     = origen.BATCH_SIZE,
    destino.DESCRIPCION    = origen.DESCRIPCION

WHEN NOT MATCHED BY TARGET THEN INSERT
    (ESQUEMA, TABLA, NOMBRE_PARQUET, TABLA_DESTINO, BATCH_SIZE, DESCRIPCION,
     COLUMNAS, FILTRO, ACTIVO)
VALUES
    (origen.ESQUEMA, origen.TABLA, origen.NOMBRE_PARQUET, origen.TABLA_DESTINO,
     origen.BATCH_SIZE, origen.DESCRIPCION,
     NULL,      -- COLUMNAS NULL = todas. Listalas en cuanto sepas cuales usas.
     NULL,      -- FILTRO: sin WHERE. Ojo con las tablas historicas grandes.
     'S');
GO

SELECT ESQUEMA, TABLA, NOMBRE_PARQUET, TABLA_DESTINO, ACTIVO, BATCH_SIZE
FROM   dbo.CTL_PARAMETROS_PARQUET
ORDER  BY ESQUEMA, TABLA;
GO


-- ============================================================================
-- 2. LAS TABLAS STG DESTINO NECESITAN DOS COLUMNAS EXTRA
-- ============================================================================
-- La extraccion inyecta en el parquet dos columnas que NO existen en el core:
--
--     FECHA_PROCESO   como PRIMERA columna
--     BATCH_ID        como ULTIMA
--
-- y la carga arma el INSERT leyendo el esquema del propio parquet. Asi que
-- toda tabla STG destino tiene que tener esas dos columnas, con esos nombres
-- exactos, o la carga falla con "Invalid column name".
--
-- Sirven para lo mismo que la bitacora pero desde los datos: con FECHA_PROCESO
-- se sabe a que dia contable pertenece cada fila, y con BATCH_ID se puede
-- borrar exactamente lo que metio una corrida concreta sin tocar el resto.
--
-- Plantilla, con FSH005 de ejemplo:
/*
IF OBJECT_ID('dbo.STG_FSH005', 'U') IS NULL
CREATE TABLE dbo.STG_FSH005 (
    FECHA_PROCESO   DATE            NULL,       -- inyectada, va PRIMERA
    -- ------------------------------------------------------------------
    -- columnas de negocio, en el MISMO orden que la columna COLUMNAS del
    -- catalogo. Los tipos salen de lo que devuelve el core:
    --   NUMERIC/DECIMAL de Bantotal  ->  DECIMAL(p, s)
    --   CHAR de longitud fija        ->  CHAR(n)  (no VARCHAR: el core
    --                                    rellena con espacios y notarias
    --                                    la diferencia al comparar)
    --   fechas como yyyymmdd         ->  INT o CHAR(8), NO DATE: el core
    --                                    guarda ceros para "sin fecha" y
    --                                    un 0 no es una fecha valida
    -- ------------------------------------------------------------------
    PGCOD           DECIMAL(3,0)    NULL,
    FSH005FEC       DECIMAL(8,0)    NULL,
    FSH005MON       DECIMAL(4,0)    NULL,
    FSH005TCV       DECIMAL(17,8)   NULL,
    -- ------------------------------------------------------------------
    BATCH_ID        VARCHAR(14)     NULL        -- inyectada, va ULTIMA
);

-- Indice util para casi cualquier consulta que se haga despues sobre STG.
CREATE INDEX IX_STG_FSH005_fecha ON dbo.STG_FSH005 (FECHA_PROCESO, BATCH_ID);
*/


-- ============================================================================
-- 3. LA TABLA STAGING LA CREA EL DAG, NO TU
-- ============================================================================
-- Con "usar_staging": true en BT2SQL_CARGA, cada tabla se carga asi:
--
--     1. CREATE  STG_FSH005_STG  con SELECT TOP 0 * INTO desde la destino
--     2. INSERT  todo el parquet en la staging
--     3. TRUNCATE destino + INSERT desde staging, EN UNA SOLA TRANSACCION
--     4. DROP    la staging
--
-- El paso 3 es el que importa. Si se truncara la destino ANTES de leer el
-- parquet y el archivo estuviera corrupto, la tabla quedaria vacia y sin
-- datos que reponer hasta la corrida siguiente. Haciendolo al final, la
-- destino solo se vacia cuando los datos nuevos ya estan dentro de la base, y
-- si algo falla el ROLLBACK la deja exactamente como estaba.
--
-- Detalle de SQL Server que no se cumple en MySQL ni en SingleStore: aqui
-- TRUNCATE TABLE SI es transaccional y se puede deshacer con ROLLBACK. En
-- MySQL hace un commit implicito y no hay vuelta atras. Por eso este patron
-- funciona aqui y el pipeline de SingleStore tuvo que resolverlo de otra forma.
--
-- No tienes que crear ninguna tabla _STG a mano.


-- ============================================================================
-- 4. CONSULTAS DE OPERACION
-- ============================================================================

-- Como fue la ultima corrida, de punta a punta
SELECT  p.batch_id,
        p.fecha_proceso,
        p.tabla_origen,
        p.estado                AS estado_extraccion,
        p.filas_procesadas,
        p.duracion_segundos     AS seg_extraccion,
        c.tabla_destino,
        c.estado                AS estado_carga,
        c.filas_cargadas,
        c.duracion_segundos     AS seg_carga
FROM    dbo.ctl_proceso_parquet p
LEFT    JOIN dbo.ctl_carga_stg c
        ON  c.batch_id        = p.batch_id
        AND c.archivo_parquet = p.archivo_parquet
WHERE   p.batch_id = (SELECT MAX(batch_id) FROM dbo.ctl_proceso_parquet)
ORDER   BY p.tabla_origen;
GO

-- Errores de los ultimos 7 dias, de las dos mitades
SELECT 'extraccion' AS fase, fec_inicio, tabla_origen AS tabla,
       LEFT(msg_error, 300) AS error
FROM   dbo.ctl_proceso_parquet
WHERE  estado = 'ERROR' AND fec_inicio >= DATEADD(day, -7, SYSDATETIME())
UNION ALL
SELECT 'carga', fec_inicio, tabla_destino, LEFT(msg_error, 300)
FROM   dbo.ctl_carga_stg
WHERE  estado = 'ERROR' AND fec_inicio >= DATEADD(day, -7, SYSDATETIME())
ORDER  BY fec_inicio DESC;
GO

-- Tablas que se extrajeron pero NO se cargaron: se pierden en silencio.
-- Casi siempre es que falta TABLA_DESTINO en el catalogo o que la fila esta
-- con ACTIVO = 'N'.
SELECT  p.batch_id, p.tabla_origen, p.esquema, p.archivo_parquet
FROM    dbo.ctl_proceso_parquet p
LEFT    JOIN dbo.CTL_PARAMETROS_PARQUET e
        ON  e.TABLA   = p.tabla_origen
        AND e.ESQUEMA = p.esquema
        AND e.ACTIVO  = 'S'
WHERE   p.estado = 'TERMINADO'
  AND   e.ID_PARAMETRO IS NULL
ORDER   BY p.batch_id DESC, p.tabla_origen;
GO

-- Tablas que llevan dias sin datos. Normalmente el FILTRO quedo mal escrito.
SELECT  tabla_origen,
        COUNT(*)            AS corridas_sin_datos,
        MAX(fecha_proceso)  AS ultima
FROM    dbo.ctl_proceso_parquet
WHERE   estado = 'SIN_DATOS'
  AND   fec_inicio >= DATEADD(day, -15, SYSDATETIME())
GROUP   BY tabla_origen
HAVING  COUNT(*) >= 3
ORDER   BY corridas_sin_datos DESC;
GO
