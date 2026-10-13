-- ============================================================================
--  BANCO DE PRUEBAS   STG -> ODS -> BDS   ::   SQL Server
-- ----------------------------------------------------------------------------
--  Crea TRES BASES DE DATOS independientes, una por capa, con su esquema, sus
--  tablas, su bitacora propia y dos procedimientos T-SQL de verdad.
--
--  Sirve para probar los dos DAG paso a paso SIN depender de ningun
--  procedimiento preexistente:
--
--      ODS_STG2ODS_CARGA_SP     STG -> ODS
--      BDS_ODS2BDS_CARGA_SP     ODS -> BDS
--
--  POR QUE TRES BASES Y NO TRES ESQUEMAS
--  -------------------------------------
--  Con tres esquemas en una sola base, una sola Connection de Airflow sirve
--  para todo y el campo Schema de esa Connection no significa nada. Con tres
--  bases:
--
--    - cada Connection apunta a SU base en el campo Schema, que es donde el
--      procedimiento se ejecuta por defecto;
--    - a cada login se le pueden dar solo los permisos de su capa, que es lo
--      que un area de seguridad va a pedir en un banco;
--    - mover una capa a otro servidor es cambiar una Connection, no el codigo;
--    - y un error de configuracion -- apuntar BDS a la base de ODS -- falla al
--      instante con "Invalid object name" en vez de escribir en la capa
--      equivocada, que es lo que pasaria con esquemas en la misma base.
--
--  LOS CRUCES ENTRE CAPAS VAN CON NOMBRE DE TRES PARTES
--  ----------------------------------------------------
--  SP_ODS_TIPO_CAMBIO vive en GNBPE_ODS y lee GNBPE_STG.GPPPBTDB.STG_FSH005.
--  SP_BDS_TIPO_CAMBIO vive en GNBPE_BDS y lee GNBPE_ODS.DATAHUB.ODS_TIPO_CAMBIO.
--
--  Eso exige que el login de cada capa tenga SELECT en la capa anterior. El
--  bloque 5 de abajo lo concede, y es deliberadamente asimetrico: ODS lee de
--  STG pero no escribe; BDS lee de ODS pero no escribe.
--
--  ORDEN DE EJECUCION
--  ------------------
--  Este archivo es idempotente: se puede volver a ejecutar entero. Las tablas
--  no se recrean si existen; los datos semilla se reponen con MERGE.
-- ============================================================================

SET NOCOUNT ON;
GO

-- ============================================================================
--  1. LAS TRES BASES
-- ============================================================================
-- COLLATE explicito y el mismo en las tres. Si una capa saliera con otra
-- collation, cualquier JOIN por texto entre capas fallaria con "cannot resolve
-- the collation conflict", y ese error aparece meses despues, en la primera
-- consulta que cruza dos capas.
IF DB_ID('GNBPE_STG') IS NULL
BEGIN
    CREATE DATABASE GNBPE_STG COLLATE Latin1_General_CI_AS;
    ALTER DATABASE GNBPE_STG SET RECOVERY SIMPLE;
    PRINT 'GNBPE_STG creada.';
END ELSE PRINT 'GNBPE_STG ya existe.';
GO

IF DB_ID('GNBPE_ODS') IS NULL
BEGIN
    CREATE DATABASE GNBPE_ODS COLLATE Latin1_General_CI_AS;
    ALTER DATABASE GNBPE_ODS SET RECOVERY SIMPLE;
    PRINT 'GNBPE_ODS creada.';
END ELSE PRINT 'GNBPE_ODS ya existe.';
GO

IF DB_ID('GNBPE_BDS') IS NULL
BEGIN
    CREATE DATABASE GNBPE_BDS COLLATE Latin1_General_CI_AS;
    ALTER DATABASE GNBPE_BDS SET RECOVERY SIMPLE;
    PRINT 'GNBPE_BDS creada.';
END ELSE PRINT 'GNBPE_BDS ya existe.';
GO


-- ============================================================================
--  2. CAPA STG   ::   GNBPE_STG.GPPPBTDB
-- ============================================================================
-- El esquema se llama GPPPBTDB porque es el nombre de la BIBLIOTECA de
-- Bantotal. En DB2 for i es una biblioteca y en SQL Server un esquema, pero
-- "GPPPBTDB.FSH005" se escribe igual en los dos: el mismo SQL sirve para
-- los dos origenes sin tocar nada.
USE GNBPE_STG;
GO

IF SCHEMA_ID('GPPPBTDB') IS NULL EXEC('CREATE SCHEMA GPPPBTDB');
GO

IF OBJECT_ID('GPPPBTDB.STG_FSH005', 'U') IS NULL
BEGIN
    CREATE TABLE GPPPBTDB.STG_FSH005 (
        -- Los tipos salen de Bantotal: las fechas son DECIMAL(8,0) con el
        -- formato AAAAMMDD, no DATE. Conservarlos asi en STG es deliberado --
        -- STG es una copia fiel del origen -- y convertirlos es trabajo de ODS.
        PGCOD           DECIMAL(3,0)    NOT NULL,
        FSH005FEC       DECIMAL(8,0)    NOT NULL,
        FSH005MON       DECIMAL(4,0)    NOT NULL,
        FSH005TCV       DECIMAL(17,8)   NULL,
        -- Las dos que anade el job de extraccion
        FECHA_PROCESO   DATE            NULL,
        BATCH_ID        VARCHAR(20)     NULL,
        CONSTRAINT PK_STG_FSH005 PRIMARY KEY CLUSTERED
            (PGCOD, FSH005FEC, FSH005MON)
    );
    PRINT 'GNBPE_STG.GPPPBTDB.STG_FSH005 creada.';
END ELSE PRINT 'STG_FSH005 ya existe.';
GO

IF OBJECT_ID('dbo.CONTROL_EJECUCIONES_SP', 'U') IS NULL
    EXEC('CREATE TABLE dbo.CONTROL_EJECUCIONES_SP (
        ID_EJECUCION BIGINT IDENTITY(1,1) PRIMARY KEY,
        NOMBRE_SP VARCHAR(256) NOT NULL, NOMBRE_PROCESO VARCHAR(256) NULL,
        ESTADO VARCHAR(20) NOT NULL, DURACION_SEGUNDOS DECIMAL(12,2) NULL,
        FILAS_AFECTADAS BIGINT NULL, MSG_ERROR VARCHAR(4000) NULL,
        RUN_ID VARCHAR(200) NULL,
        FECHA_REGISTRO DATETIME2(3) NOT NULL DEFAULT (SYSDATETIME()))');
GO

-- DATOS SEMILLA
-- Tres fechas y dos monedas a proposito: con una sola fila no se distingue
-- "cargo bien" de "cargo una fila por casualidad", y sin varias fechas no se
-- puede comprobar que el filtro por fecha del SP hace algo.
MERGE GPPPBTDB.STG_FSH005 WITH (HOLDLOCK) AS d
USING (VALUES
    (1, 20261009, 1,   1.00000000),
    (1, 20261009, 2,   3.75100000),
    (1, 20261010, 1,   1.00000000),
    (1, 20261010, 2,   3.75400000),
    (1, 20261010, 840, 3.75400000),
    (1, 20261011, 1,   1.00000000),
    (1, 20261011, 2,   3.76000000)
) AS o (PGCOD, FSH005FEC, FSH005MON, FSH005TCV)
    ON  d.PGCOD = o.PGCOD AND d.FSH005FEC = o.FSH005FEC
    AND d.FSH005MON = o.FSH005MON
WHEN MATCHED THEN UPDATE SET d.FSH005TCV = o.FSH005TCV,
                             d.FECHA_PROCESO = '2026-10-10',
                             d.BATCH_ID = 'SEMILLA'
WHEN NOT MATCHED BY TARGET THEN
    INSERT (PGCOD, FSH005FEC, FSH005MON, FSH005TCV, FECHA_PROCESO, BATCH_ID)
    VALUES (o.PGCOD, o.FSH005FEC, o.FSH005MON, o.FSH005TCV, '2026-10-10', 'SEMILLA');

PRINT CONCAT('STG_FSH005: ', (SELECT COUNT(*) FROM GPPPBTDB.STG_FSH005), ' fila(s).');
GO


-- ============================================================================
--  3. CAPA ODS   ::   GNBPE_ODS.DATAHUB
-- ============================================================================
USE GNBPE_ODS;
GO

IF SCHEMA_ID('DATAHUB') IS NULL EXEC('CREATE SCHEMA DATAHUB');
GO

IF OBJECT_ID('DATAHUB.ODS_TIPO_CAMBIO', 'U') IS NULL
BEGIN
    CREATE TABLE DATAHUB.ODS_TIPO_CAMBIO (
        -- Aqui SI es DATE. Convertir el DECIMAL(8,0) de Bantotal a una fecha
        -- de verdad es el trabajo que justifica que exista la capa ODS: STG es
        -- una copia fiel del origen, ODS es el dato ya tipado y limpio.
        COD_EMPRESA     INT             NOT NULL,
        FEC_TIPO_CAMBIO DATE            NOT NULL,
        COD_MONEDA      INT             NOT NULL,
        MTO_TIPO_CAMBIO DECIMAL(17,8)   NULL,
        -- Trazabilidad de la carga, no del negocio
        FECHA_CARGA     DATETIME2(3)    NOT NULL
            CONSTRAINT DF_OTC_FECHA DEFAULT (SYSDATETIME()),
        RUN_ID          VARCHAR(200)    NULL,
        CONSTRAINT PK_ODS_TIPO_CAMBIO PRIMARY KEY CLUSTERED
            (COD_EMPRESA, FEC_TIPO_CAMBIO, COD_MONEDA)
    );
    PRINT 'GNBPE_ODS.DATAHUB.ODS_TIPO_CAMBIO creada.';
END ELSE PRINT 'ODS_TIPO_CAMBIO ya existe.';
GO

IF OBJECT_ID('dbo.CONTROL_EJECUCIONES_SP', 'U') IS NULL
    EXEC('CREATE TABLE dbo.CONTROL_EJECUCIONES_SP (
        ID_EJECUCION BIGINT IDENTITY(1,1) PRIMARY KEY,
        NOMBRE_SP VARCHAR(256) NOT NULL, NOMBRE_PROCESO VARCHAR(256) NULL,
        ESTADO VARCHAR(20) NOT NULL, DURACION_SEGUNDOS DECIMAL(12,2) NULL,
        FILAS_AFECTADAS BIGINT NULL, MSG_ERROR VARCHAR(4000) NULL,
        RUN_ID VARCHAR(200) NULL,
        FECHA_REGISTRO DATETIME2(3) NOT NULL DEFAULT (SYSDATETIME()))');
GO

-- ----------------------------------------------------------------------------
--  SP_ODS_TIPO_CAMBIO   ::   STG -> ODS
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE DATAHUB.SP_ODS_TIPO_CAMBIO
    -- Sin parametros, porque el catalogo lo declara con PARAMETROS: [].
    -- Si manana hace falta la fecha, se anade aqui Y en el catalogo: el
    -- operador manda los parametros como marcadores '?', nunca concatenados.
    AS
BEGIN
    SET NOCOUNT ON;
    -- Sin esto, un error aborta la sentencia y SIGUE con la siguiente: se
    -- borraria el destino y despues fallaria el INSERT, dejando la capa vacia.
    SET XACT_ABORT ON;

    DECLARE @FILAS INT;

    BEGIN TRANSACTION;

    -- TRUNCATE + INSERT y no MERGE: esta es una tabla de maestros pequena y la
    -- carga es completa, asi que reescribirla entera es mas simple y mas rapido
    -- que decidir fila a fila. En una tabla de movimientos seria al contrario.
    --
    -- Va DENTRO de la transaccion: quien consulte ODS mientras el proceso corre
    -- ve la tabla completa -- la vieja o la nueva --, nunca vacia.
    TRUNCATE TABLE DATAHUB.ODS_TIPO_CAMBIO;

    INSERT INTO DATAHUB.ODS_TIPO_CAMBIO
        (COD_EMPRESA, FEC_TIPO_CAMBIO, COD_MONEDA, MTO_TIPO_CAMBIO, RUN_ID)
    SELECT
        CONVERT(INT, s.PGCOD),
        -- AAAAMMDD numerico -> DATE. CONVERT con estilo 112 es el camino
        -- directo; TRY_CONVERT para que una fecha invalida en el origen
        -- -- un 20260230, que en DECIMAL es un numero perfectamente valido --
        -- no tumbe la carga entera.
        TRY_CONVERT(DATE, CONVERT(CHAR(8), CONVERT(INT, s.FSH005FEC)), 112),
        CONVERT(INT, s.FSH005MON),
        s.FSH005TCV,
        CONVERT(VARCHAR(200), SESSION_CONTEXT(N'run_id'))
    FROM GNBPE_STG.GPPPBTDB.STG_FSH005 s
    -- La fila con fecha invalida se descarta aqui, no se cuela como NULL. Una
    -- fecha NULL en la clave primaria haria fallar el INSERT con un mensaje
    -- que no menciona la fecha.
    WHERE TRY_CONVERT(DATE,
              CONVERT(CHAR(8), CONVERT(INT, s.FSH005FEC)), 112) IS NOT NULL;

    SET @FILAS = @@ROWCOUNT;

    COMMIT TRANSACTION;

    -- Las descartadas se cuentan y se avisan. Que una carga descarte filas en
    -- silencio es la diferencia entre un dato incompleto que alguien detecta y
    -- uno que nadie detecta.
    DECLARE @DESCARTADAS INT = (
        SELECT COUNT(*) FROM GNBPE_STG.GPPPBTDB.STG_FSH005 s
        WHERE TRY_CONVERT(DATE,
                  CONVERT(CHAR(8), CONVERT(INT, s.FSH005FEC)), 112) IS NULL);

    IF @DESCARTADAS > 0
        RAISERROR('ODS_TIPO_CAMBIO: %d fila(s) descartada(s) por FSH005FEC invalida.',
                  0, 1, @DESCARTADAS) WITH NOWAIT;

    RAISERROR('ODS_TIPO_CAMBIO: %d fila(s) cargada(s).', 0, 1, @FILAS) WITH NOWAIT;
END
GO


-- ============================================================================
--  4. CAPA BDS   ::   GNBPE_BDS.DATAHUB
-- ============================================================================
USE GNBPE_BDS;
GO

IF SCHEMA_ID('DATAHUB') IS NULL EXEC('CREATE SCHEMA DATAHUB');
GO

IF OBJECT_ID('DATAHUB.BDS_TIPO_CAMBIO', 'U') IS NULL
BEGIN
    CREATE TABLE DATAHUB.BDS_TIPO_CAMBIO (
        COD_EMPRESA      INT            NOT NULL,
        FEC_TIPO_CAMBIO  DATE           NOT NULL,
        COD_MONEDA       INT            NOT NULL,
        MTO_TIPO_CAMBIO  DECIMAL(17,8)  NULL,
        -- Lo que BDS anade sobre ODS: el dato de negocio derivado. Aqui el
        -- inverso, que es lo que se usa para convertir de moneda extranjera a
        -- local sin dividir en cada consulta.
        MTO_TIPO_CAMBIO_INV DECIMAL(17,8) NULL,
        IND_MONEDA_LOCAL CHAR(1)        NULL,
        FECHA_CARGA      DATETIME2(3)   NOT NULL
            CONSTRAINT DF_BTC_FECHA DEFAULT (SYSDATETIME()),
        CONSTRAINT PK_BDS_TIPO_CAMBIO PRIMARY KEY CLUSTERED
            (COD_EMPRESA, FEC_TIPO_CAMBIO, COD_MONEDA)
    );
    PRINT 'GNBPE_BDS.DATAHUB.BDS_TIPO_CAMBIO creada.';
END ELSE PRINT 'BDS_TIPO_CAMBIO ya existe.';
GO

IF OBJECT_ID('dbo.CONTROL_EJECUCIONES_SP', 'U') IS NULL
    EXEC('CREATE TABLE dbo.CONTROL_EJECUCIONES_SP (
        ID_EJECUCION BIGINT IDENTITY(1,1) PRIMARY KEY,
        NOMBRE_SP VARCHAR(256) NOT NULL, NOMBRE_PROCESO VARCHAR(256) NULL,
        ESTADO VARCHAR(20) NOT NULL, DURACION_SEGUNDOS DECIMAL(12,2) NULL,
        FILAS_AFECTADAS BIGINT NULL, MSG_ERROR VARCHAR(4000) NULL,
        RUN_ID VARCHAR(200) NULL,
        FECHA_REGISTRO DATETIME2(3) NOT NULL DEFAULT (SYSDATETIME()))');
GO

-- ----------------------------------------------------------------------------
--  SP_BDS_TIPO_CAMBIO   ::   ODS -> BDS
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE DATAHUB.SP_BDS_TIPO_CAMBIO
    AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @FILAS INT;

    -- SE COMPRUEBA EL INSUMO ANTES DE BORRAR NADA.
    --
    -- La compuerta del DAG ya lo hace, pero este procedimiento se puede
    -- ejecutar a mano, y entonces nadie lo comprueba. Sin esto, un ODS vacio
    -- produce un BDS vacio y el proceso termina en EXITO: la capa queda
    -- borrada y nada lo dice.
    IF NOT EXISTS (SELECT 1 FROM GNBPE_ODS.DATAHUB.ODS_TIPO_CAMBIO)
        THROW 50010, 'GNBPE_ODS.DATAHUB.ODS_TIPO_CAMBIO esta vacia. No se toca BDS: un insumo vacio no debe vaciar el destino. Ejecute antes ODS_STG2ODS_CARGA_SP.', 1;

    BEGIN TRANSACTION;

    TRUNCATE TABLE DATAHUB.BDS_TIPO_CAMBIO;

    INSERT INTO DATAHUB.BDS_TIPO_CAMBIO
        (COD_EMPRESA, FEC_TIPO_CAMBIO, COD_MONEDA, MTO_TIPO_CAMBIO,
         MTO_TIPO_CAMBIO_INV, IND_MONEDA_LOCAL)
    SELECT
        o.COD_EMPRESA, o.FEC_TIPO_CAMBIO, o.COD_MONEDA, o.MTO_TIPO_CAMBIO,
        -- NULLIF antes de dividir. Un tipo de cambio en cero es un dato malo,
        -- no un motivo para abortar la carga de las demas monedas: sale NULL y
        -- se ve en la tabla.
        CASE WHEN NULLIF(o.MTO_TIPO_CAMBIO, 0) IS NULL THEN NULL
             ELSE CAST(1.0 / o.MTO_TIPO_CAMBIO AS DECIMAL(17,8)) END,
        CASE WHEN o.COD_MONEDA = 1 THEN 'S' ELSE 'N' END
    FROM GNBPE_ODS.DATAHUB.ODS_TIPO_CAMBIO o;

    SET @FILAS = @@ROWCOUNT;

    COMMIT TRANSACTION;

    RAISERROR('BDS_TIPO_CAMBIO: %d fila(s) cargada(s).', 0, 1, @FILAS) WITH NOWAIT;
END
GO


-- ============================================================================
--  5. PERMISOS   ::   CADA CAPA LEE LA ANTERIOR, NO LA ESCRIBE
-- ============================================================================
--  Aqui esta la parte que hace que las conexiones sean independientes DE
--  VERDAD y no solo por el nombre. Tres logins, uno por capa, cada uno con lo
--  minimo:
--
--      usr_stg   escribe en STG
--      usr_ods   escribe en ODS, LEE de STG
--      usr_bds   escribe en BDS, LEE de ODS
--
--  La asimetria es el punto: si usr_bds pudiera escribir en ODS, un error de
--  configuracion -- apuntar la Connection de BDS al catalogo de ODS -- escribiria
--  en la capa equivocada en vez de fallar.
--
--  DESCOMENTE Y PONGA SUS PROPIAS CLAVES. No las deje en el archivo: este
--  repositorio ya tuvo credenciales en git una vez.
-- ============================================================================
/*
USE master;
GO
CREATE LOGIN usr_stg WITH PASSWORD = '<ponga-una-clave>', CHECK_POLICY = ON;
CREATE LOGIN usr_ods WITH PASSWORD = '<ponga-una-clave>', CHECK_POLICY = ON;
CREATE LOGIN usr_bds WITH PASSWORD = '<ponga-una-clave>', CHECK_POLICY = ON;
GO

USE GNBPE_STG;
CREATE USER usr_stg FOR LOGIN usr_stg;
ALTER ROLE db_datareader ADD MEMBER usr_stg;
ALTER ROLE db_datawriter ADD MEMBER usr_stg;
-- ODS entra a STG SOLO a leer
CREATE USER usr_ods FOR LOGIN usr_ods;
ALTER ROLE db_datareader ADD MEMBER usr_ods;
GO

USE GNBPE_ODS;
CREATE USER usr_ods FOR LOGIN usr_ods;
ALTER ROLE db_datareader ADD MEMBER usr_ods;
ALTER ROLE db_datawriter ADD MEMBER usr_ods;
GRANT EXECUTE ON SCHEMA::DATAHUB TO usr_ods;
-- La bitacora vive en dbo, no en DATAHUB
GRANT INSERT, SELECT ON dbo.CONTROL_EJECUCIONES_SP TO usr_ods;
-- BDS entra a ODS SOLO a leer
CREATE USER usr_bds FOR LOGIN usr_bds;
ALTER ROLE db_datareader ADD MEMBER usr_bds;
GO

USE GNBPE_BDS;
CREATE USER usr_bds FOR LOGIN usr_bds;
ALTER ROLE db_datareader ADD MEMBER usr_bds;
ALTER ROLE db_datawriter ADD MEMBER usr_bds;
GRANT EXECUTE ON SCHEMA::DATAHUB TO usr_bds;
GRANT INSERT, SELECT ON dbo.CONTROL_EJECUCIONES_SP TO usr_bds;
GO
*/


-- ============================================================================
--  6. COMPROBACION DEL MONTAJE
-- ============================================================================
SELECT 'STG' AS CAPA, 'GNBPE_STG.GPPPBTDB.STG_FSH005' AS TABLA,
       (SELECT COUNT(*) FROM GNBPE_STG.GPPPBTDB.STG_FSH005) AS FILAS
UNION ALL
SELECT 'ODS', 'GNBPE_ODS.DATAHUB.ODS_TIPO_CAMBIO',
       (SELECT COUNT(*) FROM GNBPE_ODS.DATAHUB.ODS_TIPO_CAMBIO)
UNION ALL
SELECT 'BDS', 'GNBPE_BDS.DATAHUB.BDS_TIPO_CAMBIO',
       (SELECT COUNT(*) FROM GNBPE_BDS.DATAHUB.BDS_TIPO_CAMBIO);
GO

-- Los dos procedimientos tienen que aparecer, uno en cada base
SELECT 'GNBPE_ODS' AS BASE, s.name AS ESQUEMA, p.name AS PROCEDIMIENTO
FROM   GNBPE_ODS.sys.procedures p
JOIN   GNBPE_ODS.sys.schemas s ON s.schema_id = p.schema_id
UNION ALL
SELECT 'GNBPE_BDS', s.name, p.name
FROM   GNBPE_BDS.sys.procedures p
JOIN   GNBPE_BDS.sys.schemas s ON s.schema_id = p.schema_id;
GO


-- ============================================================================
--  7. PRUEBA MANUAL, SIN AIRFLOW
-- ----------------------------------------------------------------------------
--  Haga esto ANTES de lanzar los DAG. Si falla aqui, el problema es SQL y no
--  Airflow, y se diagnostica en un minuto en vez de en un log de tarea.
-- ============================================================================
/*
-- 7.1  BDS sobre ODS vacia: tiene que FALLAR con el mensaje 50010.
--      Es la comprobacion mas importante de las tres: demuestra que un insumo
--      vacio no vacia el destino.
USE GNBPE_BDS;
EXEC DATAHUB.SP_BDS_TIPO_CAMBIO;

-- 7.2  STG -> ODS
USE GNBPE_ODS;
EXEC DATAHUB.SP_ODS_TIPO_CAMBIO;
SELECT * FROM DATAHUB.ODS_TIPO_CAMBIO ORDER BY FEC_TIPO_CAMBIO, COD_MONEDA;
--      Esperado: 7 filas, FEC_TIPO_CAMBIO como DATE (2026-10-09..11)

-- 7.3  ODS -> BDS, ahora si
USE GNBPE_BDS;
EXEC DATAHUB.SP_BDS_TIPO_CAMBIO;
SELECT * FROM DATAHUB.BDS_TIPO_CAMBIO ORDER BY FEC_TIPO_CAMBIO, COD_MONEDA;
--      Esperado: 7 filas, con MTO_TIPO_CAMBIO_INV calculado y
--      IND_MONEDA_LOCAL = 'S' solo en COD_MONEDA = 1

-- 7.4  Idempotencia: relanzar los dos no debe duplicar nada
USE GNBPE_ODS; EXEC DATAHUB.SP_ODS_TIPO_CAMBIO;
USE GNBPE_BDS; EXEC DATAHUB.SP_BDS_TIPO_CAMBIO;
SELECT (SELECT COUNT(*) FROM GNBPE_ODS.DATAHUB.ODS_TIPO_CAMBIO) AS ODS,
       (SELECT COUNT(*) FROM GNBPE_BDS.DATAHUB.BDS_TIPO_CAMBIO) AS BDS;
--      Esperado: 7 y 7, otra vez

-- 7.5  Una fecha invalida en el origen no debe tumbar la carga
USE GNBPE_STG;
INSERT INTO GPPPBTDB.STG_FSH005
    (PGCOD, FSH005FEC, FSH005MON, FSH005TCV, FECHA_PROCESO, BATCH_ID)
VALUES (9, 20260230, 2, 3.80000000, '2026-10-10', 'FECHA_MALA');
USE GNBPE_ODS; EXEC DATAHUB.SP_ODS_TIPO_CAMBIO;
--      Esperado: avisa "1 fila(s) descartada(s)" y carga las 7 buenas
USE GNBPE_STG;
DELETE FROM GPPPBTDB.STG_FSH005 WHERE BATCH_ID = 'FECHA_MALA';
*/
