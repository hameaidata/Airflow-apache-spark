-- ===========================================================================
-- 00_bt2sql_base_y_esquema.sql
--
-- Crea la base GNBPE_DATAHUB y el esquema GPPPBTDB que simula la biblioteca de
-- Bantotal, con las dos tablas de origen que necesita la prueba.
--
-- ORDEN DE EJECUCION DE LOS SCRIPTS
--   00_bt2sql_base_y_esquema.sql     <- este
--   20_bt2sql_control_sqlserver.sql     las tres tablas de control
--   90_bt2sql_prueba_loopback.sql       STG_FSH005 y la fila del catalogo
--
-- IMPORTANTE ANTES DE EJECUTARLO EN DBEAVER
--   CREATE DATABASE no se puede ejecutar dentro de una transaccion. Si la
--   conexion esta en manual-commit -el valor por defecto de las conexiones
--   marcadas como Production-, falla con:
--       "CREATE DATABASE statement not allowed within multi-statement
--        transaction"
--   Cambie la conexion a auto-commit antes de correr este script: el boton de
--   commit en la barra de herramientas, o click derecho en la conexion >
--   Transaction mode > Auto-commit. Los otros dos scripts corren igual en
--   cualquiera de los dos modos.
--
--   Y la conexion tiene que apuntar a master, no a GNBPE_DATAHUB: la base
--   todavia no existe.
--
-- Alt+X para ejecutar el script completo.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. LA BASE
-- ---------------------------------------------------------------------------
USE master;
GO

IF DB_ID('GNBPE_DATAHUB') IS NULL
BEGIN
    -- Collation explicita y no la del servidor, porque de la del servidor no
    -- se sabe nada: una instalacion con SQL_Latin1_General_CP1_CI_AS y otra
    -- con Modern_Spanish_CI_AS ordenan y comparan distinto en cuanto aparece
    -- una enie o una tilde, y eso se descubre tarde y en una comparacion que
    -- no cuadra.
    CREATE DATABASE GNBPE_DATAHUB
        COLLATE Latin1_General_CI_AS;

    -- SIMPLE en la de pruebas: con FULL el log crece sin limite hasta que
    -- alguien haga un backup del log, y nadie va a hacerle backups a esto.
    -- En produccion esto va en FULL, que es lo que permite recuperar a un
    -- punto en el tiempo.
    ALTER DATABASE GNBPE_DATAHUB SET RECOVERY SIMPLE;
END
GO

USE GNBPE_DATAHUB;
GO

-- ---------------------------------------------------------------------------
-- 2. EL ESQUEMA QUE SIMULA LA BIBLIOTECA DE BANTOTAL
-- ---------------------------------------------------------------------------
-- El job arma la consulta como "SELECT ... FROM esquema.tabla"
-- (bt2sql_extraccion_spark.py:283). En Bantotal GPPPBTDB es una biblioteca de
-- IBM i; aqui es un esquema de SQL Server. El SQL que sale es identico, y por
-- eso la Variable no necesita cambiar 'esquema' al pasar a produccion.
--
-- EXEC() y no CREATE SCHEMA directo: CREATE SCHEMA tiene que ser la primera
-- sentencia de su lote, asi que dentro de un IF no compila.
IF SCHEMA_ID('GPPPBTDB') IS NULL
    EXEC('CREATE SCHEMA GPPPBTDB');
GO

-- ---------------------------------------------------------------------------
-- 3. FST017  -  la fecha de proceso
-- ---------------------------------------------------------------------------
-- De aqui sale la fecha de proceso de toda la corrida. El job la lee del
-- ORIGEN y no del destino a proposito (bt2sql_extraccion_spark.py:570): la STG
-- del destino la llena este mismo proceso, asi que leerla de ahi seria
-- preguntarle a la corrida anterior.
--
-- PGFCIE es DECIMAL(8,0) con la fecha como yyyymmdd, igual que el core, porque
-- el job hace strptime("%Y%m%d") sobre lo que venga. Un 0 o un NULL aqui corta
-- la corrida antes de tocar ninguna tabla, y eso es lo correcto: sin fecha de
-- proceso no hay nada que extraer.
IF OBJECT_ID('GPPPBTDB.FST017', 'U') IS NULL
    CREATE TABLE GPPPBTDB.FST017 (
        PGCOD   DECIMAL(3,0)    NOT NULL,      -- codigo de empresa
        PGFCIE  DECIMAL(8,0)    NOT NULL,      -- fecha de cierre, yyyymmdd
        CONSTRAINT PK_FST017 PRIMARY KEY (PGCOD)
    );
GO

IF NOT EXISTS (SELECT 1 FROM GPPPBTDB.FST017)
    INSERT INTO GPPPBTDB.FST017 (PGCOD, PGFCIE) VALUES (1, 20261010);
GO

-- ---------------------------------------------------------------------------
-- 4. FSH005  -  tipo de cambio
-- ---------------------------------------------------------------------------
-- Las cuatro columnas son EXACTAMENTE las que declara 'tipos' en la Variable,
-- y en ese orden. No es casualidad: la entrada de FSH005 tiene columnas = ""
-- (todas), la carga lee las columnas del parquet y hace
--     SELECT TOP 0 <esas columnas> INTO staging FROM <destino>
-- (bt2sql_carga_spark.py:271-281). Una quinta columna aqui que STG_FSH005 no
-- tenga hace fallar esa sentencia.
IF OBJECT_ID('GPPPBTDB.FSH005', 'U') IS NULL
    CREATE TABLE GPPPBTDB.FSH005 (
        PGCOD       DECIMAL(3,0)    NOT NULL,  -- empresa
        FSH005FEC   DECIMAL(8,0)    NOT NULL,  -- yyyymmdd numerico, NO date
        FSH005MON   DECIMAL(4,0)    NOT NULL,  -- codigo de moneda
        FSH005TCV   DECIMAL(17,8)   NOT NULL,  -- tipo de cambio venta
        CONSTRAINT PK_FSH005 PRIMARY KEY (PGCOD, FSH005FEC, FSH005MON)
    );
GO

IF NOT EXISTS (SELECT 1 FROM GPPPBTDB.FSH005)
    INSERT INTO GPPPBTDB.FSH005 (PGCOD, FSH005FEC, FSH005MON, FSH005TCV)
    VALUES
        (1, 20261008, 2, 3.75100000),
        (1, 20261008, 3, 4.08200000),
        (1, 20261009, 2, 3.74800000),
        (1, 20261009, 3, 4.09500000),
        (1, 20261010, 2, 3.75300000),
        (1, 20261010, 3, 4.10100000);
GO

-- ---------------------------------------------------------------------------
-- 5. VERIFICACION
-- ---------------------------------------------------------------------------
-- Esta es, literal, la consulta que el job ejecuta como sql_fecha. Si devuelve
-- una fila con 20261010, el paso de la fecha de proceso va a funcionar.
SELECT TOP 1 PGFCIE FROM GPPPBTDB.FST017;
GO

SELECT DB_NAME()                                        AS base,
       SCHEMA_NAME(SCHEMA_ID('GPPPBTDB'))               AS esquema,
       (SELECT COUNT(*) FROM GPPPBTDB.FST017)           AS filas_fst017,
       (SELECT COUNT(*) FROM GPPPBTDB.FSH005)           AS filas_fsh005,
       CONVERT(VARCHAR(8), (SELECT TOP 1 PGFCIE FROM GPPPBTDB.FST017))
                                                        AS fecha_de_proceso;
GO

-- ---------------------------------------------------------------------------
-- 6. DESHACER  (descomentar solo para limpiar)
-- ---------------------------------------------------------------------------
-- USE master;
-- ALTER DATABASE GNBPE_DATAHUB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
-- DROP DATABASE GNBPE_DATAHUB;
