-- ===========================================================================
-- 90_bt2sql_prueba_loopback.sql
--
-- Juego de pruebas de STG_BT2SQL_CARGA_SPARK **sin el IBM i**.
--
-- Simula Bantotal con un esquema GPPPBTDB dentro del propio SQL Server, para
-- que el pipeline completo -fecha de proceso, catalogo, extraccion a parquet,
-- staging y carga- se pueda recorrer de punta a punta con una sola base.
--
-- Esto funciona porque el job arma la consulta como "SELECT ... FROM
-- esquema.tabla" (bt2sql_extraccion_spark.py:283). En Bantotal GPPPBTDB es una
-- biblioteca; en SQL Server es un esquema. El SQL que sale es el mismo.
--
-- COMO EJECUTARLO
--   DBeaver, conexion a GNBPE_DATAHUB, Alt+X (Execute script). Los lotes van
--   separados con GO, que DBeaver entiende en dialecto SQL Server.
--   Si la conexion esta en modo manual-commit, Commit al terminar.
--
-- REQUISITOS PREVIOS, EN ESTE ORDEN
--   00_bt2sql_base_y_esquema.sql     base GNBPE_DATAHUB + esquema GPPPBTDB + origen
--   20_bt2sql_control_sqlserver.sql  CTL_PARAMETROS_PARQUET, ctl_proceso_parquet,
--                                    ctl_carga_stg
--
-- PARA DESHACERLO
--   El bloque final, comentado, borra todo lo que crea este script.
-- ===========================================================================
USE GNBPE_DATAHUB;
GO

-- ---------------------------------------------------------------------------
-- 1. ORIGEN SIMULADO
-- ---------------------------------------------------------------------------
-- Se mudo a 00_bt2sql_base_y_esquema.sql, que crea la base GNBPE_DATAHUB, el
-- esquema GPPPBTDB y las tablas FST017 y FSH005. Dos scripts creando lo mismo
-- es una fuente de divergencia; ejecute el 00 primero.

-- ---------------------------------------------------------------------------
-- 2. DESTINO
-- ---------------------------------------------------------------------------
-- FECHA_PROCESO primera y BATCH_ID ultima no es decoracion: la extraccion las
-- inyecta en ese orden exacto (bt2sql_extraccion_spark.py:632-635) y la carga
-- arma el INSERT leyendo las columnas del parquet. Si faltan, el error es
-- "Invalid column name".
--
-- Con tipos_desde_destino = true esta tabla manda: cuando una entrada no
-- declara "tipos", el job lee su INFORMATION_SCHEMA.COLUMNS y castea el
-- parquet a estos tipos. Es decir, los tipos del parquet salen de aqui.
IF OBJECT_ID('dbo.STG_FSH005', 'U') IS NULL
    CREATE TABLE dbo.STG_FSH005 (
        FECHA_PROCESO   DATE            NULL,       -- inyectada, PRIMERA
        PGCOD           DECIMAL(3,0)    NULL,
        FSH005FEC       DECIMAL(8,0)    NULL,
        FSH005MON       DECIMAL(4,0)    NULL,
        FSH005TCV       DECIMAL(17,8)   NULL,
        BATCH_ID        VARCHAR(14)     NULL        -- inyectada, ULTIMA
    );
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'IX_STG_FSH005_fecha'
                 AND object_id = OBJECT_ID('dbo.STG_FSH005'))
    CREATE INDEX IX_STG_FSH005_fecha
        ON dbo.STG_FSH005 (FECHA_PROCESO, BATCH_ID);
GO

-- ---------------------------------------------------------------------------
-- 3. CATALOGO
-- ---------------------------------------------------------------------------
-- Con validacion_catalogo = 'estricto', una tabla declarada en la Variable y
-- no registrada aqui CORTA la corrida en validar_catalogo. Y si TABLA_DESTINO
-- no coincide con tabla_destino de la Variable, tambien.
--
-- MERGE y no INSERT para que el script sea reejecutable. WITH (HOLDLOCK)
-- porque sin el dos ejecuciones simultaneas pueden insertar la misma fila dos
-- veces: el SELECT de busqueda y el INSERT no son atomicos por si solos.
MERGE dbo.CTL_PARAMETROS_PARQUET WITH (HOLDLOCK) AS d
USING (VALUES
    ('GPPPBTDB', 'FSH005', NULL, NULL, 'S', 'STG_FSH005',
     'PGCOD:DECIMAL(3,0)|FSH005FEC:DECIMAL(8,0)|FSH005MON:DECIMAL(4,0)|FSH005TCV:DECIMAL(17,8)',
     'STG_FSH005', 5000)
) AS o (ESQUEMA, TABLA, COLUMNAS, FILTRO, ACTIVO, NOMBRE_PARQUET,
        TIPOS, TABLA_DESTINO, BATCH_SIZE)
   ON  d.ESQUEMA = o.ESQUEMA AND d.TABLA = o.TABLA
WHEN MATCHED THEN UPDATE SET
        d.COLUMNAS       = o.COLUMNAS,
        d.FILTRO         = o.FILTRO,
        d.ACTIVO         = o.ACTIVO,
        d.NOMBRE_PARQUET = o.NOMBRE_PARQUET,
        d.TIPOS          = o.TIPOS,
        d.TABLA_DESTINO  = o.TABLA_DESTINO,
        d.BATCH_SIZE     = o.BATCH_SIZE
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ESQUEMA, TABLA, COLUMNAS, FILTRO, ACTIVO, NOMBRE_PARQUET,
            TIPOS, TABLA_DESTINO, BATCH_SIZE)
    VALUES (o.ESQUEMA, o.TABLA, o.COLUMNAS, o.FILTRO, o.ACTIVO,
            o.NOMBRE_PARQUET, o.TIPOS, o.TABLA_DESTINO, o.BATCH_SIZE);
GO

-- ---------------------------------------------------------------------------
-- 4. VERIFICACION
-- ---------------------------------------------------------------------------
SELECT 'fecha de proceso' AS que, CAST(PGFCIE AS VARCHAR(8)) AS valor
FROM   GPPPBTDB.FST017
UNION ALL
SELECT 'filas en el origen', CAST(COUNT(*) AS VARCHAR(20)) FROM GPPPBTDB.FSH005
UNION ALL
SELECT 'filas en la STG', CAST(COUNT(*) AS VARCHAR(20)) FROM dbo.STG_FSH005
UNION ALL
SELECT 'filas en el catalogo', CAST(COUNT(*) AS VARCHAR(20))
FROM   dbo.CTL_PARAMETROS_PARQUET WHERE ACTIVO = 'S';
GO

-- Las tres tablas de control tienen que existir ya. Si alguna sale en 0,
-- falta ejecutar 20_bt2sql_control_sqlserver.sql.
SELECT name AS tabla_de_control
FROM   sys.tables
WHERE  name IN ('CTL_PARAMETROS_PARQUET', 'ctl_proceso_parquet', 'ctl_carga_stg');
GO

-- ---------------------------------------------------------------------------
-- 5. DESHACER  (descomentar solo para limpiar)
-- ---------------------------------------------------------------------------
-- DELETE FROM dbo.CTL_PARAMETROS_PARQUET WHERE ESQUEMA = 'GPPPBTDB' AND TABLA = 'FSH005';
-- DELETE FROM dbo.ctl_carga_stg;
-- DELETE FROM dbo.ctl_proceso_parquet;
-- DROP TABLE IF EXISTS dbo.STG_FSH005;
-- (el origen y la base se deshacen desde 00_bt2sql_base_y_esquema.sql)
