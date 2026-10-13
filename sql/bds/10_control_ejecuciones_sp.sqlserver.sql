-- ============================================================================
--  CONTROL_EJECUCIONES_SP   ::   SQL Server
-- ----------------------------------------------------------------------------
--  La bitacora de los dos pipelines por stored procedure:
--
--      ODS_STG2ODS_CARGA_SP     STG -> ODS
--      BDS_ODS2BDS_CARGA_SP     ODS -> BDS
--
--  Los dos la declaran en AUDITORIA.TABLA_LOG de sus Variables
--  (DAG_ODS_TABLAS y DAG_BDS_TABLAS), y hasta ahora NO tenia DDL en el
--  repositorio: el codigo avisaba y seguia cargando sin registrar nada.
--
--  POR QUE EL CODIGO NO LA CREA SOLO
--  ---------------------------------
--  plugins/operators/sp_operator.py si lo hace: si la tabla no existe, la
--  crea. Aqui no, a proposito. Inventar una tabla de auditoria con las
--  columnas que a uno le parezcan, en una base bancaria, es peor que no
--  tenerla: queda una tabla que nadie diseno, que el area de datos no conoce,
--  y que el dia de una revision no cuadra con nada.
--
--  Asi que la DDL vive aqui, versionada, y el proceso avisa cuando falta.
--
--  LAS COLUMNAS SON LAS QUE ESCRIBE etl_datahub_sql/ejecutar.py
--  ------------------------------------------------------------
--  Si cambia una, cambie tambien el INSERT de _registrar() en ese archivo. El
--  codigo captura el error y lo registra en el log nombrando las columnas que
--  espera, asi que una divergencia no tumba la carga -- pero deja de haber
--  bitacora, que es casi peor porque no se nota.
-- ============================================================================

USE GNBPE_DATAHUB;
GO

IF OBJECT_ID('dbo.CONTROL_EJECUCIONES_SP', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.CONTROL_EJECUCIONES_SP (

        ID_EJECUCION      BIGINT         IDENTITY(1,1) NOT NULL,

        -- Nombre calificado tal como se llamo: DATAHUB.SP_ODS_TIPO_CAMBIO.
        -- Con esquema, porque el mismo nombre corto puede existir en dos.
        NOMBRE_SP         VARCHAR(256)   NOT NULL,

        -- NOMBRE_PROCESO del catalogo (CARGA_STG_ODS_TIPO_CAMBIO). Es el
        -- nombre con el que el equipo habla del proceso, y no coincide con el
        -- del procedimiento. Guardar los dos evita la traduccion mental.
        NOMBRE_PROCESO    VARCHAR(256)   NULL,

        -- 'OK' o 'ERROR'. Sin estado intermedio a proposito: el registro se
        -- escribe UNA vez, al terminar. Un 'EN_PROCESO' que nadie cierra
        -- -- porque el worker murio -- es un estado que miente, y despues hay
        -- que escribir una consulta para distinguir los que de verdad siguen
        -- corriendo. Lo que esta corriendo se mira en Airflow.
        ESTADO            VARCHAR(20)    NOT NULL,

        DURACION_SEGUNDOS DECIMAL(12,2)  NULL,

        -- NULL cuando el motor no informa rowcount, que con un CALL es lo
        -- habitual. NULL y no 0: cero filas afectadas y "no se sabe" son
        -- cosas distintas, y un 0 inventado haria pensar que el SP no cargo.
        FILAS_AFECTADAS   BIGINT         NULL,

        -- 4000 porque es lo que el catalogo declara en error_size_limit de los
        -- pipelines hermanos. Una traza de SQL Server completa pasa de eso, y
        -- el codigo la recorta; el mensaje util esta siempre al principio.
        MSG_ERROR         VARCHAR(4000)  NULL,

        -- run_id de Airflow. Es la llave para cruzar esta tabla con el log de
        -- la corrida: sin ella, dos ejecuciones del mismo SP el mismo dia son
        -- indistinguibles.
        RUN_ID            VARCHAR(200)   NULL,

        FECHA_REGISTRO    DATETIME2(3)   NOT NULL
            CONSTRAINT DF_CESP_FECHA DEFAULT (SYSDATETIME()),

        CONSTRAINT PK_CONTROL_EJECUCIONES_SP PRIMARY KEY CLUSTERED (ID_EJECUCION),
        CONSTRAINT CK_CESP_ESTADO CHECK (ESTADO IN ('OK', 'ERROR'))
    );

    -- Las dos consultas que de verdad se hacen sobre esta tabla:
    --   "que fallo en la corrida de hoy"  -> por RUN_ID
    --   "este SP cuanto tarda, y desde cuando" -> por NOMBRE_SP y fecha
    CREATE NONCLUSTERED INDEX IX_CESP_RUN
        ON dbo.CONTROL_EJECUCIONES_SP (RUN_ID)
        INCLUDE (NOMBRE_SP, ESTADO, DURACION_SEGUNDOS);

    CREATE NONCLUSTERED INDEX IX_CESP_SP_FECHA
        ON dbo.CONTROL_EJECUCIONES_SP (NOMBRE_SP, FECHA_REGISTRO DESC)
        INCLUDE (ESTADO, DURACION_SEGUNDOS, FILAS_AFECTADAS);

    PRINT 'CONTROL_EJECUCIONES_SP creada.';
END
ELSE
BEGIN
    PRINT 'CONTROL_EJECUCIONES_SP ya existe. No se toca.';
    -- Si ya existe con otro esquema, esto dice cual es y permite comparar con
    -- el INSERT de _registrar(). Es la causa mas probable de que la bitacora
    -- "no registre nada" sin que el DAG falle.
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM   INFORMATION_SCHEMA.COLUMNS
    WHERE  TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'CONTROL_EJECUCIONES_SP'
    ORDER  BY ORDINAL_POSITION;
END
GO


-- ============================================================================
--  LAS DOS CONSULTAS DE OPERACION
-- ============================================================================

-- Que paso en la ultima corrida
/*
SELECT TOP 50 RUN_ID, NOMBRE_PROCESO, NOMBRE_SP, ESTADO,
       DURACION_SEGUNDOS, FILAS_AFECTADAS, FECHA_REGISTRO,
       LEFT(MSG_ERROR, 200) AS ERROR
FROM   dbo.CONTROL_EJECUCIONES_SP
ORDER  BY ID_EJECUCION DESC;
*/

-- Como evoluciona cada SP: si uno empieza a tardar el doble, sale aqui antes
-- de que alguien se queje
/*
SELECT NOMBRE_SP,
       COUNT(*)                                AS CORRIDAS,
       SUM(CASE WHEN ESTADO='ERROR' THEN 1 ELSE 0 END) AS CON_ERROR,
       CAST(AVG(DURACION_SEGUNDOS) AS DECIMAL(12,1))   AS SEG_PROMEDIO,
       MAX(DURACION_SEGUNDOS)                  AS SEG_PEOR,
       MAX(FECHA_REGISTRO)                     AS ULTIMA
FROM   dbo.CONTROL_EJECUCIONES_SP
WHERE  FECHA_REGISTRO >= DATEADD(DAY, -30, SYSDATETIME())
GROUP  BY NOMBRE_SP
ORDER  BY SEG_PROMEDIO DESC;
*/
