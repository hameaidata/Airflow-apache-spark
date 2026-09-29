-- ============================================================================
--  BT2SQL - Tablas de control en SQL Server 2022
-- ============================================================================
--
--  QUE ES ESTO
--  -----------
--  Las tres tablas que sostienen el rastro de auditoria del pipeline
--  Bantotal -> parquet -> STG. Son los mismos nombres que ya usa el pipeline
--  de SingleStore, a proposito: las consultas de operacion que el equipo ya
--  tiene escritas sirven para los dos sin cambiar una linea.
--
--      CTL_PARAMETROS_PARQUET   QUE se extrae y A DONDE va      (catalogo)
--      ctl_proceso_parquet      QUE paso en cada extraccion     (bitacora)
--      ctl_carga_stg            QUE paso en cada carga          (bitacora)
--
--  El catalogo lo administra el equipo de datos. Las dos bitacoras las
--  escriben los DAGs y nadie las edita a mano.
--
--  COMO SE ENCADENA TODO
--  ---------------------
--      extraccion  lee   CTL_PARAMETROS_PARQUET   (que tablas, que columnas)
--                  abre  una fila en ctl_proceso_parquet POR TABLA
--                  escribe  /data/bt2sql/<yyyyMMdd>/<TABLA>/<TABLA>.parquet
--                  cierra la fila con la RUTA COMPLETA y el numero de filas
--
--      carga       lee   ctl_proceso_parquet del mismo batch_id
--                        unida a CTL_PARAMETROS_PARQUET para saber el destino
--                  abre  una fila en ctl_carga_stg POR ARCHIVO
--                  sube  el parquet a la tabla STG
--                  cierra la fila
--
--  El punto de encuentro entre las dos mitades es ctl_proceso_parquet: la
--  extraccion deja ahi la ruta, la carga la lee. Ninguna de las dos tiene una
--  ruta escrita en el codigo, que es lo que permite que la carpeta cambie cada
--  dia sin que nadie edite nada.
--
--  SOBRE LA RUTA QUE SE GUARDA
--  ---------------------------
--  Se guarda la ruta DENTRO DEL CONTENEDOR, que siempre es Linux, porque es la
--  unica que los procesos pueden abrir. Donde este esa carpeta en el host se
--  decide en el .env y no afecta a estos datos:
--
--      BT2SQL_PARQUET_HOST_DIR=./data/bt2sql          (Windows, en el proyecto)
--      BT2SQL_PARQUET_HOST_DIR=/datos/datahub/bt2sql  (Red Hat)
--      BT2SQL_PARQUET_CONTAINER_DIR=/data/bt2sql      (las dos)
--
--  COMO SE EJECUTA
--  ---------------
--      sqlcmd -S <servidor> -d GNBPE_DATAHUB -U <usuario> -P <clave> \
--             -i sql/bt2sql/20_bt2sql_control_sqlserver.sql
--
--  Es idempotente: se puede correr las veces que haga falta. Solo crea lo que
--  no existe y nunca borra datos.
-- ============================================================================

USE GNBPE_DATAHUB;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO


-- ============================================================================
-- 1. CTL_PARAMETROS_PARQUET - el catalogo
-- ============================================================================
-- Una fila por tabla de Bantotal que se quiere traer.
--
-- Esta tabla es la que decide QUE se extrae. La Variable BT2SQL_EXTRACCION
-- decide CUANDO (diario, semanal, mensual). Una tabla tiene que estar en los
-- dos sitios para entrar en una corrida: aqui con ACTIVO='S', y alli en
-- "procesos" con estado=1 y el flag del tipo de ejecucion en 1.
--
-- Esa doble llave parece redundante y no lo es: permite desactivar una tabla
-- para TODOS los calendarios de golpe (ACTIVO='N', un UPDATE) sin tocar la
-- Variable, y sacarla solo del diario (estado_diario=0) sin tocar la base.
-- ============================================================================
IF OBJECT_ID('dbo.CTL_PARAMETROS_PARQUET', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.CTL_PARAMETROS_PARQUET (

        ID_PARAMETRO    INT             IDENTITY(1,1) NOT NULL,

        -- Biblioteca de Bantotal (GPPPBTDB en preproduccion). Junto con TABLA
        -- forma el nombre que se consulta contra el core: GPPPBTDB.FSH005
        ESQUEMA         VARCHAR(128)    NOT NULL,

        -- Tabla o fisico de Bantotal, tal cual esta en el core.
        TABLA           VARCHAR(128)    NOT NULL,

        -- Columnas a traer, separadas por coma. NULL o vacio = todas.
        -- Listarlas es lo recomendable: un SELECT * contra el core trae
        -- columnas que nadie usa y que igual hay que mover por la red.
        COLUMNAS        VARCHAR(4000)   NULL,

        -- WHERE opcional, SIN la palabra WHERE. Ejemplo:
        --     FSH005FEC >= 20260101
        --
        -- CUIDADO: esto es SQL libre que se concatena en la consulta contra el
        -- core. No se puede validar sin un parser, asi que quien pueda editar
        -- esta columna puede leer cualquier cosa del origen. Dale permiso de
        -- escritura solo a quien ya tendria ese acceso de todos modos.
        FILTRO          VARCHAR(4000)   NULL,

        -- 'S' o 'N'. El SELECT del catalogo solo trae las 'S'.
        ACTIVO          CHAR(1)         NOT NULL CONSTRAINT DF_CPP_ACTIVO DEFAULT ('S'),

        -- Nombre logico del proceso. Es la llave que cruza con
        -- "nombre_proceso" de la Variable BT2SQL_EXTRACCION.
        -- Por convencion: STG_<TABLA>.
        NOMBRE_PARQUET  VARCHAR(128)    NOT NULL,

        -- Tipos forzados, si hace falta. Formato: COLUMNA:TIPO, separados por
        -- coma. Se usa cuando el core devuelve un DECIMAL sin escala que
        -- pandas infiere mal.
        TIPOS           VARCHAR(4000)   NULL,

        -- Tabla STG destino en GNBPE_DATAHUB. Es lo que la carga necesita para
        -- saber donde poner el archivo.
        TABLA_DESTINO   VARCHAR(128)    NOT NULL,

        -- Filas por lote al insertar. NULL -> batch_default de BT2SQL_CARGA.
        BATCH_SIZE      INT             NULL,

        DESCRIPCION     VARCHAR(500)    NULL,
        FEC_CREACION    DATETIME2(3)    NOT NULL CONSTRAINT DF_CPP_FEC DEFAULT (SYSDATETIME()),

        CONSTRAINT PK_CTL_PARAMETROS_PARQUET PRIMARY KEY CLUSTERED (ID_PARAMETRO),

        -- La carga une por (TABLA, ESQUEMA). Si hubiera dos filas con el mismo
        -- par, el join multiplicaria las filas y el mismo parquet se cargaria
        -- dos veces sin que nada avisara. Esta restriccion lo hace imposible.
        CONSTRAINT UK_CTL_PARAMETROS_PARQUET UNIQUE (ESQUEMA, TABLA),

        CONSTRAINT CK_CPP_ACTIVO CHECK (ACTIVO IN ('S', 'N'))
    );

    CREATE INDEX IX_CPP_ACTIVO ON dbo.CTL_PARAMETROS_PARQUET (ACTIVO) INCLUDE (TABLA, ESQUEMA);

    PRINT 'CTL_PARAMETROS_PARQUET creada.';
END
ELSE
    PRINT 'CTL_PARAMETROS_PARQUET ya existe. No se toca.';
GO


-- ============================================================================
-- 2. ctl_proceso_parquet - bitacora de extraccion
-- ============================================================================
-- UNA FILA POR TABLA Y POR CORRIDA. No una por corrida: si 20 tablas se
-- extraen y una falla, se ve exactamente cual, con su error, su hora y su
-- duracion, sin abrir el log de Airflow.
--
-- Es ademas el catalogo de archivos: archivo_parquet guarda la ruta COMPLETA,
-- y de ahi la saca la carga.
--
-- El nombre va en minusculas porque asi esta en la Variable y asi estaba ya en
-- SingleStore. SQL Server no distingue mayusculas en los nombres de objeto con
-- la intercalacion habitual, asi que da igual; se deja igual para que las
-- consultas se puedan copiar entre los dos motores.
-- ============================================================================
IF OBJECT_ID('dbo.ctl_proceso_parquet', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ctl_proceso_parquet (

        id_log              BIGINT          IDENTITY(1,1) NOT NULL,

        -- Constante 'EXTRACCION_BANTOTAL'. Distingue estas filas de las que
        -- escribe el pipeline de SingleStore si algun dia comparten tabla.
        nom_proceso         VARCHAR(100)    NOT NULL,

        -- Biblioteca del core. Segunda mitad de la llave del join.
        esquema             VARCHAR(128)    NULL,

        -- Tabla de origen. Primera mitad de la llave del join.
        tabla_origen        VARCHAR(150)    NULL,

        -- RUTA ABSOLUTA Y COMPLETA dentro del contenedor:
        --     /data/bt2sql/20260926/FSH005/FSH005.parquet
        -- NULL cuando la tabla no devolvio filas: en ese caso no se escribe
        -- archivo, y la carga sabe que no hay nada que subir.
        archivo_parquet     VARCHAR(500)    NULL,

        -- Identificador de la corrida, yyyyMMddHHmmss. Viaja por XCom a la
        -- carga, para que suba exactamente lo que se acaba de extraer y no los
        -- archivos de ayer si la extraccion de hoy fallo.
        batch_id            VARCHAR(14)     NULL,

        -- Fecha de NEGOCIO (la que devuelve sql_fecha contra FST017 del core),
        -- no la del reloj. Es tambien el nombre de la carpeta del dia. Por eso
        -- una corrida lanzada a las 2 de la madrugada se archiva bajo el dia
        -- contable correcto y no bajo el siguiente.
        fecha_proceso       DATE            NULL,

        -- Cada cuantos lotes hace commit la carga. NULL -> commit_default.
        commit_every        INT             NULL,

        fec_inicio          DATETIME2(3)    NOT NULL,
        fec_termino         DATETIME2(3)    NULL,

        -- EJECUTANDO | TERMINADO | SIN_DATOS | ERROR
        --
        -- SIN_DATOS no es un error: la tabla se consulto bien y no tenia
        -- filas. Se separa de TERMINADO a proposito, porque la carga NO debe
        -- tocar una tabla destino que no recibio datos (truncarla la dejaria
        -- vacia sin motivo), y porque una tabla que aparece dias seguidos como
        -- SIN_DATOS casi siempre significa que el FILTRO quedo mal.
        estado              VARCHAR(20)     NOT NULL,

        -- socket.gethostname() del worker. Util cuando hay varios y solo en
        -- uno falta el montaje de la carpeta de parquet.
        host_name           VARCHAR(100)    NULL,

        msg_error           VARCHAR(4000)   NULL,
        filas_procesadas    BIGINT          NULL,
        duracion_segundos   DECIMAL(12,2)   NULL,

        CONSTRAINT PK_ctl_proceso_parquet PRIMARY KEY CLUSTERED (id_log)
    );

    -- La carga filtra por batch_id + estado en cada corrida. Sin este indice
    -- son recorridos completos de una tabla que crece todos los dias.
    CREATE INDEX IX_cpp_batch  ON dbo.ctl_proceso_parquet (batch_id, estado)
        INCLUDE (tabla_origen, esquema, archivo_parquet, commit_every);

    CREATE INDEX IX_cpp_tabla  ON dbo.ctl_proceso_parquet (tabla_origen, estado, batch_id);
    CREATE INDEX IX_cpp_fecha  ON dbo.ctl_proceso_parquet (fecha_proceso);

    PRINT 'ctl_proceso_parquet creada.';
END
ELSE
    PRINT 'ctl_proceso_parquet ya existe. No se toca.';
GO


-- ============================================================================
-- 3. ctl_carga_stg - bitacora de carga
-- ============================================================================
-- Una fila por ARCHIVO cargado. La contraparte de ctl_proceso_parquet: con las
-- dos unidas por batch_id se sigue una tabla de punta a punta, desde que sale
-- del core hasta que esta en STG.
-- ============================================================================
IF OBJECT_ID('dbo.ctl_carga_stg', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ctl_carga_stg (

        id_log              BIGINT          IDENTITY(1,1) NOT NULL,

        -- Constante 'PY_CARGA_BT2SQL_STG' (viene de nom_proceso en la Variable
        -- BT2SQL_CARGA). Distingue estas filas de las del pipeline SingleStore.
        nom_proceso         VARCHAR(100)    NOT NULL,

        -- El mismo batch_id de la extraccion. Es lo que une las dos bitacoras.
        batch_id            VARCHAR(14)     NULL,

        archivo_parquet     VARCHAR(500)    NULL,
        tabla_destino       VARCHAR(150)    NULL,

        fec_inicio          DATETIME2(3)    NOT NULL,
        fec_termino         DATETIME2(3)    NULL,

        -- INICIADO | EJECUTADO | FINALIZADO | ERROR
        -- Los cuatro valores salen de la Variable BT2SQL_CARGA, no estan
        -- escritos en el codigo.
        estado              VARCHAR(20)     NOT NULL,

        host_name           VARCHAR(100)    NULL,
        msg_error           VARCHAR(4000)   NULL,
        filas_cargadas      BIGINT          NULL,
        duracion_segundos   DECIMAL(12,2)   NULL,

        CONSTRAINT PK_ctl_carga_stg PRIMARY KEY CLUSTERED (id_log)
    );

    CREATE INDEX IX_ccs_batch ON dbo.ctl_carga_stg (batch_id, estado);
    CREATE INDEX IX_ccs_fecha ON dbo.ctl_carga_stg (fec_inicio DESC);

    PRINT 'ctl_carga_stg creada.';
END
ELSE
    PRINT 'ctl_carga_stg ya existe. No se toca.';
GO


-- ============================================================================
-- 4. ALTERs para una instalacion que ya tenga las tablas
-- ============================================================================
-- Si las tablas vienen de una version anterior, estos ALTER agregan lo que
-- falta y no hacen nada si ya esta. Se pueden correr sin miedo.
-- ============================================================================
IF COL_LENGTH('dbo.CTL_PARAMETROS_PARQUET', 'TABLA_DESTINO') IS NULL
    ALTER TABLE dbo.CTL_PARAMETROS_PARQUET ADD TABLA_DESTINO VARCHAR(128) NULL;
GO
IF COL_LENGTH('dbo.CTL_PARAMETROS_PARQUET', 'BATCH_SIZE') IS NULL
    ALTER TABLE dbo.CTL_PARAMETROS_PARQUET ADD BATCH_SIZE INT NULL;
GO
IF COL_LENGTH('dbo.ctl_proceso_parquet', 'esquema') IS NULL
    ALTER TABLE dbo.ctl_proceso_parquet ADD esquema VARCHAR(128) NULL;
GO
IF COL_LENGTH('dbo.ctl_proceso_parquet', 'commit_every') IS NULL
    ALTER TABLE dbo.ctl_proceso_parquet ADD commit_every INT NULL;
GO
IF COL_LENGTH('dbo.ctl_proceso_parquet', 'batch_id') IS NULL
    ALTER TABLE dbo.ctl_proceso_parquet ADD batch_id VARCHAR(14) NULL;
GO
IF COL_LENGTH('dbo.ctl_proceso_parquet', 'fecha_proceso') IS NULL
    ALTER TABLE dbo.ctl_proceso_parquet ADD fecha_proceso DATE NULL;
GO


-- ============================================================================
-- 5. COMPROBACION
-- ============================================================================
SELECT  t.name                              AS tabla,
        (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS columnas,
        (SELECT COUNT(*) FROM sys.indexes  i WHERE i.object_id = t.object_id
                                              AND i.type > 0)                AS indices
FROM    sys.tables t
WHERE   t.name IN ('CTL_PARAMETROS_PARQUET', 'ctl_proceso_parquet', 'ctl_carga_stg')
ORDER BY t.name;
GO

PRINT '';
PRINT '--------------------------------------------------------------';
PRINT 'Listo. Siguiente paso: 21_bt2sql_catalogo_ejemplo.sql';
PRINT '--------------------------------------------------------------';
GO
