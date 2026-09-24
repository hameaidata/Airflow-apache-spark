/* ===========================================================================
   S2SQL - Tablas de control del pipeline SingleStore -> SQL Server 2022
   ---------------------------------------------------------------------------
   Se ejecuta UNA VEZ, en la base DESTINO de SQL Server.

       sqlcmd -S 10.0.0.10,1433 -d MI_BASE -U usuario -P clave -i 10_s2sql_control_sqlserver.sql

   POR QUE EL CONTROL VIVE AQUI Y NO EN SINGLESTORE
   ------------------------------------------------
   Porque quien consume estos datos trabaja contra SQL Server. Teniendo el log
   al lado de las tablas cargadas, puede contestar "esta tabla de cuando es" con
   un SELECT, sin pedir acceso al origen ni abrir Airflow.

   LAS TRES TABLAS
   ---------------
       CTL_S2SQL_CATALOGO   que se exporta y como      (lo edita el equipo)
       CTL_S2SQL_LOTE       una fila por corrida        (lo escribe el DAG)
       CTL_S2SQL_LOG_CARGA  una fila por tabla y corrida(lo escribe el DAG)

   Todo lleva el prefijo CTL_S2SQL_ para que se distinga de las tablas de
   control del pipeline BT (CTL_PROCESO_PARQUET, CTL_CFG_PROCESOS), que viven
   en SingleStore.
=========================================================================== */

SET NOCOUNT ON;
GO

/* --- Esquemas -------------------------------------------------------------
   CTL guarda el control; STG las tablas intermedias de carga. Se separan del
   esquema de negocio para que un GRANT sobre los datos no arrastre permiso
   sobre el catalogo: quien pueda escribir en CTL_S2SQL_CATALOGO puede decidir
   que se lee del origen y que se sobreescribe en el destino.
--------------------------------------------------------------------------- */
IF SCHEMA_ID('CTL') IS NULL EXEC('CREATE SCHEMA CTL');
GO
IF SCHEMA_ID('STG') IS NULL EXEC('CREATE SCHEMA STG');
GO


/* ===========================================================================
   1. CTL_S2SQL_CATALOGO - que tablas se exportan y como
   ---------------------------------------------------------------------------
   Es la UNICA fuente de la forma del lote. El DAG la lee al arrancar la
   corrida y crea un carril (extraer + cargar) por cada fila con activo = 1.
   No se consulta en tiempo de parseo, asi que si SQL Server esta caido el DAG
   sigue visible en la interfaz y falla la primera tarea con un error claro.
=========================================================================== */
IF OBJECT_ID('CTL.CTL_S2SQL_CATALOGO') IS NULL
CREATE TABLE CTL.CTL_S2SQL_CATALOGO (

    id_catalogo      INT            IDENTITY(1,1) NOT NULL,

    -- ORIGEN, en SingleStore
    esquema_origen   SYSNAME        NOT NULL,
    tabla_origen     SYSNAME        NOT NULL,

    -- DESTINO, en esta misma base
    esquema_destino  SYSNAME        NOT NULL,
    tabla_destino    SYSNAME        NOT NULL,

    /* REEMPLAZO   : TRUNCATE + INSERT en una transaccion. La destino queda
                     exactamente igual que el origen.
       INCREMENTAL : solo las filas posteriores a la ultima marca cargada.
                     No corrige filas que cambiaron en el origen.
       MERGE       : actualiza las que cambiaron e inserta las nuevas.
       El CHECK evita que un modo mal escrito llegue hasta la carga: el DAG
       tambien lo valida, pero es mejor que la base no admita el dato malo. */
    modo_carga       VARCHAR(12)    NOT NULL,

    /* Lista separada por comas. NULL o vacio = todas las columnas (SELECT *).
       Conviene declararlas: si manana alguien anade una columna al origen, con
       lista explicita el parquet no cambia de forma y la carga sigue igual;
       con SELECT * aparece una columna que la tabla destino no tiene y la
       carga falla. */
    columnas         NVARCHAR(MAX)  NULL,

    /* SQL libre que se concatena al WHERE del origen, por ejemplo
       ESTADO = 'A' AND FECHA >= '2026-01-01'.
       ATENCION: esto no se puede validar sin un parser, asi que quien pueda
       editar esta columna puede escribir cualquier consulta contra el origen.
       Trata el permiso de UPDATE sobre esta tabla como un permiso de lectura
       sobre toda la base de SingleStore. */
    filtro_where     NVARCHAR(1000) NULL,

    /* Columna de corte para INCREMENTAL (una fecha o un id creciente).
       Obligatoria en ese modo: sin ella no hay forma de saber desde donde
       continuar y cada corrida duplicaria la tabla. */
    columna_marca    SYSNAME        NULL,

    /* Clave de negocio para MERGE, separada por comas. Tiene que ser UNICA en
       el origen: si no, SQL Server aborta con el error 8672 y el mensaje no
       dice ni que tabla ni que clave. El pipeline lo comprueba antes y lo
       explica. */
    claves_merge     NVARCHAR(500)  NULL,

    -- Filas por executemany. NULL = usa escritura.batch_filas de la Variable.
    batch_filas      INT            NULL,

    activo           BIT            NOT NULL CONSTRAINT DF_s2sql_cat_activo DEFAULT (0),
    orden            INT            NOT NULL CONSTRAINT DF_s2sql_cat_orden  DEFAULT (0),
    descripcion      NVARCHAR(500)  NULL,
    fec_creacion     DATETIME2(3)   NOT NULL CONSTRAINT DF_s2sql_cat_fec    DEFAULT (SYSDATETIME()),

    CONSTRAINT PK_CTL_S2SQL_CATALOGO PRIMARY KEY CLUSTERED (id_catalogo),

    /* Una tabla de origen no puede exportarse dos veces: las dos corridas
       escribirian el MISMO parquet (/data/s2sql/<fecha>/<TABLA>.parquet) y la
       segunda pisaria a la primera. */
    CONSTRAINT UQ_CTL_S2SQL_CATALOGO_ORIGEN UNIQUE (esquema_origen, tabla_origen),

    CONSTRAINT CK_CTL_S2SQL_CATALOGO_MODO
        CHECK (modo_carga IN ('REEMPLAZO', 'INCREMENTAL', 'MERGE')),

    -- Las reglas de cada modo, en la base y no solo en el codigo.
    CONSTRAINT CK_CTL_S2SQL_CATALOGO_INCREMENTAL
        CHECK (modo_carga <> 'INCREMENTAL' OR columna_marca IS NOT NULL),
    CONSTRAINT CK_CTL_S2SQL_CATALOGO_MERGE
        CHECK (modo_carga <> 'MERGE' OR claves_merge IS NOT NULL)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CTL_S2SQL_CATALOGO_ACTIVO')
CREATE NONCLUSTERED INDEX IX_CTL_S2SQL_CATALOGO_ACTIVO
    ON CTL.CTL_S2SQL_CATALOGO (activo, orden) INCLUDE (tabla_origen);
GO


/* ===========================================================================
   2. CTL_S2SQL_LOTE - una fila por corrida del DAG
=========================================================================== */
IF OBJECT_ID('CTL.CTL_S2SQL_LOTE') IS NULL
CREATE TABLE CTL.CTL_S2SQL_LOTE (

    -- yyyyMMddHHmmss. Lo genera preparar_lote y viaja por XCom a las demas
    -- tareas, para que todas escriban bajo el mismo identificador.
    batch_id         CHAR(14)       NOT NULL,

    -- Fecha de NEGOCIO, no la del reloj. Es tambien el nombre de la carpeta:
    -- /data/s2sql/<yyyyMMdd>/. Si una corrida arranca a las 23:58 y cruza la
    -- medianoche, todos sus archivos siguen cayendo en la carpeta correcta.
    fecha_proceso    DATE           NOT NULL,

    fec_inicio       DATETIME2(3)   NOT NULL,
    fec_fin          DATETIME2(3)   NULL,

    -- EJECUTANDO | TERMINADO | ERROR
    estado           VARCHAR(20)    NOT NULL,

    tablas_total     INT            NOT NULL CONSTRAINT DF_s2sql_lote_tot DEFAULT (0),
    tablas_ok        INT            NOT NULL CONSTRAINT DF_s2sql_lote_ok  DEFAULT (0),
    tablas_error     INT            NOT NULL CONSTRAINT DF_s2sql_lote_err DEFAULT (0),

    -- Nombre del worker que abrio el lote. Con varias replicas de worker, es
    -- lo que permite ir al contenedor correcto a buscar el log.
    host_name        NVARCHAR(100)  NULL,

    CONSTRAINT PK_CTL_S2SQL_LOTE PRIMARY KEY CLUSTERED (batch_id),
    CONSTRAINT CK_CTL_S2SQL_LOTE_ESTADO
        CHECK (estado IN ('EJECUTANDO', 'TERMINADO', 'ERROR'))
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CTL_S2SQL_LOTE_FECHA')
CREATE NONCLUSTERED INDEX IX_CTL_S2SQL_LOTE_FECHA
    ON CTL.CTL_S2SQL_LOTE (fecha_proceso DESC, fec_inicio DESC);
GO


/* ===========================================================================
   3. CTL_S2SQL_LOG_CARGA - una fila por tabla y corrida
   ---------------------------------------------------------------------------
   Hace dos trabajos a la vez:

     a) Es la bitacora: que tabla, cuando, cuantas filas, con que error.
     b) Es la MARCA DE AGUA del modo INCREMENTAL. El punto de corte no se
        guarda en una tabla aparte, se deduce de aqui:

            SELECT MAX(marca_hasta) FROM CTL.CTL_S2SQL_LOG_CARGA
            WHERE tabla_origen = ? AND estado = 'TERMINADO'

        Asi no hay dos sitios que puedan discrepar. Y como una corrida fallida
        no escribe marca_hasta, la siguiente vuelve a arrancar desde donde
        arranco esta: un fallo intermedio no se traga filas.
=========================================================================== */
IF OBJECT_ID('CTL.CTL_S2SQL_LOG_CARGA') IS NULL
CREATE TABLE CTL.CTL_S2SQL_LOG_CARGA (

    id_log             BIGINT         IDENTITY(1,1) NOT NULL,
    batch_id           CHAR(14)       NOT NULL,

    tabla_origen       SYSNAME        NOT NULL,
    tabla_destino      SYSNAME        NOT NULL,
    modo_carga         VARCHAR(12)    NOT NULL,

    -- RUTA COMPLETA del parquet dentro del contenedor, por ejemplo
    -- /data/s2sql/20260924/CLIENTES.parquet. Guardar solo el nombre no sirve:
    -- la carpeta cambia cada dia y entonces la fila no permite encontrar el
    -- archivo. NULL cuando la extraccion no trajo filas.
    archivo_parquet    NVARCHAR(500)  NULL,

    fec_inicio         DATETIME2(3)   NOT NULL,
    fec_fin            DATETIME2(3)   NULL,

    -- EJECUTANDO | TERMINADO | SIN_DATOS | ERROR
    -- SIN_DATOS no es un error: el origen no tenia filas nuevas. Se distingue
    -- a proposito, porque en modo REEMPLAZO "sin datos" y "cero filas" llevan
    -- a decisiones opuestas: la primera no toca la destino, la segunda la
    -- dejaria vacia.
    estado             VARCHAR(20)    NOT NULL,

    filas_leidas       BIGINT         NULL,   -- salieron del origen
    filas_escritas     BIGINT         NULL,   -- entraron en la staging
    filas_insertadas   BIGINT         NULL,   -- INSERT en la destino
    filas_actualizadas BIGINT         NULL,   -- UPDATE en la destino (solo MERGE)

    -- Valores de columna_marca usados y alcanzados. Se guardan como texto
    -- porque la columna puede ser una fecha, un entero o un GUID segun la
    -- tabla, y una sola columna tiene que servir para todas.
    marca_desde        NVARCHAR(100)  NULL,
    marca_hasta        NVARCHAR(100)  NULL,

    duracion_segundos  DECIMAL(12,2)  NULL,

    -- El ancho tiene que coincidir con limites.mensaje_error de la Variable.
    -- El codigo recorta a esa medida: si no, una traza larga hace fallar el
    -- propio INSERT del log y se pierde el error que importaba.
    msg_error          NVARCHAR(4000) NULL,

    host_name          NVARCHAR(100)  NULL,

    CONSTRAINT PK_CTL_S2SQL_LOG_CARGA PRIMARY KEY CLUSTERED (id_log),
    CONSTRAINT FK_CTL_S2SQL_LOG_LOTE
        FOREIGN KEY (batch_id) REFERENCES CTL.CTL_S2SQL_LOTE (batch_id),
    CONSTRAINT CK_CTL_S2SQL_LOG_ESTADO
        CHECK (estado IN ('EJECUTANDO', 'TERMINADO', 'SIN_DATOS', 'ERROR'))
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CTL_S2SQL_LOG_BATCH')
CREATE NONCLUSTERED INDEX IX_CTL_S2SQL_LOG_BATCH
    ON CTL.CTL_S2SQL_LOG_CARGA (batch_id) INCLUDE (estado, tabla_origen);
GO

/* Indice de la marca de agua. Es la consulta que corre al abrir CADA lote, una
   vez por tabla incremental, y sin el se convierte en un scan de un log que
   crece sin parar. */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_CTL_S2SQL_LOG_MARCA')
CREATE NONCLUSTERED INDEX IX_CTL_S2SQL_LOG_MARCA
    ON CTL.CTL_S2SQL_LOG_CARGA (tabla_origen, estado) INCLUDE (marca_hasta);
GO


/* ===========================================================================
   4. CONSULTAS DE OPERACION
=========================================================================== */

-- Como fue la ultima corrida
/*
SELECT  l.batch_id, l.fecha_proceso, l.estado, l.tablas_total, l.tablas_ok,
        l.tablas_error, DATEDIFF(SECOND, l.fec_inicio, l.fec_fin) AS segundos
FROM    CTL.CTL_S2SQL_LOTE l
WHERE   l.batch_id = (SELECT MAX(batch_id) FROM CTL.CTL_S2SQL_LOTE);
*/

-- Detalle tabla por tabla de esa corrida
/*
SELECT  tabla_origen, tabla_destino, modo_carga, estado,
        filas_leidas, filas_insertadas, filas_actualizadas,
        marca_desde, marca_hasta, duracion_segundos, LEFT(msg_error, 200) AS error
FROM    CTL.CTL_S2SQL_LOG_CARGA
WHERE   batch_id = (SELECT MAX(batch_id) FROM CTL.CTL_S2SQL_LOTE)
ORDER   BY tabla_origen;
*/

-- "Esta tabla de cuando es". Esta es la pregunta que justifica tener el log
-- aqui y no en SingleStore.
/*
SELECT  tabla_destino,
        MAX(fec_fin)     AS ultima_carga,
        MAX(marca_hasta) AS ultimo_dato_cargado
FROM    CTL.CTL_S2SQL_LOG_CARGA
WHERE   estado = 'TERMINADO'
GROUP   BY tabla_destino
ORDER   BY ultima_carga DESC;
*/

-- Tablas activas en el catalogo que nunca se cargaron bien
/*
SELECT  c.tabla_origen, c.modo_carga, c.descripcion
FROM    CTL.CTL_S2SQL_CATALOGO c
WHERE   c.activo = 1
  AND   NOT EXISTS (SELECT 1 FROM CTL.CTL_S2SQL_LOG_CARGA g
                    WHERE g.tabla_origen = c.tabla_origen AND g.estado = 'TERMINADO');
*/

-- Errores de los ultimos 7 dias
/*
SELECT  fec_inicio, batch_id, tabla_origen, LEFT(msg_error, 300) AS error
FROM    CTL.CTL_S2SQL_LOG_CARGA
WHERE   estado = 'ERROR' AND fec_inicio >= DATEADD(DAY, -7, SYSDATETIME())
ORDER   BY fec_inicio DESC;
*/

-- Reintentar solo lo que fallo: copia el resultado al parametro solo_tablas
-- al relanzar el DAG desde "Trigger DAG w/ config".
/*
SELECT  STRING_AGG(tabla_origen, '", "') AS para_solo_tablas
FROM    CTL.CTL_S2SQL_LOG_CARGA
WHERE   batch_id = 'PON_AQUI_EL_BATCH' AND estado = 'ERROR';
*/
