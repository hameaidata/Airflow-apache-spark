-- ============================================================================
--  BT2SQL - TABLAS PARA MEDIR TIEMPO DE CARGA
-- ============================================================================
--
--  PARA QUE ES ESTO
--  ----------------
--  Las cuatro tablas de Bantotal que trae la Variable tienen entre 5 y unas
--  pocas decenas de filas. Sirven para comprobar que el pipeline funciona, y
--  no sirven para NADA mas: con 5 filas el 95% del tiempo es arrancar la JVM,
--  asi que cualquier medida dice lo mismo pase lo que pase con la
--  configuracion. Para medir hace falta volumen.
--
--  Aqui hay tres tablas, y las tres miden cosas DISTINTAS a proposito:
--
--    GPPPBTDB.CARDTX   284.807 filas x 31 columnas, TODAS numericas.
--                      Mide el coste del casteo de tipos y de una tabla ancha.
--                      Origen: Kaggle  mlg-ulb/creditcardfraud
--
--    GPPPBTDB.PAYTX    ~6,3 millones de filas x 11 columnas, tipos mixtos.
--                      Mide el escalado por NUMERO DE FILAS y el particionado.
--                      Origen: Kaggle  ealaxi/paysim1
--
--    GPPPBTDB.BTGEN    las filas que usted diga, generadas aqui mismo.
--                      Mide la CURVA: la misma tabla con 10 mil, 100 mil, 1
--                      millon y 10 millones de filas, moviendo una sola
--                      variable. Sin descargar nada.
--
--  POR QUE BTGEN ADEMAS DE LAS DE KAGGLE
--  -------------------------------------
--  Porque una medida aislada no es una medida. "6 millones de filas en 4
--  minutos" no dice si el pipeline escala: para saberlo hace falta la misma
--  tabla a varios tamanos, y eso con datasets descargados significa recortar
--  ficheros a mano. BTGEN lo hace con un numero.
--  Las de Kaggle aportan lo que un generador no da: anchura real, tipos
--  mixtos, y valores que no se comprimen de forma irrealmente buena en
--  parquet (una columna generada con un patron se comprime muchisimo mejor
--  que datos reales, y eso falsea tanto el tamano en disco como el tiempo).
--
--  LAS COLUMNAS DE PARTICION, QUE SON LA PALANCA QUE MAS IMPORTA
--  ------------------------------------------------------------
--  Spark lee JDBC en UNA sola conexion salvo que se le diga por donde partir.
--  Las tres tablas traen a proposito una columna entera, densa y bien
--  repartida para eso:
--
--      CARDTX   TIME_SEG    0 .. 172792
--      PAYTX    STEP        1 .. 743
--      BTGEN    ID          1 .. <n>
--
--  Esa columna es la que va en "particion" en la Variable. Sin ella, subir
--  NUM_EXECUTORS no cambia nada en la lectura: no hay como repartir el
--  trabajo.
--
--  NOMBRES EN MAYUSCULAS Y SIN CAMELCASE
--  -------------------------------------
--  Los CSV de Kaggle traen nameOrig, oldbalanceOrg, isFraud. Aqui van
--  NAME_ORIG, OLD_BALANCE_ORG, IS_FRAUD. No es estetica: el origen final es
--  IBM i, donde los nombres son cortos y en mayuscula, y el pipeline cruza
--  nombres de columna entre el catalogo, la Variable y el parquet. Probar con
--  camelCase seria probar un caso que en produccion no va a existir.
-- ============================================================================

USE GNBPE_DATAHUB;
GO

SET NOCOUNT ON;
GO


-- ============================================================================
--  1. CARDTX - 284.807 filas x 31 columnas, todas numericas
-- ============================================================================
--  Kaggle: mlg-ulb/creditcardfraud   (creditcard.csv, ~144 MB, 284.807 filas)
--
--  V1..V28 son componentes PCA: FLOAT es el tipo correcto y no un atajo.
--  Forzarlos a DECIMAL aqui mediria el coste de una conversion que en
--  produccion no haria falta.
-- ============================================================================
IF OBJECT_ID('GPPPBTDB.CARDTX', 'U') IS NULL
BEGIN
    CREATE TABLE GPPPBTDB.CARDTX (
        -- Segundos transcurridos desde la primera transaccion del dataset.
        -- Entero, denso y de rango conocido: la columna de particion.
        TIME_SEG        DECIMAL(12,0)   NOT NULL,
        V1              FLOAT           NULL,
        V2              FLOAT           NULL,
        V3              FLOAT           NULL,
        V4              FLOAT           NULL,
        V5              FLOAT           NULL,
        V6              FLOAT           NULL,
        V7              FLOAT           NULL,
        V8              FLOAT           NULL,
        V9              FLOAT           NULL,
        V10             FLOAT           NULL,
        V11             FLOAT           NULL,
        V12             FLOAT           NULL,
        V13             FLOAT           NULL,
        V14             FLOAT           NULL,
        V15             FLOAT           NULL,
        V16             FLOAT           NULL,
        V17             FLOAT           NULL,
        V18             FLOAT           NULL,
        V19             FLOAT           NULL,
        V20             FLOAT           NULL,
        V21             FLOAT           NULL,
        V22             FLOAT           NULL,
        V23             FLOAT           NULL,
        V24             FLOAT           NULL,
        V25             FLOAT           NULL,
        V26             FLOAT           NULL,
        V27             FLOAT           NULL,
        V28             FLOAT           NULL,
        AMOUNT          DECIMAL(18,2)   NULL,
        CLASS           SMALLINT        NULL
    );
    PRINT 'GPPPBTDB.CARDTX creada.';
END
ELSE
    PRINT 'GPPPBTDB.CARDTX ya existe. No se toca.';
GO

-- La STG lleva las mismas columnas MAS las dos de control que anade el job.
IF OBJECT_ID('GPPPBTDB.STG_CARDTX', 'U') IS NULL
BEGIN
    CREATE TABLE GPPPBTDB.STG_CARDTX (
        TIME_SEG        DECIMAL(12,0)   NULL,
        V1              FLOAT           NULL,
        V2              FLOAT           NULL,
        V3              FLOAT           NULL,
        V4              FLOAT           NULL,
        V5              FLOAT           NULL,
        V6              FLOAT           NULL,
        V7              FLOAT           NULL,
        V8              FLOAT           NULL,
        V9              FLOAT           NULL,
        V10             FLOAT           NULL,
        V11             FLOAT           NULL,
        V12             FLOAT           NULL,
        V13             FLOAT           NULL,
        V14             FLOAT           NULL,
        V15             FLOAT           NULL,
        V16             FLOAT           NULL,
        V17             FLOAT           NULL,
        V18             FLOAT           NULL,
        V19             FLOAT           NULL,
        V20             FLOAT           NULL,
        V21             FLOAT           NULL,
        V22             FLOAT           NULL,
        V23             FLOAT           NULL,
        V24             FLOAT           NULL,
        V25             FLOAT           NULL,
        V26             FLOAT           NULL,
        V27             FLOAT           NULL,
        V28             FLOAT           NULL,
        AMOUNT          DECIMAL(18,2)   NULL,
        CLASS           SMALLINT        NULL,
        -- Las anade el job de extraccion, no vienen del origen.
        FECHA_PROCESO   DATE            NULL,
        BATCH_ID        VARCHAR(20)     NULL
    );
    PRINT 'GPPPBTDB.STG_CARDTX creada.';
END
ELSE
    PRINT 'GPPPBTDB.STG_CARDTX ya existe. No se toca.';
GO


-- ============================================================================
--  2. PAYTX - ~6,3 millones de filas x 11 columnas, tipos mixtos
-- ============================================================================
--  Kaggle: ealaxi/paysim1   (PS_20174392719_1491204439457_log.csv, ~470 MB)
--
--  Esta es la que de verdad mide escalado. Con 6 millones de filas el
--  arranque de la JVM deja de dominar el tiempo y se empieza a ver el efecto
--  de NUM_EXECUTORS, de la particion y de archivos_por_tabla.
-- ============================================================================
IF OBJECT_ID('GPPPBTDB.PAYTX', 'U') IS NULL
BEGIN
    CREATE TABLE GPPPBTDB.PAYTX (
        -- Hora simulada, 1..743. Columna de particion.
        STEP                DECIMAL(6,0)    NOT NULL,
        -- CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER
        TIPO_MOV            VARCHAR(12)     NULL,
        AMOUNT              DECIMAL(18,2)   NULL,
        NAME_ORIG           VARCHAR(20)     NULL,
        OLD_BALANCE_ORG     DECIMAL(18,2)   NULL,
        NEW_BALANCE_ORIG    DECIMAL(18,2)   NULL,
        NAME_DEST           VARCHAR(20)     NULL,
        OLD_BALANCE_DEST    DECIMAL(18,2)   NULL,
        NEW_BALANCE_DEST    DECIMAL(18,2)   NULL,
        IS_FRAUD            SMALLINT        NULL,
        IS_FLAGGED_FRAUD    SMALLINT        NULL
    );
    PRINT 'GPPPBTDB.PAYTX creada.';
END
ELSE
    PRINT 'GPPPBTDB.PAYTX ya existe. No se toca.';
GO

IF OBJECT_ID('GPPPBTDB.STG_PAYTX', 'U') IS NULL
BEGIN
    CREATE TABLE GPPPBTDB.STG_PAYTX (
        STEP                DECIMAL(6,0)    NULL,
        TIPO_MOV            VARCHAR(12)     NULL,
        AMOUNT              DECIMAL(18,2)   NULL,
        NAME_ORIG           VARCHAR(20)     NULL,
        OLD_BALANCE_ORG     DECIMAL(18,2)   NULL,
        NEW_BALANCE_ORIG    DECIMAL(18,2)   NULL,
        NAME_DEST           VARCHAR(20)     NULL,
        OLD_BALANCE_DEST    DECIMAL(18,2)   NULL,
        NEW_BALANCE_DEST    DECIMAL(18,2)   NULL,
        IS_FRAUD            SMALLINT        NULL,
        IS_FLAGGED_FRAUD    SMALLINT        NULL,
        FECHA_PROCESO       DATE            NULL,
        BATCH_ID            VARCHAR(20)     NULL
    );
    PRINT 'GPPPBTDB.STG_PAYTX creada.';
END
ELSE
    PRINT 'GPPPBTDB.STG_PAYTX ya existe. No se toca.';
GO


-- ============================================================================
--  3. BTGEN - la curva, sin descargar nada
-- ============================================================================
IF OBJECT_ID('GPPPBTDB.BTGEN', 'U') IS NULL
BEGIN
    CREATE TABLE GPPPBTDB.BTGEN (
        ID              BIGINT          NOT NULL,   -- columna de particion
        PGCOD           DECIMAL(3,0)    NULL,
        FECHA_MOV       DECIMAL(8,0)    NULL,       -- AAAAMMDD, como Bantotal
        MONEDA          DECIMAL(4,0)    NULL,
        IMPORTE         DECIMAL(17,2)   NULL,
        TASA            DECIMAL(17,8)   NULL,
        CUENTA          VARCHAR(20)     NULL,
        GLOSA           VARCHAR(100)    NULL,
        CONSTRAINT PK_BTGEN PRIMARY KEY CLUSTERED (ID)
    );
    PRINT 'GPPPBTDB.BTGEN creada.';
END
ELSE
    PRINT 'GPPPBTDB.BTGEN ya existe. No se toca.';
GO

IF OBJECT_ID('GPPPBTDB.STG_BTGEN', 'U') IS NULL
BEGIN
    CREATE TABLE GPPPBTDB.STG_BTGEN (
        ID              BIGINT          NULL,
        PGCOD           DECIMAL(3,0)    NULL,
        FECHA_MOV       DECIMAL(8,0)    NULL,
        MONEDA          DECIMAL(4,0)    NULL,
        IMPORTE         DECIMAL(17,2)   NULL,
        TASA            DECIMAL(17,8)   NULL,
        CUENTA          VARCHAR(20)     NULL,
        GLOSA           VARCHAR(100)    NULL,
        FECHA_PROCESO   DATE            NULL,
        BATCH_ID        VARCHAR(20)     NULL
    );
    PRINT 'GPPPBTDB.STG_BTGEN creada.';
END
ELSE
    PRINT 'GPPPBTDB.STG_BTGEN ya existe. No se toca.';
GO


-- ============================================================================
--  3b. RELLENAR BTGEN - cambie @filas y ejecute este bloque
-- ============================================================================
--  Se genera con un CROSS JOIN de sys.all_objects sobre si mismo y no con un
--  bucle ni con una CTE recursiva, por dos razones:
--
--    - una CTE recursiva tiene tope en 32767 niveles (MAXRECURSION) y hay que
--      pelearse con la opcion para pasar de ahi;
--    - un bucle de INSERTs hace una transaccion por fila y para 10 millones
--      tarda horas.
--
--  Asi son unos segundos por millon. GLOSA se construye con valores que
--  varian de verdad: si todas las filas llevaran el mismo texto, parquet lo
--  comprimiria a casi nada y el tiempo medido no se parecería al real.
-- ============================================================================
DECLARE @filas BIGINT = 1000000;        -- <<<<<< LA UNICA LINEA QUE SE CAMBIA

TRUNCATE TABLE GPPPBTDB.BTGEN;

;WITH numeros AS (
    SELECT TOP (@filas)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM   sys.all_objects a
    CROSS JOIN sys.all_objects b
    CROSS JOIN sys.all_objects c
)
INSERT INTO GPPPBTDB.BTGEN
    (ID, PGCOD, FECHA_MOV, MONEDA, IMPORTE, TASA, CUENTA, GLOSA)
SELECT
    n,
    1 + (n % 9),
    CONVERT(DECIMAL(8,0),
            CONVERT(CHAR(8), DATEADD(DAY, -(n % 730), CAST(GETDATE() AS DATE)), 112)),
    CASE n % 3 WHEN 0 THEN 1 WHEN 1 THEN 2 ELSE 840 END,
    CONVERT(DECIMAL(17,2), (n % 9973) + (n % 97) / 100.0),
    CONVERT(DECIMAL(17,8), 3.5 + (n % 1000) / 10000.0),
    CONCAT('CTA', RIGHT(CONCAT('00000000000', n), 11)),
    CONCAT('MOV ', n, ' ', CONVERT(CHAR(36), NEWID()))
FROM numeros;

PRINT CONCAT('BTGEN rellenada con ', @@ROWCOUNT, ' filas.');
GO


-- ============================================================================
--  4. EL CATALOGO - sin esta parte validar_catalogo falla
-- ============================================================================
--  validacion_catalogo esta en 'estricto' en la Variable: una tabla declarada
--  en "tablas" y NO registrada aqui es un error, no un aviso. Asi que estas
--  tres filas no son opcionales.
--
--  COLUMNAS se deja en NULL (= todas) a proposito: el objetivo es medir el
--  coste de mover la tabla entera, no de un subconjunto.
--  TIPOS tambien en NULL: la Variable trae tipos_desde_destino = true, asi
--  que el job los deduce leyendo INFORMATION_SCHEMA.COLUMNS de la STG. Es el
--  camino que se quiere ejercitar, y ademas evita mantener la lista dos veces.
--
--  MERGE y no INSERT, para poder volver a correr el script.
-- ============================================================================
MERGE dbo.CTL_PARAMETROS_PARQUET WITH (HOLDLOCK) AS destino
USING (VALUES
    ('GPPPBTDB', 'CARDTX', 'STG_CARDTX', 'STG_CARDTX', 50000,
     'Prueba de carga: 284.807 filas x 31 columnas numericas (Kaggle mlg-ulb/creditcardfraud)'),
    ('GPPPBTDB', 'PAYTX',  'STG_PAYTX',  'STG_PAYTX',  50000,
     'Prueba de carga: ~6,3M filas x 11 columnas mixtas (Kaggle ealaxi/paysim1)'),
    ('GPPPBTDB', 'BTGEN',  'STG_BTGEN',  'STG_BTGEN',  50000,
     'Prueba de carga: filas generadas, para la curva de escalado')
) AS origen (ESQUEMA, TABLA, NOMBRE_PARQUET, TABLA_DESTINO, BATCH_SIZE, DESCRIPCION)
    ON  destino.ESQUEMA = origen.ESQUEMA
    AND destino.TABLA   = origen.TABLA
WHEN MATCHED THEN UPDATE SET
    destino.NOMBRE_PARQUET = origen.NOMBRE_PARQUET,
    destino.TABLA_DESTINO  = origen.TABLA_DESTINO,
    destino.BATCH_SIZE     = origen.BATCH_SIZE,
    destino.DESCRIPCION    = origen.DESCRIPCION,
    destino.ACTIVO         = 'S'
WHEN NOT MATCHED BY TARGET THEN INSERT
    (ESQUEMA, TABLA, COLUMNAS, FILTRO, ACTIVO,
     NOMBRE_PARQUET, TIPOS, TABLA_DESTINO, BATCH_SIZE, DESCRIPCION)
VALUES
    (origen.ESQUEMA, origen.TABLA, NULL, NULL, 'S',
     origen.NOMBRE_PARQUET, NULL, origen.TABLA_DESTINO,
     origen.BATCH_SIZE, origen.DESCRIPCION);

PRINT CONCAT('Catalogo: ', @@ROWCOUNT, ' fila(s) afectada(s).');
GO


-- ============================================================================
--  5. COMPROBACION
-- ============================================================================
SELECT 'CARDTX' AS tabla, COUNT(*) AS filas FROM GPPPBTDB.CARDTX
UNION ALL SELECT 'PAYTX',  COUNT(*) FROM GPPPBTDB.PAYTX
UNION ALL SELECT 'BTGEN',  COUNT(*) FROM GPPPBTDB.BTGEN;

SELECT ESQUEMA, TABLA, NOMBRE_PARQUET, TABLA_DESTINO, ACTIVO, BATCH_SIZE
FROM   dbo.CTL_PARAMETROS_PARQUET
WHERE  TABLA IN ('CARDTX', 'PAYTX', 'BTGEN')
ORDER  BY TABLA;
GO


-- ============================================================================
--  6. CARGAR LOS CSV DE KAGGLE
-- ============================================================================
--  BULK INSERT lee el fichero desde el SERVIDOR de SQL Server, no desde su
--  equipo. Con SQL Server en Docker, la carpeta tiene que estar montada en el
--  contenedor; con SQL Server local, la ruta es la del disco del servidor y la
--  cuenta de servicio necesita permiso de lectura.
--
--  FIRSTROW = 2 salta la cabecera. FORMAT='CSV' respeta las comillas, que en
--  PAYTX hacen falta porque TIPO_MOV es texto.
--
--  OJO CON CARDTX: el CSV trae las columnas en el orden
--      Time, V1..V28, Amount, Class
--  que es exactamente el orden de la tabla. Si Kaggle cambia el orden, hay que
--  usar un fichero de formato; BULK INSERT va por POSICION, no por nombre, y
--  un desajuste no falla: mete los valores en la columna equivocada.
-- ============================================================================
/*
BULK INSERT GPPPBTDB.CARDTX
FROM '/var/opt/mssql/import/creditcard.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, TABLOCK, BATCHSIZE = 50000);

BULK INSERT GPPPBTDB.PAYTX
FROM '/var/opt/mssql/import/PS_20174392719_1491204439457_log.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, TABLOCK, BATCHSIZE = 50000);
*/
-- ============================================================================
