-- ============================================================================
--  SP_BDS_SALDOS_OPERATIVOS   ::   SQL Server      v2
-- ----------------------------------------------------------------------------
--  Convierte los saldos de cierre (BDS_SALDOS_CIERRE), que solo existen los
--  dias habiles y solo cuando hubo movimiento, en una serie DIARIA COMPLETA
--  por operacion, y calcula el saldo promedio acumulado del mes.
--
--  LA GARANTIA DE ESTE PROCESO
--  ---------------------------
--  Para cada llave (ID_OPERACION_CIERRE, COD_RUBRO) y cada mes en el que esa
--  llave aparece AL MENOS UNA VEZ, la salida tiene una fila por cada dia del
--  mes: 28, 29, 30 o 31. Ni mas ni menos.
--
--  Un mes en el que la llave no aparece ninguna vez NO se crea. Arrastrarla
--  exigiria una regla de cierre -vencimiento, estado, meses sin movimiento- y
--  sin ella una operacion cancelada reviviria para siempre.
--
--  La comprobacion 2 de VALIDAR_SALDOS_OPERATIVOS.sql verifica el conteo
--  exacto contra los dias del mes. Tiene que devolver CERO filas.
--
-- ============================================================================
--  QUE CAMBIA EN LA v2
-- ----------------------------------------------------------------------------
--  UN ARREGLO DE LOGICA, y es el motivo principal de esta version:
--
--   0. ANCLA_PREV YA NO EXIGE TIPO_ORIGEN = 'ORIGINAL'
--      La v1 de SQL Server lo exigia en las DOS anclas. La version de
--      SingleStore lo exige solo en ANCLA_NEXT, a proposito y documentado. Las
--      dos versiones habian divergido en LOGICA, no en sintaxis, y la de aqui
--      tenia el peor comportamiento:
--
--      una copia heredada del ultimo dia habil del mes ANTERIOR -el sabado 31
--      que se copia al domingo 1- es presencia real de la operacion en este
--      mes y tiene que anclar igual que un dato propio. Con el filtro puesto,
--      una llave cuya unica fila del mes es esa copia se quedaba con UN dia de
--      treinta: las dos anclas salian NULL y el WHERE descartaba los otros 29.
--
--      Eso rompe la garantia de arriba, y lo rompe en silencio: la llave sale
--      con un dia, el promedio divide entre 1 y el numero es plausible.
--
--      La asimetria se mantiene: hacia atras se hereda de lo que haya, hacia
--      adelante solo de un dato propio.
--
--  Y CUATRO DE RENDIMIENTO:
--
--   A. INDICE ADECUADO A CADA TEMPORAL, EN VEZ DE COLUMNSTORE EN LAS DOS
--      #SO_DIARIO se usa para JOINS -tres veces, siempre por
--      (ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO)-, y para eso gana un
--      indice rowstore agrupado por esa clave: el optimizador hace seeks en
--      vez de escanear el segmento columnar entero. La v1 le ponia columnstore,
--      que es lo correcto para agregar y lo equivocado para unir.
--
--      #SO_COMPLETO si se queda en columnstore: ahi lo que sigue es un GROUP BY
--      sobre casi todas las filas, que es justo donde columnstore gana.
--
--   B. OPTION (RECOMPILE) EN LAS TRES SENTENCIAS GRANDES
--      El procedimiento recibe un rango de fechas. Sin RECOMPILE, SQL Server
--      cachea el plan del PRIMER rango con el que se llamo y lo reutiliza: el
--      plan de una corrida de 31 dias se aplica a una de 400, o al contrario.
--      Es la causa mas comun de que un proceso que iba bien empiece a tardar
--      diez veces mas sin que nadie haya tocado nada.
--
--      El coste de recompilar es de milisegundos frente a minutos de proceso.
--
--   C. DELETE POR LOTES, OPCIONAL
--      Borrar 18 millones de filas en una sentencia escribe los 18 millones en
--      el log de transacciones antes de tocar un dato, y el log no se puede
--      truncar hasta el COMMIT. Con @P_LOTE_DELETE > 0 se borra por lotes.
--
--      NO ES GRATIS, y por eso es un parametro y no el comportamiento por
--      defecto: el borrado por lotes ocurre FUERA de la transaccion, asi que
--      entre el ultimo lote y el INSERT hay una ventana en la que el rango
--      esta vacio. Con @P_LOTE_DELETE = 0 (el defecto) se conserva la garantia
--      de la v1: quien consulte durante el proceso ve el rango completo -el
--      viejo o el nuevo-, nunca vacio ni a medias.
--
--   D. PROYECCION ESTRECHA
--      La v1 hacia SELECT * INTO #SO_DIARIO desde una CTE que incluia la
--      columna RN del ROW_NUMBER. Esa columna no se usa en ningun sitio y se
--      arrastraba por 35 millones de filas.
--
-- ============================================================================
--  LO QUE ESTE PROCEDIMIENTO NO PUEDE HACER POR SI MISMO
-- ----------------------------------------------------------------------------
--  Necesita este indice en BDS_CALENDARIOS, y no lo crea porque crear indices
--  en tablas permanentes bloquea la tabla y lo decide el DBA:
--
--      CREATE NONCLUSTERED INDEX IX_BDS_CALENDARIOS_HABIL
--          ON dbo.BDS_CALENDARIOS (COD_CALENDARIO, IND_DIA_HABIL, FEC_CALENDARIO);
--
--  Sin el, el OUTER APPLY del paso 3 -que busca el ultimo dia habil anterior a
--  cada dia del rango- se ejecuta como un escaneo del calendario por cada dia.
--
--  Y para el LEFT JOIN final contra BDS_SALDOS_CIERRE:
--
--      CREATE NONCLUSTERED INDEX IX_BDS_SALDOS_CIERRE_OP_FECHA
--          ON dbo.BDS_SALDOS_CIERRE (ID_OPERACION, FECHA_PROCESO)
--          INCLUDE (COD_EJECUTIVO);
--
-- ============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE dbo.SP_BDS_SALDOS_OPERATIVOS
    @P_FECHA_PROCESO DATE,
    @P_DIAS          INT,
    -- 0 = un solo DELETE dentro de la transaccion (atomico, log grande).
    -- > 0 = borrado por lotes de ese tamano, fuera de la transaccion.
    -- Ver el punto C de la cabecera antes de ponerlo en > 0.
    @P_LOTE_DELETE   INT = 0
AS
BEGIN
    -- Sin esto, cada INSERT, UPDATE y DELETE devuelve su "(N rows affected)".
    -- En un proceso de decenas de millones de filas eso es trafico de red y,
    -- con algunos drivers, cursores intermedios que consumen memoria del
    -- cliente para nada.
    SET NOCOUNT ON;

    -- El comportamiento por defecto de SQL Server ante un error dentro de un
    -- procedimiento es ABORTAR LA SENTENCIA Y SEGUIR con la siguiente. Aqui eso
    -- significaria borrar el destino y despues fallar el INSERT, dejando el
    -- rango vacio. XACT_ABORT ON hace que cualquier error aborte la
    -- transaccion entera.
    SET XACT_ABORT ON;

    DECLARE @V_FECHA_FIN        DATE,
            @V_FECHA_INICIO     DATE,
            @V_FECHA_INICIO_MES DATE,
            @V_FECHA_CALCULADA  DATE,
            @V_BORRADAS         BIGINT = 0,
            @V_LOTE             BIGINT;

    -- ========================================================================
    -- 1. LAS TRES FECHAS, Y POR QUE SON TRES
    -- ------------------------------------------------------------------------
    --  @V_FECHA_FIN         ultimo dia a procesar. Lo da el parametro.
    --
    --  @V_FECHA_INICIO      desde donde se LEE el origen. Es el ultimo dia
    --                       HABIL anterior o igual a (fin - @P_DIAS). Habil y
    --                       no natural: si fuera natural y cayera en domingo,
    --                       no habria de donde arrastrar el saldo del lunes.
    --
    --  @V_FECHA_INICIO_MES  desde donde se ESCRIBE el destino. El promedio
    --                       acumulado necesita el mes entero, asi que se lee
    --                       desde antes de lo que se escribe. Escribir el mes
    --                       a medias dejaria promedios calculados sobre una
    --                       parte del mes, que es peor que no tenerlos.
    -- ========================================================================
    SET @V_FECHA_FIN        = @P_FECHA_PROCESO;
    SET @V_FECHA_CALCULADA  = DATEADD(DAY, -@P_DIAS, @V_FECHA_FIN);

    -- DATEFROMPARTS y no construir la fecha como texto. La traduccion directa
    -- del DATE_FORMAT(x,'%Y-%m-01') de MySQL seria
    -- CONVERT(DATE, CONVERT(CHAR(8), x, 112)), que pasa por cadena y depende
    -- del SET DATEFORMAT y del idioma de la sesion: el mismo procedimiento
    -- daria resultados distintos segun como este configurada la conexion.
    SET @V_FECHA_INICIO_MES = DATEFROMPARTS(
            YEAR(@V_FECHA_CALCULADA), MONTH(@V_FECHA_CALCULADA), 1);

    SELECT @V_FECHA_INICIO = MAX(FEC_CALENDARIO)
    FROM   dbo.BDS_CALENDARIOS
    WHERE  COD_CALENDARIO = 1
      AND  IND_DIA_HABIL  = 'S'
      AND  FEC_CALENDARIO <= @V_FECHA_CALCULADA;

    -- ========================================================================
    -- 2. LAS TRES VALIDACIONES QUE PARAN EL PROCESO
    -- ------------------------------------------------------------------------
    -- Las tres fallan ANTES de tocar el destino. Un proceso que borra y despues
    -- descubre que no tiene datos deja la capa vacia.
    -- ========================================================================
    IF @V_FECHA_INICIO IS NULL
        THROW 50001, 'BDS_CALENDARIOS no tiene ningun dia habil anterior o igual a la fecha calculada. Sin eso no hay desde donde arrastrar los saldos. Cargue el calendario del periodo.', 1;

    IF NOT EXISTS (SELECT 1 FROM dbo.BDS_SALDOS_CIERRE
                   WHERE FECHA_PROCESO BETWEEN @V_FECHA_INICIO AND @V_FECHA_FIN)
        THROW 50002, 'BDS_SALDOS_CIERRE no tiene filas en el rango. No hay nada que procesar.', 1;

    -- El calendario tiene que llegar hasta el final del rango. Si no, el paso 4
    -- generaria menos dias de los que el mes tiene y el promedio dividiria mal
    -- SIN fallar. Es el fallo mas caro de este proceso, porque el resultado es
    -- plausible: nadie va a sospechar de un promedio que existe.
    IF (SELECT MAX(FEC_CALENDARIO) FROM dbo.BDS_CALENDARIOS
        WHERE COD_CALENDARIO = 1) < @V_FECHA_FIN
        THROW 50003, 'El calendario no llega hasta la fecha de proceso. Cargue BDS_CALENDARIOS antes de continuar.', 1;

    -- ========================================================================
    -- 3. PROPAGACION A DIAS NO HABILES   ->   #SO_DIARIO
    -- ------------------------------------------------------------------------
    -- Un sabado no tiene cierre. Su saldo es el del viernes. Para eso esta
    -- CALENDARIO: cada dia del rango se empareja con el ultimo dia habil
    -- anterior o igual, y de ahi copia los importes.
    --
    -- Tabla TEMPORAL (#) y no real: existe solo en esta sesion, desaparece sola
    -- al terminar el procedimiento, y dos ejecuciones simultaneas obtienen cada
    -- una la suya aunque se llamen igual. En el script original estas eran
    -- tablas REALES y dos corridas a la vez se pisaban los datos.
    -- ========================================================================
    DROP TABLE IF EXISTS #SO_DIARIO;

    ;WITH CALENDARIO AS (
        -- OUTER APPLY y no una subconsulta correlacionada con MAX(): es la
        -- misma semantica, pero SQL Server lo ejecuta como un TOP 1 con seek
        -- sobre el indice del calendario en lugar de un agregado por fila.
        SELECT c.FEC_CALENDARIO AS FECHA_PROCESO,
               c.IND_DIA_HABIL,
               h.FEC_CALENDARIO AS FECHA_ORIGEN
        FROM dbo.BDS_CALENDARIOS c
        OUTER APPLY (
            SELECT TOP (1) h2.FEC_CALENDARIO
            FROM dbo.BDS_CALENDARIOS h2
            WHERE h2.COD_CALENDARIO = 1
              AND h2.IND_DIA_HABIL  = 'S'
              AND h2.FEC_CALENDARIO <= c.FEC_CALENDARIO
            ORDER BY h2.FEC_CALENDARIO DESC
        ) h
        WHERE c.COD_CALENDARIO = 1
          AND c.FEC_CALENDARIO BETWEEN @V_FECHA_INICIO AND @V_FECHA_FIN
    ),
    ORIGEN AS (
        SELECT *
        FROM dbo.BDS_SALDOS_CIERRE
        WHERE FECHA_PROCESO BETWEEN @V_FECHA_INICIO AND @V_FECHA_FIN
    ),
    PROPAGADO AS (
        SELECT
            CONCAT(o.COD_EMPRESA,  '|', o.COD_MODULO,        '|',
                   o.COD_SUCURSAL, '|', o.COD_MONEDA,        '|',
                   o.COD_PAPEL,    '|', o.NUM_CUENTA_BT,     '|',
                   o.COD_OPERACION,'|', o.COD_SUB_OPERACION, '|',
                   o.COD_TIPO_OPERACION) AS ID_OPERACION_CIERRE,
            cal.FECHA_PROCESO,
            o.COD_EMPRESA, o.COD_SUCURSAL, o.COD_RUBRO, o.COD_MONEDA,
            o.COD_PAPEL, o.NUM_CUENTA_BT, o.COD_OPERACION, o.COD_SUB_OPERACION,
            o.COD_TIPO_OPERACION, o.COD_MODULO, o.FEC_VENCIMIENTO, o.FEC_VALOR,
            o.IND_CATEGORIA_RIESGO, o.COD_ACTI_BCO_CENTRAL, o.COD_PRODUCTO,
            o.MTO_SALDO_ORIGEN, o.MTO_SALDO_MN, o.MTO_SALDO_ME, o.MTO_SALDO_MO,
            o.MTO_INTERES, o.MTO_PREVISIONES,
            cal.IND_DIA_HABIL,
            -- ORIGINAL    = el dato es de ese dia
            -- COPIA_HABIL = se copio del ultimo dia habil anterior
            CASE WHEN cal.FECHA_PROCESO = cal.FECHA_ORIGEN
                 THEN 'ORIGINAL' ELSE 'COPIA_HABIL' END AS TIPO_ORIGEN,
            o.FUENTE, o.FECHA_CARGA, o.BATCH_ID
        FROM CALENDARIO cal
        JOIN ORIGEN o ON o.FECHA_PROCESO = cal.FECHA_ORIGEN
    ),
    NUMERADO AS (
        -- Una sola fila por (operacion, rubro, dia).
        --
        -- El original particionaba por 23 columnas, incluidos los importes, lo
        -- que NO garantiza unicidad por dia: dos filas del mismo dia con
        -- importes distintos sobrevivian las dos y el duplicado llegaba al
        -- final. Aqui la particion es la clave de negocio de verdad, y el
        -- desempate es total: carga mas reciente, luego saldo mayor, luego
        -- BATCH_ID. Sin esa ultima columna el resultado seguiria dependiendo
        -- del orden de lectura, que no es estable entre corridas.
        SELECT p.*,
               ROW_NUMBER() OVER (
                   PARTITION BY p.ID_OPERACION_CIERRE, p.COD_RUBRO, p.FECHA_PROCESO
                   ORDER BY p.FECHA_CARGA DESC,
                            COALESCE(p.MTO_SALDO_MN, 0) DESC,
                            p.BATCH_ID DESC
               ) AS RN
        FROM PROPAGADO p
    )
    -- v2 / D: la lista de columnas es explicita y NO incluye RN. La v1 hacia
    -- SELECT * y arrastraba esa columna por 35 millones de filas sin que nada
    -- la leyera.
    SELECT ID_OPERACION_CIERRE, FECHA_PROCESO,
           COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
           NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION, COD_TIPO_OPERACION,
           COD_MODULO, FEC_VENCIMIENTO, FEC_VALOR, IND_CATEGORIA_RIESGO,
           COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
           MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
           MTO_INTERES, MTO_PREVISIONES,
           IND_DIA_HABIL, TIPO_ORIGEN, FUENTE, FECHA_CARGA, BATCH_ID
    INTO   #SO_DIARIO
    FROM   NUMERADO
    WHERE  RN = 1
    OPTION (RECOMPILE);          -- v2 / B

    -- v2 / A: ROWSTORE agrupado por la clave de los joins, no columnstore.
    --
    -- #SO_DIARIO se une tres veces -dos en el paso 4 y una en el UNION ALL- y
    -- siempre por (ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO). Con este
    -- indice el optimizador hace seeks; con columnstore tiene que recorrer el
    -- segmento. Columnstore es para agregar, y aqui no se agrega nada.
    --
    -- No es UNIQUE aunque la clave lo sea por construccion (RN = 1): si por
    -- cualquier motivo dejara de serlo, un indice unico abortaria el proceso
    -- con un error de clave duplicada en vez de dejar que la validacion lo
    -- detecte y lo explique.
    CREATE CLUSTERED INDEX IX_SO_DIARIO
        ON #SO_DIARIO (ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO);

    -- ========================================================================
    -- 4. RELLENO DE HUECOS   ->   #SO_COMPLETO
    -- ------------------------------------------------------------------------
    -- POR QUE SE PUEDE HACER EN UNA SOLA PASADA
    --
    -- El script original resolvia esto con TRES CTE recursivas: los dias que
    -- faltan al FINAL del mes, EN MEDIO, y al PRINCIPIO. Pero "los dias que
    -- faltan" es el calendario del mes menos los dias que si estan. Un LEFT
    -- JOIN contra el calendario lo da entero, sin recursion y sin distinguir
    -- casos.
    --
    -- Lo unico que cambia entre los tres casos es DE DONDE salen los atributos
    -- de la fila generada, y eso se resuelve con dos funciones de ventana:
    --
    --     ANCLA_PREV   ultima fecha con datos <= este dia  (huecos y fin de mes)
    --     ANCLA_NEXT   primera fecha con datos >= este dia (principio de mes)
    --     ANCLA        COALESCE(ANCLA_PREV, ANCLA_NEXT)
    --
    -- Los IMPORTES de las filas generadas van a CERO. Solo se heredan los
    -- atributos.
    -- ========================================================================
    DROP TABLE IF EXISTS #SO_COMPLETO;

    -- v2 / A: aqui SI columnstore. Lo que sigue en el paso 6 es un GROUP BY
    -- sobre practicamente todas las filas de esta tabla, que es el caso donde
    -- columnstore gana de largo frente a cualquier indice rowstore.
    CREATE TABLE #SO_COMPLETO (
        ID_OPERACION_CIERRE  VARCHAR(200)   NULL,
        FECHA_PROCESO        DATE           NULL,
        COD_EMPRESA          INT            NULL,
        COD_SUCURSAL         INT            NULL,
        COD_RUBRO            VARCHAR(40)    NULL,
        COD_MONEDA           INT            NULL,
        COD_PAPEL            INT            NULL,
        NUM_CUENTA_BT        BIGINT         NULL,
        COD_OPERACION        BIGINT         NULL,
        COD_SUB_OPERACION    INT            NULL,
        COD_TIPO_OPERACION   INT            NULL,
        COD_MODULO           INT            NULL,
        FEC_VENCIMIENTO      DATE           NULL,
        FEC_VALOR            DATE           NULL,
        IND_CATEGORIA_RIESGO VARCHAR(10)    NULL,
        COD_ACTI_BCO_CENTRAL VARCHAR(40)    NULL,
        COD_PRODUCTO         VARCHAR(40)    NULL,
        MTO_SALDO_ORIGEN     DECIMAL(18,2)  NULL,
        MTO_SALDO_MN         DECIMAL(18,2)  NULL,
        MTO_SALDO_ME         DECIMAL(18,2)  NULL,
        MTO_SALDO_MO         DECIMAL(18,2)  NULL,
        MTO_INTERES          DECIMAL(18,2)  NULL,
        MTO_PREVISIONES      DECIMAL(18,2)  NULL,
        IND_DIA_HABIL        CHAR(1)        NULL,
        FUENTE               VARCHAR(50)    NULL,
        FECHA_CARGA          DATETIME2(3)   NULL,
        BATCH_ID             VARCHAR(50)    NULL
    );
    --
    -- AVISO SOBRE ESTOS TIPOS
    -- Estan puestos para que la tabla se pueda crear con columnstore antes del
    -- INSERT, que es lo que evita escribir en rowstore y reconstruir despues.
    -- Si alguno NO coincide con el de BDS_SALDOS_CIERRE, el INSERT hara una
    -- conversion implicita -mas lenta- o fallara por truncamiento. Ajustelos
    -- contra la DDL real con:
    --
    --   SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
    --          NUMERIC_PRECISION, NUMERIC_SCALE
    --   FROM INFORMATION_SCHEMA.COLUMNS
    --   WHERE TABLE_NAME = 'BDS_SALDOS_CIERRE' ORDER BY ORDINAL_POSITION;
    --
    -- Si prefiere no mantener esta lista, sustituya el CREATE TABLE y el
    -- INSERT de abajo por un "SELECT ... INTO #SO_COMPLETO FROM RESULTADO"
    -- seguido de "CREATE CLUSTERED COLUMNSTORE INDEX": los tipos se deducen,
    -- a cambio de una escritura extra de toda la tabla.
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_SO_COMPLETO ON #SO_COMPLETO;

    ;WITH LLAVES AS (
        -- LA LLAVE ES (ID_OPERACION_CIERRE, COD_RUBRO), Y LA REGLA ES:
        -- si aparece AL MENOS UNA VEZ en un mes, ese mes se completa ENTERO.
        -- De dia 1 a fin de mes, sin mirar en que dia del mes estaba su primera
        -- ni su ultima fila.
        SELECT ID_OPERACION_CIERRE,
               COD_RUBRO,
               YEAR(FECHA_PROCESO)  AS ANIO,
               MONTH(FECHA_PROCESO) AS MES
        FROM #SO_DIARIO
        GROUP BY ID_OPERACION_CIERRE, COD_RUBRO,
                 YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
    ),
    ESPINA AS (
        -- Todos los dias del mes de cada llave, acotados al rango de proceso.
        -- Aqui aparecen los dias que faltaban, sin recursion.
        --
        -- DISTINCT sobre el calendario: si BDS_CALENDARIOS trae una fila
        -- repetida para un dia -tipico de los feriados, que se cargan aparte-
        -- este join la duplicaria, y con ella TODA fila de relleno de ese dia.
        -- Las filas que ya existen no se duplican porque pasan por el
        -- ROW_NUMBER del paso 3; las de relleno no tenian esa proteccion.
        SELECT k.ID_OPERACION_CIERRE, k.COD_RUBRO, k.ANIO, k.MES,
               c.FEC_CALENDARIO AS FECHA_PROCESO,
               c.IND_DIA_HABIL
        FROM LLAVES k
        JOIN (SELECT DISTINCT FEC_CALENDARIO, IND_DIA_HABIL
                FROM dbo.BDS_CALENDARIOS
               WHERE COD_CALENDARIO = 1) c
          ON YEAR(c.FEC_CALENDARIO)  = k.ANIO
         AND MONTH(c.FEC_CALENDARIO) = k.MES
         AND c.FEC_CALENDARIO <= @V_FECHA_FIN
    ),
    CON_ANCLA AS (
        SELECT e.ID_OPERACION_CIERRE, e.COD_RUBRO, e.FECHA_PROCESO,
               e.IND_DIA_HABIL,
               d.ID_OPERACION_CIERRE AS EXISTE,
               -- v2 / 0  --  SIN filtrar por TIPO_ORIGEN, a proposito.
               --
               -- Una copia heredada del ultimo dia habil del mes ANTERIOR es
               -- presencia real de la operacion en este mes y tiene que anclar
               -- igual que un dato propio. La v1 exigia 'ORIGINAL' aqui, y una
               -- llave cuya unica fila del mes era esa copia se quedaba con UN
               -- dia de treinta.
               --
               -- Da igual que la copia traiga importes en cero: lo que importa
               -- es que la operacion existe ese mes. Los dias generados van a
               -- cero de todas formas.
               MAX(CASE WHEN d.ID_OPERACION_CIERRE IS NOT NULL
                        THEN e.FECHA_PROCESO END) OVER V_ATRAS    AS ANCLA_PREV,
               -- Esta SI exige ORIGINAL, y la asimetria es deliberada: hacia
               -- atras se hereda de lo que haya; hacia adelante -cuando no hay
               -- nada detras porque el hueco esta antes del primer registro del
               -- mes- solo de un dato propio.
               MIN(CASE WHEN d.ID_OPERACION_CIERRE IS NOT NULL
                         AND d.TIPO_ORIGEN = 'ORIGINAL'
                        THEN e.FECHA_PROCESO END) OVER V_ADELANTE AS ANCLA_NEXT
        FROM ESPINA e
        LEFT JOIN #SO_DIARIO d
               ON d.ID_OPERACION_CIERRE = e.ID_OPERACION_CIERRE
              AND d.COD_RUBRO           = e.COD_RUBRO
              AND d.FECHA_PROCESO       = e.FECHA_PROCESO
        -- La clausula WINDOW es de SQL Server 2022. En 2019 y anteriores hay
        -- que repetir el OVER completo en las dos expresiones de arriba.
        WINDOW
            V_ATRAS AS (PARTITION BY e.ID_OPERACION_CIERRE, e.COD_RUBRO,
                                     e.ANIO, e.MES
                        ORDER BY e.FECHA_PROCESO
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            V_ADELANTE AS (PARTITION BY e.ID_OPERACION_CIERRE, e.COD_RUBRO,
                                        e.ANIO, e.MES
                           ORDER BY e.FECHA_PROCESO
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ),
    RESULTADO AS (
        -- Las filas que SI existen, tal cual
        SELECT ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
               COD_RUBRO, COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION,
               COD_SUB_OPERACION, COD_TIPO_OPERACION, COD_MODULO,
               FEC_VENCIMIENTO, FEC_VALOR, IND_CATEGORIA_RIESGO,
               COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
               MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
               MTO_INTERES, MTO_PREVISIONES,
               IND_DIA_HABIL, FUENTE, FECHA_CARGA, BATCH_ID
        FROM #SO_DIARIO

        UNION ALL

        -- Las que faltaban: atributos del ancla, importes a cero
        SELECT a.ID_OPERACION_CIERRE, a.FECHA_PROCESO,
               b.COD_EMPRESA, b.COD_SUCURSAL, a.COD_RUBRO, b.COD_MONEDA,
               b.COD_PAPEL, b.NUM_CUENTA_BT, b.COD_OPERACION,
               b.COD_SUB_OPERACION, b.COD_TIPO_OPERACION, b.COD_MODULO,
               b.FEC_VENCIMIENTO, b.FEC_VALOR, b.IND_CATEGORIA_RIESGO,
               b.COD_ACTI_BCO_CENTRAL, b.COD_PRODUCTO,
               0, 0, 0, 0, 0, 0,
               a.IND_DIA_HABIL, b.FUENTE, b.FECHA_CARGA, b.BATCH_ID
        FROM CON_ANCLA a
        JOIN #SO_DIARIO b
          ON b.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
         AND b.COD_RUBRO           = a.COD_RUBRO
         AND b.FECHA_PROCESO       = COALESCE(a.ANCLA_PREV, a.ANCLA_NEXT)
        WHERE a.EXISTE IS NULL
          AND COALESCE(a.ANCLA_PREV, a.ANCLA_NEXT) IS NOT NULL
    )
    INSERT INTO #SO_COMPLETO WITH (TABLOCK) (
        ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
        COD_RUBRO, COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION,
        COD_SUB_OPERACION, COD_TIPO_OPERACION, COD_MODULO, FEC_VENCIMIENTO,
        FEC_VALOR, IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
        MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
        MTO_INTERES, MTO_PREVISIONES, IND_DIA_HABIL, FUENTE, FECHA_CARGA,
        BATCH_ID)
    -- WITH (TABLOCK) no es decoracion: es lo que habilita la carga en bloque
    -- sobre un columnstore vacio. Sin el, el INSERT va fila a fila al
    -- deltastore y despues hay que compactarlo.
    SELECT * FROM RESULTADO
    OPTION (RECOMPILE);          -- v2 / B

    -- ========================================================================
    -- 5. BORRAR EL RANGO DESTINO
    -- ------------------------------------------------------------------------
    -- Va DESPUES de haber construido las temporales, no antes: si algo falla en
    -- los pasos 3 o 4, el destino conserva los datos de la corrida anterior en
    -- vez de quedarse vacio.
    --
    -- En el script original este DELETE estaba COMENTADO (linea 1377) mientras
    -- el INSERT no lo estaba: relanzar el proceso duplicaba todo el rango.
    --
    -- v2 / C: dos modos. Lea el punto C de la cabecera antes de usar el
    -- segundo, porque cambia una garantia.
    -- ========================================================================
    IF @P_LOTE_DELETE > 0
    BEGIN
        -- Por lotes, FUERA de la transaccion. Cada DELETE es su propia
        -- transaccion implicita, asi que el log se puede truncar entre lotes.
        -- El precio: entre el ultimo lote y el COMMIT del INSERT hay una
        -- ventana en la que el rango esta vacio.
        WHILE 1 = 1
        BEGIN
            DELETE TOP (@P_LOTE_DELETE)
            FROM dbo.BDS_SALDOS_OPERATIVOS
            WHERE FECHA_PROCESO BETWEEN @V_FECHA_INICIO_MES AND @V_FECHA_FIN;

            SET @V_LOTE = @@ROWCOUNT;
            SET @V_BORRADAS = @V_BORRADAS + @V_LOTE;
            IF @V_LOTE = 0 BREAK;
        END
        RAISERROR('Borrado por lotes: %I64d fila(s) en el rango %s .. %s', 0, 1,
                  @V_BORRADAS,
                  @V_FECHA_INICIO_MES, @V_FECHA_FIN) WITH NOWAIT;
    END

    -- ========================================================================
    -- 6. CLASIFICAR, AGREGAR Y ESCRIBIR
    -- ------------------------------------------------------------------------
    -- Sustituye a DIEZ tablas del script original: TABLA_FINAL_SIN_NOMBRE,
    -- SIN_DUPLICADO, TABLA_FINAL_SIN_NOMBRE_PASIVOS, SIN_DUPLICADO_PASIVOS,
    -- UNION_PASIVOS_ACTIVOS, UNION_PASIVOS_ACTIVOS_SIN_DUPLICADO,
    -- UNION_PASIVOS_ACTIVOS_DEPURADA, TEMPORAL_TABLE, TABLA_1_HAMER y
    -- TABLA_2_HAMER_V2.
    --
    -- El DELETE del modo atomico y el INSERT van dentro de UNA transaccion:
    -- quien consulte BDS_SALDOS_OPERATIVOS mientras el proceso corre ve el
    -- rango COMPLETO -el viejo o el nuevo-, nunca vacio ni a medias. Con
    -- XACT_ABORT ON, cualquier error deshace las dos cosas.
    -- ========================================================================
    BEGIN TRANSACTION;

    IF @P_LOTE_DELETE = 0
        DELETE FROM dbo.BDS_SALDOS_OPERATIVOS
         WHERE FECHA_PROCESO BETWEEN @V_FECHA_INICIO_MES AND @V_FECHA_FIN;

    ;WITH CLASIFICADO AS (
        -- Activos y pasivos en UNA pasada, con prioridad explicita.
        --
        -- El original hacia dos SELECT y un UNION ALL, y una fila que cumpliera
        -- los dos criterios salia DUPLICADA: una vez con los buckets calculados
        -- y otra con ceros. Cual sobrevivia dependia del orden de lectura.
        SELECT c.*,
               CASE WHEN LEFT(c.COD_RUBRO, 4) IN (
                        '1401','1403','1404','1405','1406',
                        '1411','1413','1414','1415','1416',
                        '1421','1423','1424','1425','1426')
                    THEN 1 ELSE 0 END AS ES_ACTIVO
        FROM #SO_COMPLETO c
        WHERE LEFT(c.COD_RUBRO, 4) IN (
                  '1401','1403','1404','1405','1406',
                  '1411','1413','1414','1415','1416',
                  '1421','1423','1424','1425','1426')
           OR c.COD_MODULO IN (20, 21, 22, 120, 155, 162, 184, 185, 321)
    ),
    CON_BUCKETS AS (
        -- Los cinco buckets eran cinco LEFT JOIN en el original, pero cada uno
        -- unia la fila CONSIGO MISMA: salian de la misma tabla agrupada por
        -- (operacion, fecha, rubro) y se unian por esas mismas tres claves.
        -- MTO_SALDO_VIGENTE_MO no era mas que MTO_SALDO_MO de la propia fila
        -- cuando su rubro casaba con '14_1'.
        --
        -- '_' es comodin de un caracter en LIKE: '14_1' casa 1401, 1411 y 1421.
        SELECT k.*,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_1' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_VIGENTE_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_3' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_RESTRUCTURADO_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_4' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_REFINANCIADO_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_5' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_VENCIDO_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_6' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_JUDIAL_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_1' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_VIGENTE_MN,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_3' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_RESTRUCTURADO_MN,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_4' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_REFINANCIADO_MN,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_5' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_VENCIDO_MN,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTRING(COD_RUBRO,1,4) LIKE '14_6' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_JUDICIAL_MN
        FROM CLASIFICADO k
    ),
    POR_DIA AS (
        -- Un rubro es un detalle contable; la salida es por OPERACION y dia.
        SELECT ID_OPERACION_CIERRE,
               FECHA_PROCESO,
               DAY(FECHA_PROCESO)           AS NUM_DIA_MES,
               DAY(EOMONTH(FECHA_PROCESO))  AS TOTAL_DIAS_MES,
               SUM(MTO_SALDO_ORIGEN)            AS TOTAL_DIA_SALDO_ORIGEN,
               SUM(MTO_SALDO_MN)                AS TOTAL_DIA_SALDO_MN,
               SUM(MTO_SALDO_ME)                AS TOTAL_DIA_SALDO_ME,
               SUM(MTO_SALDO_MO)                AS TOTAL_DIA_SALDO_MO,
               SUM(MTO_INTERES)                 AS TOTAL_DIA_INTERES,
               SUM(MTO_PREVISIONES)             AS TOTAL_DIA_PREVISIONES,
               SUM(MTO_SALDO_VIGENTE_MO)        AS TOTAL_DIA_SALDO_VIGENTE_MO,
               SUM(MTO_SALDO_RESTRUCTURADO_MO)  AS TOTAL_DIA_SALDO_RESTRUCTURADO_MO,
               SUM(MTO_SALDO_REFINANCIADO_MO)   AS TOTAL_DIA_SALDO_REFINANCIADO_MO,
               SUM(MTO_SALDO_VENCIDO_MO)        AS TOTAL_DIA_SALDO_VENCIDO_MO,
               SUM(MTO_SALDO_JUDIAL_MO)         AS TOTAL_DIA_SALDO_JUDIAL_MO,
               SUM(MTO_SALDO_VIGENTE_MN)        AS TOTAL_DIA_SALDO_VIGENTE_MN,
               SUM(MTO_SALDO_RESTRUCTURADO_MN)  AS TOTAL_DIA_SALDO_RESTRUCTURADO_MN,
               SUM(MTO_SALDO_REFINANCIADO_MN)   AS TOTAL_DIA_SALDO_REFINANCIADO_MN,
               SUM(MTO_SALDO_VENCIDO_MN)        AS TOTAL_DIA_SALDO_VENCIDO_MN,
               SUM(MTO_SALDO_JUDICIAL_MN)       AS TOTAL_DIA_SALDO_JUDICIAL_MN
        FROM CON_BUCKETS
        GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO
    ),
    PROMEDIOS AS (
        -- Promedio ACUMULADO del mes: suma corrida hasta el dia, dividida entre
        -- el numero de dia. El dia 15 promedia 15 dias, no el mes entero.
        --
        -- La expresion mantiene la forma del original -dividir y luego CAST-
        -- para que los decimales salgan identicos y el script de comparacion
        -- pueda exigir igualdad estricta en vez de una tolerancia.
        SELECT p.*,
            CAST(SUM(TOTAL_DIA_SALDO_ORIGEN)           OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_ORIGEN,
            CAST(SUM(TOTAL_DIA_SALDO_MN)               OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_MN,
            CAST(SUM(TOTAL_DIA_SALDO_ME)               OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_ME,
            CAST(SUM(TOTAL_DIA_SALDO_MO)               OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_MO,
            CAST(SUM(TOTAL_DIA_INTERES)                OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_INTERES,
            CAST(SUM(TOTAL_DIA_PREVISIONES)            OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_PREVISIONES,
            CAST(SUM(TOTAL_DIA_SALDO_VIGENTE_MO)       OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_VIGENTE_MO,
            CAST(SUM(TOTAL_DIA_SALDO_RESTRUCTURADO_MO) OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_RESTRUCTURADO_MO,
            CAST(SUM(TOTAL_DIA_SALDO_REFINANCIADO_MO)  OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_REFINANCIADO_MO,
            CAST(SUM(TOTAL_DIA_SALDO_VENCIDO_MO)       OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_VENCIDO_MO,
            CAST(SUM(TOTAL_DIA_SALDO_JUDIAL_MO)        OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_JUDIAL_MO,
            CAST(SUM(TOTAL_DIA_SALDO_VIGENTE_MN)       OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_VIGENTE_MN,
            CAST(SUM(TOTAL_DIA_SALDO_RESTRUCTURADO_MN) OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_RESTRUCTURADO_MN,
            CAST(SUM(TOTAL_DIA_SALDO_REFINANCIADO_MN)  OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_REFINANCIADO_MN,
            CAST(SUM(TOTAL_DIA_SALDO_VENCIDO_MN)       OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_VENCIDO_MN,
            CAST(SUM(TOTAL_DIA_SALDO_JUDICIAL_MN)      OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_JUDICIAL_MN
        FROM POR_DIA p
        WINDOW W AS (
            PARTITION BY ID_OPERACION_CIERRE,
                         YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
            ORDER BY FECHA_PROCESO
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ),
    ATRIBUTOS AS (
        -- Los atributos de la operacion, uno por (operacion, dia).
        --
        -- El original usaba "SELECT ... GROUP BY op, fecha" con columnas sin
        -- agregar. En SingleStore eso devuelve una fila arbitraria del grupo;
        -- en SQL Server ni siquiera compila. Aqui el desempate es explicito
        -- para que la salida sea reproducible en los dos motores.
        SELECT * FROM (
            SELECT ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA,
                   COD_SUCURSAL, COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT,
                   COD_OPERACION, COD_SUB_OPERACION, COD_TIPO_OPERACION,
                   COD_MODULO, FEC_VENCIMIENTO, FEC_VALOR,
                   IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, FUENTE,
                   IND_DIA_HABIL, FECHA_CARGA, BATCH_ID,
                   ROW_NUMBER() OVER (
                       PARTITION BY ID_OPERACION_CIERRE, FECHA_PROCESO
                       ORDER BY FECHA_CARGA DESC, COD_RUBRO ASC
                   ) AS RN
            FROM CON_BUCKETS
        ) a
        WHERE RN = 1
    )
    INSERT INTO dbo.BDS_SALDOS_OPERATIVOS (
        ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
        COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION,
        COD_TIPO_OPERACION, COD_MODULO, COD_EJECUTIVO, FEC_VENCIMIENTO,
        FEC_VALOR, IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL,
        TOTAL_DIAS_MES,
        TOTAL_DIA_SALDO_ORIGEN, TOTAL_DIA_SALDO_MN, TOTAL_DIA_SALDO_ME,
        TOTAL_DIA_SALDO_MO, TOTAL_DIA_INTERES, TOTAL_DIA_PREVISIONES,
        TOTAL_DIA_SALDO_VIGENTE_MO, TOTAL_DIA_SALDO_RESTRUCTURADO_MO,
        TOTAL_DIA_SALDO_REFINANCIADO_MO, TOTAL_DIA_SALDO_VENCIDO_MO,
        TOTAL_DIA_SALDO_JUDIAL_MO,
        TOTAL_DIA_SALDO_VIGENTE_MN, TOTAL_DIA_SALDO_RESTRUCTURADO_MN,
        TOTAL_DIA_SALDO_REFINANCIADO_MN, TOTAL_DIA_SALDO_VENCIDO_MN,
        TOTAL_DIA_SALDO_JUDIAL_MN,
        TOTAL_AVG_SALDO_ORIGEN, TOTAL_AVG_SALDO_MN, TOTAL_AVG_SALDO_ME,
        TOTAL_AVG_SALDO_MO, TOTAL_AVG_INTERES, TOTAL_AVG_PREVISIONES,
        TOTAL_AVG_SALDO_VIGENTE_MO, TOTAL_AVG_SALDO_RESTRUCTURADO_MO,
        TOTAL_AVG_SALDO_REFINANCIADO_MO, TOTAL_AVG_SALDO_VENCIDO_MO,
        TOTAL_AVG_SALDO_JUDICIAL_MO,
        TOTAL_AVG_SALDO_VIGENTE_MN, TOTAL_AVG_SALDO_RESTRUCTURADO_MN,
        TOTAL_AVG_SALDO_REFINANCIADO_MN, TOTAL_AVG_SALDO_VENCIDO_MN,
        TOTAL_AVG_SALDO_JUDICIAL_MN,
        FUENTE, IND_DIA_HABIL, FECHA_CARGA, BATCH_ID
    )
    SELECT
        a.ID_OPERACION_CIERRE, a.FECHA_PROCESO, a.COD_EMPRESA, a.COD_SUCURSAL,
        a.COD_MONEDA, a.COD_PAPEL, a.NUM_CUENTA_BT, a.COD_OPERACION,
        a.COD_SUB_OPERACION, a.COD_TIPO_OPERACION, a.COD_MODULO,
        -- COD_EJECUTIVO se resuelve aqui con un LEFT JOIN. El original lo
        -- dejaba en NULL y lo arreglaba con un UPDATE final sobre toda la
        -- tabla de salida -- una segunda escritura de 18 millones de filas
        -- para rellenar una columna.
        sc.COD_EJECUTIVO,
        a.FEC_VENCIMIENTO, a.FEC_VALOR, a.IND_CATEGORIA_RIESGO,
        a.COD_ACTI_BCO_CENTRAL,
        b.TOTAL_DIAS_MES,
        b.TOTAL_DIA_SALDO_ORIGEN, b.TOTAL_DIA_SALDO_MN, b.TOTAL_DIA_SALDO_ME,
        b.TOTAL_DIA_SALDO_MO, b.TOTAL_DIA_INTERES, b.TOTAL_DIA_PREVISIONES,
        b.TOTAL_DIA_SALDO_VIGENTE_MO, b.TOTAL_DIA_SALDO_RESTRUCTURADO_MO,
        b.TOTAL_DIA_SALDO_REFINANCIADO_MO, b.TOTAL_DIA_SALDO_VENCIDO_MO,
        b.TOTAL_DIA_SALDO_JUDIAL_MO,
        b.TOTAL_DIA_SALDO_VIGENTE_MN, b.TOTAL_DIA_SALDO_RESTRUCTURADO_MN,
        b.TOTAL_DIA_SALDO_REFINANCIADO_MN, b.TOTAL_DIA_SALDO_VENCIDO_MN,
        b.TOTAL_DIA_SALDO_JUDICIAL_MN,
        b.AVG_SALDO_ORIGEN, b.AVG_SALDO_MN, b.AVG_SALDO_ME, b.AVG_SALDO_MO,
        b.AVG_INTERES, b.AVG_PREVISIONES,
        b.AVG_SALDO_VIGENTE_MO, b.AVG_SALDO_RESTRUCTURADO_MO,
        b.AVG_SALDO_REFINANCIADO_MO, b.AVG_SALDO_VENCIDO_MO,
        b.AVG_SALDO_JUDIAL_MO,
        b.AVG_SALDO_VIGENTE_MN, b.AVG_SALDO_RESTRUCTURADO_MN,
        b.AVG_SALDO_REFINANCIADO_MN, b.AVG_SALDO_VENCIDO_MN,
        b.AVG_SALDO_JUDICIAL_MN,
        a.FUENTE, a.IND_DIA_HABIL, a.FECHA_CARGA, a.BATCH_ID
    FROM ATRIBUTOS a
    JOIN PROMEDIOS b
      ON b.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
     AND b.FECHA_PROCESO       = a.FECHA_PROCESO
    LEFT JOIN dbo.BDS_SALDOS_CIERRE sc
      ON sc.ID_OPERACION  = a.ID_OPERACION_CIERRE
     AND sc.FECHA_PROCESO = a.FECHA_PROCESO
    WHERE a.FECHA_PROCESO BETWEEN @V_FECHA_INICIO_MES AND @V_FECHA_FIN
    OPTION (RECOMPILE);          -- v2 / B

    COMMIT TRANSACTION;

    -- ========================================================================
    -- 7. LIMPIEZA
    -- ------------------------------------------------------------------------
    -- Las tablas # se borran solas al terminar el procedimiento, pero soltarlas
    -- aqui devuelve el espacio de tempdb sin esperar a que se cierre la sesion,
    -- que en un pool de conexiones puede tardar mucho.
    -- ========================================================================
    DROP TABLE IF EXISTS #SO_DIARIO;
    DROP TABLE IF EXISTS #SO_COMPLETO;
END
GO

-- ============================================================================
--  COMO SE LLAMA
-- ----------------------------------------------------------------------------
--  Modo atomico (por defecto). El rango nunca se ve vacio:
--      EXEC dbo.SP_BDS_SALDOS_OPERATIVOS @P_FECHA_PROCESO = '2026-04-30',
--                                        @P_DIAS = 31;
--
--  Modo por lotes. Mas rapido y amable con el log, pero hay una ventana en la
--  que el rango esta vacio:
--      EXEC dbo.SP_BDS_SALDOS_OPERATIVOS @P_FECHA_PROCESO = '2026-04-30',
--                                        @P_DIAS = 31,
--                                        @P_LOTE_DELETE = 100000;
--
--  Y DESPUES, SIEMPRE, la validacion. La comprobacion 2 verifica la garantia
--  de la cabecera -- un conteo por llave y mes igual a los dias del mes -- y
--  tiene que devolver CERO filas:
--      sql/sp_reescritos/VALIDAR_SALDOS_OPERATIVOS.sql
-- ============================================================================
