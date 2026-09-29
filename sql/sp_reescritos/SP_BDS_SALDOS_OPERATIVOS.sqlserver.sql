-- ============================================================================
--  SP_BDS_SALDOS_OPERATIVOS   ::   SQL Server 2022
-- ----------------------------------------------------------------------------
--  Puerto a T-SQL de la version reescrita. La logica es la MISMA que en
--  SP_BDS_SALDOS_OPERATIVOS.singlestore.sql: mismo orden de pasos, mismos
--  criterios, mismos nombres de CTE. Lo que cambia es la sintaxis y tres cosas
--  que en SQL Server se hacen mejor de otra forma, marcadas cada una con
--  "DIFERENCIA CON SINGLESTORE".
--
--  Lee la cabecera de la version SingleStore para el detalle de QUE hace el
--  proceso y de los ocho defectos del script original que esta reescritura
--  corrige. Aqui solo se documenta lo propio de SQL Server.
--
-- ============================================================================
--  EQUIVALENCIAS QUE HUBO QUE TRADUCIR
-- ----------------------------------------------------------------------------
--  Ninguna es cosmetica; las cuatro primeras cambian el resultado si se
--  traducen mal, y las tres ultimas ni siquiera compilan en el otro motor.
--
--   SingleStore / MySQL                     SQL Server 2022
--   -------------------------------------   -----------------------------------
--   DATE_SUB(d, INTERVAL n DAY)             DATEADD(day, -n, d)
--   DATE_FORMAT(d, '%Y-%m-01')              DATEFROMPARTS(YEAR(d), MONTH(d), 1)
--   LAST_DAY(d)                             EOMONTH(d)
--   SUBSTR(x, 1, 4)                         SUBSTRING(x, 1, 4)
--   CREATE TEMPORARY TABLE t AS SELECT      SELECT ... INTO #t
--   RAISE USER_EXCEPTION(msg)               THROW 50000, @msg, 1
--   IF ... THEN ... END IF                  IF ... BEGIN ... END
--
--  El comodin '_' de LIKE significa lo mismo en los dos motores, asi que
--  '14_1' sigue casando 1401, 1411 y 1421 sin cambios.
--
--  La clausula WINDOW (WINDOW W AS (...)) existe en SQL Server DESDE 2022. En
--  2019 o anterior hay que repetir el OVER completo en las 16 expresiones. Si
--  este SP tiene que correr en una instancia anterior, esa es la unica parte
--  que hay que desplegar distinta.
--
-- ============================================================================
--  EJECUCION
-- ----------------------------------------------------------------------------
--      EXEC dbo.SP_BDS_SALDOS_OPERATIVOS @P_FECHA_PROCESO = '2026-07-01',
--                                        @P_DIAS          = 30;
-- ============================================================================

USE GNBPE_DATAHUB;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE dbo.SP_BDS_SALDOS_OPERATIVOS
    @P_FECHA_PROCESO DATE,
    @P_DIAS          INT
AS
BEGIN
    -- Sin esto, cada INSERT, UPDATE y DELETE devuelve su "(N rows affected)".
    -- En un proceso de decenas de millones de filas eso es trafico de red y,
    -- con algunos drivers, cursores intermedios que consumen memoria del
    -- cliente para nada.
    SET NOCOUNT ON;

    -- El comportamiento por defecto de SQL Server ante un error dentro de un
    -- procedimiento es ABORTAR LA SENTENCIA Y SEGUIR con la siguiente. En un
    -- proceso como este eso significaria borrar el destino y despues fallar el
    -- INSERT, dejando el rango vacio. XACT_ABORT ON hace que cualquier error
    -- en tiempo de ejecucion aborte la transaccion entera.
    SET XACT_ABORT ON;

    DECLARE @V_FECHA_FIN        DATE,
            @V_FECHA_INICIO     DATE,
            @V_FECHA_INICIO_MES DATE,
            @V_FECHA_CALCULADA  DATE,
            @V_FILAS_ORIGEN     BIGINT,
            @V_MAX_CALENDARIO   DATE,
            @V_MSG              NVARCHAR(2000);

    -- ========================================================================
    -- 1. LA VENTANA DE PROCESO
    -- ------------------------------------------------------------------------
    --   @V_FECHA_FIN         ultimo dia a procesar
    --   @V_FECHA_INICIO      desde donde se LEE. Ultimo dia HABIL <= (fin - dias),
    --                        porque es de donde se arrastran los saldos.
    --   @V_FECHA_INICIO_MES  desde donde se ESCRIBE. El promedio mensual
    --                        necesita el mes completo.
    -- ========================================================================
    SET @V_FECHA_FIN        = @P_FECHA_PROCESO;
    SET @V_FECHA_CALCULADA  = DATEADD(day, -@P_DIAS, @V_FECHA_FIN);
    SET @V_FECHA_INICIO_MES = DATEFROMPARTS(YEAR(@V_FECHA_CALCULADA),
                                            MONTH(@V_FECHA_CALCULADA), 1);

    SELECT @V_FECHA_INICIO = MAX(FEC_CALENDARIO)
      FROM dbo.BDS_CALENDARIOS
     WHERE COD_CALENDARIO = 1
       AND IND_DIA_HABIL  = 'S'
       AND FEC_CALENDARIO <= @V_FECHA_CALCULADA;

    -- ========================================================================
    -- 2. VALIDACIONES
    -- ------------------------------------------------------------------------
    -- Fallar aqui, en un segundo y diciendo que falta, es mucho mas barato que
    -- fallar cuarenta minutos despues con el destino a medio escribir.
    -- ========================================================================
    IF @V_FECHA_INICIO IS NULL
    BEGIN
        SET @V_MSG = CONCAT(
            'BDS_CALENDARIOS no tiene ningun dia habil (COD_CALENDARIO=1) en o antes de ',
            CONVERT(varchar(10), @V_FECHA_CALCULADA, 23),
            '. Sin eso no hay desde donde arrastrar los saldos. ',
            'Revisa que el calendario este cargado hasta esa fecha.');
        THROW 50001, @V_MSG, 1;
    END

    SELECT @V_FILAS_ORIGEN = COUNT_BIG(*)
      FROM dbo.BDS_SALDOS_CIERRE
     WHERE FECHA_PROCESO BETWEEN @V_FECHA_INICIO AND @V_FECHA_FIN;

    IF @V_FILAS_ORIGEN = 0
    BEGIN
        SET @V_MSG = CONCAT(
            'BDS_SALDOS_CIERRE no tiene filas entre ',
            CONVERT(varchar(10), @V_FECHA_INICIO, 23), ' y ',
            CONVERT(varchar(10), @V_FECHA_FIN, 23),
            '. El proceso se detiene SIN borrar el destino: si continuara, el DELETE ',
            'dejaria el rango vacio y nada avisaria de que no habia origen.');
        THROW 50002, @V_MSG, 1;
    END

    SELECT @V_MAX_CALENDARIO = MAX(FEC_CALENDARIO)
      FROM dbo.BDS_CALENDARIOS WHERE COD_CALENDARIO = 1;

    IF @V_MAX_CALENDARIO < @V_FECHA_FIN
    BEGIN
        SET @V_MSG = CONCAT(
            'BDS_CALENDARIOS solo llega hasta ',
            CONVERT(varchar(10), @V_MAX_CALENDARIO, 23),
            ' y se pidio procesar hasta ', CONVERT(varchar(10), @V_FECHA_FIN, 23),
            '. Los dias que falten no apareceran en la salida, sin error, y el ',
            'promedio de ese mes saldria dividido entre menos dias de los que toca.');
        THROW 50003, @V_MSG, 1;
    END

    -- ========================================================================
    -- 3. LA SERIE DIARIA, YA PROPAGADA A DIAS NO HABILES
    -- ------------------------------------------------------------------------
    -- Sustituye a ocho tablas del original.
    --
    -- DIFERENCIA CON SINGLESTORE
    -- Se usa una tabla temporal local (#) y no una tabla normal: vive solo en
    -- esta sesion, desaparece sola al terminar el procedimiento, y dos
    -- ejecuciones simultaneas obtienen cada una la suya aunque se llamen igual.
    -- En el script original estas eran tablas REALES y dos corridas a la vez se
    -- pisaban los datos.
    -- ========================================================================
    DROP TABLE IF EXISTS #SO_DIARIO;

    WITH CALENDARIO AS (
        -- Para cada dia del rango, el ultimo dia habil anterior o igual: de ahi
        -- copia sus importes. Sabado y domingo copian del viernes.
        --
        -- DIFERENCIA CON SINGLESTORE
        -- Aqui se resuelve con OUTER APPLY en vez de una subconsulta
        -- correlacionada en el SELECT. Es la misma semantica, pero SQL Server
        -- lo ejecuta como un TOP 1 con seek sobre el indice del calendario en
        -- lugar de un agregado por fila.
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
            -- El relleno de huecos solo se ancla en ORIGINAL: anclarse en una
            -- copia propagaria una copia de una copia.
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
        -- del orden de lectura.
        SELECT p.*,
               ROW_NUMBER() OVER (
                   PARTITION BY p.ID_OPERACION_CIERRE, p.COD_RUBRO, p.FECHA_PROCESO
                   ORDER BY p.FECHA_CARGA DESC,
                            COALESCE(p.MTO_SALDO_MN, 0) DESC,
                            p.BATCH_ID DESC
               ) AS RN
        FROM PROPAGADO p
    )
    SELECT * INTO #SO_DIARIO FROM NUMERADO WHERE RN = 1;

    -- DIFERENCIA CON SINGLESTORE
    -- SingleStore distribuye y ordena por su shard key; aqui hay que decirle a
    -- SQL Server como queremos leer la tabla. Sin este indice, los dos joins
    -- del paso 4 y el del paso 6 son escaneos completos de decenas de millones
    -- de filas. Es columnstore porque lo que sigue son agregados sobre casi
    -- todas las filas, que es justo donde gana frente a un indice rowstore.
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_SO_DIARIO ON #SO_DIARIO;

    -- ========================================================================
    -- 4. RELLENO DE HUECOS
    -- ------------------------------------------------------------------------
    -- Sustituye a seis tablas y TRES CTE recursivas del original.
    --
    -- Las tres recursivas resolvian el mismo problema en tres trozos: los dias
    -- que faltan al FINAL del mes, EN MEDIO y al PRINCIPIO. Pero "los dias que
    -- faltan" es el calendario del mes menos los dias que si estan, y eso es un
    -- LEFT JOIN, sin recursion y sin distinguir casos.
    --
    -- Lo unico que cambiaba entre los tres era de donde salen los atributos de
    -- la fila generada, y eso lo dan dos funciones de ventana:
    --
    --     ANCLA_PREV   ultima fecha con datos <= este dia   (huecos y fin de mes)
    --     ANCLA_NEXT   primera fecha con datos >= este dia  (principio de mes)
    --     ANCLA        COALESCE(ANCLA_PREV, ANCLA_NEXT)
    --
    -- El COALESCE reproduce el criterio del original: si hay algo antes se
    -- hereda de atras; si no -porque el hueco esta antes del primer registro
    -- del mes- se hereda de adelante.
    --
    -- La equivalencia entre las tres recursivas y esta pasada esta verificada
    -- fila por fila en scripts/verificar_relleno_sp.py, sobre los ocho casos
    -- borde (hueco al principio, en medio, al final, los tres juntos, un mes de
    -- un solo dia, un mes sin huecos, dos rubros con huecos distintos, y un mes
    -- de 31 dias).
    --
    -- Los IMPORTES de las filas generadas van a CERO, igual que en el original.
    -- Solo se heredan los atributos.
    -- ========================================================================
    DROP TABLE IF EXISTS #SO_COMPLETO;

    WITH LLAVES AS (
        -- Cada (operacion, rubro) con el mes que hay que completar.
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
        SELECT k.ID_OPERACION_CIERRE, k.COD_RUBRO, k.ANIO, k.MES,
               c.FEC_CALENDARIO AS FECHA_PROCESO,
               c.IND_DIA_HABIL
        FROM LLAVES k
        JOIN dbo.BDS_CALENDARIOS c
          ON c.COD_CALENDARIO = 1
         AND YEAR(c.FEC_CALENDARIO)  = k.ANIO
         AND MONTH(c.FEC_CALENDARIO) = k.MES
         AND c.FEC_CALENDARIO <= @V_FECHA_FIN
    ),
    CON_ANCLA AS (
        SELECT e.ID_OPERACION_CIERRE, e.COD_RUBRO, e.FECHA_PROCESO, e.IND_DIA_HABIL,
               d.ID_OPERACION_CIERRE AS EXISTE,
               MAX(CASE WHEN d.ID_OPERACION_CIERRE IS NOT NULL
                         AND d.TIPO_ORIGEN = 'ORIGINAL'
                        THEN e.FECHA_PROCESO END) OVER V_ATRAS    AS ANCLA_PREV,
               MIN(CASE WHEN d.ID_OPERACION_CIERRE IS NOT NULL
                         AND d.TIPO_ORIGEN = 'ORIGINAL'
                        THEN e.FECHA_PROCESO END) OVER V_ADELANTE AS ANCLA_NEXT
        FROM ESPINA e
        LEFT JOIN #SO_DIARIO d
               ON d.ID_OPERACION_CIERRE = e.ID_OPERACION_CIERRE
              AND d.COD_RUBRO           = e.COD_RUBRO
              AND d.FECHA_PROCESO       = e.FECHA_PROCESO
        -- La clausula WINDOW es de SQL Server 2022. En 2019 hay que repetir el
        -- OVER completo en las dos expresiones de arriba.
        WINDOW
            V_ATRAS AS (PARTITION BY e.ID_OPERACION_CIERRE, e.COD_RUBRO, e.ANIO, e.MES
                        ORDER BY e.FECHA_PROCESO
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            V_ADELANTE AS (PARTITION BY e.ID_OPERACION_CIERRE, e.COD_RUBRO, e.ANIO, e.MES
                           ORDER BY e.FECHA_PROCESO
                           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ),
    RESULTADO AS (
        -- Las filas que SI existen, tal cual
        SELECT ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
               COD_RUBRO, COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION,
               COD_SUB_OPERACION, COD_TIPO_OPERACION, COD_MODULO, FEC_VENCIMIENTO,
               FEC_VALOR, IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
               MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
               MTO_INTERES, MTO_PREVISIONES,
               IND_DIA_HABIL, FUENTE, FECHA_CARGA, BATCH_ID
        FROM #SO_DIARIO

        UNION ALL

        -- Las que faltaban: atributos del ancla, importes a cero
        SELECT a.ID_OPERACION_CIERRE, a.FECHA_PROCESO,
               b.COD_EMPRESA, b.COD_SUCURSAL, a.COD_RUBRO, b.COD_MONEDA,
               b.COD_PAPEL, b.NUM_CUENTA_BT, b.COD_OPERACION, b.COD_SUB_OPERACION,
               b.COD_TIPO_OPERACION, b.COD_MODULO, b.FEC_VENCIMIENTO, b.FEC_VALOR,
               b.IND_CATEGORIA_RIESGO, b.COD_ACTI_BCO_CENTRAL, b.COD_PRODUCTO,
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
    SELECT * INTO #SO_COMPLETO FROM RESULTADO;

    CREATE CLUSTERED COLUMNSTORE INDEX CCI_SO_COMPLETO ON #SO_COMPLETO;

    -- ========================================================================
    -- 5 y 6. REESCRIBIR EL RANGO DESTINO, EN UNA TRANSACCION
    -- ------------------------------------------------------------------------
    -- DIFERENCIA CON SINGLESTORE
    -- Aqui el DELETE y el INSERT van dentro de una transaccion explicita. En
    -- SQL Server eso significa que quien consulte BDS_SALDOS_OPERATIVOS
    -- mientras el proceso corre ve el rango COMPLETO -el viejo o el nuevo-,
    -- nunca vacio ni a medias. Con XACT_ABORT ON, cualquier error deshace las
    -- dos cosas.
    --
    -- El DELETE va despues de haber construido las temporales, no antes: si
    -- algo falla en los pasos 3 o 4, el destino conserva la corrida anterior.
    --
    -- En el original este DELETE estaba COMENTADO (linea 1377) mientras el
    -- INSERT no lo estaba: relanzar el proceso duplicaba todo el rango.
    -- ========================================================================
    BEGIN TRANSACTION;

    DELETE FROM dbo.BDS_SALDOS_OPERATIVOS
     WHERE FECHA_PROCESO BETWEEN @V_FECHA_INICIO_MES AND @V_FECHA_FIN;

    WITH CLASIFICADO AS (
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
        -- O sea que MTO_SALDO_VIGENTE_MO no era mas que MTO_SALDO_MO de la
        -- propia fila cuando su rubro casaba con '14_1'.
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
            PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
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
            SELECT ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
                   COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION,
                   COD_SUB_OPERACION, COD_TIPO_OPERACION, COD_MODULO,
                   FEC_VENCIMIENTO, FEC_VALOR, IND_CATEGORIA_RIESGO,
                   COD_ACTI_BCO_CENTRAL, FUENTE, IND_DIA_HABIL, FECHA_CARGA,
                   BATCH_ID,
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
        -- COD_EJECUTIVO se resuelve aqui. El original lo dejaba en NULL y lo
        -- arreglaba con un UPDATE final sobre toda la tabla de salida.
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
    WHERE a.FECHA_PROCESO BETWEEN @V_FECHA_INICIO_MES AND @V_FECHA_FIN;

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
