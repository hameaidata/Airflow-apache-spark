-- ============================================================================
--  SP_BDS_SALDOS_OPERATIVOS   ::   SingleStore
-- ----------------------------------------------------------------------------
--  Reescritura del script SP_BDS_SALDOS_OPERATIVOS.sql siguiendo el patron de
--  SP_CREAR_VARIACION_CONTABLE: sin tablas intermedias materializadas, sin
--  pasadas de UPDATE, idempotente por rango de fechas.
--
--      25 tablas creadas y dropeadas   ->   2 tablas TEMPORALES
--      3 CTE recursivas                ->   1 join contra el calendario
--      2 UPDATE sobre toda la salida   ->   0
--      1574 lineas                     ->   ~380
--
--  LA LOGICA DE NEGOCIO NO CAMBIA. Lo que cambia es como se calcula.
--
-- ============================================================================
--  QUE HACE ESTE PROCESO
-- ----------------------------------------------------------------------------
--  Convierte los saldos de cierre (BDS_SALDOS_CIERRE), que solo existen los
--  dias habiles y solo cuando hubo movimiento, en una serie DIARIA COMPLETA por
--  operacion, y calcula el saldo promedio acumulado del mes.
--
--  Tres cosas en orden:
--
--   1. PROPAGACION A DIAS NO HABILES
--      Un sabado no tiene cierre. Su saldo es el del viernes. Para eso esta
--      CALENDARIO: cada dia del rango se empareja con el ultimo dia habil
--      anterior o igual, y de ahi copia los importes.
--
--   2. RELLENO DE HUECOS DENTRO DEL MES
--      Una operacion sin movimiento durante diez dias no genera filas. Para que
--      el promedio mensual divida entre los dias correctos, esos dias tienen
--      que existir con importe CERO y los atributos de la fila mas cercana.
--
--   3. PROMEDIO ACUMULADO
--      Suma corrida del mes dividida entre el dia del mes. El dia 15 es el
--      promedio de los 15 primeros dias, no del mes entero.
--
-- ============================================================================
--  DEFECTOS DEL SCRIPT ORIGINAL QUE ESTA VERSION CORRIGE
-- ----------------------------------------------------------------------------
--  Se documentan uno por uno porque son la justificacion de cada cambio, y
--  porque quien compare las dos versiones va a preguntar por que falta algo.
--
--  1. EL SPLIT DE OPERACIONES ERA UN NO-OP  (lineas 196-229 del original)
--     Se partia la base en "las que estan en BDS_OPERACIONES" y "las que no", y
--     se volvian a unir con UNION ALL. Eso devuelve el conjunto de partida. Dos
--     materializaciones completas de 35 millones de filas para nada.
--
--     Y podia ser algo peor que inutil: si BDS_OPERACIONES.ID_OPERACION tiene
--     un solo NULL, el "NOT IN (SELECT ...)" devuelve CERO filas -- asi funciona
--     NOT IN con nulos en SQL-- y esa mitad de los datos desaparecia sin ningun
--     error. Aqui no se hace el split.
--
--  2. LOS CINCO LEFT JOIN DE BUCKETS ERAN AUTO-JOINS  (lineas 794-890)
--     BASE_14_1_VIGENTE_MO salia de UNION_ALL_TABLE_1 agrupada por
--     (ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO) y se volvia a unir por
--     esas MISMAS tres claves. Es decir: la fila se unia consigo misma.
--
--     MTO_SALDO_VIGENTE_MO no era mas que MTO_SALDO_MO de la propia fila cuando
--     su rubro casaba con '14_1'. Aqui son cinco CASE. Ademas de ser cinco joins
--     menos sobre 68 millones de filas, elimina el GROUP BY arbitrario que
--     habia dentro de cada uno.
--
--  3. EL PASO DE "DEPURACION" TAMPOCO HACIA NADA  (lineas 1038-1063)
--     Filtraba por "CNT = 1 OR (CNT > 1 AND algun importe <> 0)", pero CNT se
--     calculaba sobre la salida de un ROW_NUMBER() = 1 que ya garantizaba una
--     sola fila por particion. CNT valia 1 siempre y el filtro no descartaba
--     nada.
--
--  4. TRES "GROUP BY" SIN AGREGAR  (lineas 911, 946, 1107)
--     SELECT * FROM t GROUP BY a,b,c deja que el motor elija una fila
--     ARBITRARIA de cada grupo. No es determinista: la misma entrada puede dar
--     salidas distintas en dos corridas, y nada avisa. En SQL Server ni siquiera
--     compila. Aqui se resuelve con ROW_NUMBER() y un desempate explicito.
--
--  5. EL UPDATE DE IND_DIA_HABIL ESTABA DUPLICADO  (lineas 751 y 918)
--     Dos pasadas identicas sobre toda la tabla. Ademas eran innecesarias: el
--     indicador sale del calendario, que ya esta en el join de la propagacion.
--
--  6. NO ERA IDEMPOTENTE  (linea 1377)
--     El DELETE del rango destino estaba COMENTADO y el INSERT no. Reejecutar
--     el proceso duplicaba todo lo del rango. Aqui el DELETE es parte del
--     procedimiento y va antes del INSERT.
--
--  7. LAS TABLAS INTERMEDIAS ERAN REALES, NO TEMPORALES
--     CREATE TABLE, no CREATE TEMPORARY TABLE. Quedaban en el esquema entre
--     corridas, y dos ejecuciones simultaneas escribian sobre las mismas tablas
--     pisandose los datos. Los DROP TABLE tampoco llevaban IF EXISTS, asi que
--     la primera corrida sobre una base limpia fallaba.
--
--  8. UNA FILA PODIA CONTARSE COMO ACTIVO Y COMO PASIVO A LA VEZ
--     Los dos conjuntos se definian con criterios distintos (COD_RUBRO para
--     activos, COD_MODULO para pasivos) y se unian con UNION ALL. Una fila que
--     cumpliera los dos salia DUPLICADA, y cual de las dos sobrevivia al dedupe
--     posterior dependia del orden en que el motor las leyera: en una version
--     con los buckets calculados y en la otra con ceros.
--
--     Aqui la clasificacion es una sola pasada con prioridad explicita: si la
--     fila es de activos, se trata como activo. Determinista.
--
-- ============================================================================
--  EJECUCION
-- ----------------------------------------------------------------------------
--      CALL SP_BDS_SALDOS_OPERATIVOS('2026-07-01', 30);
--
--  P_FECHA_PROCESO  ultimo dia a procesar
--  P_DIAS           cuantos dias hacia atras. Define el inicio del rango.
--
--  El proceso reescribe COMPLETO el rango [primer dia del mes de inicio,
--  P_FECHA_PROCESO] en BDS_SALDOS_OPERATIVOS. Se puede relanzar las veces que
--  haga falta sin duplicar nada.
-- ============================================================================

DELIMITER //

CREATE OR REPLACE PROCEDURE `DATAHUB`.`SP_BDS_SALDOS_OPERATIVOS`(
    P_FECHA_PROCESO DATE NULL,
    P_DIAS          INT  NULL
) RETURNS void AS

DECLARE
    V_FECHA_FIN        DATE;
    V_FECHA_INICIO     DATE;
    V_FECHA_INICIO_MES DATE;
    V_FECHA_CALCULADA  DATE;
    V_FILAS_ORIGEN     BIGINT;

BEGIN

    -- ========================================================================
    -- 1. LA VENTANA DE PROCESO
    -- ------------------------------------------------------------------------
    -- Tres fechas, y las tres significan cosas distintas:
    --
    --   V_FECHA_FIN         ultimo dia a procesar. Lo da el parametro.
    --
    --   V_FECHA_INICIO      desde donde se LEE el origen. Es el ultimo dia
    --                       HABIL anterior o igual a (fin - P_DIAS). Tiene que
    --                       ser habil porque es de donde se arrastran los
    --                       saldos hacia adelante: si cayera en domingo no
    --                       habria nada que arrastrar.
    --
    --   V_FECHA_INICIO_MES  desde donde se ESCRIBE el destino. El promedio
    --                       mensual necesita el mes completo, asi que se
    --                       recalcula desde el dia 1 aunque la lectura empiece
    --                       mas tarde.
    -- ========================================================================
    V_FECHA_FIN = P_FECHA_PROCESO;
    V_FECHA_CALCULADA = DATE_SUB(V_FECHA_FIN, INTERVAL P_DIAS DAY);
    V_FECHA_INICIO_MES = DATE(DATE_FORMAT(V_FECHA_CALCULADA, '%Y-%m-01'));

    SELECT MAX(FEC_CALENDARIO)
      INTO V_FECHA_INICIO
      FROM BDS_CALENDARIOS
     WHERE COD_CALENDARIO = 1
       AND IND_DIA_HABIL  = 'S'
       AND FEC_CALENDARIO <= V_FECHA_CALCULADA;

    -- ========================================================================
    -- 2. VALIDACIONES
    -- ------------------------------------------------------------------------
    -- Fallar aqui, en un segundo y con un mensaje que dice que falta, es mucho
    -- mas barato que fallar cuarenta minutos despues con 68 millones de filas a
    -- medio escribir. El original no validaba nada.
    -- ========================================================================
    IF V_FECHA_INICIO IS NULL THEN
        RAISE USER_EXCEPTION(CONCAT(
            'BDS_CALENDARIOS no tiene ningun dia habil (COD_CALENDARIO=1) en o antes de ',
            V_FECHA_CALCULADA, '. Sin eso no hay desde donde arrastrar los saldos. ',
            'Revisa que el calendario este cargado hasta esa fecha.'));
    END IF;

    SELECT COUNT(*)
      INTO V_FILAS_ORIGEN
      FROM BDS_SALDOS_CIERRE
     WHERE FECHA_PROCESO BETWEEN V_FECHA_INICIO AND V_FECHA_FIN;

    IF V_FILAS_ORIGEN = 0 THEN
        RAISE USER_EXCEPTION(CONCAT(
            'BDS_SALDOS_CIERRE no tiene filas entre ', V_FECHA_INICIO, ' y ', V_FECHA_FIN,
            '. El proceso se detiene SIN borrar el destino: si continuara, el DELETE ',
            'dejaria el rango vacio y nada avisaria de que no habia origen.'));
    END IF;

    -- Comprobar que el calendario cubre TODO el rango. Si se queda corto, los
    -- dias que falten simplemente no aparecen en la salida -sin error- y el
    -- promedio de ese mes sale dividido entre menos dias de los que toca.
    IF (SELECT MAX(FEC_CALENDARIO) FROM BDS_CALENDARIOS WHERE COD_CALENDARIO = 1)
       < V_FECHA_FIN THEN
        RAISE USER_EXCEPTION(CONCAT(
            'BDS_CALENDARIOS solo llega hasta ',
            (SELECT MAX(FEC_CALENDARIO) FROM BDS_CALENDARIOS WHERE COD_CALENDARIO = 1),
            ' y se pidio procesar hasta ', V_FECHA_FIN,
            '. Carga el calendario antes de continuar.'));
    END IF;

    -- ========================================================================
    -- 3. LA SERIE DIARIA, YA PROPAGADA A DIAS NO HABILES
    -- ------------------------------------------------------------------------
    -- Sustituye a OCHO tablas del original: TEMP_BASE_INICIAL,
    -- TEMP_BASE_COMPLETA_POR_MES, CALENDARIO_GLOBAL, BASE_TABLA_RANGO_FECHA,
    -- TABLA_REGISTROS_MISMO_DIA_OPERACIONES,
    -- TABLA_REGISTROS_OPERACIONES_ANTERIORES, UNION_ALL_TABLE y
    -- UNION_ALL_TABLE_1.
    --
    -- TEMPORARY y no una tabla normal: vive solo en esta sesion, desaparece
    -- sola al terminar, y dos corridas simultaneas no se pisan.
    -- ========================================================================
    DROP TABLE IF EXISTS TMP_SO_DIARIO;

    CREATE TEMPORARY TABLE TMP_SO_DIARIO AS
    WITH CALENDARIO AS (
        -- Para cada dia del rango, el ultimo dia habil anterior o igual.
        -- Ese FECHA_ORIGEN es de donde el dia copia sus importes: el sabado y
        -- el domingo copian del viernes, el 1 de enero del 31 de diciembre.
        SELECT
            c.FEC_CALENDARIO AS FECHA_PROCESO,
            c.IND_DIA_HABIL,
            (SELECT MAX(h.FEC_CALENDARIO)
               FROM BDS_CALENDARIOS h
              WHERE h.COD_CALENDARIO = 1
                AND h.IND_DIA_HABIL  = 'S'
                AND h.FEC_CALENDARIO <= c.FEC_CALENDARIO) AS FECHA_ORIGEN
        FROM BDS_CALENDARIOS c
        WHERE c.COD_CALENDARIO = 1
          AND c.FEC_CALENDARIO BETWEEN V_FECHA_INICIO AND V_FECHA_FIN
    ),
    ORIGEN AS (
        SELECT *
        FROM BDS_SALDOS_CIERRE
        WHERE FECHA_PROCESO BETWEEN V_FECHA_INICIO AND V_FECHA_FIN
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
            -- ORIGINAL   = el dato es de ese dia
            -- COPIA_HABIL= se copio del ultimo dia habil anterior
            -- Se conserva porque el relleno de huecos solo se ancla en filas
            -- ORIGINAL: anclarse en una copia propagaria una copia de una copia.
            CASE WHEN cal.FECHA_PROCESO = cal.FECHA_ORIGEN
                 THEN 'ORIGINAL' ELSE 'COPIA_HABIL' END AS TIPO_ORIGEN,
            o.FUENTE, o.FECHA_CARGA, o.BATCH_ID
        FROM CALENDARIO cal
        JOIN ORIGEN o ON o.FECHA_PROCESO = cal.FECHA_ORIGEN
    )
    -- Una sola fila por (operacion, rubro, dia).
    --
    -- El original particionaba por 23 columnas -incluidos los importes-, lo que
    -- NO garantiza unicidad por dia: dos filas del mismo dia con importes
    -- distintos sobrevivian las dos, y el duplicado se arrastraba hasta el
    -- final. Aqui la particion es la clave de negocio de verdad.
    --
    -- El desempate es explicito y total: gana la carga mas reciente, y si
    -- empatan, el saldo mayor, y si tambien empata, el BATCH_ID. Sin esa
    -- ultima columna el resultado seguiria dependiendo del orden de lectura.
    SELECT * FROM (
        SELECT p.*,
               ROW_NUMBER() OVER (
                   PARTITION BY p.ID_OPERACION_CIERRE, p.COD_RUBRO, p.FECHA_PROCESO
                   ORDER BY p.FECHA_CARGA DESC,
                            COALESCE(p.MTO_SALDO_MN, 0) DESC,
                            p.BATCH_ID DESC
               ) AS RN
        FROM PROPAGADO p
    ) x
    WHERE RN = 1;

    -- ========================================================================
    -- 4. RELLENO DE HUECOS
    -- ------------------------------------------------------------------------
    -- Sustituye a SEIS tablas y TRES CTE recursivas del original:
    -- DATAHUB_NULL_FIN_MES + TEMP_AUXILIAR_FIN_MES,
    -- DATAHUB_NULL_INTERMEDIO + TEMP_AUXILIAR_INTERMEDIO,
    -- DATAHUB_NULL_INICIO_MES + TEMP_AUXILIAR_INICIO_MES, mas los tres INSERT
    -- con NOT EXISTS que las volcaban.
    --
    -- POR QUE SE PUEDE HACER EN UNA SOLA PASADA
    --
    -- Las tres CTE recursivas resolvian el mismo problema en tres trozos:
    -- generar los dias que faltan al FINAL del mes, EN MEDIO, y al PRINCIPIO.
    -- Pero "los dias que faltan" es simplemente el calendario del mes menos los
    -- dias que si estan. Un LEFT JOIN contra el calendario lo da entero, sin
    -- recursion y sin distinguir casos.
    --
    -- Lo unico que cambia entre los tres casos es DE DONDE salen los atributos
    -- de la fila generada, y eso se resuelve con dos funciones de ventana:
    --
    --     ANCLA_PREV   ultima fecha con datos <= este dia   (huecos y fin de mes)
    --     ANCLA_NEXT   primera fecha con datos >= este dia  (principio de mes)
    --     ANCLA        COALESCE(ANCLA_PREV, ANCLA_NEXT)
    --
    -- El COALESCE reproduce exactamente el criterio del original: si hay algo
    -- antes se hereda de atras, y si no -porque el hueco esta antes del primer
    -- registro del mes- se hereda de adelante.
    --
    -- Los IMPORTES de las filas generadas van a CERO, igual que en el original.
    -- Solo se heredan los atributos.
    -- ========================================================================
    DROP TABLE IF EXISTS TMP_SO_COMPLETO;

    CREATE TEMPORARY TABLE TMP_SO_COMPLETO AS
    WITH LLAVES AS (
        -- Cada (operacion, rubro) con el mes que hay que completar y sus
        -- limites reales. Los limites importan: se completa el mes entero solo
        -- DENTRO de la vida de la operacion en ese mes, no antes de que
        -- existiera ni despues de que se cancelara.
        SELECT
            ID_OPERACION_CIERRE,
            COD_RUBRO,
            YEAR(FECHA_PROCESO)  AS ANIO,
            MONTH(FECHA_PROCESO) AS MES,
            MIN(FECHA_PROCESO)   AS PRIMERA_FECHA,
            MAX(FECHA_PROCESO)   AS ULTIMA_FECHA
        FROM TMP_SO_DIARIO
        GROUP BY ID_OPERACION_CIERRE, COD_RUBRO,
                 YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
    ),
    ESPINA AS (
        -- Todos los dias del mes de cada llave, acotados al rango de proceso.
        -- Aqui es donde aparecen los dias que faltaban, sin recursion.
        SELECT
            k.ID_OPERACION_CIERRE,
            k.COD_RUBRO,
            k.ANIO,
            k.MES,
            c.FEC_CALENDARIO AS FECHA_PROCESO,
            c.IND_DIA_HABIL,
            k.PRIMERA_FECHA,
            k.ULTIMA_FECHA
        FROM LLAVES k
        JOIN BDS_CALENDARIOS c
          ON c.COD_CALENDARIO = 1
         AND YEAR(c.FEC_CALENDARIO)  = k.ANIO
         AND MONTH(c.FEC_CALENDARIO) = k.MES
         AND c.FEC_CALENDARIO <= V_FECHA_FIN
    ),
    CON_ANCLA AS (
        SELECT
            e.*,
            d.ID_OPERACION_CIERRE AS EXISTE,
            -- Ultima fecha CON DATOS hasta este dia, mirando hacia atras.
            MAX(CASE WHEN d.ID_OPERACION_CIERRE IS NOT NULL
                      AND d.TIPO_ORIGEN = 'ORIGINAL'
                     THEN e.FECHA_PROCESO END)
                OVER (PARTITION BY e.ID_OPERACION_CIERRE, e.COD_RUBRO, e.ANIO, e.MES
                      ORDER BY e.FECHA_PROCESO
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ANCLA_PREV,
            -- Primera fecha CON DATOS desde este dia, mirando hacia adelante.
            -- Solo se usa cuando no hay nada detras.
            MIN(CASE WHEN d.ID_OPERACION_CIERRE IS NOT NULL
                      AND d.TIPO_ORIGEN = 'ORIGINAL'
                     THEN e.FECHA_PROCESO END)
                OVER (PARTITION BY e.ID_OPERACION_CIERRE, e.COD_RUBRO, e.ANIO, e.MES
                      ORDER BY e.FECHA_PROCESO
                      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ANCLA_NEXT
        FROM ESPINA e
        LEFT JOIN TMP_SO_DIARIO d
               ON d.ID_OPERACION_CIERRE = e.ID_OPERACION_CIERRE
              AND d.COD_RUBRO           = e.COD_RUBRO
              AND d.FECHA_PROCESO       = e.FECHA_PROCESO
    )
    -- Las filas que SI existen, tal cual
    SELECT
        ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
        COD_RUBRO, COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION,
        COD_SUB_OPERACION, COD_TIPO_OPERACION, COD_MODULO, FEC_VENCIMIENTO,
        FEC_VALOR, IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
        MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
        MTO_INTERES, MTO_PREVISIONES,
        IND_DIA_HABIL, FUENTE, FECHA_CARGA, BATCH_ID
    FROM TMP_SO_DIARIO

    UNION ALL

    -- Las que faltaban: atributos del ancla, importes a cero
    SELECT
        a.ID_OPERACION_CIERRE, a.FECHA_PROCESO,
        b.COD_EMPRESA, b.COD_SUCURSAL, a.COD_RUBRO, b.COD_MONEDA,
        b.COD_PAPEL, b.NUM_CUENTA_BT, b.COD_OPERACION, b.COD_SUB_OPERACION,
        b.COD_TIPO_OPERACION, b.COD_MODULO, b.FEC_VENCIMIENTO, b.FEC_VALOR,
        b.IND_CATEGORIA_RIESGO, b.COD_ACTI_BCO_CENTRAL, b.COD_PRODUCTO,
        0, 0, 0, 0, 0, 0,
        a.IND_DIA_HABIL, b.FUENTE, b.FECHA_CARGA, b.BATCH_ID
    FROM CON_ANCLA a
    JOIN TMP_SO_DIARIO b
      ON b.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
     AND b.COD_RUBRO           = a.COD_RUBRO
     AND b.FECHA_PROCESO       = COALESCE(a.ANCLA_PREV, a.ANCLA_NEXT)
    WHERE a.EXISTE IS NULL
      AND COALESCE(a.ANCLA_PREV, a.ANCLA_NEXT) IS NOT NULL;

    -- ========================================================================
    -- 5. LIMPIAR EL RANGO DESTINO
    -- ------------------------------------------------------------------------
    -- Va DESPUES de haber construido las temporales, no antes. Asi, si algo
    -- falla en los pasos 3 o 4, el destino sigue con los datos de la corrida
    -- anterior en vez de quedarse vacio.
    --
    -- En el original este DELETE estaba comentado (linea 1377) mientras el
    -- INSERT no lo estaba: relanzar el proceso duplicaba todo el rango.
    -- ========================================================================
    DELETE FROM BDS_SALDOS_OPERATIVOS
     WHERE FECHA_PROCESO BETWEEN V_FECHA_INICIO_MES AND V_FECHA_FIN;

    -- ========================================================================
    -- 6. CLASIFICAR, AGREGAR Y ESCRIBIR
    -- ------------------------------------------------------------------------
    -- Sustituye a DIEZ tablas del original: TABLA_FINAL_SIN_NOMBRE,
    -- SIN_DUPLICADO, TABLA_FINAL_SIN_NOMBRE_PASIVOS, SIN_DUPLICADO_PASIVOS,
    -- UNION_PASIVOS_ACTIVOS, UNION_PASIVOS_ACTIVOS_SIN_DUPLICADO,
    -- UNION_PASIVOS_ACTIVOS_DEPURADA, TEMPORAL_TABLE, TABLA_1_HAMER y
    -- TABLA_2_HAMER_V2.
    --
    -- COD_EJECUTIVO se resuelve aqui con un LEFT JOIN. El original lo dejaba en
    -- NULL y lo arreglaba con un UPDATE final sobre toda la tabla de salida.
    -- ========================================================================
    INSERT INTO BDS_SALDOS_OPERATIVOS (
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
    WITH CLASIFICADO AS (
        -- Activos y pasivos en UNA pasada, con prioridad explicita.
        --
        -- El original hacia dos SELECT y un UNION ALL, y una fila que cumpliera
        -- los dos criterios salia DUPLICADA: una vez con los buckets calculados
        -- y otra con ceros. Cual sobrevivia al dedupe dependia del orden de
        -- lectura del motor.
        --
        -- Aqui el CASE decide: si es de activos, se trata como activo. Los
        -- buckets son CASE sobre la propia fila, no cinco LEFT JOIN, porque
        -- -como se explica arriba- eso es lo que los joins del original
        -- calculaban en realidad.
        SELECT
            c.*,
            CASE WHEN LEFT(c.COD_RUBRO, 4) IN (
                     '1401','1403','1404','1405','1406',
                     '1411','1413','1414','1415','1416',
                     '1421','1423','1424','1425','1426')
                 THEN 1 ELSE 0 END AS ES_ACTIVO
        FROM TMP_SO_COMPLETO c
        WHERE LEFT(c.COD_RUBRO, 4) IN (
                  '1401','1403','1404','1405','1406',
                  '1411','1413','1414','1415','1416',
                  '1421','1423','1424','1425','1426')
           OR c.COD_MODULO IN (20, 21, 22, 120, 155, 162, 184, 185, 321)
    ),
    CON_BUCKETS AS (
        SELECT
            k.*,
            -- '_' es comodin de un caracter en LIKE: '14_1' casa 1401, 1411, 1421
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_1' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_VIGENTE_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_3' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_RESTRUCTURADO_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_4' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_REFINANCIADO_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_5' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_VENCIDO_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_6' THEN MTO_SALDO_MO ELSE 0 END AS MTO_SALDO_JUDIAL_MO,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_1' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_VIGENTE_MN,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_3' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_RESTRUCTURADO_MN,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_4' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_REFINANCIADO_MN,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_5' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_VENCIDO_MN,
            CASE WHEN ES_ACTIVO = 1 AND SUBSTR(COD_RUBRO,1,4) LIKE '14_6' THEN MTO_SALDO_MN ELSE 0 END AS MTO_SALDO_JUDICIAL_MN
        FROM CLASIFICADO k
    ),
    POR_DIA AS (
        -- Un rubro es un detalle contable; la salida es por OPERACION y dia.
        -- Aqui se suman los rubros de cada operacion.
        SELECT
            ID_OPERACION_CIERRE,
            FECHA_PROCESO,
            DAY(FECHA_PROCESO)            AS NUM_DIA_MES,
            DAY(LAST_DAY(FECHA_PROCESO))  AS TOTAL_DIAS_MES,
            SUM(MTO_SALDO_ORIGEN)               AS TOTAL_DIA_SALDO_ORIGEN,
            SUM(MTO_SALDO_MN)                   AS TOTAL_DIA_SALDO_MN,
            SUM(MTO_SALDO_ME)                   AS TOTAL_DIA_SALDO_ME,
            SUM(MTO_SALDO_MO)                   AS TOTAL_DIA_SALDO_MO,
            SUM(MTO_INTERES)                    AS TOTAL_DIA_INTERES,
            SUM(MTO_PREVISIONES)                AS TOTAL_DIA_PREVISIONES,
            SUM(MTO_SALDO_VIGENTE_MO)           AS TOTAL_DIA_SALDO_VIGENTE_MO,
            SUM(MTO_SALDO_RESTRUCTURADO_MO)     AS TOTAL_DIA_SALDO_RESTRUCTURADO_MO,
            SUM(MTO_SALDO_REFINANCIADO_MO)      AS TOTAL_DIA_SALDO_REFINANCIADO_MO,
            SUM(MTO_SALDO_VENCIDO_MO)           AS TOTAL_DIA_SALDO_VENCIDO_MO,
            SUM(MTO_SALDO_JUDIAL_MO)            AS TOTAL_DIA_SALDO_JUDIAL_MO,
            SUM(MTO_SALDO_VIGENTE_MN)           AS TOTAL_DIA_SALDO_VIGENTE_MN,
            SUM(MTO_SALDO_RESTRUCTURADO_MN)     AS TOTAL_DIA_SALDO_RESTRUCTURADO_MN,
            SUM(MTO_SALDO_REFINANCIADO_MN)      AS TOTAL_DIA_SALDO_REFINANCIADO_MN,
            SUM(MTO_SALDO_VENCIDO_MN)           AS TOTAL_DIA_SALDO_VENCIDO_MN,
            SUM(MTO_SALDO_JUDICIAL_MN)          AS TOTAL_DIA_SALDO_JUDICIAL_MN
        FROM CON_BUCKETS
        GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO
    ),
    PROMEDIOS AS (
        -- Promedio ACUMULADO del mes: suma corrida hasta el dia, dividida entre
        -- el numero de dia. El dia 15 promedia 15 dias, no el mes entero.
        --
        -- La expresion se deja EXACTAMENTE con la forma del original
        -- -dividir y luego CAST, no al reves- para que los decimales salgan
        -- identicos y el script de comparacion pueda exigir igualdad estricta
        -- en vez de una tolerancia. Si algun dia las columnas MTO_* pasaran a
        -- ser enteras, aqui habria que castear ANTES de dividir: division
        -- entera entre entero trunca, y el promedio saldria sistematicamente
        -- por debajo sin que nada avise.
        SELECT
            p.*,
            CAST(SUM(TOTAL_DIA_SALDO_ORIGEN)             OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_ORIGEN,
            CAST(SUM(TOTAL_DIA_SALDO_MN)                 OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_MN,
            CAST(SUM(TOTAL_DIA_SALDO_ME)                 OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_ME,
            CAST(SUM(TOTAL_DIA_SALDO_MO)                 OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_MO,
            CAST(SUM(TOTAL_DIA_INTERES)                  OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_INTERES,
            CAST(SUM(TOTAL_DIA_PREVISIONES)              OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_PREVISIONES,
            CAST(SUM(TOTAL_DIA_SALDO_VIGENTE_MO)         OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_VIGENTE_MO,
            CAST(SUM(TOTAL_DIA_SALDO_RESTRUCTURADO_MO)   OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_RESTRUCTURADO_MO,
            CAST(SUM(TOTAL_DIA_SALDO_REFINANCIADO_MO)    OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_REFINANCIADO_MO,
            CAST(SUM(TOTAL_DIA_SALDO_VENCIDO_MO)         OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_VENCIDO_MO,
            CAST(SUM(TOTAL_DIA_SALDO_JUDIAL_MO)          OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_JUDIAL_MO,
            CAST(SUM(TOTAL_DIA_SALDO_VIGENTE_MN)         OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_VIGENTE_MN,
            CAST(SUM(TOTAL_DIA_SALDO_RESTRUCTURADO_MN)   OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_RESTRUCTURADO_MN,
            CAST(SUM(TOTAL_DIA_SALDO_REFINANCIADO_MN)    OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_REFINANCIADO_MN,
            CAST(SUM(TOTAL_DIA_SALDO_VENCIDO_MN)         OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_VENCIDO_MN,
            CAST(SUM(TOTAL_DIA_SALDO_JUDICIAL_MN)        OVER W / NUM_DIA_MES AS DECIMAL(18,4)) AS AVG_SALDO_JUDICIAL_MN
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
        -- agregar, que en SingleStore devuelve una fila ARBITRARIA del grupo.
        -- Aqui el desempate es explicito para que la salida sea reproducible.
        SELECT * FROM (
            SELECT
                ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
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
    SELECT
        a.ID_OPERACION_CIERRE, a.FECHA_PROCESO, a.COD_EMPRESA, a.COD_SUCURSAL,
        a.COD_MONEDA, a.COD_PAPEL, a.NUM_CUENTA_BT, a.COD_OPERACION,
        a.COD_SUB_OPERACION, a.COD_TIPO_OPERACION, a.COD_MODULO,
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
    LEFT JOIN BDS_SALDOS_CIERRE sc
      ON sc.ID_OPERACION  = a.ID_OPERACION_CIERRE
     AND sc.FECHA_PROCESO = a.FECHA_PROCESO
    WHERE a.FECHA_PROCESO BETWEEN V_FECHA_INICIO_MES AND V_FECHA_FIN;

    -- ========================================================================
    -- 7. LIMPIEZA
    -- ------------------------------------------------------------------------
    -- Las TEMPORARY desaparecen solas al cerrar la sesion, pero una sesion de
    -- Airflow puede reutilizarse para varias tareas. Soltarlas aqui libera la
    -- memoria sin esperar.
    -- ========================================================================
    DROP TABLE IF EXISTS TMP_SO_DIARIO;
    DROP TABLE IF EXISTS TMP_SO_COMPLETO;

END //

DELIMITER ;
