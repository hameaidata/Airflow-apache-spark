-- ============================================================================
--  SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS   ::   SingleStore
-- ----------------------------------------------------------------------------
--  Reescritura de SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS.sql (que por dentro se
--  declaraba SP_BDS_SDC_TEST_V3).
--
--      5 tablas reales creadas y dropeadas   ->   2 TEMPORARY
--      721 lineas                            ->   ~470
--
-- ============================================================================
--  ATENCION: ESTA VERSION NO ES EQUIVALENTE A LA ANTERIOR, A PROPOSITO
-- ----------------------------------------------------------------------------
--  El original tenia tres errores que le impedian ejecutarse, y dos de ellos
--  no se pueden "traducir": hay que decidir que se queria hacer. Lo decidido,
--  confirmado con el area:
--
--   A. LA TABLA FINAL SOLO RECIBIA LOS DIAS NO HABILES
--
--      La etapa 4 calculaba TEMP_2 con TODO el resultado. La etapa 5 calculaba
--      TEMP_3 con "WHERE cnh.IND_DIA_HABIL <> 'S'", o sea solo los dias no
--      habiles que faltaban. Y el INSERT final era:
--
--          INSERT INTO BDS_SALDOS_DIARIOS_CONSOLIDADOS (...)
--          SELECT DISTINCT * FROM BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_3;
--
--      TEMP_2 se calculaba, servia de origen a TEMP_3, y se tiraba. Los dias
--      habiles -el grueso de los datos- nunca llegaban al destino.
--
--      AQUI SE INSERTAN LOS DOS: habiles y no habiles.
--
--   B. EL FILTRO ES_RUBRO_6006 NO FILTRABA NADA
--
--      Se llamaba asi pero decia:
--          COD_RUBRO LIKE '1%' OR '2%' OR '3%' OR ... OR '9%'
--      es decir, cualquier rubro que empiece por un digito del 1 al 9. Y
--      despues un "WHERE ES_RUBRO_6006 = 1" que por tanto no descartaba nada
--      salvo los rubros que empezaran por '0'.
--
--      AQUI SE QUITA. Dos calculos menos sobre toda la tabla y una columna
--      menos que induce a error al leer el codigo. Si algun dia hace falta
--      filtrar de verdad por el rubro 6006, el sitio es el WHERE del paso 4 y
--      la condicion es COD_RUBRO LIKE '6006%'.
--
--   C. TEMP_3 LEIA COLUMNAS QUE TEMP_2 NO TENIA
--
--      Pedia rh.FUENTE y rh.FEC_CARGA, y el SELECT que creaba TEMP_2 producia
--      21 columnas, ninguna con esos nombres. Ademas TEMP_3 acababa con 23
--      columnas y el INSERT listaba 21, con un "SELECT DISTINCT *". Tres
--      formas distintas de fallar en la misma sentencia.
--
--      AQUI las columnas van nombradas una por una en el INSERT, que es lo que
--      convierte ese fallo en un error de compilacion en vez de en una
--      sorpresa en produccion.
--
-- ============================================================================
--  OTROS DEFECTOS CORREGIDOS
-- ----------------------------------------------------------------------------
--   1. NO ERA IDEMPOTENTE. El DELETE del rango estaba comentado (lineas 28-30)
--      y el INSERT no. Reejecutar duplicaba.
--
--   2. CREATE TABLE TEMP_2 SIN DROP DELANTE. A la segunda corrida moria con
--      "table already exists".
--
--   3. v_filas = ROW_COUNT() SE EJECUTABA DESPUES DE CINCO "DROP TABLE", asi
--      que la bitacora registraba lo que devolvio un DROP, nunca el INSERT.
--      Aqui se captura justo despues del INSERT.
--
--   4. TEMP_3 NO FILTRABA POR COD_CALENDARIO. Recorria BDS_CALENDARIOS entero
--      sin acotar el calendario ni en cnh ni en ch. Con mas de un calendario
--      cargado, cada dia no habil se multiplicaba por el numero de calendarios.
--
--   5. TABLAS REALES, NO TEMPORALES. Dos corridas simultaneas escribian sobre
--      las mismas cinco tablas.
--
--   6. LA JUNTA DE VARIACION MENSUAL TRAIA DICIEMBRE PARA NADA. El LEFT JOIN
--      calculaba el mes anterior incluyendo el salto de anio, pero el CASE de
--      arriba ignora ese valor cuando el mes es enero. Se conserva la regla de
--      negocio -enero no se compara- y se quita el trabajo inutil.
--
-- ============================================================================
--  QUE HACE EL PROCESO
-- ----------------------------------------------------------------------------
--  Toma los saldos consolidados diarios de ODS y produce una serie DIARIA
--  COMPLETA, con la variacion contra el cierre del mes anterior y el promedio
--  acumulado del mes. Cuatro rellenos distintos, y conviene no confundirlos
--  porque rellenan cosas distintas y con criterios distintos:
--
--   1. DIA NO HABIL         copia los IMPORTES del ultimo dia habil
--   2. PROYECCION           copia el dia de proceso hacia los dias siguientes
--                           hasta el proximo cierre
--   3. HASTA FIN DE MES     genera dias con importe CERO desde el ultimo dia
--                           real hasta el fin de mes
--   4. MES SIGUIENTE        si una llave tenia saldo a fin de mes y no aparece
--                           el mes siguiente, genera sus dias habiles a CERO
--   5. INTRAMES             dias habiles del mes sin fila, a CERO
--
--  Los rellenos 1 y 2 COPIAN importes; los 3, 4 y 5 los ponen a CERO. Esa es
--  la diferencia que hay que respetar al tocar este codigo.
--
-- ============================================================================
--  DOS CLAVES DISTINTAS, Y NO ES UN DESCUIDO MIO
-- ----------------------------------------------------------------------------
--  El original usa DOS claves de emparejamiento segun la etapa:
--
--      LLAVE CORTA (6)   EMPRESA, SUCURSAL, RUBRO, MONEDA, PAPEL, MODULO
--      LLAVE LARGA (10)  ... + TITULO, CAPITULO, PLAZO, GRUPO
--
--  Etapas 1 y 2 usan la corta; etapas 3 (intrames) y 4 usan la larga. Se
--  conserva tal cual para no cambiar resultados, pero es una inconsistencia
--  real: si dos filas comparten la llave corta y difieren en la larga, el
--  relleno de la etapa 2 elige una de las dos con ROW_NUMBER y la etapa 4 las
--  trata como distintas. Vale la pena revisarlo con el area de datos.
--
-- ============================================================================
--  EJECUCION
--      CALL SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS('2026-07-01', 45);
-- ============================================================================

DELIMITER //

CREATE OR REPLACE PROCEDURE `DATAHUB`.`SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS`(
    P_FECHA_PROCESO DATE NULL,
    P_NUMERO_DIAS   INT  NULL
) RETURNS void AS

DECLARE
    V_NOM_PROCESO         VARCHAR(200) = 'SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS';
    V_TABLA_ORIGEN        VARCHAR(200) = 'ODS_SALDOS_DIARIOS_CONSOLIDADOS';
    V_TABLA_DESTINO       VARCHAR(200) = 'BDS_SALDOS_DIARIOS_CONSOLIDADOS';
    V_ID_LOG              BIGINT = 0;
    V_FILAS               BIGINT = 0;
    V_MSG_ERROR           TEXT   = NULL;
    V_DIAS                INT;
    V_FECHA_DESDE         DATE;   -- inicio de la ventana de LECTURA
    V_FECHA_DESDE_SALIDA  DATE;   -- inicio de la ventana de ESCRITURA
    V_CIERRE_SIGUIENTE    DATE;

BEGIN

    V_DIAS = COALESCE(P_NUMERO_DIAS, 45);

    IF P_FECHA_PROCESO IS NULL THEN
        RAISE USER_EXCEPTION('P_FECHA_PROCESO es obligatoria.');
    END IF;
    IF V_DIAS <= 0 THEN
        RAISE USER_EXCEPTION(CONCAT('P_NUMERO_DIAS debe ser positivo y llego ', V_DIAS, '.'));
    END IF;

    -- Se LEE 30 dias mas atras de lo que se ESCRIBE. No es un margen de
    -- seguridad: el promedio acumulado y la variacion mensual necesitan el mes
    -- anterior completo, y sin el las primeras filas del rango saldrian con un
    -- promedio calculado sobre medio mes.
    V_FECHA_DESDE        = DATE_SUB(P_FECHA_PROCESO, INTERVAL V_DIAS + 30 DAY);
    V_FECHA_DESDE_SALIDA = DATE_SUB(P_FECHA_PROCESO, INTERVAL V_DIAS DAY);

    INSERT INTO ctl_log_proceso (nom_proceso, tabla_origen, tabla_destino, fec_inicio, estado)
    VALUES (V_NOM_PROCESO, V_TABLA_ORIGEN, V_TABLA_DESTINO, NOW(6), 'EJECUTANDO');
    V_ID_LOG = LAST_INSERT_ID();

    BEGIN

        SELECT MIN(FEC_CALENDARIO) INTO V_CIERRE_SIGUIENTE
          FROM BDS_CALENDARIOS
         WHERE COD_CALENDARIO = 1
           AND FEC_CALENDARIO > P_FECHA_PROCESO
           AND IND_DIA_HABIL  = 'S';

        IF V_CIERRE_SIGUIENTE IS NULL THEN
            RAISE USER_EXCEPTION(CONCAT(
                'BDS_CALENDARIOS no tiene ningun dia habil despues de ', P_FECHA_PROCESO,
                '. Sin eso no se puede proyectar hasta el proximo cierre. ',
                'Carga el calendario mas alla de esa fecha.'));
        END IF;

        -- ====================================================================
        -- PASO 1. LA BASE: ODS + DIAS NO HABILES + PROYECCION
        -- --------------------------------------------------------------------
        -- Sustituye a BASE_UNION_INICIO. Tres ramas, las mismas que el
        -- original, pero sin materializar nada por el camino.
        -- ====================================================================
        DROP TABLE IF EXISTS TMP_SDC_BASE;

        CREATE TEMPORARY TABLE TMP_SDC_BASE AS
        WITH ORIGEN AS (
            SELECT FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO,
                   COD_MONEDA, COD_PAPEL, COD_MODULO, COD_TITULO, COD_CAPITULO,
                   COD_PLAZO, COD_GRUPO, MTO_SALDO_MO, MTO_SALDO_ME,
                   MTO_SALDO_MN, BATCH_ID
            FROM ODS_SALDOS_DIARIOS_CONSOLIDADOS
            WHERE FECHA_PROCESO > V_FECHA_DESDE
        ),
        -- Cada dia NO habil del rango con el ultimo dia habil que lo precede.
        DIAS_NO_HABILES AS (
            SELECT nh.FEC_CALENDARIO AS DIA_NO_HABIL,
                   (SELECT MAX(h.FEC_CALENDARIO)
                      FROM BDS_CALENDARIOS h
                     WHERE h.COD_CALENDARIO = 1
                       AND h.IND_DIA_HABIL  = 'S'
                       AND h.FEC_CALENDARIO < nh.FEC_CALENDARIO) AS DIA_HABIL_ANTERIOR
            FROM BDS_CALENDARIOS nh
            WHERE nh.COD_CALENDARIO = 1
              AND nh.IND_DIA_HABIL  = 'N'
              AND nh.FEC_CALENDARIO >= V_FECHA_DESDE
              AND nh.FEC_CALENDARIO <= P_FECHA_PROCESO
        ),
        -- Dias posteriores al proceso y anteriores al proximo cierre habil.
        UNIVERSO_PROYECCION AS (
            SELECT FEC_CALENDARIO
            FROM BDS_CALENDARIOS
            WHERE COD_CALENDARIO = 1
              AND FEC_CALENDARIO > P_FECHA_PROCESO
              AND FEC_CALENDARIO < V_CIERRE_SIGUIENTE
        )

        -- RAMA 1: lo que ya existe en ODS
        SELECT * FROM ORIGEN

        UNION ALL

        -- RAMA 2: dias no habiles, COPIANDO los importes del dia habil anterior,
        -- solo para las llaves que no tengan ya una fila propia ese dia.
        SELECT d.DIA_NO_HABIL, o.COD_EMPRESA, o.COD_SUCURSAL, o.COD_RUBRO,
               o.COD_MONEDA, o.COD_PAPEL, o.COD_MODULO, o.COD_TITULO,
               o.COD_CAPITULO, o.COD_PLAZO, o.COD_GRUPO, o.MTO_SALDO_MO,
               o.MTO_SALDO_ME, o.MTO_SALDO_MN, o.BATCH_ID
        FROM DIAS_NO_HABILES d
        JOIN ODS_SALDOS_DIARIOS_CONSOLIDADOS o
          ON o.FECHA_PROCESO = d.DIA_HABIL_ANTERIOR
        LEFT JOIN ODS_SALDOS_DIARIOS_CONSOLIDADOS ya
               ON ya.FECHA_PROCESO = d.DIA_NO_HABIL
              AND ya.COD_EMPRESA   = o.COD_EMPRESA
              AND ya.COD_SUCURSAL  = o.COD_SUCURSAL
              AND ya.COD_RUBRO     = o.COD_RUBRO
              AND ya.COD_MONEDA    = o.COD_MONEDA
              AND ya.COD_PAPEL     = o.COD_PAPEL
              AND ya.COD_MODULO    = o.COD_MODULO
        WHERE ya.COD_EMPRESA IS NULL

        UNION ALL

        -- RAMA 3: proyeccion del dia de proceso hacia adelante, hasta el
        -- proximo cierre. Los importes se COPIAN tal cual.
        SELECT u.FEC_CALENDARIO, o.COD_EMPRESA, o.COD_SUCURSAL, o.COD_RUBRO,
               o.COD_MONEDA, o.COD_PAPEL, o.COD_MODULO, o.COD_TITULO,
               o.COD_CAPITULO, o.COD_PLAZO, o.COD_GRUPO, o.MTO_SALDO_MO,
               o.MTO_SALDO_ME, o.MTO_SALDO_MN, o.BATCH_ID
        FROM UNIVERSO_PROYECCION u
        CROSS JOIN ODS_SALDOS_DIARIOS_CONSOLIDADOS o
        WHERE o.FECHA_PROCESO = P_FECHA_PROCESO;

        -- ====================================================================
        -- PASO 2. LOS TRES RELLENOS A CERO
        -- --------------------------------------------------------------------
        -- Sustituye a BASE_UNION_COMPLETADA y BASE_UNION_FINAL_2, o sea a dos
        -- tablas materializadas y nueve CTE.
        --
        -- Las tres ramas ponen importes a CERO y heredan solo los atributos.
        -- No se pueden fusionar en una: rellenan periodos distintos y dos de
        -- ellas miran solo dias habiles mientras la primera mira todos.
        -- ====================================================================
        DROP TABLE IF EXISTS TMP_SDC_COMPLETO;

        CREATE TEMPORARY TABLE TMP_SDC_COMPLETO AS
        WITH
        -- Una fila representativa por (llave corta, dia). El original lo hacia
        -- con ROW_NUMBER ordenando por TITULO, SUCURSAL, CAPITULO, PLAZO,
        -- GRUPO; se conserva ese orden exacto para no cambiar que fila gana.
        DETALLE AS (
            SELECT * FROM (
                SELECT b.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO,
                                        COD_MONEDA, COD_PAPEL, COD_MODULO,
                                        FECHA_PROCESO
                           ORDER BY COD_TITULO, COD_SUCURSAL, COD_CAPITULO,
                                    COD_PLAZO, COD_GRUPO
                       ) AS RN
                FROM TMP_SDC_BASE b
            ) x WHERE RN = 1
        ),
        -- Ultimo dia con dato de cada (llave corta, mes) que NO cierra en fin
        -- de mes. COD_RUBRO != 0 es del original.
        ULTIMO_REAL AS (
            SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
                   COD_MODULO,
                   MAX(FECHA_PROCESO) AS FEC_ULTIMO_REAL
            FROM ODS_SALDOS_DIARIOS_CONSOLIDADOS
            WHERE FECHA_PROCESO > V_FECHA_DESDE
              AND COD_RUBRO != 0
            GROUP BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
                     COD_PAPEL, COD_MODULO,
                     YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
            HAVING MAX(FECHA_PROCESO) <> LAST_DAY(MAX(FECHA_PROCESO))
        ),
        -- Llaves con saldo distinto de cero el ultimo dia del mes.
        CIERRE_CON_SALDO AS (
            SELECT * FROM TMP_SDC_BASE
            WHERE FECHA_PROCESO = LAST_DAY(FECHA_PROCESO)
              AND MTO_SALDO_MO <> 0
        ),
        MESES_PRESENTES AS (
            SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
                   COD_MODULO,
                   YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES
            FROM TMP_SDC_BASE
            GROUP BY 1,2,3,4,5,6,7,8
        ),
        -- Llaves de la etapa larga (10 columnas) por mes, para el relleno
        -- intrames. Ojo: aqui el original cambia de llave corta a larga.
        LLAVES_MES AS (
            SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
                   COD_MODULO, COD_TITULO, COD_CAPITULO, COD_PLAZO, COD_GRUPO,
                   YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
                   MAX(BATCH_ID) AS BATCH_ID
            FROM TMP_SDC_BASE
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
        )

        SELECT * FROM TMP_SDC_BASE

        UNION ALL

        -- RELLENO A: desde el ultimo dia real hasta fin de mes, a CERO.
        -- TODOS los dias del calendario, habiles o no.
        SELECT cal.FEC_CALENDARIO, d.COD_EMPRESA, d.COD_SUCURSAL, d.COD_RUBRO,
               d.COD_MONEDA, d.COD_PAPEL, d.COD_MODULO, d.COD_TITULO,
               d.COD_CAPITULO, d.COD_PLAZO, d.COD_GRUPO,
               CAST(0 AS DECIMAL(17,2)), CAST(0 AS DECIMAL(17,2)),
               CAST(0 AS DECIMAL(17,2)), d.BATCH_ID
        FROM ULTIMO_REAL u
        JOIN DETALLE d
          ON d.COD_EMPRESA   = u.COD_EMPRESA  AND d.COD_SUCURSAL = u.COD_SUCURSAL
         AND d.COD_RUBRO     = u.COD_RUBRO    AND d.COD_MONEDA   = u.COD_MONEDA
         AND d.COD_PAPEL     = u.COD_PAPEL    AND d.COD_MODULO   = u.COD_MODULO
         AND d.FECHA_PROCESO = u.FEC_ULTIMO_REAL
        JOIN BDS_CALENDARIOS cal
          ON cal.COD_CALENDARIO = 1
         AND cal.FEC_CALENDARIO >  u.FEC_ULTIMO_REAL
         AND cal.FEC_CALENDARIO <= LAST_DAY(u.FEC_ULTIMO_REAL)
        LEFT JOIN TMP_SDC_BASE ya
               ON ya.COD_EMPRESA   = u.COD_EMPRESA  AND ya.COD_SUCURSAL = u.COD_SUCURSAL
              AND ya.COD_RUBRO     = u.COD_RUBRO    AND ya.COD_MONEDA   = u.COD_MONEDA
              AND ya.COD_PAPEL     = u.COD_PAPEL    AND ya.COD_MODULO   = u.COD_MODULO
              AND ya.FECHA_PROCESO = cal.FEC_CALENDARIO
        WHERE ya.COD_EMPRESA IS NULL

        UNION ALL

        -- RELLENO B: la llave tenia saldo a fin de mes y no aparece el mes
        -- siguiente. Se generan sus dias HABILES del mes siguiente, a CERO.
        SELECT cal.FEC_CALENDARIO, c.COD_EMPRESA, c.COD_SUCURSAL, c.COD_RUBRO,
               c.COD_MONEDA, c.COD_PAPEL, c.COD_MODULO, c.COD_TITULO,
               c.COD_CAPITULO, c.COD_PLAZO, c.COD_GRUPO,
               CAST(0 AS DECIMAL(17,2)), CAST(0 AS DECIMAL(17,2)),
               CAST(0 AS DECIMAL(17,2)), c.BATCH_ID
        FROM CIERRE_CON_SALDO c
        LEFT JOIN MESES_PRESENTES m
               ON m.COD_EMPRESA  = c.COD_EMPRESA  AND m.COD_SUCURSAL = c.COD_SUCURSAL
              AND m.COD_RUBRO    = c.COD_RUBRO    AND m.COD_MONEDA   = c.COD_MONEDA
              AND m.COD_PAPEL    = c.COD_PAPEL    AND m.COD_MODULO   = c.COD_MODULO
              AND m.ANIO = YEAR(DATE_ADD(c.FECHA_PROCESO, INTERVAL 1 DAY))
              AND m.MES  = MONTH(DATE_ADD(c.FECHA_PROCESO, INTERVAL 1 DAY))
        JOIN BDS_CALENDARIOS cal
          ON cal.COD_CALENDARIO = 1
         AND cal.IND_DIA_HABIL  = 'S'
         AND cal.FEC_CALENDARIO >  c.FECHA_PROCESO
         AND cal.FEC_CALENDARIO <= LAST_DAY(DATE_ADD(c.FECHA_PROCESO, INTERVAL 1 DAY))
        WHERE m.COD_EMPRESA IS NULL

        UNION ALL

        -- RELLENO C: dias HABILES del mes sin fila, a CERO. Llave larga.
        SELECT cal.FEC_CALENDARIO, l.COD_EMPRESA, l.COD_SUCURSAL, l.COD_RUBRO,
               l.COD_MONEDA, l.COD_PAPEL, l.COD_MODULO, l.COD_TITULO,
               l.COD_CAPITULO, l.COD_PLAZO, l.COD_GRUPO,
               CAST(0 AS DECIMAL(17,2)), CAST(0 AS DECIMAL(17,2)),
               CAST(0 AS DECIMAL(17,2)), l.BATCH_ID
        FROM LLAVES_MES l
        JOIN BDS_CALENDARIOS cal
          ON cal.COD_CALENDARIO = 1
         AND cal.IND_DIA_HABIL  = 'S'
         AND YEAR(cal.FEC_CALENDARIO)  = l.ANIO
         AND MONTH(cal.FEC_CALENDARIO) = l.MES
        LEFT JOIN TMP_SDC_BASE ya
               ON ya.COD_EMPRESA   = l.COD_EMPRESA  AND ya.COD_SUCURSAL = l.COD_SUCURSAL
              AND ya.COD_RUBRO     = l.COD_RUBRO    AND ya.COD_MONEDA   = l.COD_MONEDA
              AND ya.COD_PAPEL     = l.COD_PAPEL    AND ya.COD_MODULO   = l.COD_MODULO
              AND ya.COD_TITULO    = l.COD_TITULO   AND ya.COD_CAPITULO = l.COD_CAPITULO
              AND ya.COD_PLAZO     = l.COD_PLAZO    AND ya.COD_GRUPO    = l.COD_GRUPO
              AND ya.FECHA_PROCESO = cal.FEC_CALENDARIO
        WHERE ya.FECHA_PROCESO IS NULL;

        -- ====================================================================
        -- PASO 3. LIMPIAR EL RANGO DESTINO
        -- --------------------------------------------------------------------
        -- Estaba comentado en el original y el INSERT no: reejecutar duplicaba
        -- todo el rango. Va aqui, cuando las temporales ya estan construidas:
        -- si algo hubiera fallado antes, el destino conserva la corrida previa.
        -- ====================================================================
        DELETE FROM BDS_SALDOS_DIARIOS_CONSOLIDADOS
         WHERE FECHA_PROCESO >  V_FECHA_DESDE_SALIDA
           AND FECHA_PROCESO <= P_FECHA_PROCESO;

        -- ====================================================================
        -- PASO 4. CALCULAR Y ESCRIBIR
        -- --------------------------------------------------------------------
        -- Sustituye a TEMP_2 y TEMP_3. Aqui se insertan LOS DOS conjuntos:
        -- los dias habiles (que el original calculaba y tiraba) y las copias
        -- de dia no habil.
        -- ====================================================================
        INSERT INTO BDS_SALDOS_DIARIOS_CONSOLIDADOS (
            FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
            COD_PAPEL, COD_MODULO, COD_TITULO, COD_CAPITULO, COD_PLAZO,
            COD_GRUPO, MTO_SALDO_MO, MTO_SALDO_ME, MTO_SALDO_MN,
            MTO_SALDO_MES_MN, MTO_SALDO_MES_ME, MTO_SALDO_MES_MO,
            MTO_AVG_MES_MO, MTO_AVG_MES_ME, MTO_AVG_MES_MN, BATCH_ID
        )
        WITH
        -- Saldo del ultimo dia de cada mes, por llave larga. Es el punto de
        -- partida de la variacion mensual.
        CIERRE_MES AS (
            SELECT b.COD_EMPRESA, b.COD_SUCURSAL, b.COD_RUBRO, b.COD_MONEDA,
                   b.COD_PAPEL, b.COD_MODULO, b.COD_TITULO, b.COD_CAPITULO,
                   b.COD_PLAZO, b.COD_GRUPO,
                   YEAR(b.FECHA_PROCESO)  AS ANIO,
                   MONTH(b.FECHA_PROCESO) AS MES,
                   b.MTO_SALDO_MO, b.MTO_SALDO_ME, b.MTO_SALDO_MN
            FROM TMP_SDC_COMPLETO b
            JOIN (
                SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
                       COD_PAPEL, COD_MODULO, COD_TITULO, COD_CAPITULO,
                       COD_PLAZO, COD_GRUPO,
                       YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
                       MAX(FECHA_PROCESO) AS FEC_ULTIMO
                FROM TMP_SDC_COMPLETO
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
            ) u
              ON u.COD_EMPRESA = b.COD_EMPRESA  AND u.COD_SUCURSAL = b.COD_SUCURSAL
             AND u.COD_RUBRO   = b.COD_RUBRO    AND u.COD_MONEDA   = b.COD_MONEDA
             AND u.COD_PAPEL   = b.COD_PAPEL    AND u.COD_MODULO   = b.COD_MODULO
             AND u.COD_TITULO  = b.COD_TITULO   AND u.COD_CAPITULO = b.COD_CAPITULO
             AND u.COD_PLAZO   = b.COD_PLAZO    AND u.COD_GRUPO    = b.COD_GRUPO
             AND u.FEC_ULTIMO  = b.FECHA_PROCESO
        ),
        CALCULADO AS (
            SELECT
                b.FECHA_PROCESO, b.COD_EMPRESA, b.COD_SUCURSAL, b.COD_RUBRO,
                b.COD_MONEDA, b.COD_PAPEL, b.COD_MODULO, b.COD_TITULO,
                b.COD_CAPITULO, b.COD_PLAZO, b.COD_GRUPO,
                b.MTO_SALDO_MO, b.MTO_SALDO_ME, b.MTO_SALDO_MN,

                -- Variacion contra el cierre del mes anterior. En enero se
                -- toma el saldo tal cual: la serie arranca de nuevo con el
                -- anio. Es regla de negocio del original y se conserva.
                CASE WHEN MONTH(b.FECHA_PROCESO) = 1
                     THEN CAST(b.MTO_SALDO_MN AS DECIMAL(17,2))
                     ELSE CAST(b.MTO_SALDO_MN - COALESCE(cm.MTO_SALDO_MN, 0) AS DECIMAL(17,2))
                END AS MTO_SALDO_MES_MN,
                CASE WHEN MONTH(b.FECHA_PROCESO) = 1
                     THEN CAST(b.MTO_SALDO_ME AS DECIMAL(17,2))
                     ELSE CAST(b.MTO_SALDO_ME - COALESCE(cm.MTO_SALDO_ME, 0) AS DECIMAL(17,2))
                END AS MTO_SALDO_MES_ME,
                CASE WHEN MONTH(b.FECHA_PROCESO) = 1
                     THEN CAST(b.MTO_SALDO_MO AS DECIMAL(17,2))
                     ELSE CAST(b.MTO_SALDO_MO - COALESCE(cm.MTO_SALDO_MO, 0) AS DECIMAL(17,2))
                END AS MTO_SALDO_MES_MO,

                -- Promedio ACUMULADO del mes. Cuando el saldo del dia es cero,
                -- el promedio se fuerza a cero: tambien es regla del original.
                CASE WHEN b.MTO_SALDO_MO = 0 THEN CAST(0 AS DECIMAL(17,4))
                     ELSE CAST(AVG(b.MTO_SALDO_MO) OVER W AS DECIMAL(17,4)) END AS MTO_AVG_MES_MO,
                CASE WHEN b.MTO_SALDO_ME = 0 THEN CAST(0 AS DECIMAL(17,4))
                     ELSE CAST(AVG(b.MTO_SALDO_ME) OVER W AS DECIMAL(17,4)) END AS MTO_AVG_MES_ME,
                CASE WHEN b.MTO_SALDO_MN = 0 THEN CAST(0 AS DECIMAL(17,4))
                     ELSE CAST(AVG(b.MTO_SALDO_MN) OVER W AS DECIMAL(17,4)) END AS MTO_AVG_MES_MN,

                b.BATCH_ID
            FROM TMP_SDC_COMPLETO b
            -- Solo se une para meses distintos de enero. En enero el CASE de
            -- arriba ignora el valor, asi que traerlo era trabajo perdido: el
            -- original hacia el join con el salto de anio incluido para nada.
            LEFT JOIN CIERRE_MES cm
                   ON MONTH(b.FECHA_PROCESO) <> 1
                  AND cm.COD_EMPRESA  = b.COD_EMPRESA  AND cm.COD_SUCURSAL = b.COD_SUCURSAL
                  AND cm.COD_RUBRO    = b.COD_RUBRO    AND cm.COD_MONEDA   = b.COD_MONEDA
                  AND cm.COD_PAPEL    = b.COD_PAPEL    AND cm.COD_MODULO   = b.COD_MODULO
                  AND cm.COD_TITULO   = b.COD_TITULO   AND cm.COD_CAPITULO = b.COD_CAPITULO
                  AND cm.COD_PLAZO    = b.COD_PLAZO    AND cm.COD_GRUPO    = b.COD_GRUPO
                  AND cm.ANIO = YEAR(b.FECHA_PROCESO)
                  AND cm.MES  = MONTH(b.FECHA_PROCESO) - 1
            WINDOW W AS (
                PARTITION BY b.COD_EMPRESA, b.COD_SUCURSAL, b.COD_RUBRO,
                             b.COD_MONEDA, b.COD_PAPEL, b.COD_MODULO,
                             b.COD_TITULO, b.COD_CAPITULO, b.COD_PLAZO,
                             b.COD_GRUPO,
                             YEAR(b.FECHA_PROCESO), MONTH(b.FECHA_PROCESO)
                ORDER BY b.FECHA_PROCESO
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ),
        EN_RANGO AS (
            SELECT * FROM CALCULADO
            WHERE FECHA_PROCESO >  V_FECHA_DESDE_SALIDA
              AND FECHA_PROCESO <= P_FECHA_PROCESO
        ),
        -- Copias de dia no habil, ya con las columnas derivadas calculadas.
        -- Es lo que el original hacia en TEMP_3, pero acotando el calendario
        -- (el original no filtraba COD_CALENDARIO en ninguna de las dos
        -- referencias, asi que con mas de un calendario multiplicaba filas).
        COPIA_NO_HABIL AS (
            SELECT nh.FEC_CALENDARIO AS FECHA_PROCESO,
                   h.COD_EMPRESA, h.COD_SUCURSAL, h.COD_RUBRO, h.COD_MONEDA,
                   h.COD_PAPEL, h.COD_MODULO, h.COD_TITULO, h.COD_CAPITULO,
                   h.COD_PLAZO, h.COD_GRUPO,
                   h.MTO_SALDO_MO, h.MTO_SALDO_ME, h.MTO_SALDO_MN,
                   h.MTO_SALDO_MES_MN, h.MTO_SALDO_MES_ME, h.MTO_SALDO_MES_MO,
                   h.MTO_AVG_MES_MO, h.MTO_AVG_MES_ME, h.MTO_AVG_MES_MN,
                   h.BATCH_ID
            FROM BDS_CALENDARIOS nh
            JOIN EN_RANGO h
              ON h.FECHA_PROCESO = (
                     SELECT MAX(c.FEC_CALENDARIO)
                       FROM BDS_CALENDARIOS c
                      WHERE c.COD_CALENDARIO = 1
                        AND c.IND_DIA_HABIL  = 'S'
                        AND c.FEC_CALENDARIO < nh.FEC_CALENDARIO)
            LEFT JOIN EN_RANGO ya
                   ON ya.FECHA_PROCESO = nh.FEC_CALENDARIO
                  AND ya.COD_EMPRESA   = h.COD_EMPRESA  AND ya.COD_SUCURSAL = h.COD_SUCURSAL
                  AND ya.COD_RUBRO     = h.COD_RUBRO    AND ya.COD_MONEDA   = h.COD_MONEDA
                  AND ya.COD_PAPEL     = h.COD_PAPEL    AND ya.COD_MODULO   = h.COD_MODULO
                  AND ya.COD_TITULO    = h.COD_TITULO   AND ya.COD_CAPITULO = h.COD_CAPITULO
                  AND ya.COD_PLAZO     = h.COD_PLAZO    AND ya.COD_GRUPO    = h.COD_GRUPO
            WHERE nh.COD_CALENDARIO = 1
              AND nh.IND_DIA_HABIL <> 'S'
              AND nh.FEC_CALENDARIO >  V_FECHA_DESDE_SALIDA
              AND nh.FEC_CALENDARIO <= P_FECHA_PROCESO
              AND ya.COD_RUBRO IS NULL
        )
        SELECT * FROM EN_RANGO
        UNION ALL
        SELECT * FROM COPIA_NO_HABIL;

        -- ROW_COUNT() se lee AQUI, pegado al INSERT. En el original estaba
        -- despues de cinco DROP TABLE, asi que la bitacora guardaba lo que
        -- devolvia un DROP y nunca las filas insertadas.
        V_FILAS = ROW_COUNT();

        DROP TABLE IF EXISTS TMP_SDC_BASE;
        DROP TABLE IF EXISTS TMP_SDC_COMPLETO;

        IF V_FILAS = 0 THEN
            V_MSG_ERROR = 'No se insertaron registros en BDS_SALDOS_DIARIOS_CONSOLIDADOS';
        END IF;

    EXCEPTION WHEN OTHERS THEN
        V_MSG_ERROR = CONCAT('[BDS_SALDOS_DIARIOS_CONSOLIDADOS] ', exception_message());
    END;

    UPDATE ctl_log_proceso
       SET estado = CASE
                        WHEN V_MSG_ERROR IS NULL                  THEN 'TERMINADO'
                        WHEN V_MSG_ERROR LIKE 'No se insertaron%' THEN 'WARNING'
                        ELSE 'ERROR'
                    END,
           fec_termino      = NOW(6),
           filas_insertadas = V_FILAS,
           msg_error        = V_MSG_ERROR
     WHERE id_log = V_ID_LOG;

    IF (V_MSG_ERROR IS NOT NULL AND V_MSG_ERROR NOT LIKE 'No se insertaron%') THEN
        RAISE USER_EXCEPTION(CONCAT('Error al cargar la tabla: ', V_MSG_ERROR));
    END IF;

END //

DELIMITER ;
