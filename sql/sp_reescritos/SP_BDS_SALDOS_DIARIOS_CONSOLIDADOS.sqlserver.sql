-- ============================================================================
--  SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS   ::   SQL Server 2022
-- ----------------------------------------------------------------------------
--  Puerto a T-SQL de la version reescrita. Misma logica, mismo orden de pasos
--  y mismos nombres de CTE que SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS.singlestore.sql.
--
--  LEA AQUELLA CABECERA ANTES DE DESPLEGAR ESTA. Explica los tres errores que
--  impedian ejecutar el original, y sobre todo las DOS DECISIONES que hubo que
--  tomar y que hacen que esta version NO sea equivalente a la anterior:
--
--     A. el INSERT final solo metia los dias NO habiles; aqui se meten los dos
--     B. el filtro ES_RUBRO_6006 no filtraba nada; aqui se quita
--
--  EQUIVALENCIAS
--      LAST_DAY(d)                     ->  EOMONTH(d)
--      DATE_SUB(d, INTERVAL n DAY)     ->  DATEADD(day, -n, d)
--      DATE_ADD(d, INTERVAL 1 DAY)     ->  DATEADD(day, 1, d)
--      NOW(6)                          ->  SYSDATETIME()
--      LAST_INSERT_ID()                ->  SCOPE_IDENTITY()
--      ROW_COUNT()                     ->  @@ROWCOUNT
--      EXCEPTION WHEN OTHERS THEN      ->  BEGIN TRY / BEGIN CATCH
--      exception_message()             ->  ERROR_MESSAGE()
--      CREATE TEMPORARY TABLE t AS     ->  SELECT ... INTO #t
--      RAISE USER_EXCEPTION(msg)       ->  THROW 50000, @msg, 1
--
--  EJECUCION
--      EXEC dbo.SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS '2026-07-01', 45;
-- ============================================================================

USE GNBPE_DATAHUB;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE dbo.SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS
    @P_FECHA_PROCESO DATE,
    @P_NUMERO_DIAS   INT = 45
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @V_NOM_PROCESO        VARCHAR(200) = 'SP_BDS_SALDOS_DIARIOS_CONSOLIDADOS',
            @V_TABLA_ORIGEN       VARCHAR(200) = 'ODS_SALDOS_DIARIOS_CONSOLIDADOS',
            @V_TABLA_DESTINO      VARCHAR(200) = 'BDS_SALDOS_DIARIOS_CONSOLIDADOS',
            @V_ID_LOG             BIGINT = 0,
            @V_FILAS              BIGINT = 0,
            @V_MSG_ERROR          NVARCHAR(MAX) = NULL,
            @V_DIAS               INT,
            @V_FECHA_DESDE        DATE,   -- ventana de LECTURA
            @V_FECHA_DESDE_SALIDA DATE,   -- ventana de ESCRITURA
            @V_CIERRE_SIGUIENTE   DATE,
            @V_MSG                NVARCHAR(2000);

    SET @V_DIAS = COALESCE(@P_NUMERO_DIAS, 45);

    IF @P_FECHA_PROCESO IS NULL
        THROW 50020, 'P_FECHA_PROCESO es obligatoria.', 1;

    IF @V_DIAS <= 0
    BEGIN
        SET @V_MSG = CONCAT('P_NUMERO_DIAS debe ser positivo y llego ', @V_DIAS, '.');
        THROW 50021, @V_MSG, 1;
    END

    -- Se LEE 30 dias mas atras de lo que se ESCRIBE. No es margen de seguridad:
    -- el promedio acumulado y la variacion mensual necesitan el mes anterior
    -- completo, y sin el las primeras filas del rango saldrian con el promedio
    -- calculado sobre medio mes.
    SET @V_FECHA_DESDE        = DATEADD(day, -(@V_DIAS + 30), @P_FECHA_PROCESO);
    SET @V_FECHA_DESDE_SALIDA = DATEADD(day, -@V_DIAS,        @P_FECHA_PROCESO);

    INSERT INTO dbo.ctl_log_proceso (nom_proceso, tabla_origen, tabla_destino, fec_inicio, estado)
    VALUES (@V_NOM_PROCESO, @V_TABLA_ORIGEN, @V_TABLA_DESTINO, SYSDATETIME(), 'EJECUTANDO');
    SET @V_ID_LOG = SCOPE_IDENTITY();

    BEGIN TRY

        SELECT @V_CIERRE_SIGUIENTE = MIN(FEC_CALENDARIO)
          FROM dbo.BDS_CALENDARIOS
         WHERE COD_CALENDARIO = 1
           AND FEC_CALENDARIO > @P_FECHA_PROCESO
           AND IND_DIA_HABIL  = 'S';

        IF @V_CIERRE_SIGUIENTE IS NULL
        BEGIN
            SET @V_MSG = CONCAT(
                'BDS_CALENDARIOS no tiene ningun dia habil despues de ',
                CONVERT(varchar(10), @P_FECHA_PROCESO, 23),
                '. Sin eso no se puede proyectar hasta el proximo cierre.');
            THROW 50022, @V_MSG, 1;
        END

        -- ====================================================================
        -- PASO 1. LA BASE: ODS + DIAS NO HABILES + PROYECCION
        -- ====================================================================
        DROP TABLE IF EXISTS #SDC_BASE;

        WITH ORIGEN AS (
            SELECT FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO,
                   COD_MONEDA, COD_PAPEL, COD_MODULO, COD_TITULO, COD_CAPITULO,
                   COD_PLAZO, COD_GRUPO, MTO_SALDO_MO, MTO_SALDO_ME,
                   MTO_SALDO_MN, BATCH_ID
            FROM dbo.ODS_SALDOS_DIARIOS_CONSOLIDADOS
            WHERE FECHA_PROCESO > @V_FECHA_DESDE
        ),
        DIAS_NO_HABILES AS (
            -- Cada dia NO habil del rango con el ultimo dia habil que lo precede.
            -- OUTER APPLY en vez de subconsulta correlacionada: mismo resultado,
            -- pero resuelto con un TOP 1 y un seek sobre el indice del calendario.
            SELECT nh.FEC_CALENDARIO AS DIA_NO_HABIL, h.DIA_HABIL_ANTERIOR
            FROM dbo.BDS_CALENDARIOS nh
            OUTER APPLY (
                SELECT TOP (1) c.FEC_CALENDARIO AS DIA_HABIL_ANTERIOR
                FROM dbo.BDS_CALENDARIOS c
                WHERE c.COD_CALENDARIO = 1 AND c.IND_DIA_HABIL = 'S'
                  AND c.FEC_CALENDARIO < nh.FEC_CALENDARIO
                ORDER BY c.FEC_CALENDARIO DESC
            ) h
            WHERE nh.COD_CALENDARIO = 1
              AND nh.IND_DIA_HABIL  = 'N'
              AND nh.FEC_CALENDARIO >= @V_FECHA_DESDE
              AND nh.FEC_CALENDARIO <= @P_FECHA_PROCESO
        ),
        UNIVERSO_PROYECCION AS (
            SELECT FEC_CALENDARIO
            FROM dbo.BDS_CALENDARIOS
            WHERE COD_CALENDARIO = 1
              AND FEC_CALENDARIO > @P_FECHA_PROCESO
              AND FEC_CALENDARIO < @V_CIERRE_SIGUIENTE
        ),
        BASE AS (
            -- RAMA 1: lo que ya existe en ODS
            SELECT * FROM ORIGEN

            UNION ALL

            -- RAMA 2: dias no habiles, COPIANDO los importes del dia habil
            -- anterior, solo para las llaves sin fila propia ese dia.
            SELECT d.DIA_NO_HABIL, o.COD_EMPRESA, o.COD_SUCURSAL, o.COD_RUBRO,
                   o.COD_MONEDA, o.COD_PAPEL, o.COD_MODULO, o.COD_TITULO,
                   o.COD_CAPITULO, o.COD_PLAZO, o.COD_GRUPO, o.MTO_SALDO_MO,
                   o.MTO_SALDO_ME, o.MTO_SALDO_MN, o.BATCH_ID
            FROM DIAS_NO_HABILES d
            JOIN dbo.ODS_SALDOS_DIARIOS_CONSOLIDADOS o
              ON o.FECHA_PROCESO = d.DIA_HABIL_ANTERIOR
            LEFT JOIN dbo.ODS_SALDOS_DIARIOS_CONSOLIDADOS ya
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
            CROSS JOIN dbo.ODS_SALDOS_DIARIOS_CONSOLIDADOS o
            WHERE o.FECHA_PROCESO = @P_FECHA_PROCESO
        )
        SELECT * INTO #SDC_BASE FROM BASE;

        CREATE CLUSTERED COLUMNSTORE INDEX CCI_SDC_BASE ON #SDC_BASE;

        -- ====================================================================
        -- PASO 2. LOS TRES RELLENOS A CERO
        -- --------------------------------------------------------------------
        -- Las tres ramas ponen importes a CERO y heredan solo los atributos.
        -- No se fusionan en una porque rellenan periodos distintos y dos miran
        -- solo dias habiles mientras la primera mira todos.
        -- ====================================================================
        DROP TABLE IF EXISTS #SDC_COMPLETO;

        WITH DETALLE AS (
            -- Una fila representativa por (llave corta, dia). Se conserva el
            -- orden de desempate exacto del original para no cambiar que fila
            -- gana: TITULO, SUCURSAL, CAPITULO, PLAZO, GRUPO.
            SELECT * FROM (
                SELECT b.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO,
                                        COD_MONEDA, COD_PAPEL, COD_MODULO,
                                        FECHA_PROCESO
                           ORDER BY COD_TITULO, COD_SUCURSAL, COD_CAPITULO,
                                    COD_PLAZO, COD_GRUPO
                       ) AS RN
                FROM #SDC_BASE b
            ) x WHERE RN = 1
        ),
        ULTIMO_REAL AS (
            SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
                   COD_MODULO, MAX(FECHA_PROCESO) AS FEC_ULTIMO_REAL
            FROM dbo.ODS_SALDOS_DIARIOS_CONSOLIDADOS
            WHERE FECHA_PROCESO > @V_FECHA_DESDE
              AND COD_RUBRO <> 0
            GROUP BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
                     COD_PAPEL, COD_MODULO,
                     YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
            HAVING MAX(FECHA_PROCESO) <> EOMONTH(MAX(FECHA_PROCESO))
        ),
        CIERRE_CON_SALDO AS (
            SELECT * FROM #SDC_BASE
            WHERE FECHA_PROCESO = EOMONTH(FECHA_PROCESO)
              AND MTO_SALDO_MO <> 0
        ),
        MESES_PRESENTES AS (
            SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
                   COD_MODULO,
                   YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES
            FROM #SDC_BASE
            GROUP BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
                     COD_PAPEL, COD_MODULO,
                     YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
        ),
        LLAVES_MES AS (
            -- Llave LARGA (10 columnas). El original cambia aqui de la corta a
            -- la larga; se conserva, pero vea la nota de la cabecera.
            SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
                   COD_MODULO, COD_TITULO, COD_CAPITULO, COD_PLAZO, COD_GRUPO,
                   YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
                   MAX(BATCH_ID) AS BATCH_ID
            FROM #SDC_BASE
            GROUP BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
                     COD_PAPEL, COD_MODULO, COD_TITULO, COD_CAPITULO,
                     COD_PLAZO, COD_GRUPO,
                     YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
        ),
        COMPLETO AS (
            SELECT * FROM #SDC_BASE

            UNION ALL

            -- RELLENO A: del ultimo dia real hasta fin de mes, a CERO.
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
            JOIN dbo.BDS_CALENDARIOS cal
              ON cal.COD_CALENDARIO = 1
             AND cal.FEC_CALENDARIO >  u.FEC_ULTIMO_REAL
             AND cal.FEC_CALENDARIO <= EOMONTH(u.FEC_ULTIMO_REAL)
            LEFT JOIN #SDC_BASE ya
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
                   ON m.COD_EMPRESA = c.COD_EMPRESA  AND m.COD_SUCURSAL = c.COD_SUCURSAL
                  AND m.COD_RUBRO   = c.COD_RUBRO    AND m.COD_MONEDA   = c.COD_MONEDA
                  AND m.COD_PAPEL   = c.COD_PAPEL    AND m.COD_MODULO   = c.COD_MODULO
                  AND m.ANIO = YEAR(DATEADD(day, 1, c.FECHA_PROCESO))
                  AND m.MES  = MONTH(DATEADD(day, 1, c.FECHA_PROCESO))
            JOIN dbo.BDS_CALENDARIOS cal
              ON cal.COD_CALENDARIO = 1
             AND cal.IND_DIA_HABIL  = 'S'
             AND cal.FEC_CALENDARIO >  c.FECHA_PROCESO
             AND cal.FEC_CALENDARIO <= EOMONTH(DATEADD(day, 1, c.FECHA_PROCESO))
            WHERE m.COD_EMPRESA IS NULL

            UNION ALL

            -- RELLENO C: dias HABILES del mes sin fila, a CERO. Llave larga.
            SELECT cal.FEC_CALENDARIO, l.COD_EMPRESA, l.COD_SUCURSAL, l.COD_RUBRO,
                   l.COD_MONEDA, l.COD_PAPEL, l.COD_MODULO, l.COD_TITULO,
                   l.COD_CAPITULO, l.COD_PLAZO, l.COD_GRUPO,
                   CAST(0 AS DECIMAL(17,2)), CAST(0 AS DECIMAL(17,2)),
                   CAST(0 AS DECIMAL(17,2)), l.BATCH_ID
            FROM LLAVES_MES l
            JOIN dbo.BDS_CALENDARIOS cal
              ON cal.COD_CALENDARIO = 1
             AND cal.IND_DIA_HABIL  = 'S'
             AND YEAR(cal.FEC_CALENDARIO)  = l.ANIO
             AND MONTH(cal.FEC_CALENDARIO) = l.MES
            LEFT JOIN #SDC_BASE ya
                   ON ya.COD_EMPRESA   = l.COD_EMPRESA  AND ya.COD_SUCURSAL = l.COD_SUCURSAL
                  AND ya.COD_RUBRO     = l.COD_RUBRO    AND ya.COD_MONEDA   = l.COD_MONEDA
                  AND ya.COD_PAPEL     = l.COD_PAPEL    AND ya.COD_MODULO   = l.COD_MODULO
                  AND ya.COD_TITULO    = l.COD_TITULO   AND ya.COD_CAPITULO = l.COD_CAPITULO
                  AND ya.COD_PLAZO     = l.COD_PLAZO    AND ya.COD_GRUPO    = l.COD_GRUPO
                  AND ya.FECHA_PROCESO = cal.FEC_CALENDARIO
            WHERE ya.FECHA_PROCESO IS NULL
        )
        SELECT * INTO #SDC_COMPLETO FROM COMPLETO;

        CREATE CLUSTERED COLUMNSTORE INDEX CCI_SDC_COMPLETO ON #SDC_COMPLETO;

        -- ====================================================================
        -- PASO 3 y 4. REESCRIBIR EL RANGO, EN UNA TRANSACCION
        -- --------------------------------------------------------------------
        -- El DELETE estaba comentado en el original y el INSERT no: reejecutar
        -- duplicaba todo. Va aqui, con las temporales ya construidas, para que
        -- un fallo anterior deje el destino con la corrida previa intacta.
        --
        -- Y aqui se insertan LOS DOS conjuntos: los dias habiles (que el
        -- original calculaba y tiraba) y las copias de dia no habil.
        -- ====================================================================
        BEGIN TRANSACTION;

        DELETE FROM dbo.BDS_SALDOS_DIARIOS_CONSOLIDADOS
         WHERE FECHA_PROCESO >  @V_FECHA_DESDE_SALIDA
           AND FECHA_PROCESO <= @P_FECHA_PROCESO;

        WITH CIERRE_MES AS (
            -- Saldo del ultimo dia de cada mes, por llave larga: el punto de
            -- partida de la variacion mensual.
            SELECT b.COD_EMPRESA, b.COD_SUCURSAL, b.COD_RUBRO, b.COD_MONEDA,
                   b.COD_PAPEL, b.COD_MODULO, b.COD_TITULO, b.COD_CAPITULO,
                   b.COD_PLAZO, b.COD_GRUPO,
                   YEAR(b.FECHA_PROCESO)  AS ANIO,
                   MONTH(b.FECHA_PROCESO) AS MES,
                   b.MTO_SALDO_MO, b.MTO_SALDO_ME, b.MTO_SALDO_MN
            FROM #SDC_COMPLETO b
            JOIN (
                SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
                       COD_PAPEL, COD_MODULO, COD_TITULO, COD_CAPITULO,
                       COD_PLAZO, COD_GRUPO,
                       YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
                       MAX(FECHA_PROCESO) AS FEC_ULTIMO
                FROM #SDC_COMPLETO
                GROUP BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
                         COD_PAPEL, COD_MODULO, COD_TITULO, COD_CAPITULO,
                         COD_PLAZO, COD_GRUPO,
                         YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
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

                -- Variacion contra el cierre del mes anterior. En enero se toma
                -- el saldo tal cual: la serie arranca de nuevo con el anio. Es
                -- regla de negocio del original y se conserva.
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
            FROM #SDC_COMPLETO b
            -- Solo se une para meses distintos de enero: en enero el CASE de
            -- arriba ignora el valor. El original hacia el join con el salto de
            -- anio incluido y despues no lo usaba.
            LEFT JOIN CIERRE_MES cm
                   ON MONTH(b.FECHA_PROCESO) <> 1
                  AND cm.COD_EMPRESA = b.COD_EMPRESA  AND cm.COD_SUCURSAL = b.COD_SUCURSAL
                  AND cm.COD_RUBRO   = b.COD_RUBRO    AND cm.COD_MONEDA   = b.COD_MONEDA
                  AND cm.COD_PAPEL   = b.COD_PAPEL    AND cm.COD_MODULO   = b.COD_MODULO
                  AND cm.COD_TITULO  = b.COD_TITULO   AND cm.COD_CAPITULO = b.COD_CAPITULO
                  AND cm.COD_PLAZO   = b.COD_PLAZO    AND cm.COD_GRUPO    = b.COD_GRUPO
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
            WHERE FECHA_PROCESO >  @V_FECHA_DESDE_SALIDA
              AND FECHA_PROCESO <= @P_FECHA_PROCESO
        ),
        COPIA_NO_HABIL AS (
            -- Copias de dia no habil con las columnas derivadas ya calculadas.
            -- Es lo que el original hacia en TEMP_3, pero ACOTANDO el
            -- calendario: alli no se filtraba COD_CALENDARIO en ninguna de las
            -- dos referencias, asi que con mas de un calendario cargado cada
            -- dia no habil se multiplicaba.
            SELECT nh.FEC_CALENDARIO AS FECHA_PROCESO,
                   h.COD_EMPRESA, h.COD_SUCURSAL, h.COD_RUBRO, h.COD_MONEDA,
                   h.COD_PAPEL, h.COD_MODULO, h.COD_TITULO, h.COD_CAPITULO,
                   h.COD_PLAZO, h.COD_GRUPO,
                   h.MTO_SALDO_MO, h.MTO_SALDO_ME, h.MTO_SALDO_MN,
                   h.MTO_SALDO_MES_MN, h.MTO_SALDO_MES_ME, h.MTO_SALDO_MES_MO,
                   h.MTO_AVG_MES_MO, h.MTO_AVG_MES_ME, h.MTO_AVG_MES_MN,
                   h.BATCH_ID
            FROM dbo.BDS_CALENDARIOS nh
            CROSS APPLY (
                SELECT TOP (1) c.FEC_CALENDARIO
                FROM dbo.BDS_CALENDARIOS c
                WHERE c.COD_CALENDARIO = 1 AND c.IND_DIA_HABIL = 'S'
                  AND c.FEC_CALENDARIO < nh.FEC_CALENDARIO
                ORDER BY c.FEC_CALENDARIO DESC
            ) prev
            JOIN EN_RANGO h ON h.FECHA_PROCESO = prev.FEC_CALENDARIO
            LEFT JOIN EN_RANGO ya
                   ON ya.FECHA_PROCESO = nh.FEC_CALENDARIO
                  AND ya.COD_EMPRESA   = h.COD_EMPRESA  AND ya.COD_SUCURSAL = h.COD_SUCURSAL
                  AND ya.COD_RUBRO     = h.COD_RUBRO    AND ya.COD_MONEDA   = h.COD_MONEDA
                  AND ya.COD_PAPEL     = h.COD_PAPEL    AND ya.COD_MODULO   = h.COD_MODULO
                  AND ya.COD_TITULO    = h.COD_TITULO   AND ya.COD_CAPITULO = h.COD_CAPITULO
                  AND ya.COD_PLAZO     = h.COD_PLAZO    AND ya.COD_GRUPO    = h.COD_GRUPO
            WHERE nh.COD_CALENDARIO = 1
              AND nh.IND_DIA_HABIL <> 'S'
              AND nh.FEC_CALENDARIO >  @V_FECHA_DESDE_SALIDA
              AND nh.FEC_CALENDARIO <= @P_FECHA_PROCESO
              AND ya.COD_RUBRO IS NULL
        )
        INSERT INTO dbo.BDS_SALDOS_DIARIOS_CONSOLIDADOS (
            FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA,
            COD_PAPEL, COD_MODULO, COD_TITULO, COD_CAPITULO, COD_PLAZO,
            COD_GRUPO, MTO_SALDO_MO, MTO_SALDO_ME, MTO_SALDO_MN,
            MTO_SALDO_MES_MN, MTO_SALDO_MES_ME, MTO_SALDO_MES_MO,
            MTO_AVG_MES_MO, MTO_AVG_MES_ME, MTO_AVG_MES_MN, BATCH_ID
        )
        SELECT * FROM EN_RANGO
        UNION ALL
        SELECT * FROM COPIA_NO_HABIL;

        -- @@ROWCOUNT se lee AQUI, pegado al INSERT. En el original estaba
        -- despues de cinco DROP TABLE, asi que la bitacora guardaba lo que
        -- devolvia un DROP y nunca las filas insertadas.
        SET @V_FILAS = @@ROWCOUNT;

        COMMIT TRANSACTION;

        DROP TABLE IF EXISTS #SDC_BASE;
        DROP TABLE IF EXISTS #SDC_COMPLETO;

        IF @V_FILAS = 0
            SET @V_MSG_ERROR = 'No se insertaron registros en BDS_SALDOS_DIARIOS_CONSOLIDADOS';

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @V_MSG_ERROR = CONCAT('[BDS_SALDOS_DIARIOS_CONSOLIDADOS] ', ERROR_MESSAGE());
    END CATCH

    UPDATE dbo.ctl_log_proceso
       SET estado = CASE
                        WHEN @V_MSG_ERROR IS NULL                  THEN 'TERMINADO'
                        WHEN @V_MSG_ERROR LIKE 'No se insertaron%' THEN 'WARNING'
                        ELSE 'ERROR'
                    END,
           fec_termino      = SYSDATETIME(),
           filas_insertadas = @V_FILAS,
           msg_error        = @V_MSG_ERROR
     WHERE id_log = @V_ID_LOG;

    IF (@V_MSG_ERROR IS NOT NULL AND @V_MSG_ERROR NOT LIKE 'No se insertaron%')
    BEGIN
        SET @V_MSG = CONCAT('Error al cargar la tabla: ', LEFT(@V_MSG_ERROR, 1800));
        THROW 50023, @V_MSG, 1;
    END
END
GO
