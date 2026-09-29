DELIMITER //
 
CREATE OR REPLACE PROCEDURE SP_BDS_SDC_TEST_V3(v_fecha_proceso DATE, v_numero_dias INT)
AS
DECLARE
    v_nom_proceso              VARCHAR(200) = 'SP_BDS_SDC_TEST_V3';
    v_tabla_origen             VARCHAR(200);
    v_tabla_destino            VARCHAR(200);
    v_id_log                   BIGINT = 0;
    v_filas                    BIGINT = 0;
    v_msg_error                TEXT = NULL;
    -- v_fecha_proceso                DATE;
    v_fe_nueva_comple          DATE;
    v_fe_cierre_siguiente      DATE;
    v_numero_dias_act          INT;
 
BEGIN
 
    -- ==========================================
    -- LIMPIEZA PREVIA
    -- ==========================================
 
    -- SELECT FECHA_PROCESO INTO v_fecha_proceso FROM ODS_PARAMETROS_CIERRE;
    -- SELECT FEC_APERTURA INTO v_fe_cierre_siguiente FROM ODS_PARAMETROS_CIERRE;

    v_numero_dias_act = COALESCE(v_numero_dias, 45);

    --DELETE FROM BDS_SALDOS_DIARIOS_CONSOLIDADOS
    -- WHERE FECHA_PROCESO <= v_fecha_proceso 
    -- AND FECHA_PROCESO > DATE_SUB(v_fecha_proceso, INTERVAL v_numero_dias_act DAY);
 
 
    -- *****************************************************************
    -- CARGA BDS_SALDOS_DIARIOS_CONSOLIDADOS
    -- *****************************************************************
    v_tabla_origen  = 'ODS_SALDOS_DIARIOS_CONSOLIDADOS';
    v_tabla_destino = 'BDS_SALDOS_DIARIOS_CONSOLIDADOS';
    v_msg_error     = NULL;
 
 
    INSERT INTO ctl_log_proceso (nom_proceso, tabla_origen, tabla_destino, fec_inicio, estado)
    VALUES (v_nom_proceso, v_tabla_origen, v_tabla_destino, NOW(6), 'INICIADO');
 
 
    v_id_log = LAST_INSERT_ID();
    UPDATE ctl_log_proceso
       SET estado = 'EJECUTANDO'
     WHERE id_log = v_id_log;
 
 
    BEGIN

        -- SELECT DATE_SUB(v_fecha_proceso, INTERVAL 59 DAY) INTO v_fe_nueva_comple;

        SELECT MIN(FEC_CALENDARIO) INTO v_fe_cierre_siguiente FROM BDS_CALENDARIOS A 
        WHERE A.COD_CALENDARIO = 1
            AND A.FEC_CALENDARIO > v_fecha_proceso
            AND A.IND_DIA_HABIL = 'S';

 
        DROP TABLE IF EXISTS BASE_UNION_INICIO;
 
        CREATE TABLE BASE_UNION_INICIO AS
        WITH BASE_1 AS (
            SELECT
                A.FECHA_PROCESO,
                A.COD_EMPRESA,
                A.COD_SUCURSAL,
                A.COD_RUBRO,
                A.COD_MONEDA,
                A.COD_PAPEL,
                A.COD_MODULO,
                A.COD_TITULO,
                A.COD_CAPITULO,
                A.COD_PLAZO,
                A.COD_GRUPO,
                A.MTO_SALDO_MO,
                A.MTO_SALDO_ME,
                A.MTO_SALDO_MN,
                A.BATCH_ID
            FROM ODS_SALDOS_DIARIOS_CONSOLIDADOS A
            WHERE A.FECHA_PROCESO > DATE_SUB(v_fecha_proceso, INTERVAL v_numero_dias_act + 30 DAY)
        ),
 
        DIAS_FALTANTES AS (
            SELECT MAX(A.FEC_CALENDARIO) AS DIA_HABIL_ANTERIOR, B.FEC_CALENDARIO AS DIA_NO_HABIL
            FROM BDS_CALENDARIOS A
            INNER JOIN BDS_CALENDARIOS B
                ON  A.COD_CALENDARIO = B.COD_CALENDARIO
                AND A.IND_DIA_HABIL  = 'S'
                AND A.FEC_CALENDARIO < B.FEC_CALENDARIO
            WHERE B.COD_CALENDARIO  = 1
            AND B.IND_DIA_HABIL   = 'N'
            AND B.FEC_CALENDARIO >= DATE_SUB(v_fecha_proceso, INTERVAL v_numero_dias_act + 30 DAY)
            AND B.FEC_CALENDARIO <= v_fecha_proceso
            GROUP BY B.FEC_CALENDARIO
        ),
 
        -- BASE_2: NOT EXISTS -> LEFT JOIN + IS NULL (evita error 1713 de SingleStore)
        BASE_2 AS (
            SELECT
                A.DIA_NO_HABIL AS FECHA_PROCESO,
                B.COD_EMPRESA,
                B.COD_SUCURSAL,
                B.COD_RUBRO,
                B.COD_MONEDA,
                B.COD_PAPEL,
                B.COD_MODULO,
                B.COD_TITULO,
                B.COD_CAPITULO,
                B.COD_PLAZO,
                B.COD_GRUPO,
                B.MTO_SALDO_MO,
                B.MTO_SALDO_ME,
                B.MTO_SALDO_MN,
                B.BATCH_ID
            FROM DIAS_FALTANTES A
            INNER JOIN ODS_SALDOS_DIARIOS_CONSOLIDADOS B
                ON A.DIA_HABIL_ANTERIOR = B.FECHA_PROCESO
            LEFT JOIN ODS_SALDOS_DIARIOS_CONSOLIDADOS real_chk
                ON  real_chk.COD_EMPRESA   = B.COD_EMPRESA
                AND real_chk.COD_SUCURSAL  = B.COD_SUCURSAL
                AND real_chk.COD_RUBRO     = B.COD_RUBRO
                AND real_chk.COD_MONEDA    = B.COD_MONEDA
                AND real_chk.COD_PAPEL     = B.COD_PAPEL
                AND real_chk.COD_MODULO    = B.COD_MODULO
                AND real_chk.FECHA_PROCESO = A.DIA_NO_HABIL
            WHERE A.DIA_NO_HABIL >= DATE_SUB(v_fecha_proceso, INTERVAL v_numero_dias_act + 30 DAY)
            AND A.DIA_NO_HABIL <= v_fecha_proceso
            AND real_chk.COD_EMPRESA IS NULL
        ),
 
        UNIVERSO AS (
            SELECT B.FEC_CALENDARIO
            FROM BDS_CODIGO_CALENDARIO A
            INNER JOIN BDS_CALENDARIOS B ON A.COD_CALENDARIO = B.COD_CALENDARIO
            WHERE A.COD_CALENDARIO = 1
            AND B.FEC_CALENDARIO > v_fecha_proceso
            AND B.FEC_CALENDARIO < v_fe_cierre_siguiente
        ),
 
        BASE_3 AS (
            SELECT
                A.FEC_CALENDARIO AS FECHA_PROCESO,
                B.COD_EMPRESA,
                B.COD_SUCURSAL,
                B.COD_RUBRO,
                B.COD_MONEDA,
                B.COD_PAPEL,
                B.COD_MODULO,
                B.COD_TITULO,
                B.COD_CAPITULO,
                B.COD_PLAZO,
                B.COD_GRUPO,
                B.MTO_SALDO_MO,
                B.MTO_SALDO_ME,
                B.MTO_SALDO_MN,
                B.BATCH_ID
            FROM UNIVERSO A
            CROSS JOIN ODS_SALDOS_DIARIOS_CONSOLIDADOS B
            WHERE B.FECHA_PROCESO = v_fecha_proceso
        )
 
        SELECT * FROM BASE_1
        UNION ALL SELECT * FROM BASE_2
        UNION ALL SELECT * FROM BASE_3;
 
 
        -- ============================================================================
        -- ETAPA 2: BASE_UNION_COMPLETADA
        -- (relleno con saldo 0 desde el último día real hasta fin de mes)
        -- ============================================================================
        DROP TABLE IF EXISTS BASE_UNION_COMPLETADA;
 
        CREATE TABLE BASE_UNION_COMPLETADA AS
        WITH ultimo_real_por_llave_mes AS (
            SELECT
                COD_EMPRESA,
                COD_SUCURSAL,
                COD_RUBRO,
                COD_MONEDA,
                COD_PAPEL,
                COD_MODULO,
                YEAR(FECHA_PROCESO) AS ANIO,
                MONTH(FECHA_PROCESO) AS MES,
                MAX(FECHA_PROCESO) AS FEC_ULTIMO_REAL
            FROM ODS_SALDOS_DIARIOS_CONSOLIDADOS
            WHERE FECHA_PROCESO > DATE_SUB(v_fecha_proceso, INTERVAL v_numero_dias_act + 30 DAY)
            AND COD_RUBRO != 0
            GROUP BY COD_EMPRESA,
                    COD_SUCURSAL,
                    COD_RUBRO,
                    COD_MONEDA,
                    COD_PAPEL,
                    COD_MODULO,
                    YEAR(FECHA_PROCESO),
                    MONTH(FECHA_PROCESO)
            HAVING MAX(FECHA_PROCESO) <> LAST_DAY(MAX(FECHA_PROCESO))
        ),
 
        ods_dedup AS (
            SELECT
                COD_EMPRESA,
                COD_SUCURSAL,
                COD_RUBRO,
                COD_MONEDA,
                COD_PAPEL,
                COD_MODULO,
                FECHA_PROCESO,
                COD_TITULO,
                COD_CAPITULO,
                COD_PLAZO,
                COD_GRUPO,
                BATCH_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY COD_EMPRESA,
                            COD_SUCURSAL,
                            COD_RUBRO,
                            COD_MONEDA,
                            COD_PAPEL,
                            COD_MODULO,
                            FECHA_PROCESO
                    ORDER BY COD_TITULO,
                            COD_SUCURSAL,
                            COD_CAPITULO,
                            COD_PLAZO,
                            COD_GRUPO
                ) AS RN
            FROM ODS_SALDOS_DIARIOS_CONSOLIDADOS
            WHERE FECHA_PROCESO > DATE_SUB(v_fecha_proceso, INTERVAL v_numero_dias_act + 30 DAY)
        ),
 
        detalle_ultimo_real AS (
            SELECT
                u.COD_EMPRESA,
                u.COD_SUCURSAL,
                u.COD_RUBRO,
                u.COD_MONEDA,
                u.COD_PAPEL,
                u.COD_MODULO,
                u.FEC_ULTIMO_REAL,
                LAST_DAY(u.FEC_ULTIMO_REAL) AS FEC_FIN_MES,
                o.COD_TITULO,
                o.COD_CAPITULO,
                o.COD_PLAZO,
                o.COD_GRUPO,
                o.BATCH_ID
            FROM ultimo_real_por_llave_mes u
            INNER JOIN ods_dedup o
                ON  o.COD_EMPRESA = u.COD_EMPRESA
                AND o.COD_SUCURSAL = u.COD_SUCURSAL
                AND o.COD_RUBRO = u.COD_RUBRO
                AND o.COD_MONEDA = u.COD_MONEDA
                AND o.COD_PAPEL = u.COD_PAPEL
                AND o.COD_MODULO = u.COD_MODULO
                AND o.FECHA_PROCESO = u.FEC_ULTIMO_REAL
                AND o.RN = 1
        ),
 
        BASE_UNION_2 AS (
            SELECT
                cal.FEC_CALENDARIO AS FECHA_PROCESO,
                d.COD_EMPRESA,
                d.COD_SUCURSAL,
                d.COD_RUBRO,
                d.COD_MONEDA,
                d.COD_PAPEL,
                d.COD_MODULO,
                d.COD_TITULO,
                d.COD_CAPITULO,
                d.COD_PLAZO,
                d.COD_GRUPO,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_MO,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_ME,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_MN,
                d.BATCH_ID
            FROM detalle_ultimo_real d
            INNER JOIN BDS_CALENDARIOS cal
                ON  cal.COD_CALENDARIO = 1
                AND cal.FEC_CALENDARIO >  d.FEC_ULTIMO_REAL
                AND cal.FEC_CALENDARIO <= d.FEC_FIN_MES
            -- BASE_UNION_INICIO ya es una tabla física (etapa 1), no una CTE
            LEFT JOIN BASE_UNION_INICIO bi
                ON  bi.COD_EMPRESA = d.COD_EMPRESA
                AND bi.COD_SUCURSAL = d.COD_SUCURSAL
                AND bi.COD_RUBRO = d.COD_RUBRO
                AND bi.COD_MONEDA = d.COD_MONEDA
                AND bi.COD_PAPEL = d.COD_PAPEL
                AND bi.COD_MODULO = d.COD_MODULO
                AND bi.FECHA_PROCESO = cal.FEC_CALENDARIO
            WHERE bi.COD_EMPRESA IS NULL
        )
 
        SELECT * FROM BASE_UNION_INICIO
        UNION ALL
        SELECT * FROM BASE_UNION_2;
 
 
        -- ============================================================================
        -- ETAPA 3: BASE_UNION_FINAL_2
        -- (continuidad de meses sin registros + relleno intramensual de días hábiles)
        -- ============================================================================
        DROP TABLE IF EXISTS BASE_UNION_FINAL_2;
 
        CREATE TABLE BASE_UNION_FINAL_2 AS
        WITH ULTIMO_DIA_MES AS (
            SELECT *
            FROM BASE_UNION_COMPLETADA
            WHERE FECHA_PROCESO = LAST_DAY(FECHA_PROCESO)
            AND MTO_SALDO_MO <> 0
        ),
 
        MESES_EXISTENTES AS (
            SELECT DISTINCT
                COD_EMPRESA,
                COD_SUCURSAL,
                COD_RUBRO,
                COD_MONEDA,
                COD_PAPEL,
                COD_MODULO,
                YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES
            FROM BASE_UNION_COMPLETADA
        ),
 
        MESES_SIN_CONTINUIDAD AS (
            SELECT
                u.COD_EMPRESA,
                u.COD_SUCURSAL,
                u.COD_RUBRO,
                u.COD_MONEDA,
                u.COD_PAPEL,
                u.COD_MODULO,
                u.COD_TITULO,
                u.COD_CAPITULO,
                u.COD_PLAZO,
                u.COD_GRUPO,
                u.BATCH_ID,
                u.FECHA_PROCESO AS FEC_ULTIMO_REAL,
                LAST_DAY(DATE_ADD(u.FECHA_PROCESO, INTERVAL 1 DAY)) AS FEC_FIN_MES_SIGUIENTE
            FROM ULTIMO_DIA_MES u
            LEFT JOIN MESES_EXISTENTES me
                ON  me.COD_EMPRESA = u.COD_EMPRESA AND me.COD_SUCURSAL = u.COD_SUCURSAL
                AND me.COD_RUBRO = u.COD_RUBRO AND me.COD_MONEDA = u.COD_MONEDA
                AND me.COD_PAPEL = u.COD_PAPEL AND me.COD_MODULO = u.COD_MODULO
                AND me.ANIO = YEAR(DATE_ADD(u.FECHA_PROCESO, INTERVAL 1 DAY))
                AND me.MES  = MONTH(DATE_ADD(u.FECHA_PROCESO, INTERVAL 1 DAY))
            WHERE me.COD_EMPRESA IS NULL
        ),
 
        BASE_UNION_MES_SIGUIENTE AS (
            SELECT
                cal.FEC_CALENDARIO AS FECHA_PROCESO,
                m.COD_EMPRESA, m.COD_SUCURSAL, m.COD_RUBRO, m.COD_MONEDA, m.COD_PAPEL, m.COD_MODULO,
                m.COD_TITULO, m.COD_CAPITULO, m.COD_PLAZO, m.COD_GRUPO,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_MO,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_ME,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_MN,
                m.BATCH_ID
            FROM MESES_SIN_CONTINUIDAD m
            INNER JOIN BDS_CALENDARIOS cal
                ON  cal.COD_CALENDARIO = 1
                AND cal.IND_DIA_HABIL  = 'S'
                AND cal.FEC_CALENDARIO >  m.FEC_ULTIMO_REAL
                AND cal.FEC_CALENDARIO <= m.FEC_FIN_MES_SIGUIENTE
        ),
 
        BASE_UNION_FINAL AS (
            SELECT * FROM BASE_UNION_COMPLETADA
            UNION ALL
            SELECT * FROM BASE_UNION_MES_SIGUIENTE
        ),
 
        RUBROS_ANIO_MES AS (
            SELECT
                COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL, COD_MODULO,
                COD_TITULO, COD_CAPITULO, COD_PLAZO, COD_GRUPO,
                YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
                MAX(BATCH_ID) AS BATCH_ID
            FROM BASE_UNION_FINAL
            GROUP BY
                COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL, COD_MODULO,
                COD_TITULO, COD_CAPITULO, COD_PLAZO, COD_GRUPO,
                YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
        ),
 
        DIAS_FALTANTES_INTRAMES AS (
            SELECT
                cal.FEC_CALENDARIO AS FECHA_PROCESO,
                r.COD_EMPRESA,
                r.COD_SUCURSAL,
                r.COD_RUBRO,
                r.COD_MONEDA,
                r.COD_PAPEL,
                r.COD_MODULO,
                r.COD_TITULO,
                r.COD_CAPITULO,
                r.COD_PLAZO,
                r.COD_GRUPO,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_MO,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_ME,
                CAST(0 AS DECIMAL(17,2)) AS MTO_SALDO_MN,
                r.BATCH_ID
            FROM RUBROS_ANIO_MES r
            INNER JOIN BDS_CALENDARIOS cal
                ON  cal.COD_CALENDARIO = 1
                AND cal.IND_DIA_HABIL  = 'S'
                AND YEAR(cal.FEC_CALENDARIO)  = r.ANIO
                AND MONTH(cal.FEC_CALENDARIO) = r.MES
            LEFT JOIN BASE_UNION_FINAL existente
                ON  existente.COD_EMPRESA  = r.COD_EMPRESA  AND existente.COD_SUCURSAL = r.COD_SUCURSAL
                AND existente.COD_RUBRO    = r.COD_RUBRO    AND existente.COD_MONEDA   = r.COD_MONEDA
                AND existente.COD_PAPEL    = r.COD_PAPEL    AND existente.COD_MODULO   = r.COD_MODULO
                AND existente.COD_TITULO   = r.COD_TITULO   AND existente.COD_CAPITULO = r.COD_CAPITULO
                AND existente.COD_PLAZO    = r.COD_PLAZO    AND existente.COD_GRUPO    = r.COD_GRUPO
                AND existente.FECHA_PROCESO = cal.FEC_CALENDARIO
            WHERE existente.FECHA_PROCESO IS NULL
        )
 
        SELECT * FROM BASE_UNION_FINAL
        UNION ALL
        SELECT * FROM DIAS_FALTANTES_INTRAMES;
 
 
        -- ============================================================================
        -- ETAPA 4: INSERT FINAL a BDS_SALDOS_DIARIOS_CONSOLIDADOS
        -- (filtro rubro 6006, variación mensual, promedio acumulado del mes)
        -- ============================================================================
        
		CREATE TABLE BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_2 AS
        WITH BDS_FILTRADA AS (
            SELECT *,
                CASE WHEN COD_RUBRO LIKE '4%' OR COD_RUBRO LIKE '5%' OR COD_RUBRO LIKE '6%'
                        OR COD_RUBRO LIKE '1%' OR COD_RUBRO LIKE '2%' OR COD_RUBRO LIKE '3%'
                        OR COD_RUBRO LIKE '7%' OR COD_RUBRO LIKE '8%' OR COD_RUBRO LIKE '9%'
                        THEN 1 ELSE 0
                END AS ES_RUBRO_6006
            FROM BASE_UNION_FINAL_2
        ),
 
        ultimo_dia_mes AS (
            SELECT
                COD_EMPRESA,
                COD_SUCURSAL,
                COD_RUBRO,
                COD_MONEDA,
                COD_PAPEL,
                COD_MODULO,
                COD_TITULO,
                COD_CAPITULO,
                COD_PLAZO,
                COD_GRUPO,
                YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
                MAX(FECHA_PROCESO) AS FEC_ULTIMO_DIA
            FROM BDS_FILTRADA
            WHERE ES_RUBRO_6006 = 1
            GROUP BY COD_EMPRESA,
                    COD_SUCURSAL,
                    COD_RUBRO,
                    COD_MONEDA,
                    COD_PAPEL,
                    COD_MODULO,
                    COD_TITULO,
                    COD_CAPITULO,
                    COD_PLAZO,
                    COD_GRUPO,
                    YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
        ),
 
        ultimo_dia_mes_monto AS (
            SELECT
                u.COD_EMPRESA,
                u.COD_SUCURSAL,
                u.COD_RUBRO,
                u.COD_MONEDA,
                u.COD_PAPEL,
                u.COD_MODULO,
                u.COD_TITULO,
                u.COD_CAPITULO,
                u.COD_PLAZO,
                u.COD_GRUPO,
                u.ANIO, u.MES,
                b.MTO_SALDO_MO AS MTO_ULTIMO_DIA_MO,
                b.MTO_SALDO_ME AS MTO_ULTIMO_DIA_ME,
                b.MTO_SALDO_MN AS MTO_ULTIMO_DIA_MN
            FROM ultimo_dia_mes u
            INNER JOIN BDS_FILTRADA b
                ON  b.COD_EMPRESA = u.COD_EMPRESA AND b.COD_SUCURSAL = u.COD_SUCURSAL
                AND b.COD_RUBRO = u.COD_RUBRO AND b.COD_MONEDA = u.COD_MONEDA
                AND b.COD_PAPEL = u.COD_PAPEL AND b.COD_MODULO = u.COD_MODULO
                AND b.COD_TITULO = u.COD_TITULO AND b.COD_CAPITULO = u.COD_CAPITULO
                AND b.COD_PLAZO = u.COD_PLAZO AND b.COD_GRUPO = u.COD_GRUPO
                AND b.FECHA_PROCESO = u.FEC_ULTIMO_DIA
            WHERE b.ES_RUBRO_6006 = 1
        ),
 
        suma_diaria_llave AS (
            SELECT
                FECHA_PROCESO,
                COD_EMPRESA,
                COD_SUCURSAL,
                COD_RUBRO,
                COD_MONEDA,
                COD_PAPEL,
                COD_MODULO,
                COD_TITULO,
                COD_CAPITULO,
                COD_PLAZO,
                COD_GRUPO,
                YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
                MTO_SALDO_MO, MTO_SALDO_ME, MTO_SALDO_MN
            FROM BDS_FILTRADA
            WHERE ES_RUBRO_6006 = 1
        ),
 
        promedios_acumulados AS (
            SELECT
                FECHA_PROCESO,
                COD_EMPRESA,
                COD_SUCURSAL,
                COD_RUBRO,
                COD_MONEDA,
                COD_PAPEL,
                COD_MODULO,
                COD_TITULO,
                COD_CAPITULO,
                COD_PLAZO,
                COD_GRUPO,
                ANIO, MES,
                CAST(AVG(MTO_SALDO_MO) OVER (
                    PARTITION BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL, COD_MODULO,
                                COD_TITULO, COD_CAPITULO, COD_PLAZO, COD_GRUPO, ANIO, MES
                    ORDER BY FECHA_PROCESO ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS DECIMAL(17,4)) AS AVG_MTO_SALDO_MO,
                CAST(AVG(MTO_SALDO_ME) OVER (
                    PARTITION BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL, COD_MODULO,
                                COD_TITULO, COD_CAPITULO, COD_PLAZO, COD_GRUPO, ANIO, MES
                    ORDER BY FECHA_PROCESO ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS DECIMAL(17,4)) AS AVG_MTO_SALDO_ME,
                CAST(AVG(MTO_SALDO_MN) OVER (
                    PARTITION BY COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL, COD_MODULO,
                                COD_TITULO, COD_CAPITULO, COD_PLAZO, COD_GRUPO, ANIO, MES
                    ORDER BY FECHA_PROCESO ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS DECIMAL(17,4)) AS AVG_MTO_SALDO_MN
            FROM suma_diaria_llave
        )
 
        SELECT
            b.FECHA_PROCESO,
            b.COD_EMPRESA,
            b.COD_SUCURSAL,
            b.COD_RUBRO,
            b.COD_MONEDA,
            b.COD_PAPEL,
            b.COD_MODULO,
            b.COD_TITULO,
            b.COD_CAPITULO,
            b.COD_PLAZO,
            b.COD_GRUPO,
            b.MTO_SALDO_MO,
            b.MTO_SALDO_ME,
            b.MTO_SALDO_MN,
            CASE WHEN MONTH(b.FECHA_PROCESO) = 1 THEN CAST(b.MTO_SALDO_MN AS DECIMAL(17,2))
                ELSE CAST(b.MTO_SALDO_MN - COALESCE(udm.MTO_ULTIMO_DIA_MN, 0) AS DECIMAL(17,2)) END AS MTO_SALDO_MES_MN,
            CASE WHEN MONTH(b.FECHA_PROCESO) = 1 THEN CAST(b.MTO_SALDO_ME AS DECIMAL(17,2))
                ELSE CAST(b.MTO_SALDO_ME - COALESCE(udm.MTO_ULTIMO_DIA_ME, 0) AS DECIMAL(17,2)) END AS MTO_SALDO_MES_ME,
            CASE WHEN MONTH(b.FECHA_PROCESO) = 1 THEN CAST(b.MTO_SALDO_MO AS DECIMAL(17,2))
                ELSE CAST(b.MTO_SALDO_MO - COALESCE(udm.MTO_ULTIMO_DIA_MO, 0) AS DECIMAL(17,2)) END AS MTO_SALDO_MES_MO,
				
            CASE WHEN b.MTO_SALDO_MO = 0 THEN CAST(0 AS DECIMAL(17,4)) ELSE pa.AVG_MTO_SALDO_MO END AS MTO_AVG_MES_MO,
            CASE WHEN b.MTO_SALDO_ME = 0 THEN CAST(0 AS DECIMAL(17,4)) ELSE pa.AVG_MTO_SALDO_ME END AS MTO_AVG_MES_ME,
            CASE WHEN b.MTO_SALDO_MN = 0 THEN CAST(0 AS DECIMAL(17,4)) ELSE pa.AVG_MTO_SALDO_MN END AS MTO_AVG_MES_MN,
            b.BATCH_ID
        FROM BDS_FILTRADA b
        LEFT JOIN ultimo_dia_mes_monto udm
            ON  udm.COD_EMPRESA = b.COD_EMPRESA AND udm.COD_SUCURSAL = b.COD_SUCURSAL
            AND udm.COD_RUBRO = b.COD_RUBRO AND udm.COD_MONEDA = b.COD_MONEDA
            AND udm.COD_PAPEL = b.COD_PAPEL AND udm.COD_MODULO = b.COD_MODULO
            AND udm.COD_TITULO = b.COD_TITULO AND udm.COD_CAPITULO = b.COD_CAPITULO
            AND udm.COD_PLAZO = b.COD_PLAZO AND udm.COD_GRUPO = b.COD_GRUPO
            AND udm.ANIO = CASE WHEN MONTH(b.FECHA_PROCESO) = 1 THEN YEAR(b.FECHA_PROCESO) - 1 ELSE YEAR(b.FECHA_PROCESO) END
            AND udm.MES  = CASE WHEN MONTH(b.FECHA_PROCESO) = 1 THEN 12 ELSE MONTH(b.FECHA_PROCESO) - 1 END
        LEFT JOIN promedios_acumulados pa
            ON  pa.COD_EMPRESA = b.COD_EMPRESA AND pa.COD_SUCURSAL = b.COD_SUCURSAL
            AND pa.COD_RUBRO = b.COD_RUBRO AND pa.COD_MONEDA = b.COD_MONEDA
            AND pa.COD_PAPEL = b.COD_PAPEL AND pa.COD_MODULO = b.COD_MODULO
            AND pa.COD_TITULO = b.COD_TITULO AND pa.COD_CAPITULO = b.COD_CAPITULO
            AND pa.COD_PLAZO = b.COD_PLAZO AND pa.COD_GRUPO = b.COD_GRUPO
            AND pa.ANIO = YEAR(b.FECHA_PROCESO) AND pa.MES = MONTH(b.FECHA_PROCESO) AND pa.FECHA_PROCESO = b.FECHA_PROCESO
        WHERE b.ES_RUBRO_6006 = 1
        AND b.FECHA_PROCESO <= v_fecha_proceso
        AND b.FECHA_PROCESO > DATE_SUB(v_fecha_proceso, INTERVAL v_numero_dias_act DAY);
		
		
        -- ============================================================================
        -- ETAPA 4: COMPLETAR RUBRO QUE NO ESTAN EN DIAS NO HABILESDUPLICANDO LOS REGISTRSO DE UN DIA 
		-- HABIL PERO CON LA FECHA DE PROCESO DEL DIA HABIL
        -- (filtro rubro 6006, variación mensual, promedio acumulado del mes)
        -- ============================================================================
		
		DROP TABLE BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_3;
		CREATE TABLE BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_3
		AS 
		SELECT 
			cnh.FEC_CALENDARIO 
			,rh.COD_EMPRESA
			,rh.COD_SUCURSAL
			,rh.COD_RUBRO
			,rh.COD_MONEDA
			,rh.COD_PAPEL
			,rh.COD_MODULO
			,rh.COD_TITULO
			,rh.COD_CAPITULO
			,rh.COD_PLAZO
			,rh.COD_GRUPO
			,rh.MTO_SALDO_MO
			,rh.MTO_SALDO_ME
			,rh.MTO_SALDO_MN
			,rh.MTO_SALDO_MES_MN
			,rh.MTO_SALDO_MES_ME
			,rh.MTO_SALDO_MES_MO
			,rh.MTO_AVG_MES_MO
			,rh.MTO_AVG_MES_ME
			,rh.MTO_AVG_MES_MN
			,rh.FUENTE
			,rh.FEC_CARGA
			,rh.BATCH_ID
			FROM BDS_CALENDARIOS cnh
			INNER JOIN  BDS_CALENDARIOS ch
			ON ch.IND_DIA_HABIL = 'S'
			and ch.FEC_CALENDARIO <cnh.FEC_CALENDARIO 
			INNER JOIN BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_2 rh
			ON rh.FECHA_PROCESO = ch.FEC_CALENDARIO 
			LEFT JOIN BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_2 rnh
			ON rnh.FECHA_PROCESO = cnh.FEC_CALENDARIO 
			AND rh.COD_EMPRESA   = rnh.COD_EMPRESA
			AND rh.COD_TITULO    = rnh.COD_TITULO
			and rh.COD_CAPITULO  = rnh.COD_CAPITULO
			AND rh.COD_PLAZO 	 = rnh.COD_PLAZO
			AND rh.COD_GRUPO     = rnh.COD_GRUPO
			AND rh.COD_SUCURSAL  = rnh.COD_SUCURSAL
			AND rh.COD_RUBRO     = rnh.COD_RUBRO
			AND rh.COD_MONEDA    = rnh.COD_MONEDA
			AND rh.COD_PAPEL     = rnh.COD_PAPEL
			AND rh.COD_MODULO    = rnh.COD_MODULO
			WHERE cnh.IND_DIA_HABIL <>'S'
			and rnh.COD_RUBRO IS NULL
			AND ch.FEC_CALENDARIO =(SELECT MAX(FEC_CALENDARIO) FROM BDS_CALENDARIOS WHERE IND_DIA_HABIL='S' AND FEC_CALENDARIO < cnh.FEC_CALENDARIO);

		-- ============================================================================
        -- ETAPA 6: INSERCION EN LA TABLA FINAL SIN DUPLICIDAD
        -- 
        -- ============================================================================
		INSERT INTO BDS_SALDOS_DIARIOS_CONSOLIDADOS (
            FECHA_PROCESO,
            COD_EMPRESA,
            COD_SUCURSAL,
            COD_RUBRO,
            COD_MONEDA,
            COD_PAPEL,
            COD_MODULO,
            COD_TITULO,
            COD_CAPITULO,
            COD_PLAZO,
            COD_GRUPO,
            MTO_SALDO_MO,
            MTO_SALDO_ME,
            MTO_SALDO_MN,
            MTO_SALDO_MES_MN,
            MTO_SALDO_MES_ME,
            MTO_SALDO_MES_MO,
            MTO_AVG_MES_MO,
            MTO_AVG_MES_ME,
            MTO_AVG_MES_MN,
            BATCH_ID
        )
		SELECT DISTINCT * FROM BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_3;
 
 
        -- ============================================================================
        -- LIMPIEZA DE TABLAS TEMPORALES DE ETAPA
        -- ============================================================================
        DROP TABLE IF EXISTS BASE_UNION_INICIO;
        DROP TABLE IF EXISTS BASE_UNION_COMPLETADA;
        DROP TABLE IF EXISTS BASE_UNION_FINAL_2;
		DROP TABLE IF EXISTS BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_2;
		DROP TABLE IF EXISTS BDS_SALDOS_DIARIOS_CONSOLIDADOS_TEMP_3;
 
        v_filas = ROW_COUNT();
        IF v_filas = 0 THEN
            v_msg_error =
                'No se insertaron registros en BDS_SALDOS_DIARIOS_CONSOLIDADOS';
       
        END IF;
 
    EXCEPTION WHEN OTHERS THEN
        v_msg_error = CONCAT('[BDS_SALDOS_DIARIOS_CONSOLIDADOS] ', exception_message());
    END;
   
    UPDATE ctl_log_proceso
       SET estado = CASE
                        WHEN v_msg_error IS NULL                  THEN 'TERMINADO'
                        WHEN v_msg_error LIKE 'No se insertaron%' THEN 'WARNING'
                        ELSE 'ERROR'
                    END,
           fec_termino      = NOW(6),
           filas_insertadas = v_filas,
           msg_error        = v_msg_error
     WHERE id_log = v_id_log;
 
 
    IF (v_msg_error IS NOT NULL
        AND v_msg_error NOT LIKE 'No se insertaron%') THEN
   
        RAISE USER_EXCEPTION(
            CONCAT('Error al cargar la tabla: ', v_msg_error)
        );
 
    END IF;
 
END //
 
DELIMITER