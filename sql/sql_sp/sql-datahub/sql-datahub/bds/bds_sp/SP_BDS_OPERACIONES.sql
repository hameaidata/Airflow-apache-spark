DELIMITER //
 
CREATE OR REPLACE PROCEDURE SP_BDS_OPERACIONES()
AS
DECLARE
    v_nom_proceso              VARCHAR(200) = 'SP_BDS_OPERACIONES';
    v_tabla_origen             VARCHAR(200);
    v_tabla_destino            VARCHAR(200);
    v_id_log                   BIGINT = 0;
    v_filas                    BIGINT = 0;
    v_msg_error                TEXT = NULL;
    v_fe_cierre                DATE;
    v_fe_cierre_siguiente      DATE;

BEGIN
  
    -- *****************************************************************
    -- CARGA BDS_SALDOS_DIARIOS_CONSOLIDADOS
    -- *****************************************************************
    v_tabla_origen  = 'ODS_OPERACIONES';
    v_tabla_destino = 'BDS_OPERACIONES';
    v_msg_error     = NULL;
    v_id_log = LAST_INSERT_ID();
    UPDATE ctl_log_proceso
       SET estado = 'EJECUTANDO'
     WHERE id_log = v_id_log;

    
    -- ****************************************************************
    -- ACTUALIZACION DE ESTADO DE REGISTROS ACTIVOS
    -- ****************************************************************
    UPDATE TABLE BDS_OPERACIONES
    SET IND_ACTIVO = 0
    --****************************************************************
    -- INICIO DE INSERCION DE NUEVOS REGISTROS DE  BDS_OPERACIONES
    --****************************************************************
    WITH BASE_ODS_OPERACIONES AS (
        SELECT
            CONCAT(
                bo.COD_EMPRESA,'|',
                bo.COD_MODULO,'|',
                bo.COD_SUCURSAL,'|',
                bo.COD_MONEDA,'|',
                bo.COD_PAPEL,'|',
                bo.NUM_CUENTA_BT,'|',
                bo.COD_OPERACION,'|',
                bo.COD_SUB_OPERACION,'|',
                bo.COD_TIPO_OPERACION
            ) AS ID_OPERACION,
            CONCAT(
                bo.COD_EMPRESA,'|',
                bo.COD_SUCURSAL,'|',
                bo.COD_MONEDA,'|',
                bo.COD_PAPEL,'|',
                bo.NUM_CUENTA_BT,'|',
                bo.COD_OPERACION,'|',
                bo.COD_SUB_OPERACION,'|',
                bo.COD_TIPO_OPERACION
            ) AS ID_SALDO_CONTABLE,
            CONCAT(
                bo.COD_EMPRESA,'|',
                bo.COD_SUCURSAL,'|',
                bo.COD_MONEDA,'|',
                bo.COD_PAPEL,'|',
                bo.NUM_CUENTA_BT,'|',
                bo.COD_OPERACION,'|',
                bo.COD_SUB_OPERACION
            ) AS ID_CRUCE,
            bo.FECHA_PROCESO,
            bo.COD_EMPRESA,
            bo.COD_MODULO,
            bo.COD_SUCURSAL,
            bo.COD_MONEDA,
            bo.COD_PAPEL,
            bo.NUM_CUENTA_BT,
            bo.COD_OPERACION,
            bo.COD_SUB_OPERACION,
            bo.COD_TIPO_OPERACION,
            bo.FEC_ALTA,
            bo.FEC_VENCIMIENTO,
            bo.NUM_DIAS_PLAZO,
            bo.TIP_TASA,
            bo.PCT_TASA,
            bo.PCT_TASA_MORA,
            bo.TIP_DIAS,
            bo.TIP_AJUSTE_VENCIMIENTO,
            bo.TIP_ANIO,
            bo.TIP_CALCULO_INTERES,
            bo.NUM_DIAS_REVISION_TASA,
            bo.MTO_DESEMBOLSADO,
            bo.MTO_INTERES_VEN,
            bo.FEC_ULTIMA_REV_TASA,
            bo.IND_AFECTADA_IVA,
            bo.COD_ESTADO,
            bo.IND_AVISO,
            bo.PCT_TASA_PLUS,
            bo.COD_NUMERO_EVENTOS,
            bo.FEC_BAJA,
            bo.DES_PERIODO,
            1 AS TIP_CUENTA,
            bo.FUENTE,
            bo.FECHA_CARGA,
            bo.BATCH_ID
        FROM ODS_OPERACIONES bo
        WHERE bo.COD_EMPRESA = 1
        AND COD_MODULO NOT IN (20,21,403,419,426,431,451,455,456,468,469,471,486)
    ),
    /*===========================================================
    =            FOTO DE SALDOS DE CIERRE
    ===========================================================*/
    BDS_SALDOS_PREVIO AS (
        SELECT
            CONCAT(
                hsc.COD_EMPRESA,'|',
                hsc.COD_MODULO_PRODUCTO,'|',
                hsc.COD_SUCURSAL,'|',
                hsc.COD_MONEDA,'|',
                hsc.COD_PAPEL,'|',
                hsc.NUM_CUENTA_BT,'|',
                hsc.COD_OPERACION,'|',
                hsc.COD_SUB_OPERACION,'|',
                hsc.COD_TIPO_OPERACION
            ) AS ID_OPERACION,
            CONCAT(
                hsc.COD_EMPRESA,'|',
                hsc.COD_SUCURSAL,'|',
                hsc.COD_MONEDA,'|',
                hsc.COD_PAPEL,'|',
                hsc.NUM_CUENTA_BT,'|',
                hsc.COD_OPERACION,'|',
                hsc.COD_SUB_OPERACION,'|',
                hsc.COD_TIPO_OPERACION
            ) AS ID_SALDO_CONTABLE,
            CONCAT(
                hsc.COD_EMPRESA,'|',
                hsc.COD_SUCURSAL,'|',
                hsc.COD_MONEDA,'|',
                hsc.COD_PAPEL,'|',
                hsc.NUM_CUENTA_BT,'|',
                hsc.COD_OPERACION,'|',
                hsc.COD_SUB_OPERACION
            ) AS ID_CRUCE,
            hsc.FECHA_PROCESO,
            hsc.COD_EMPRESA,
            hsc.COD_MODULO_PRODUCTO AS COD_MODULO,
            hsc.COD_SUCURSAL,
            hsc.COD_MONEDA,
            hsc.COD_PAPEL,
            hsc.NUM_CUENTA_BT,
            hsc.COD_OPERACION,
            hsc.COD_SUB_OPERACION,
            hsc.COD_TIPO_OPERACION,
            hsc.COD_RUBRO,
            NULL AS FEC_VENCIMIENTO,
            NULL AS NUM_DIAS_PLAZO,
            NULL AS TIP_TASA,
            NULL AS PCT_TASA,
            NULL AS TIP_DIA,
            NULL AS TIP_ANIO,
            hsc.MTO_SALDO_ORIGEN,
            hsc.FUENTE,
            hsc.FECHA_CARGA,
            hsc.BATCH_ID,
            CASE
                WHEN hsc.COD_MODULO = 65
                    AND hsc.COD_RUBRO LIKE '1%' THEN 1
                ELSE 2
            END AS TIP_CUENTA
        FROM BDS_SALDOS_CIERRE hsc
        WHERE hsc.COD_EMPRESA = 1
            
    ),
    /*===========================================================
    =            FILTRO POR MODULO DE ODS_OPERACIONES
    ===========================================================*/
    BDS_SALDOS AS (
        SELECT
            hsc.ID_OPERACION,
            hsc.ID_SALDO_CONTABLE,
            hsc.ID_CRUCE,
            hsc.FECHA_PROCESO,
            hsc.COD_EMPRESA,
            hsc.COD_MODULO,
            hsc.COD_SUCURSAL,
            hsc.COD_MONEDA,
            hsc.COD_PAPEL,
            hsc.NUM_CUENTA_BT,
            hsc.COD_OPERACION,
            hsc.COD_SUB_OPERACION,
            hsc.COD_TIPO_OPERACION,
            hsc.COD_RUBRO,
            hsc.FEC_VENCIMIENTO,
            hsc.NUM_DIAS_PLAZO,
            hsc.TIP_TASA,
            hsc.PCT_TASA,
            hsc.TIP_DIA,
            hsc.TIP_ANIO,
            hsc.MTO_SALDO_ORIGEN,
            hsc.FUENTE,
            hsc.FECHA_CARGA,
            hsc.BATCH_ID,
            CASE
                WHEN hsc.COD_MODULO = 65
                    AND hsc.COD_RUBRO LIKE '1%' THEN 1
                ELSE 2
            END AS TIP_CUENTA
        FROM BDS_SALDOS_PREVIO hsc
    WHERE hsc.COD_EMPRESA = 1
        AND hsc.COD_MODULO NOT IN (403,419,426,431,451,455,456,468,469,471,486)
        AND NOT EXISTS (
            SELECT 1
            FROM BASE_ODS_OPERACIONES b
            WHERE b.COD_MODULO = hsc.COD_MODULO
                AND b.ID_CRUCE   = hsc.ID_CRUCE
            )
    
        AND (
            hsc.COD_MODULO IN (
                SELECT DISTINCT COD_MODULO
                FROM ODS_OPERACIONES
            )
            OR hsc.COD_MODULO IN (20,21,27,63,65,45,162,321)
            )

            
    ),

    /*===========================================================
    = SOLO REGISTROS DE SALDOS QUE NO EXISTAN EN OPERACIONES
    ===========================================================*/
    
    BASE_PASIVOS AS (
        SELECT
            s.ID_OPERACION,
            s.ID_SALDO_CONTABLE,
            s.ID_CRUCE,
            s.FECHA_PROCESO,
            s.COD_EMPRESA,
            s.COD_MODULO,
            s.COD_SUCURSAL,
            s.COD_MONEDA,
            s.COD_PAPEL,
            s.NUM_CUENTA_BT,
            s.COD_OPERACION,
            s.COD_SUB_OPERACION,
            s.COD_TIPO_OPERACION,
            NULL AS FEC_ALTA,
            s.FEC_VENCIMIENTO,
            s.NUM_DIAS_PLAZO,
            s.TIP_TASA,
            NULL AS PCT_TASA,
            NULL AS PCT_TASA_MORA,
            s.TIP_DIA AS TIP_DIAS,
            NULL AS TIP_AJUSTE_VENCIMIENTO,
            s.TIP_ANIO,
            NULL AS TIP_CALCULO_INTERES,
            NULL AS NUM_DIAS_REVISION_TASA,
            s.MTO_SALDO_ORIGEN AS MTO_DESEMBOLSADO,
            NULL AS MTO_INTERES_VEN,
            NULL AS FEC_ULTIMA_REV_TASA,
            NULL AS IND_AFECTADA_IVA,
            NULL AS COD_ESTADO,
            NULL AS IND_AVISO,
            NULL AS PCT_TASA_PLUS,
            NULL AS COD_NUMERO_EVENTOS,
            NULL AS FEC_BAJA,
            NULL AS DES_PERIODO,
            s.TIP_CUENTA,
            s.FUENTE,
            DATE(s.FECHA_CARGA) AS FECHA_CARGA,
            s.BATCH_ID
        FROM BDS_SALDOS s
        WHERE s.ID_OPERACION NOT IN (
            SELECT ID_OPERACION
            FROM BASE_ODS_OPERACIONES
        )
        
    ),
    RESULTADO_FINAL AS (
        SELECT *
        FROM BASE_ODS_OPERACIONES
        UNION ALL
        SELECT *
        FROM BASE_PASIVOS
    ),
    CUENTA_UNICA AS (
        SELECT
            NUM_CUENTA_BT,
            MAX(COD_EJECUTIVO) AS COD_EJECUTIVO
        FROM ODS_CUENTA
        GROUP BY NUM_CUENTA_BT
    ),

    ANALISIS_FINAL AS (
        SELECT
            rf.ID_OPERACION,
            rf.ID_SALDO_CONTABLE,
            rf.ID_CRUCE,
            rf.FECHA_PROCESO,
            rf.COD_EMPRESA,
            rf.COD_MODULO,
            rf.COD_SUCURSAL,
            rf.COD_MONEDA,
            rf.COD_PAPEL,
            rf.NUM_CUENTA_BT,
            cu.COD_EJECUTIVO,
            rf.COD_OPERACION,
            rf.COD_SUB_OPERACION,
            rf.COD_TIPO_OPERACION,
            rf.FEC_ALTA,
            rf.FEC_VENCIMIENTO,
            rf.NUM_DIAS_PLAZO,
            rf.TIP_TASA,
            rf.PCT_TASA,
            rf.PCT_TASA_MORA,
            rf.TIP_DIAS,
            rf.TIP_AJUSTE_VENCIMIENTO,
            rf.TIP_ANIO,
            rf.TIP_CALCULO_INTERES,
            rf.NUM_DIAS_REVISION_TASA,
            rf.MTO_DESEMBOLSADO,
            rf.MTO_INTERES_VEN,
            rf.FEC_ULTIMA_REV_TASA,
            rf.IND_AFECTADA_IVA,
            rf.COD_ESTADO,
            rf.IND_AVISO,
            rf.PCT_TASA_PLUS,
            rf.COD_NUMERO_EVENTOS,
            rf.FEC_BAJA,
            rf.DES_PERIODO,
            rf.TIP_CUENTA,
            rf.FUENTE,
            DATE(rf.FECHA_CARGA) AS FECHA_CARGA,
            rf.BATCH_ID,
            SHA2(
                CONCAT_WS('|',
                    COALESCE(rf.ID_SALDO_CONTABLE,''),
                    COALESCE(rf.ID_CRUCE,''),
                    COALESCE(rf.COD_EMPRESA,''),
                    COALESCE(rf.COD_MODULO,''),
                    COALESCE(rf.COD_SUCURSAL,''),
                    COALESCE(rf.COD_MONEDA,''),
                    COALESCE(rf.COD_PAPEL,''),
                    COALESCE(rf.NUM_CUENTA_BT,''),
                    COALESCE(rf.COD_OPERACION,''),
                    COALESCE(rf.COD_SUB_OPERACION,''),
                    COALESCE(rf.COD_TIPO_OPERACION,''),
                    COALESCE(rf.FEC_ALTA,''),
                    COALESCE(rf.FEC_VENCIMIENTO,''),
                    COALESCE(rf.NUM_DIAS_PLAZO,''),
                    COALESCE(rf.TIP_TASA,''),
                    COALESCE(rf.PCT_TASA,''),
                    COALESCE(rf.PCT_TASA_MORA,''),
                    COALESCE(rf.TIP_DIAS,''),
                    COALESCE(rf.TIP_AJUSTE_VENCIMIENTO,''),
                    COALESCE(rf.TIP_ANIO,''),
                    COALESCE(rf.TIP_CALCULO_INTERES,''),
                    COALESCE(rf.NUM_DIAS_REVISION_TASA,''),
                    COALESCE(rf.MTO_DESEMBOLSADO,''),
                    COALESCE(rf.MTO_INTERES_VEN,''),
                    COALESCE(rf.FEC_ULTIMA_REV_TASA,''),
                    COALESCE(rf.IND_AFECTADA_IVA,''),
                    COALESCE(rf.COD_ESTADO,''),
                    COALESCE(rf.IND_AVISO,''),
                    COALESCE(rf.PCT_TASA_PLUS,''),
                    COALESCE(rf.COD_NUMERO_EVENTOS,''),
                    COALESCE(rf.FEC_BAJA,''),
                    COALESCE(rf.DES_PERIODO,'')
                ),
                256
            ) AS HASH_CONCADENADO
        FROM RESULTADO_FINAL rf
        LEFT JOIN CUENTA_UNICA cu
            ON cu.NUM_CUENTA_BT = rf.NUM_CUENTA_BT
    )

    INSERT INTO BDS_OPERACIONES (
        ID_OPERACION,
        ID_SALDO_CONTABLE,
        ID_CRUCE,
        FECHA_PROCESO,
        COD_EMPRESA,
        COD_MODULO,
        COD_SUCURSAL,
        COD_MONEDA,
        COD_PAPEL,
        NUM_CUENTA_BT,
        COD_EJECUTIVO,
        COD_OPERACION,
        COD_SUB_OPERACION,
        COD_TIPO_OPERACION,
        FEC_ALTA,
        FEC_VENCIMIENTO,
        NUM_DIAS_PLAZO,
        TIP_TASA,
        PCT_TASA,
        PCT_TASA_MORA,
        TIP_DIAS,
        TIP_AJUSTE_VENCIMIENTO,
        TIP_ANIO,
        TIP_CALCULO_INTERES,
        NUM_DIAS_REVISION_TASA,
        MTO_DESEMBOLSADO,
        MTO_INTERES_VEN,
        FEC_ULTIMA_REV_TASA,
        IND_AFECTADA_IVA,
        COD_ESTADO,
        IND_AVISO,
        PCT_TASA_PLUS,
        COD_NUMERO_EVENTOS,
        FEC_BAJA,
        DES_PERIODO,
        TIP_CUENTA,
        IND_ACTIVO,
        FEC_INICIO_VIGENCIA,
        FEC_FIN_VIGENCIA,
        FEC_ACTUALIZACION_REGISTRO,
        FECHA_CARGA,
        FUENTE,
        BATCH_ID,
        HASH_CONCADENADO
    )
    SELECT
        ID_OPERACION,
        ID_SALDO_CONTABLE,
        ID_CRUCE,
        FECHA_PROCESO,
        COD_EMPRESA,
        COD_MODULO,
        COD_SUCURSAL,
        COD_MONEDA,
        COD_PAPEL,
        NUM_CUENTA_BT,
        COD_EJECUTIVO,
        COD_OPERACION,
        COD_SUB_OPERACION,
        COD_TIPO_OPERACION,
        FEC_ALTA,
        FEC_VENCIMIENTO,
        NUM_DIAS_PLAZO,
        TIP_TASA,
        PCT_TASA,
        PCT_TASA_MORA,
        TIP_DIAS,
        TIP_AJUSTE_VENCIMIENTO,
        TIP_ANIO,
        TIP_CALCULO_INTERES,
        NUM_DIAS_REVISION_TASA,
        MTO_DESEMBOLSADO,
        MTO_INTERES_VEN,
        FEC_ULTIMA_REV_TASA,
        IND_AFECTADA_IVA,
        COD_ESTADO,
        IND_AVISO,
        PCT_TASA_PLUS,
        COD_NUMERO_EVENTOS,
        FEC_BAJA,
        DES_PERIODO,
        TIP_CUENTA,
        1 AS IND_ACTIVO,
        NULL FEC_INICIO_VIGENCIA,
        NULL FEC_FIN_VIGENCIA,
        NULL FEC_ACTUALIZACION_REGISTRO,
        DATE(FECHA_CARGA) AS FECHA_CARGA,
        FUENTE,
        BATCH_ID,
        HASH_CONCADENADO
    FROM (
        SELECT
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY ID_OPERACION
                ORDER BY
                    CASE
                        WHEN TIP_CUENTA = 2 THEN 1
                        ELSE 2
                    END,
                    FECHA_PROCESO DESC
            ) rn
        FROM ANALISIS_FINAL t
    ) x
    WHERE rn = 1;
    v_filas = ROW_COUNT();

    IF v_filas = 0 THEN
        SET v_msg_error = 'No se insertaron registros en BDS_OPERACIONES';
    END IF;

    UPDATE ctl_log_proceso
       SET estado = CASE
                        WHEN v_msg_error IS NULL THEN 'TERMINADO'
                        ELSE 'WARNING'
                    END,
           fec_termino      = NOW(6),
           filas_insertadas = v_filas,
           msg_error        = v_msg_error
     WHERE id_log = v_id_log;

END //
DELIMITER ;