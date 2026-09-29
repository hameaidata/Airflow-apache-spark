
DELIMITER //

CREATE PROCEDURE SP_BDS_SALDOS_CIERRE_PA (v_fe_cierre DATE, p_dias INT DEFAULT NULL)
AS
DECLARE
    v_tabla_origen      VARCHAR(200) DEFAULT 'ODS_SALDOS_CIERRE';
    v_tabla_destino     VARCHAR(200) DEFAULT 'BDS_SALDOS_CIERRE';
    v_nom_proceso       VARCHAR(200) DEFAULT 'SP_BDS_SALDOS_CIERRE';
    v_id_log        BIGINT DEFAULT 0;
	v_filas         BIGINT DEFAULT 0;
    v_fecha_inicio DATE;
    v_msg_error VARCHAR(5000) DEFAULT NULL;
    v_fecha_proceso  DATE;
BEGIN
	v_msg_error = NULL;
	-- LOGICA DE ASIGANICION EN LA FORMA QUE SE DEBE DE EJECURTAR
    
    INSERT INTO ctl_log_proceso (
        nom_proceso,
        tabla_origen,
        tabla_destino,
        fec_inicio,
        estado
    )
    VALUES (
        v_nom_proceso,
        v_tabla_origen,
        v_tabla_destino,
        NOW(6),
        'INICIADO'
    );

    v_id_log = LAST_INSERT_ID();
    UPDATE ctl_log_proceso
       SET estado = 'EJECUTANDO'
    WHERE id_log = v_id_log;

    BEGIN
    DELETE FROM  BDS_SALDOS_CIERRE  
	WHERE FECHA_PROCESO >=v_fe_cierre AND FECHA_PROCESO <= DATE_SUB(v_fe_cierre, INTERVAL p_dias DAY);

	DROP TABLE UNION_TABLE_FINAL_TEMP_1_TEST_BKP;
	
    CREATE TABLE UNION_TABLE_FINAL_TEMP_1_TEST_BKP AS
        WITH DIAS_FALTANTES AS (
            SELECT
                MAX(A.FEC_CALENDARIO) AS DIA_HABIL_ANTERIOR,
                B.FEC_CALENDARIO AS DIA_NO_HABIL
            FROM BDS_CALENDARIOS A
            INNER JOIN BDS_CALENDARIOS B
                ON A.COD_CALENDARIO = B.COD_CALENDARIO
                AND A.IND_DIA_HABIL = 'S'
                AND A.FEC_CALENDARIO < B.FEC_CALENDARIO
            WHERE B.COD_CALENDARIO = 1
                AND B.IND_DIA_HABIL = 'N'
                AND B.FEC_CALENDARIO <= v_fe_cierre
            GROUP BY B.FEC_CALENDARIO
            ),
            PROXIMO_DIA_HABIL AS (
                SELECT  MIN(FEC_CALENDARIO) AS FECHA
                FROM BDS_CALENDARIOS
                WHERE COD_CALENDARIO = 1
                AND FEC_CALENDARIO >v_fe_cierre     
            ),
            UNIVERSO AS
                (SELECT
                    A.FEC_CALENDARIO
                FROM BDS_CALENDARIOS A
                CROSS JOIN  PROXIMO_DIA_HABIL P
                WHERE
                    A.COD_CALENDARIO = 1
                    AND A.FEC_CALENDARIO > v_fe_cierre
                    AND A.FEC_CALENDARIO < P.FECHA
                ),
            BASE_0 AS (
            SELECT
                    CONCAT(
                        COD_EMPRESA,'|',
                        COD_MODULO,'|',
                        COD_SUCURSAL,'|',
                        COD_MONEDA,'|',
                        COD_PAPEL,'|',
                        NUM_CUENTA_BT,'|',
                        COD_OPERACION,'|',
                        COD_SUB_OPERACION,'|',
                        COD_TIPO_OPERACION
                        ) AS ID_OPERACION,
                    CONCAT(COD_EMPRESA,'|',
                        COD_SUCURSAL,'|',
                        COD_MONEDA,'|',
                        COD_PAPEL,'|',
                        NUM_CUENTA_BT,'|',
                        COD_OPERACION,'|',
                        COD_SUB_OPERACION,'|',
                        COD_TIPO_OPERACION
                    ) AS ID_SALDO_CONTABLE,
                    CONCAT(
                        COD_EMPRESA,'|',
                        COD_SUCURSAL,'|',
                        COD_MONEDA,'|',
                        COD_PAPEL,'|',
                        NUM_CUENTA_BT,'|',
                        COD_OPERACION,'|',
                        COD_SUB_OPERACION,'|'
                        ) AS ID_CRUCE,
                    FECHA_PROCESO
                    ,COD_EMPRESA
                    ,COD_SUCURSAL
                    ,COD_RUBRO
                    ,COD_MONEDA
                    ,COD_PAPEL
                    ,NUM_CUENTA_BT
                    ,COD_OPERACION
                    ,COD_SUB_OPERACION
                    ,COD_TIPO_OPERACION
                    ,COD_MODULO
                    ,COD_MODULO AS COD_MODULO_PRODUCTO
                    ,FEC_VENCIMIENTO
                    ,FEC_VALOR
                    ,IND_CATEGORIA_RIESGO
                    ,COD_ACTI_BCO_CENTRAL
                    ,COD_PRODUCTO
                    ,MTO_SALDO_ORIGEN
                    ,MTO_SALDO_MN
                    ,MTO_SALDO_ME
                    ,MTO_SALDO_MO
                    ,MTO_INTERES
                    ,MTO_PREVISIONES
	                ,FECHA_CARGA
	                ,FUENTE
	                ,BATCH_ID
	            FROM ODS_SALDOS_CIERRE
                WHERE FECHA_PROCESO >= DATE_SUB(v_fe_cierre, INTERVAL p_dias DAY)
                AND FECHA_PROCESO <= v_fe_cierre),
            BASE_1 AS (
                SELECT
                CONCAT(
                        B.COD_EMPRESA,'|',
                        B.COD_MODULO,'|',
                        B.COD_SUCURSAL,'|',
                        B.COD_MONEDA,'|',
                        B.COD_PAPEL,'|',
                        B.NUM_CUENTA_BT,'|',
                        B.COD_OPERACION,'|',
                        B.COD_SUB_OPERACION,'|',
                        B.COD_TIPO_OPERACION
                    ) AS ID_OPERACION,
                CONCAT(B.COD_EMPRESA,'|',
                        B.COD_SUCURSAL,'|',
                        B.COD_MONEDA,'|',
                        B.COD_PAPEL,'|',
                        B.NUM_CUENTA_BT,'|',
                        B.COD_OPERACION,'|',
                        B.COD_SUB_OPERACION,'|',
                        B.COD_TIPO_OPERACION
                    ) AS ID_SALDO_CONTABLE,
                CONCAT(
                        B.COD_EMPRESA,'|',
                        B.COD_SUCURSAL,'|',
                        B.COD_MONEDA,'|',
                        B.COD_PAPEL,'|',
                        B.NUM_CUENTA_BT,'|',
                        B.COD_OPERACION,'|',
                        B.COD_SUB_OPERACION,'|'
                    ) AS ID_CRUCE,
                A.DIA_NO_HABIL AS FECHA_PROCESO
                ,B.COD_EMPRESA
                ,B.COD_SUCURSAL
                ,B.COD_RUBRO
                ,B.COD_MONEDA
                ,B.COD_PAPEL
                ,B.NUM_CUENTA_BT
                ,B.COD_OPERACION
                ,B.COD_SUB_OPERACION
                ,B.COD_TIPO_OPERACION
                ,B.COD_MODULO
                ,B.COD_MODULO AS COD_MODULO_PRODUCTO
                ,B.FEC_VENCIMIENTO
                ,B.FEC_VALOR
                ,B.IND_CATEGORIA_RIESGO
                ,B.COD_ACTI_BCO_CENTRAL
                ,B.COD_PRODUCTO
                ,B.MTO_SALDO_ORIGEN
                ,B.MTO_SALDO_MN
                ,B.MTO_SALDO_ME
                ,B.MTO_SALDO_MO
                ,B.MTO_INTERES
                ,B.MTO_PREVISIONES
                ,B.FECHA_CARGA
                ,B.FUENTE
                ,B.BATCH_ID
            FROM  DIAS_FALTANTES A
            INNER JOIN ODS_SALDOS_CIERRE B
            ON A.DIA_HABIL_ANTERIOR = B.FECHA_PROCESO
            WHERE B.FECHA_PROCESO >= DATE_SUB(v_fe_cierre, INTERVAL p_dias DAY)
            ),
            BASE_2 AS(
                SELECT
                CONCAT(
                        B.COD_EMPRESA,'|',
                        B.COD_MODULO,'|',
                        B.COD_SUCURSAL,'|',
                        B.COD_MONEDA,'|',
                        B.COD_PAPEL,'|',
                        B.NUM_CUENTA_BT,'|',
                        B.COD_OPERACION,'|',
                        B.COD_SUB_OPERACION,'|',
                        B.COD_TIPO_OPERACION
                    ) AS ID_OPERACION,
                CONCAT(B.COD_EMPRESA,'|',
                        B.COD_SUCURSAL,'|',
                        B.COD_MONEDA,'|',
                        B.COD_PAPEL,'|',
                        B.NUM_CUENTA_BT,'|',
                        B.COD_OPERACION,'|',
                        B.COD_SUB_OPERACION,'|',
                        B.COD_TIPO_OPERACION
                    ) AS ID_SALDO_CONTABLE,
                CONCAT(
                        B.COD_EMPRESA,'|',
                        B.COD_SUCURSAL,'|',
                        B.COD_MONEDA,'|',
                        B.COD_PAPEL,'|',
                        B.NUM_CUENTA_BT,'|',
                        B.COD_OPERACION,'|',
                        B.COD_SUB_OPERACION,'|'
                    ) AS ID_CRUCE,     
            A.FEC_CALENDARIO AS FEC_PROCESO
            ,B.COD_EMPRESA
            ,B.COD_SUCURSAL
            ,B.COD_RUBRO
            ,B.COD_MONEDA
            ,B.COD_PAPEL
            ,B.NUM_CUENTA_BT
            ,B.COD_OPERACION
            ,B.COD_SUB_OPERACION
            ,B.COD_TIPO_OPERACION
            ,B.COD_MODULO
            ,B.COD_MODULO AS COD_MODULO_PRODUCTO
            ,B.FEC_VENCIMIENTO
            ,B.FEC_VALOR
            ,B.IND_CATEGORIA_RIESGO
            ,B.COD_ACTI_BCO_CENTRAL
            ,B.COD_PRODUCTO
            ,B.MTO_SALDO_ORIGEN
            ,B.MTO_SALDO_MN
            ,B.MTO_SALDO_ME
            ,B.MTO_SALDO_MO
            ,B.MTO_INTERES
            ,B.MTO_PREVISIONES
            ,B.FECHA_CARGA
            ,B.FUENTE
            ,B.BATCH_ID
            FROM UNIVERSO A
            CROSS JOIN ODS_SALDOS_CIERRE B
            WHERE FECHA_PROCESO = v_fe_cierre
            ),
            UNION_TABLE_FINAL AS (
            SELECT * FROM BASE_0
            UNION ALL
            SELECT  * FROM BASE_1
            UNION ALL
            SELECT * FROM BASE_2
            )
            SELECT * FROM UNION_TABLE_FINAL;


    INSERT INTO BDS_SALDOS_CIERRE(
    ID_OPERACION
        ,ID_SALDO_CONTABLE
        ,ID_CRUCE
        ,FECHA_PROCESO
        ,COD_EMPRESA
        ,COD_SUCURSAL
        ,COD_RUBRO
        ,COD_MONEDA
        ,COD_PAPEL
        ,NUM_CUENTA_BT
        ,COD_OPERACION
        ,COD_SUB_OPERACION
        ,COD_TIPO_OPERACION
        ,COD_MODULO
        ,COD_MODULO_PRODUCTO
        ,FEC_VENCIMIENTO
        ,FEC_VALOR
        ,IND_CATEGORIA_RIESGO
        ,COD_ACTI_BCO_CENTRAL
        ,COD_PRODUCTO
        ,MTO_SALDO_ORIGEN
        ,MTO_SALDO_MN
        ,MTO_SALDO_ME
        ,MTO_SALDO_MO
        ,MTO_INTERES
        ,MTO_PREVISIONES
        ,FUENTE
        ,FECHA_CARGA
        ,BATCH_ID
        ,COD_EJECUTIVO)
        SELECT ID_OPERACION
            ,ID_SALDO_CONTABLE
            ,ID_CRUCE
            ,FECHA_PROCESO
            ,COD_EMPRESA
            ,COD_SUCURSAL
            ,COD_RUBRO
            ,COD_MONEDA
            ,COD_PAPEL
            ,NUM_CUENTA_BT
            ,COD_OPERACION
            ,COD_SUB_OPERACION
            ,COD_TIPO_OPERACION
            ,COD_MODULO
            ,COD_MODULO_PRODUCTO
            ,FEC_VENCIMIENTO
            ,FEC_VALOR
            ,IND_CATEGORIA_RIESGO
            ,COD_ACTI_BCO_CENTRAL
            ,COD_PRODUCTO
            ,MTO_SALDO_ORIGEN
            ,MTO_SALDO_MN
            ,MTO_SALDO_ME
            ,MTO_SALDO_MO
            ,MTO_INTERES
            ,MTO_PREVISIONES
            ,FUENTE
            ,FECHA_CARGA
            ,BATCH_ID
            ,NULL AS COD_EJECUTIVO FROM UNION_TABLE_FINAL_TEMP_1_TEST_BKP;
        v_filas = ROW_COUNT();
        
        IF v_filas = 0 THEN
            v_msg_error = 'No se insertaron registros en BDS_SALDOS_CIERRE';
        END IF;
        
        IF v_msg_error IS NULL THEN
        
            UPDATE ctl_log_proceso
            SET estado = 'TERMINADO',
                fec_termino = NOW(6),
                filas_insertadas = v_filas
            WHERE id_log = v_id_log;
        
        ELSE
        
            UPDATE ctl_log_proceso
            SET estado = 'WARNING',
                fec_termino = NOW(6),
                filas_insertadas = v_filas,
                msg_error = v_msg_error
            WHERE id_log = v_id_log;
        
        END IF;
    END;


END //