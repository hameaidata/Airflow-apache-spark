DELIMITER $$

CREATE OR REPLACE PROCEDURE SP_CU_REPORTE_DETALLE_PRODUCTO()
AS

DECLARE v_fecha_proceso DATE;

BEGIN 
    SELECT DATE_FORMAT(
        COALESCE(DATE_SUB(MAX(FECHA_PROCESO), INTERVAL 60 DAY),
        '%Y-%m-01')
    )
    INTO v_fecha_proceso
    FROM CU_REPORTE_POSICION_ME;

    DELETE FROM CU_REPORTE_POSICION_ME
    WHERE FECHA_PROCESO >= v_fecha_proceso

    v_tabla_orige = 'CU_REPORTE_BALANCE_GENERAL Y CU_REPORTE_RESULTADOS'
    v_tabla_destino ='CU_REPORTE_POSICION_ME'
    v_msg_error= NULL,
    INSERT INTO ctl_log_proceso (nom_proceso, tabla_origen, tabla_destino, fec_inicio, estado)
    VALUES (v_nom_proceso, v_tabla_origen, v_tabla_destino, NOW(6), 'INICIADO');

    v_id_log = LAST_INSERT_ID();
    UPDATE ctl_log_proceso
        SET estado = 'EJECUTANDO'
    WHERE id_log = v_id_log;
    BEGIN
        INSERT INTO CU_REPORTE_POSICION_ME(
            FECHA_PROCESO,
            NIVEL_01,
            NIVEL_02,
            COD_RUBRO_SBS,
            COD_RUBRO_BT,
            DES_RUBRO,
            COD_MONEDA_SALDO,
            COD_MONEDA_RUBRO,
            MTO_SALDO_MO,
            MTO_SALDO_ME,
            MTO_SALDO_MN,
            MTO_SALDO_MES_MO,
            MTO_SALDO_MES_ME,
            MTO_SALDO_MES_MN
        )
        WITH BASE_1 AS (
            SELECT FECHA_PROCESO,
                'POSICION ME' AS NIVEL_01,
                'CONTABLE' AS NIVEL_02,
                COD_RUBRO_SBS,
                COD_RUBRO_BT,
                DES_RUBRO,
                COD_MONEDA_SALDO,
                COD_MONEDA_RUBRO,
                SALDO_MO AS TO_MTO_SALDO_MO,
                SALDO_ME AS MTO_SALDO_ME,
                SALDO_MN AS MTO_SALDO_MN,
                NULL  MTO_SALDO_MES_MO,
                NULL MTO_SALDO_MES_ME,
                NULL MTO_SALDO_MES_MN
            FROM CU_REPORTE_BALANCE_GENERAL
            WHERE FECHA_PROCESO >= v_fecha_proceso 
            AND TIPO_CUENTA='POSICION ME'
        ),
        BASE_2 AS (
            SELECT 
                FECHA_PROCESO,
                'POSICION ME' AS NIVEL_01,
                'DERIVADOS' AS NIVEL_02,
                COD_RUBRO_SBS,
                COD_RUBRO_BT,
                DES_RUBRO,
                COD_MONEDA_SALDO,
                COD_MONEDA_RUBRO,
                SALDO_MO AS MTO_SALDO_MO,
                SALDO_ME AS MTO_SALDO_ME,
                SALDO_MN AS MTO_SALDO_MN,
                NULL AS MTO_SALDO_MES_MO,
                NULL AS MTO_SALDO_MES_ME,
                NULL AS MTO_SALDO_MES_MN
            FROM  CU_REPORTE_BALANCE_GENERAL
            WHERE FECHA_PROCESO >= v_fecha_proceso
            AND TIPO_GRUPO = 'POSICION NETA DE FORWARDS'
        ),
        BASE_3 AS (
            SELECT	
                FECHA_PROCESO,
                'UTILIDAD' NIVEL_01,
                'DERIVADOS' NIVEL_02,
                COD_RUBRO_SBS,
                COD_RUBRO,
                DES_RUBRO,
                COD_MONEDA_SALDO,
                COD_MONEDA_RUBRO,
                MTO_SALDO_MO,
                MTO_SALDO_ME,
                MTO_SALDO_MN,
                MTO_SALDO_MES_MO,
                MTO_SALDO_MES_ME,
                MTO_SALDO_MES_MN
            FROM CU_REPORTE_RESULTADOS
            WHERE FECHA_PROCESO >= v_fecha_proceso AND
            DES_RUBRO_ORDEN_2 = 'Derivados de Negociación'

        ),
        BASE_4 AS (
            SELECT	FECHA_PROCESO,
                'UTILIDAD' NIVEL_01,
                'CONTABLE' NIVEL_02,
                COD_RUBRO_SBS,
                COD_RUBRO,
                DES_RUBRO,
                COD_MONEDA_SALDO,
                COD_MONEDA_RUBRO,
                MTO_SALDO_MO,
                MTO_SALDO_ME,
                MTO_SALDO_MN,
                MTO_SALDO_MES_MO,
                MTO_SALDO_MES_ME,
                MTO_SALDO_MES_MN
            FROM CU_REPORTE_RESULTADOS
            WHERE FECHA_PROCESO >= v_fecha_proceso AND
                DES_RUBRO_ORDEN_2 = 'Utilidad-Pérdida en Diferencia de Cambio'

        ),
        UNION_TABLAS AS (
            SELECT * FROM BASE_1 
            UNION ALL
            SELECT * FROM BASE_2
            UNION ALL
            SELECT  * FROM BASE_3
            UNION ALL
            SELECT * FROM BASE_4
        )
        SELECT * FROM UNION_TABLAS;
    v_filas = ROW_COUNT()
    IF v_filas=0 THEN
        SET v_msg_error = "No se insertaron registros en CU_REPORTE_POSICION_ME"
    END IF;
    EXCEPTION WHEN OTHERS THEN
        v_msg_error = CONCAT('[CU_REPORTE_POSICION_ME] ', exception_message());
    END;
    UPDATE ctl_log_proceso
       SET estado = CASE
                        WHEN v_msg_error IS NULL
                        THEN 'TERMINADO'
                        ELSE 'WARNING'
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
END$$

DELIMITER ;
