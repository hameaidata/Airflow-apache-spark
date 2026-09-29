DELIMITER $$

CREATE OR REPLACE PROCEDURE SP_CU_REPORTE_DETALLE_PRODUCTO()
AS

DECLARE v_fecha_proceso DATE;


BEGIN 
    SELECT  DATE_FORMAT(
        COALESCE(DATE_SUB(FECHA_PROCESO), INTERVAL 60 DAY)
        '%Y-%m-01'
    )
    INTO v_fecha_proceso
    FROM CU_REPORTE_DETALLE_PROCESO;

    --======================================
    --|          LIMPIEZA                  |
    --======================================
    DELETE FROM CU_REPORTE_DETALLE_PROCESO
    WHERE FECHA_PROCESO >=v_fecha_proceso

    v_tabla_origen = '';
    V_tabla_destino ='CU_REPORTE_DETALLE_PROCESO'; 
    v_msg_error = NULL;

    INSERT INTO ctl_log_proceso (nom_proceso, tabla_origen, tabla_destino, fec_inicio, estado)
    VALUES (v_nom_proceso, v_tabla_origen, v_tabla_destino, NOW(6), 'INICIADO');

    v_id_log = LAST_INSERT_ID();
    UPDATE ctl_log_proceso
        SET estado = 'EJECUTANDO'
    WHERE id_log = v_id_log;

    BEGIN 
        INSERT INTO CU_REPORTE_DETALLE_PROCESO(
            FECHA_PROCESO,
            NIVEL_01,
            NIVEL_02,
            NIVEL_03,
            COD_RUBRO_SBS,
            COD_RUBRO_BT,
            DES_RUBRO,
            COD_MONEDA_SALDO,
            COD_MONEDA_RUBRO,
            DES_MONEDA,
            MTO_SALDO_MO,
            MTO_SALDO_ME,
            MTO_SALDO_MN,
            MTO_SALDO_MES_MO,
            MTO_SALDO_MES_ME,
            MTO_SALDO_MES_MN
        )
        -- CARTERA
        WITH TB_CARTERA AS (
            SELECT	BSDC.FECHA_PROCESO,
                'CARTERA' AS NIVEL_01,
                'CARTERA' NIVEL_02,
                CASE WHEN BMRO.COD_MODULO = '239' THEN 'Convenios'
                    WHEN BMRO.COD_MODULO = '102' THEN 'Hipotecario'
                    WHEN BMRO.COD_MODULO = '111' AND B.COD_GRUPO = '9' THEN 'IFIS'
                    WHEN BMRO.COD_MODULO = '111' AND B.COD_GRUPO IN (10,11,12,13) THEN 'Capital de Trabajo'
                    WHEN BMRO.COD_MODULO IN ('101','140') THEN 'Mi Vivienda'
                    WHEN BMRO.COD_MODULO = '115' AND B.COD_GRUPO IN (10,11,12,13) THEN 'Mediano Plazo'
                    WHEN BMRO.COD_MODULO = '36' THEN 'Leasing'
                    WHEN BMRO.COD_MODULO = '103' THEN 'Préstamo Personal'
                    WHEN BMRO.COD_MODULO IN ('112','113','114') THEN 'Comex'
                    WHEN BMRO.COD_MODULO = '65' THEN 'Tarjeta'
                    WHEN BMRO.COD_MODULO = '139' THEN 'Confirming'
                    WHEN BMRO.COD_MODULO = '71' THEN 'Descuento de Letras'
                    WHEN BMRO.COD_MODULO = '117' THEN 'Créditos por Liquidar'
                    WHEN BMRO.COD_MODULO = '116' THEN 'Financiamiento Inmobiliario'
                    WHEN BMRO.COD_MODULO = '149' THEN 'Factoring'
                    WHEN BMRO.COD_MODULO = '142' THEN 'Activo Fijo Bien Mueble'
                ELSE NULL
                END AS NIVEL_03,
                B.COD_RUBRO_SBS,
                B.COD_RUBRO,
                B.DES_RUBRO,
                BSDC.COD_MONEDA,
                SUBSTRING(B.COD_RUBRO,3,1) AS COD_MONEDA_RUBRO,
                BM.DES_MONEDA,
                BSDC.MTO_SALDO_MO * - 1 AS MTO_SALDO_MO,
                BSDC.MTO_SALDO_ME * - 1 AS MTO_SALDO_ME,
                BSDC.MTO_SALDO_MN * - 1 AS MTO_SALDO_MN,
                BSDC.MTO_SALDO_MES_MO * - 1 AS MTO_SALDO_MES_MO,
                BSDC.MTO_SALDO_MES_ME * - 1 AS MTO_SALDO_MES_ME,
                BSDC.MTO_SALDO_MES_MN * - 1 AS MTO_SALDO_MES_MN
            FROM BDS_SALDOS_DIARIOS_CONSOLIDADOS  BSDC
            LEFT JOIN(
                    SELECT FECHA_PROCESO,
                            COD_MODULO,
                            COD_RUBRO_RELACIONADO,
                            NOM_RUBRO_RELACIONADO,
                            FUENTE,
                            FECHA_CARGA
                    FROM BDS_MODULO_RUBRO_OPERATIVO
                    WHERE COD_MODULO IN (36 ,49 ,65 ,71 ,101,102,103,105,106,111,112,113,114,115,116,117,139,140,141,142,149,151,152,239) AND 
                        COD_RUBRO_RELACIONADO LIKE '14%') BMRO ON 
                        BSDC.COD_RUBRO = BMRO.COD_RUBRO_RELACIONADO
            LEFT JOIN  	BDS_MONEDA BM ON BM.COD_MONEDA = BSDC.COD_MONEDA
            LEFT JOIN 	BDS_CONSOLIDADO_SBS_BT B ON BSDC.COD_RUBRO = B.COD_RUBRO
            WHERE BSDC.FECHA_PROCESO >= v_fecha_proceso AND
                ((BMRO.COD_MODULO IN ('239','102','101','140','36','103','65','139','71','117','116','149','142',
                '112','113','114')) OR
                (BMRO.COD_MODULO = '115' AND B.COD_GRUPO IN (10,11,12,13))	OR
                (BMRO.COD_MODULO = '111' AND B.COD_GRUPO IN (10,11,12,13))	OR
                (BMRO.COD_MODULO = '111' AND B.COD_GRUPO = '9')) AND
                (B.DIG4 LIKE '14_1' OR B.DIG4 LIKE '14_3' OR B.DIG4 LIKE '14_4' OR B.DIG4 LIKE '14_5' OR B.DIG4 LIKE '14_6')
        ),
        TB_PASIVOS AS (
            SELECT
                BSDC.FECHA_PROCESO,
                        'PRODUCTOS PASIVOS' AS NIVEL_01,
                        CASE
                            WHEN B.DIG4 LIKE '21_1'
                                OR DIG6 LIKE '21_801'
                                OR B.DIG4 LIKE '21_2'
                                OR DIG6 LIKE '21_802'
                                OR B.DIG4 LIKE '21_3'
                                OR DIG6 LIKE '21_803'
                                OR B.DIG4 LIKE '21_6'
                                OR DIG4 LIKE '21_7'
                                OR DIG6 LIKE '21_807'
                            THEN 'OBLIGACIONES CON EL PUBLICO'
                            WHEN B.DIG4 LIKE '23_1'
                                OR B.DIG6 LIKE '23_801'
                                OR B.DIG6 LIKE '23_802'
                                OR B.DIG4 LIKE '23_2'
                                OR B.DIG4 LIKE '23_3'
                                OR B.DIG6 LIKE '23_803'
                            THEN 'DEPOSITOS DE EMPRESAS DEL SIST FINANCIERO Y ORG FINANC'
                    ELSE NULL
                END AS NIVEL_02,
                        CASE
                    WHEN B.DIG4 LIKE '21_1'
                    OR DIG6 LIKE '21_801' THEN 'Obligaciones a la Vista'
                    WHEN B.DIG4 LIKE '21_3'
                    OR DIG6 LIKE '21_803' THEN 'Obligaciones por Cuentas a Plazo'
                    WHEN B.DIG4 LIKE '21_2'
                    OR DIG6 LIKE '21_802' THEN 'Obligaciones por Cuentas de Ahorro'
                    WHEN B.DIG4 LIKE '21_6'
                    OR DIG4 LIKE '21_7'
                    OR DIG6 LIKE '21_807' THEN 'Obligaciones Restringidas (DPF, Cta Cte, Ahorro) y CTS Propios'
                    WHEN B.DIG4 LIKE '23_1'
                    OR B.DIG6 LIKE '23_801' THEN 'Cuenta Corriente'
                    WHEN B.DIG4 LIKE '23_2'
                    OR B.DIG6 LIKE '23_802' THEN 'Cuenta Ahorro'
                    WHEN B.DIG4 LIKE '23_3'
                    OR B.DIG6 LIKE '23_803' THEN 'Depósitos a Plazo'
                    ELSE NULL
                END AS NIVEL_03,
                        B.COD_RUBRO_SBS,
                        B.COD_RUBRO,
                        B.DES_RUBRO,
                        BSDC.COD_MONEDA,
                        SUBSTRING(B.COD_RUBRO, 3, 1) AS COD_MONEDA_RUBRO,
                        BM.DES_MONEDA,
                        BSDC.MTO_SALDO_MO,
                        BSDC.MTO_SALDO_ME,
                        BSDC.MTO_SALDO_MN,
                        BSDC.MTO_SALDO_MES_MO,
                        BSDC.MTO_SALDO_MES_ME,
                        BSDC.MTO_SALDO_MES_MN
            FROM
                BDS_SALDOS_DIARIOS_CONSOLIDADOS BSDC
            LEFT JOIN BDS_MONEDA BM ON
                BM.COD_MONEDA = BSDC.COD_MONEDA
            LEFT JOIN BDS_CONSOLIDADO_SBS_BT B ON
                BSDC.COD_RUBRO = B.COD_RUBRO
            WHERE
                BSDC.FECHA_PROCESO >= v_fecha_proceso
                AND(B.DIG4 LIKE '21_1' OR
                        B.DIG6 LIKE '21_801' OR
                        B.DIG4 LIKE '21_2' OR
                        B.DIG6 LIKE '21_802' OR
                        B.DIG4 LIKE '21_3' OR
                        B.DIG6 LIKE '21_803' OR
                        B.DIG4 LIKE '21_6' OR
                        B.DIG4 LIKE '21_7' OR 
                        B.DIG6 LIKE '21_807' OR
                        B.DIG4 LIKE '23_1' OR
                        B.DIG6 LIKE '23_801' OR
                        B.DIG6 LIKE '23_802' OR
                        B.DIG4 LIKE '23_2' OR
                        B.DIG4 LIKE '23_3' OR
                        B.DIG6 LIKE '23_803')
        ),
        TB_ADEUDOS AS (
            SELECT
                BSDC.FECHA_PROCESO,
                'ADEUDADOS' AS NIVEL_01,
                'ADEUDADOS' AS NIVEL_02,
                CASE
                    WHEN B.DIG10 LIKE '2612020101' THEN 'MiVivienda'
                    WHEN B.DIG10 LIKE '26_8020101' THEN 'BANCOLDEX'
                    WHEN B.DIG6 LIKE '262202' THEN 'COFIDE'
                    ELSE NULL
                END AS NIVEL_03,
                B.COD_RUBRO_SBS,
                B.COD_RUBRO,
                B.DES_RUBRO,
                BSDC.COD_MONEDA,
                SUBSTRING(B.COD_RUBRO, 3, 1) AS COD_MONEDA_RUBRO,
                BM.DES_MONEDA,
                BSDC.MTO_SALDO_MO,
                BSDC.MTO_SALDO_ME,
                BSDC.MTO_SALDO_MN,
                BSDC.MTO_SALDO_MES_MO,
                BSDC.MTO_SALDO_MES_ME,
                BSDC.MTO_SALDO_MES_MN
            FROM
                BDS_SALDOS_DIARIOS_CONSOLIDADOS BSDC
            LEFT JOIN BDS_MONEDA BM ON
                BM.COD_MONEDA = BSDC.COD_MONEDA
            LEFT JOIN BDS_CONSOLIDADO_SBS_BT B ON
                BSDC.COD_RUBRO = B.COD_RUBRO
            WHERE
                BSDC.FECHA_PROCESO >= v_fecha_proceso
                AND (B.DIG10 LIKE '2612020101'
                    OR B.DIG10 LIKE '26_8020101'
                    OR B.DIG6 LIKE '262202')
        ),
        TB_REPOS AS (
            SELECT BSDC.FECHA_PROCESO,
                'REPOS' AS NIVEL_01,
                'REPOS' AS NIVEL_02,
                CASE 	WHEN B.COD_RUBRO IN ('2514110101001001','2514110101001002') THEN 'Repos de Valores'
                        WHEN B.COD_RUBRO = '2514110101001003' THEN 'Repos de Divisas'
                ELSE NULL
                END AS NIVEL_03,
                B.COD_RUBRO_SBS,
                B.COD_RUBRO,
                B.DES_RUBRO,
                BSDC.COD_MONEDA,
                SUBSTRING(B.COD_RUBRO,3,1) AS COD_MONEDA_RUBRO,
                BM.DES_MONEDA,
                BSDC.MTO_SALDO_MO,
                BSDC.MTO_SALDO_ME,
                BSDC.MTO_SALDO_MN,
                BSDC.MTO_SALDO_MES_MO,
                BSDC.MTO_SALDO_MES_ME,
                BSDC.MTO_SALDO_MES_MN
            FROM BDS_SALDOS_DIARIOS_CONSOLIDADOS  BSDC
            LEFT JOIN BDS_MONEDA BM ON BM.COD_MONEDA = BSDC.COD_MONEDA
            LEFT JOIN BDS_CONSOLIDADO_SBS_BT B ON BSDC.COD_RUBRO = B.COD_RUBRO
            WHERE BSDC.FECHA_PROCESO >= v_fecha_proceso
                AND (B.DIG10 LIKE '25_4110101')
        ),
        UNION_TABLAS AS (
            SELECT * FROM TB_CARTERA
            UNION ALL
            SELECT * FROM TB_PASIVOS
            UNION ALL
            SELECT * FROM TB_ADEUDOS
            UNION ALL
            SELECT * FROM TB_REPOS
        )
        SELECT * FROM UNION_TABLAS;
    v_filas =ROW_COUNT();
    IF v_filas= 0 THEN
        SET v_msg_error= "No se insertarin registro en CU_REPORTE_DETALLE_PROCESO"

    END IF;
    EXCEPTION WHEN OTHERS THEN
        v_msg_error = CONCAT('[CU_REPORTE_DETALLE_PROCESO] ', exception_message());
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