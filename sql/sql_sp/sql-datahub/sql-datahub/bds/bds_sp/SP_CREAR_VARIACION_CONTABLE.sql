CREATE OR REPLACE PROCEDURE `DATAHUB`.`SP_CREAR_VARIACION_CONTABLE`(P_OPCION int(11) NULL, P_FECHA date NULL) RETURNS void DEFINER = '72731132'@'%' AS 
DECLARE 
	 V_NOMBRE_PROCESO  VARCHAR(200)= 'SP_REPORTE_CONTABLE_RESULTADO_05';
	 V_FECHA_VARIACION DATE;


BEGIN
    IF P_OPCION = 2 THEN

        /*
            CIERRE MENSUAL

            2026-04-30 vs 2026-03-31
            2026-01-31 vs 2025-12-31
        */

        SELECT MAX(FECHA_PROCESO)
        INTO V_FECHA_VARIACION
        FROM BDS_SALDOS_CIERRE_JARED
        WHERE FECHA_PROCESO < P_FECHA
          AND DAY(FECHA_PROCESO) = DAY(LAST_DAY(FECHA_PROCESO));

    ELSEIF P_OPCION = 3 THEN

        /*
            DIARIO

            2026-04-15 vs 2026-04-14
        */

        SELECT MAX(FECHA_PROCESO)
        INTO V_FECHA_VARIACION
        FROM BDS_SALDOS_CIERRE_JARED
        WHERE FECHA_PROCESO < P_FECHA;

    ELSEIF P_OPCION = 4 THEN

        /*
            DIARIO VS ULTIMO CIERRE MENSUAL

            2026-04-15 vs 2026-03-31
            2026-01-15 vs 2025-12-31
        */

        SELECT MAX(FECHA_PROCESO)
        INTO V_FECHA_VARIACION
        FROM BDS_SALDOS_CIERRE_JARED
        WHERE FECHA_PROCESO < P_FECHA
          AND DAY(FECHA_PROCESO) = DAY(LAST_DAY(FECHA_PROCESO));

    END IF;

	IF V_FECHA_VARIACION IS NULL THEN
	    RAISE;
	END IF;

    /*=========================================================
      LIMPIAR REPROCESO
    =========================================================*/

    DELETE
    FROM BDS_SALDOS_CIERRE_VARIACION
    WHERE FECHA_PROCESO = P_FECHA;

    /*=========================================================
      1. EXISTE EN AMBOS PERIODOS
    =========================================================*/

    INSERT INTO BDS_SALDOS_CIERRE_VARIACION
    (
        ID_OPERACION,
        FECHA_PROCESO,
        FECHA_PROCESO_MA,

        COD_EMPRESA,
        COD_SUCURSAL,
        COD_RUBRO,
        NUM_CUENTA_BT,

        SALDO_MN_MA,
        SALDO_MN_ACTUAL,
        SALDO_MN_VARIACION,

        SALDO_MO_MA,
        SALDO_MO_ACTUAL,
        SALDO_MO_VARIACION,
		
		COD_TIPO_OPERACION_ORIGEN,
		COD_MODULO_PRODUCTO,
		FLG_PRODUCTO_GASTO
    )
    SELECT
        a.ID_OPERACION,
        P_FECHA,
        V_FECHA_VARIACION,

        a.COD_EMPRESA,
        a.COD_SUCURSAL,
        a.COD_RUBRO,
        a.NUM_CUENTA_BT,

        b.MTO_SALDO_MN,
        a.MTO_SALDO_MN,
        a.MTO_SALDO_MN - b.MTO_SALDO_MN,

        b.MTO_SALDO_MO,
        a.MTO_SALDO_MO,
        a.MTO_SALDO_MO - b.MTO_SALDO_MO,
		a.COD_TIPO_OPERACION_ORIGEN,
		a.COD_MODULO_PRODUCTO,
		(Case 
			When a.COD_MODULO_PRODUCTO=20  Then (Case When a.COD_TIPO_OPERACION_ORIGEN=8  Then 10 Else 7 End)
			When a.COD_MODULO_PRODUCTO=21  Then 8
			When a.COD_MODULO_PRODUCTO=22  Then (Case When a.COD_TIPO_OPERACION_ORIGEN=10 Then 11 Else 9 End)
			When a.COD_MODULO_PRODUCTO=120 Then (Case When a.COD_TIPO_OPERACION_ORIGEN=30 Then 4  Else 3 End)
			When a.COD_MODULO_PRODUCTO=155 Then 10
			When a.COD_MODULO_PRODUCTO=185 Then (Case When a.COD_TIPO_OPERACION_ORIGEN=0  Then 5
													When a.COD_TIPO_OPERACION_ORIGEN=15 Then 3
													When a.COD_TIPO_OPERACION_ORIGEN=3  Then 10 Else 2 End)
			When a.COD_MODULO_PRODUCTO=321 Then 6
			Else 0 End) AS FLG_PRODUCTO_GASTO

    FROM BDS_SALDOS_CIERRE_JARED a
    INNER JOIN BDS_SALDOS_CIERRE_JARED b
           ON a.COD_EMPRESA        = b.COD_EMPRESA
          AND a.COD_SUCURSAL       = b.COD_SUCURSAL
          AND a.COD_RUBRO          = b.COD_RUBRO
          AND a.COD_MONEDA         = b.COD_MONEDA
          AND a.COD_PAPEL          = b.COD_PAPEL
          AND a.NUM_CUENTA_BT      = b.NUM_CUENTA_BT
          AND a.COD_OPERACION      = b.COD_OPERACION
          AND a.COD_SUB_OPERACION  = b.COD_SUB_OPERACION
          AND a.COD_TIPO_OPERACION = b.COD_TIPO_OPERACION
    WHERE a.FECHA_PROCESO = P_FECHA
      AND b.FECHA_PROCESO = V_FECHA_VARIACION;

    /*=========================================================
      2. EXISTIA EN FECHA_VARIACION
         Y NO EXISTE EN FECHA ACTUAL
    =========================================================*/

    INSERT INTO BDS_SALDOS_CIERRE_VARIACION
    (	ID_OPERACION,
        FECHA_PROCESO,
        FECHA_PROCESO_MA,

        COD_EMPRESA,
        COD_SUCURSAL,
        COD_RUBRO,
        NUM_CUENTA_BT,

        SALDO_MN_MA,
        SALDO_MN_ACTUAL,
        SALDO_MN_VARIACION,

        SALDO_MO_MA,
        SALDO_MO_ACTUAL,
        SALDO_MO_VARIACION,
		COD_TIPO_OPERACION_ORIGEN,
		COD_MODULO_PRODUCTO,
		FLG_PRODUCTO_GASTO
    )
    SELECT
		a.ID_OPERACION,
        P_FECHA,
        V_FECHA_VARIACION,

        a.COD_EMPRESA,
        a.COD_SUCURSAL,
        a.COD_RUBRO,
        a.NUM_CUENTA_BT,

        a.MTO_SALDO_MN,
        0,
        -a.MTO_SALDO_MN,

        a.MTO_SALDO_MO,
        0,
        -a.MTO_SALDO_MO,
		a.COD_TIPO_OPERACION_ORIGEN,
		a.COD_MODULO_PRODUCTO,
		(Case 
			When a.COD_MODULO_PRODUCTO=20  Then (Case When a.COD_TIPO_OPERACION_ORIGEN=8  Then 10 Else 7 End)
			When a.COD_MODULO_PRODUCTO=21  Then 8
			When a.COD_MODULO_PRODUCTO=22  Then (Case When a.COD_TIPO_OPERACION_ORIGEN=10 Then 11 Else 9 End)
			When a.COD_MODULO_PRODUCTO=120 Then (Case When a.COD_TIPO_OPERACION_ORIGEN=30 Then 4  Else 3 End)
			When a.COD_MODULO_PRODUCTO=155 Then 10
			When a.COD_MODULO_PRODUCTO=185 Then (Case When a.COD_TIPO_OPERACION_ORIGEN=0  Then 5
													When a.COD_TIPO_OPERACION_ORIGEN=15 Then 3
													When a.COD_TIPO_OPERACION_ORIGEN=3  Then 10 Else 2 End)
			When a.COD_MODULO_PRODUCTO=321 Then 6
			Else 0 End) AS FLG_PRODUCTO_GASTO

    FROM BDS_SALDOS_CIERRE_JARED a
    LEFT JOIN BDS_SALDOS_CIERRE_JARED b
           ON a.COD_EMPRESA        = b.COD_EMPRESA
          AND a.COD_SUCURSAL       = b.COD_SUCURSAL
          AND a.COD_RUBRO          = b.COD_RUBRO
          AND a.COD_MONEDA         = b.COD_MONEDA
          AND a.COD_PAPEL          = b.COD_PAPEL
          AND a.NUM_CUENTA_BT      = b.NUM_CUENTA_BT
          AND a.COD_OPERACION      = b.COD_OPERACION
          AND a.COD_SUB_OPERACION  = b.COD_SUB_OPERACION
          AND a.COD_TIPO_OPERACION = b.COD_TIPO_OPERACION
          AND b.FECHA_PROCESO      = P_FECHA
    WHERE a.FECHA_PROCESO = V_FECHA_VARIACION
      AND b.ID_OPERACION IS NULL;

    /*=========================================================
      3. EXISTE EN FECHA ACTUAL
         Y NO EXISTIA EN FECHA_VARIACION
    =========================================================*/

    INSERT INTO BDS_SALDOS_CIERRE_VARIACION
    (
        ID_OPERACION,
        FECHA_PROCESO,
        FECHA_PROCESO_MA,

        COD_EMPRESA,
        COD_SUCURSAL,
        COD_RUBRO,
        NUM_CUENTA_BT,

        SALDO_MN_MA,
        SALDO_MN_ACTUAL,
        SALDO_MN_VARIACION,

        SALDO_MO_MA,
        SALDO_MO_ACTUAL,
        SALDO_MO_VARIACION,
		COD_TIPO_OPERACION_ORIGEN,
		COD_MODULO_PRODUCTO,
		FLG_PRODUCTO_GASTO
		
    )
    SELECT
        a.ID_OPERACION,
        P_FECHA,
        NULL,

        a.COD_EMPRESA,
        a.COD_SUCURSAL,
        a.COD_RUBRO,
        a.NUM_CUENTA_BT,

        0,
        a.MTO_SALDO_MN,
        a.MTO_SALDO_MN,

        0,
        a.MTO_SALDO_MO,
        a.MTO_SALDO_MO,
		a.COD_TIPO_OPERACION_ORIGEN,
		a.COD_MODULO_PRODUCTO,
		(Case 
			When a.COD_MODULO_PRODUCTO=20  Then (Case When a.COD_TIPO_OPERACION_ORIGEN=8  Then 10 Else 7 End)
			When a.COD_MODULO_PRODUCTO=21  Then 8
			When a.COD_MODULO_PRODUCTO=22  Then (Case When a.COD_TIPO_OPERACION_ORIGEN=10 Then 11 Else 9 End)
			When a.COD_MODULO_PRODUCTO=120 Then (Case When a.COD_TIPO_OPERACION_ORIGEN=30 Then 4  Else 3 End)
			When a.COD_MODULO_PRODUCTO=155 Then 10
			When a.COD_MODULO_PRODUCTO=185 Then (Case When a.COD_TIPO_OPERACION_ORIGEN=0  Then 5
													When a.COD_TIPO_OPERACION_ORIGEN=15 Then 3
													When a.COD_TIPO_OPERACION_ORIGEN=3  Then 10 Else 2 End)
			When a.COD_MODULO_PRODUCTO=321 Then 6
			Else 0 End) AS FLG_PRODUCTO_GASTO	

    FROM BDS_SALDOS_CIERRE_JARED a
    LEFT JOIN BDS_SALDOS_CIERRE_JARED b
           ON a.COD_EMPRESA        = b.COD_EMPRESA
          AND a.COD_SUCURSAL       = b.COD_SUCURSAL
          AND a.COD_RUBRO          = b.COD_RUBRO
          AND a.COD_MONEDA         = b.COD_MONEDA
          AND a.COD_PAPEL          = b.COD_PAPEL
          AND a.NUM_CUENTA_BT      = b.NUM_CUENTA_BT
          AND a.COD_OPERACION      = b.COD_OPERACION
          AND a.COD_SUB_OPERACION  = b.COD_SUB_OPERACION
          AND a.COD_TIPO_OPERACION = b.COD_TIPO_OPERACION
          AND b.FECHA_PROCESO      = V_FECHA_VARIACION
    WHERE a.FECHA_PROCESO = P_FECHA
      AND b.ID_OPERACION IS NULL;
	  
END;