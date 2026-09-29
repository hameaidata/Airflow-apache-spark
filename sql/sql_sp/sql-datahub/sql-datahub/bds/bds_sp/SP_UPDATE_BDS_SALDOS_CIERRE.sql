

DELIMITER //

CREATE PROCEDURE SP_BDS_SALDOS_CIERRE_PROMEDIOS_final_1 (
    p_tipo_ejecucion VARCHAR(20),
    p_fe_cierre DATE,
    p_fe_cierre_siguiente DATE,
    p_fecha_proceso DATE
)
AS
DECLARE
    v_tabla_origen      VARCHAR(200) DEFAULT 'BDS_CALENDARIOS,BDS_SALDOS_CIERRE';
    v_tabla_destino     VARCHAR(200) DEFAULT 'BDS_SALDOS_PROMEDIOS_MENSUALES';
    v_nom_proceso       VARCHAR(200) DEFAULT 'SP_BDS_SALDOS_CIERRE_PROMEDIOS';
    v_id_log        BIGINT DEFAULT 0;
	v_filas         BIGINT DEFAULT 0;
    v_msg_error VARCHAR(5000) DEFAULT NULL;
    v_fe_cierre  DATE;
    v_fe_cierre_siguiente  DATE;
    v_fecha_proceso  DATE;
BEGIN
	v_msg_error = NULL;
	-- LOGICA DE ASIGANICION EN LA FORMA QUE SE DEBE DE EJECURTAR
    IF UPPER(p_tipo_ejecucion) = 'MANUAL' THEN

        v_fe_cierre = p_fe_cierre;
        v_fe_cierre_siguiente = p_fe_cierre_siguiente;
        v_fecha_proceso = p_fecha_proceso;

        DELETE
        FROM BDS_SALDOS_CIERRE_FINAL_1
        WHERE FECHA_PROCESO = v_fe_cierre;

    ELSEIF UPPER(p_tipo_ejecucion) = 'FULL' THEN

        SELECT FECHA_PROCESO
        INTO v_fe_cierre
        FROM ODS_PARAMETROS_CIERRE;

        SELECT FEC_APERTURA
        INTO v_fe_cierre_siguiente
        FROM ODS_PARAMETROS_CIERRE;

        v_fecha_proceso = NULL;

        TRUNCATE TABLE BDS_SALDOS_CIERRE_FINAL_1;

    ELSEIF UPPER(p_tipo_ejecucion) = 'AUTO' THEN

        SELECT MAX(FECHA_PROCESO)
        INTO v_fecha_proceso
        FROM ODS_SALDOS_CIERRE;

        v_fe_cierre = v_fecha_proceso;

        SELECT DATE_ADD(v_fe_cierre, INTERVAL 1 DAY)
        INTO v_fe_cierre_siguiente;

        DELETE
        FROM BDS_SALDOS_CIERRE_FINAL_1
        WHERE FECHA_PROCESO = v_fe_cierre;

    END IF;

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

    DROP TABLE DIAS_FALTANTES;
    CREATE TABLE DIAS_FALTANTES AS
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
        
    )
    SELECT * FROM DIAS_FALTANTES;

    DROP TABLE UNIVERSO;
    CREATE TABLE UNIVERSO AS
    WITH 
    UNIVERSO AS 
        (SELECT 
            B.FEC_CALENDARIO
        FROM BDS_CALENDARIOS A
        INNER JOIN BDS_CALENDARIOS B
        ON A.COD_CALENDARIO = B.COD_CALENDARIO
        WHERE 
            A.COD_CALENDARIO = 1
            AND B.FEC_CALENDARIO > v_fe_cierre
            AND B.FEC_CALENDARIO < v_fe_cierre_siguiente
    )
    
    SELECT * FROM UNIVERSO;
    -- =======================================================================
    --  FILTRADO DE REGISTROS DESDE UNA FECHA_PROCESO >v_fecha_inicio
    --  FILTR DE REGISTROS DESDE UNA FECHA_PROGRESO >v_fecha_fin
    -- =======================================================================
    -- =======================================================================
    --  INSERCION DE DATOS EN BDS_SALDOS_CIERRE
    --
    -- =======================================================================
    CREATE TABLE BDS_OPERACIONES_COD_NODULO_PRODUCTO AS
    SELECT DISTINCT  
                        CONCAT(
                COD_EMPRESA,'|',
                COD_SUCURSAL,'|',
                COD_MONEDA,'|',
                COD_PAPEL,'|',
                NUM_CUENTA_BT,'|',
                COD_OPERACION,'|',
                COD_SUB_OPERACION,'|'
                ) AS ID_CRUCE,
                COD_EMPRESA,
                COD_SUCURSAL,
                COD_MONEDA,
                COD_PAPEL,
                NUM_CUENTA_BT,
                COD_OPERACION,
                COD_SUB_OPERACION,
                COD_MODULO
                FROM (
                SELECT DISTINCT COD_EMPRESA,
                    COD_SUCURSAL,
                    COD_MONEDA,
                    COD_PAPEL,
                    NUM_CUENTA_BT,
                    COD_OPERACION,
                    COD_SUB_OPERACION,
                    COD_MODULO
                FROM BDS_OPERACIONES
                WHERE NUM_CUENTA_BT <>'999999999'
                -- AND IND_ACTIVO =1
                GROUP BY COD_EMPRESA,
                            COD_SUCURSAL,
                            COD_MONEDA,
                            COD_PAPEL,
                            NUM_CUENTA_BT,
                            COD_OPERACION,
                            COD_SUB_OPERACION,
                            COD_MODULO)
            WHERE NUM_CUENTA_BT <>'999999999'
                GROUP BY COD_EMPRESA,
                        COD_SUCURSAL,
                        COD_MONEDA,
                        COD_PAPEL,
                        NUM_CUENTA_BT,
                        COD_OPERACION,
                        COD_SUB_OPERACION
    ORDER BY NUM_CUENTA_BT, COD_OPERACION;

    -- ===================================================
    -- ACTUALIZACION DE REGISTROS 
    --
    -- ===================================================

    UPDATE BDS_SALDOS_CIERRE t1
        JOIN (
            SELECT
                ID_CRUCE,
                MAX(COD_MODULO) AS COD_MODULO
            FROM BDS_OPERACIONES_COD_NODULO_PRODUCTO
            GROUP BY ID_CRUCE
            HAVING COUNT(*) = 1
        ) t2
            ON t1.ID_CRUCE = t2.ID_CRUCE
        SET t1.COD_MODULO_PRODUCTO = t2.COD_MODULO;    
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


END //
