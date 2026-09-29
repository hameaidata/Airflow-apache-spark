DELIMITER //

CREATE OR REPLACE PROCEDURE SP_BDS_MAESTRO_TABLAS() AS
DECLARE

    -- =========================================================
    -- VARIABLES GENERALES
    -- =========================================================
    v_nom_proceso   VARCHAR(200) DEFAULT 'SP_ODS_MAESTRO_TABLAS';
    v_tabla_origen  VARCHAR(200);
    v_tabla_destino VARCHAR(200);
    v_id_log        BIGINT DEFAULT 0;
    v_filas         BIGINT DEFAULT 0;
    v_msg_error     TEXT DEFAULT NULL;

    -- ==============================================================================
    -- INSERTANDO REGISTROS ODS_MODULO_PRODUCTO
    -- ==============================================================================
BEGIN
    v_tabla_origen  = 'STG_FBC206';
    v_tabla_destino = 'ODS_MODULO_PRODUCTO';
    v_msg_error     = NULL;

    INSERT INTO ctl_log_proceso
    (
        nom_proceso,
        tabla_origen,
        tabla_destino,
        fec_inicio,
        estado
    )
    VALUES
    (
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

    TRUNCATE TABLE ODS_MODULO_PRODUCTO;

    INSERT INTO ODS_MODULO_PRODUCTO
    (
        FECHA_PROCESO,
        COD_EMPRESA,
        NUM_CODIGO,
        COD_MODULO,
        COD_TIPO_OPERACION,
        BC206ID3,
        BC206ID4,
        TIP_REGISTRO,
        TIP_RELACION,
        TIP_NEGOCIO,
        TIP_BALANCE,
        DES_MODULO,
        DES_TIPO_OPERACION,
        FUENTE,
        FECHA_CARGA,
        BATCH_ID
    )
    SELECT
        A.FECHA_PROCESO,
        A.BC205EMP,
        A.BC205COD,
        A.BC206ID1,
        A.BC206ID2,
        A.BC206ID3,
        A.BC206ID4,
        A.BC206NRO1,
        A.BC206NRO2,
        A.BC206NRO3,
        A.BC206CHR1,
        A.BC206CHR2,
        A.BC206CHR3,
        A.FUENTE,
        A.FECHA_CARGA,
        A.BATCH_ID
    FROM STG_FBC206 A
    WHERE A.BC205COD = 6007;

    v_filas = ROW_COUNT();

    IF v_filas = 0 THEN
        SET v_msg_error = 'No se insertaron registros en ODS_MODULO_PRODUCTO';
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


    -- ==============================================================================
    -- INSERTANDO REGISTROS ODS_SEGMENTOS
    -- ==============================================================================

    v_tabla_origen  = 'STG_FBC206';
    v_tabla_destino = 'ODS_SEGMENTOS';
    v_msg_error     = NULL;

    INSERT INTO ctl_log_proceso
    (
        nom_proceso,
        tabla_origen,
        tabla_destino,
        fec_inicio,
        estado
    )
    VALUES
    (
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

    TRUNCATE TABLE ODS_SEGMENTOS;

    INSERT INTO ODS_SEGMENTOS
    (
        FECHA_PROCESO,
        COD_EMPRESA,
        NUM_CODIGO,
        COD_TIPO,
        COD_SEGMENTO,
        BC206ID3,
        BC206ID4,
        BC206NRO1,
        BC206NRO2,
        BC206NRO3,
        NOM_ABREVIADO,
        DES_NOMBRE,
        BC206CHR3,
        FUENTE,
        FECHA_CARGA,
        BATCH_ID
    )
    SELECT
        A.FECHA_PROCESO,
        A.BC205EMP,
        A.BC205COD,
        A.BC206ID1,
        A.BC206ID2,
        A.BC206ID3,
        A.BC206ID4,
        A.BC206NRO1,
        A.BC206NRO2,
        A.BC206NRO3,
        A.BC206CHR1,
        A.BC206CHR2,
        A.BC206CHR3,
        A.FUENTE,
        A.FECHA_CARGA,
        A.BATCH_ID
    FROM STG_FBC206 A
    WHERE A.BC205COD = 6006;

    v_filas = ROW_COUNT();

    IF v_filas = 0 THEN
        SET v_msg_error = 'No se insertaron registros en ODS_SEGMENTOS';
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


    -- ==============================================================================
    -- INSERTANDO REGISTROS ODS_EJECUTIVOS
    -- ==============================================================================

    v_tabla_origen  = 'STG_FBC206';
    v_tabla_destino = 'ODS_EJECUTIVOS';
    v_msg_error     = NULL;

    INSERT INTO ctl_log_proceso
    (
        nom_proceso,
        tabla_origen,
        tabla_destino,
        fec_inicio,
        estado
    )
    VALUES
    (
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

    TRUNCATE TABLE ODS_EJECUTIVOS;

    INSERT INTO ODS_EJECUTIVOS
    (
        FECHA_PROCESO,
        COD_EMPRESA,
        NUM_CODIGO,
        COD_EJECUTIVO,
        BC206ID3,
        BC206ID4,
        NUM_SEGMENTO,
        NUM_BANCA,
        BC206NRO3,
        NOM_EJECUTIVO,
        NOM_JEFE_EQUIPO,
        BC206CHR3,
        FUENTE,
        FECHA_CARGA,
        BATCH_ID
    )
    SELECT
        CASE
            WHEN A.BC206ID1 IS NOT NULL
             AND LENGTH(CAST(A.BC206ID1 AS CHAR)) = 8
             AND CAST(SUBSTRING(CAST(A.BC206ID1 AS CHAR),5,2) AS UNSIGNED) BETWEEN 1 AND 12
             AND CAST(SUBSTRING(CAST(A.BC206ID1 AS CHAR),7,2) AS UNSIGNED) BETWEEN 1 AND 31
            THEN STR_TO_DATE(CAST(A.BC206ID1 AS CHAR), '%Y%m%d')
            ELSE NULL
        END AS FECHA_PROCESO,
        A.BC205EMP,
        A.BC205COD,
        A.BC206ID2,
        A.BC206ID3,
        A.BC206ID4,
        A.BC206NRO1,
        A.BC206NRO2,
        A.BC206NRO3,
        A.BC206CHR1,
        A.BC206CHR2,
        A.BC206CHR3,
        A.FUENTE,
        A.FECHA_CARGA,
        A.BATCH_ID
    FROM STG_FBC206 A
    WHERE A.BC205COD = 6008
      AND A.BC206ID1 <> '99999999';

    v_filas = ROW_COUNT();

    IF v_filas = 0 THEN
        SET v_msg_error = 'No se insertaron registros en ODS_EJECUTIVOS';
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

    -- ==============================================================================
    -- INSERTANDO REGISTROS ODS_SEGMENTO_PATRIMONIO_EFECTIVO
    -- ==============================================================================

    v_tabla_origen  = 'STG_FBC206';
    v_tabla_destino = 'ODS_PATRIMONIO_EFECTIVO';
    v_msg_error     = NULL;

    INSERT INTO ctl_log_proceso
    (
        nom_proceso,
        tabla_origen,
        tabla_destino,
        fec_inicio,
        estado
    )
    VALUES
    (
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

    TRUNCATE TABLE ODS_PATRIMONIO_EFECTIVO;

    INSERT INTO ODS_PATRIMONIO_EFECTIVO
    (
        FECHA_PROCESO,
        COD_EMPRESA,
        NUM_CODIGO,
		PERIODO,
		MTO_PATRIMONIO_EFECTIVO,
        FUENTE,
        FECHA_CARGA,
        BATCH_ID
    )
    SELECT
        A.FECHA_PROCESO,
        A.BC205EMP,
		A.BC205COD,
		A.BC206ID1,
		A.BC206CHR1,
        A.FUENTE,
        A.FECHA_CARGA,
        A.BATCH_ID
    FROM STG_FBC206 A
    WHERE A.BC205COD = 90009
      AND A.BC206ID1 <> '99999999';

    v_filas = ROW_COUNT();

    IF v_filas = 0 THEN
        SET v_msg_error = 'No se insertaron registros en ODS_PATRIMONIO_EFECTIVO';
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