-- =============================================================================
-- STORED PROCEDURE: sp_calcular_saldos_operativos_hb
-- =============================================================================
-- PROPÓSITO
--   Calcula los saldos operativos diarios y promedios mensuales (histórico
--   builder / "HB") de operaciones activas y pasivas, a partir del cierre
--   diario en BDS_SALDOS_CIERRE, rellenando huecos de días sin registro
--   (inicio de mes, fin de mes y tramos intermedios) usando el calendario
--   de días hábiles BDS_CALENDARIOS. El resultado se persiste en
--   BDS_SALDOS_OPERATIVOS_HB.
--
-- ORIGEN
--   Reescritura del script original de depuración (sin nombre de SP,
--   variables de sesión @v_..., sin transacción ni manejo de errores).
--   Esta versión conserva la lógica de negocio original, corrige errores
--   detectados y la empaqueta como procedimiento almacenado productivo.
--
-- DIALECTO
--   Escrito en sintaxis MySQL/MariaDB (DATE_SUB, DATE_ADD, LAST_DAY, CTE
--   recursivas, CREATE TEMPORARY TABLE ... AS SELECT), que es la sintaxis
--   del script original. Si el motor real de destino es SQL Server o DB2,
--   esta versión debe traducirse (DATEADD/DATEDIFF, tablas #temp, MERGE,
--   TRY/CATCH) antes de desplegarla; los comentarios de cada bloque están
--   pensados para facilitar esa migración.
--
-- PARÁMETROS
--   p_fecha_proceso        DATE  Fecha de proceso (antes @v_fecha_proceso).
--   p_dias_habiles_atras   INT   Días a retroceder antes de buscar el día
--                                hábil de inicio (antes @p_dias).
--
-- CORRECCIONES RESPECTO AL SCRIPT ORIGINAL (leer antes de aprobar)
--   1. BUG DE FECHA: el DELETE final usaba el literal '2026-02-01' en vez
--      de la fecha de inicio de mes calculada. Corregido para usar
--      siempre v_fecha_inicio_mes.
--   2. DEDUPLICACIÓN NO DETERMINÍSTICA: el ROW_NUMBER() de
--      UNION_PASIVOS_ACTIVOS_SIN_DUPLICADO ordenaba por una columna ya
--      incluida en el PARTITION BY (empate total). Ahora se desempata por
--      FECHA_CARGA DESC, BATCH_ID DESC, igual que el resto del pipeline.
--   3. "GROUP BY *" NO ESTÁNDAR: SIN_DUPLICADO, SIN_DUPLICADO_PASIVOS y
--      TEMPORAL_TABLE hacían SELECT * ... GROUP BY (subconjunto de
--      columnas), válido solo con ONLY_FULL_GROUP_BY desactivado y con
--      resultado no determinístico en las columnas no agrupadas.
--      Reemplazado por ROW_NUMBER() explícito.
--   4. Todos los DROP TABLE ahora son DROP TEMPORARY TABLE IF EXISTS.
--   5. Se agregaron índices sobre (ID_OPERACION_CIERRE, FECHA_PROCESO,
--      COD_RUBRO) en las tablas intermedias que participan en joins o
--      agrupaciones, dado el volumen (decenas de millones de filas según
--      los conteos del script original).
--   6. Se eliminaron los SELECT/COUNT(*) de depuración embebidos en la
--      lógica; se dejan como bloque opcional de validación al final,
--      fuera de la ruta transaccional.
--   7. Se envuelve el proceso completo en una transacción con manejo de
--      excepción (ROLLBACK + limpieza de temporales + RESIGNAL), ya que
--      el DELETE + INSERT final debe ser atómico.
--   8. Nombres de tablas temporales renombrados a un estándar de negocio
--      (prefijo TMP_, sin nombres personales ni "SIN_NOMBRE").
--   9. Las listas de códigos hardcodeadas (COD_MODULO de pasivos,
--      prefijos de COD_RUBRO de activos) se documentan explícitamente y
--      se recomienda migrarlas a tablas de parámetros (ver sección final).
--
-- NOTA DE NEGOCIO PENDIENTE DE VALIDAR (no modificado, solo documentado)
--   El promedio mensual (AVG_*) se calcula como
--     SUM(saldo_diario) OVER (PARTITION BY operación, año, mes ORDER BY fecha)
--     / TOTAL_DIAS_DEL_MES
--   Es decir, divide el acumulado corrido entre el total de días del mes
--   (no entre los días transcurridos). Esto significa que el promedio solo
--   es correcto en el último día del mes; en días intermedios subestima el
--   promedio real. Se preserva el comportamiento original porque parece
--   intencional (promedio prorrateado a cierre de mes), pero debe
--   confirmarlo el dueño de negocio antes de usarse para reportes
--   regulatorios.
-- =============================================================================

DROP PROCEDURE IF EXISTS sp_calcular_saldos_operativos_hb;

DELIMITER $$

CREATE PROCEDURE sp_calcular_saldos_operativos_hb (
    IN p_fecha_proceso      DATE,
    IN p_dias_habiles_atras INT
)
proc_body: BEGIN

    -- -------------------------------------------------------------------
    -- Variables locales (reemplazan a las variables de sesión @v_...,
    -- que no son seguras si el procedimiento se invoca concurrentemente
    -- sobre conexiones reutilizadas de un pool).
    -- -------------------------------------------------------------------
    DECLARE v_fecha_fin          DATE;
    DECLARE v_fecha_inicio_mes   DATE;
    DECLARE v_fecha_inicio_habil DATE;

    -- -------------------------------------------------------------------
    -- Manejo de errores: ante cualquier excepción SQL, se revierte la
    -- transacción, se limpian las tablas temporales (evita basura en
    -- conexiones reutilizadas) y se relanza el error al llamador.
    -- -------------------------------------------------------------------
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;

        DROP TEMPORARY TABLE IF EXISTS TMP_BASE_PERIODO_HABIL;
        DROP TEMPORARY TABLE IF EXISTS TMP_BASE_MESES_COMPLETOS;
        DROP TEMPORARY TABLE IF EXISTS TMP_CALENDARIO_DIAS_HABILES;
        DROP TEMPORARY TABLE IF EXISTS TMP_BASE_RANGO_FECHAS;
        DROP TEMPORARY TABLE IF EXISTS TMP_OPERACIONES_VIGENTES;
        DROP TEMPORARY TABLE IF EXISTS TMP_OPERACIONES_HISTORICAS;
        DROP TEMPORARY TABLE IF EXISTS TMP_UNION_OPERACIONES;
        DROP TEMPORARY TABLE IF EXISTS TMP_UNION_OPERACIONES_DEDUP;
        DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_FIN_MES;
        DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_FIN_MES;
        DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_INTERMEDIAS;
        DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_INTERMEDIO;
        DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_INICIO_MES;
        DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_INICIO_MES;
        DROP TEMPORARY TABLE IF EXISTS TMP_ACTIVOS_POR_CATEGORIA;
        DROP TEMPORARY TABLE IF EXISTS TMP_ACTIVOS_DEDUP;
        DROP TEMPORARY TABLE IF EXISTS TMP_PASIVOS;
        DROP TEMPORARY TABLE IF EXISTS TMP_PASIVOS_DEDUP;
        DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS;
        DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS_DEDUP;
        DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS_DEPURADA;
        DROP TEMPORARY TABLE IF EXISTS TMP_ATRIBUTOS_BASE;
        DROP TEMPORARY TABLE IF EXISTS TMP_TOTALES_DIARIOS;
        DROP TEMPORARY TABLE IF EXISTS TMP_SALDOS_PROMEDIOS;

        RESIGNAL;
    END;

    START TRANSACTION;

    -- =====================================================================
    -- PASO 1. Cálculo de parámetros de fecha
    --   - v_fecha_fin: fecha de proceso recibida.
    --   - v_fecha_inicio_habil: primer día hábil <= (fecha_proceso - N días).
    --   - v_fecha_inicio_mes: primer día del mes de (fecha_proceso - N días).
    -- =====================================================================
    SELECT
        p_fecha_proceso,
        DATE_FORMAT(DATE_SUB(p_fecha_proceso, INTERVAL p_dias_habiles_atras DAY), '%Y-%m-01'),
        MAX(c.FEC_CALENDARIO)
    INTO
        v_fecha_fin,
        v_fecha_inicio_mes,
        v_fecha_inicio_habil
    FROM BDS_CALENDARIOS c
    WHERE c.COD_CALENDARIO = 1
      AND c.IND_DIA_HABIL = 'S'
      AND c.FEC_CALENDARIO <= DATE_SUB(p_fecha_proceso, INTERVAL p_dias_habiles_atras DAY);

    -- =====================================================================
    -- PASO 2. Base inicial: cierres dentro de la ventana hábil
    --   (antes: TEMP_BASE_INICIAL)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_BASE_PERIODO_HABIL;
    CREATE TEMPORARY TABLE TMP_BASE_PERIODO_HABIL AS
    SELECT
        MONTH(FECHA_PROCESO) AS MES,
        YEAR(FECHA_PROCESO)  AS ANIO,
        bc.*
    FROM BDS_SALDOS_CIERRE bc
    WHERE bc.FECHA_PROCESO >= v_fecha_inicio_habil
      AND bc.FECHA_PROCESO <= v_fecha_fin;

    CREATE INDEX ix_tmp_base_periodo_habil
        ON TMP_BASE_PERIODO_HABIL (FECHA_PROCESO);

    -- =====================================================================
    -- PASO 3. Expansión al mes completo (desde el 1° del mes anterior al
    --   primer registro, hasta el último registro del periodo)
    --   (antes: TEMP_BASE_COMPLETA_POR_MES)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_BASE_MESES_COMPLETOS;
    CREATE TEMPORARY TABLE TMP_BASE_MESES_COMPLETOS AS
    WITH rango AS (
        SELECT
            DATE_ADD(LAST_DAY(DATE_SUB(MIN(FECHA_PROCESO), INTERVAL 1 MONTH)), INTERVAL 1 DAY) AS INICIO_FECHA,
            MAX(FECHA_PROCESO) AS FIN_FECHA
        FROM TMP_BASE_PERIODO_HABIL
    )
    SELECT bho.*
    FROM TMP_BASE_PERIODO_HABIL bho
    CROSS JOIN rango r
    WHERE bho.FECHA_PROCESO BETWEEN r.INICIO_FECHA AND r.FIN_FECHA;

    CREATE INDEX ix_tmp_base_meses_completos
        ON TMP_BASE_MESES_COMPLETOS (FECHA_PROCESO);

    -- =====================================================================
    -- PASO 4. Calendario extendido de días hábiles/no hábiles para el
    --   rango de meses completos, con la "fecha origen" (último día hábil
    --   <= fecha) que se usará para replicar saldos en días no hábiles.
    --   (antes: CALENDARIO_GLOBAL)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_CALENDARIO_DIAS_HABILES;
    CREATE TEMPORARY TABLE TMP_CALENDARIO_DIAS_HABILES AS
    WITH rango AS (
        SELECT
            DATE_ADD(LAST_DAY(DATE_SUB(MIN(FECHA_PROCESO), INTERVAL 1 MONTH)), INTERVAL 1 DAY) AS INICIO_FECHA,
            MAX(FECHA_PROCESO) AS FIN_FECHA
        FROM TMP_BASE_PERIODO_HABIL
    ),
    dias_calendario AS (
        SELECT cal.FEC_CALENDARIO AS FECHA_PROCESO, cal.IND_DIA_HABIL
        FROM BDS_CALENDARIOS cal
        INNER JOIN rango r
            ON cal.FEC_CALENDARIO BETWEEN r.INICIO_FECHA AND r.FIN_FECHA
        WHERE cal.COD_CALENDARIO = 1
    )
    SELECT
        d.FECHA_PROCESO,
        d.IND_DIA_HABIL,
        MAX(h.FEC_CALENDARIO) AS FECHA_ORIGEN
    FROM dias_calendario d
    INNER JOIN BDS_CALENDARIOS h
        ON h.FEC_CALENDARIO <= d.FECHA_PROCESO
       AND h.IND_DIA_HABIL = 'S'
    GROUP BY d.FECHA_PROCESO, d.IND_DIA_HABIL;

    CREATE INDEX ix_tmp_calendario_habiles
        ON TMP_CALENDARIO_DIAS_HABILES (FECHA_PROCESO, FECHA_ORIGEN);

    -- =====================================================================
    -- PASO 5. Expansión de cada operación a todas las fechas del
    --   calendario, replicando el saldo del último día hábil de origen en
    --   los días no hábiles ("copia hábil").
    --   (antes: BASE_TABLA_RANGO_FECHA)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_BASE_RANGO_FECHAS;
    CREATE TEMPORARY TABLE TMP_BASE_RANGO_FECHAS AS
    SELECT
        CONCAT(
            o.COD_EMPRESA, '|', o.COD_MODULO, '|', o.COD_SUCURSAL, '|',
            o.COD_MONEDA, '|', o.COD_PAPEL, '|', o.NUM_CUENTA_BT, '|',
            o.COD_OPERACION, '|', o.COD_SUB_OPERACION, '|', o.COD_TIPO_OPERACION
        ) AS ID_OPERACION_CIERRE,
        cal.FECHA_PROCESO,
        o.COD_EMPRESA,
        o.COD_SUCURSAL,
        o.COD_RUBRO,
        o.COD_MONEDA,
        o.COD_PAPEL,
        o.NUM_CUENTA_BT,
        o.COD_OPERACION,
        o.COD_SUB_OPERACION,
        o.COD_TIPO_OPERACION,
        o.COD_MODULO,
        o.FEC_VENCIMIENTO,
        o.FEC_VALOR,
        o.IND_CATEGORIA_RIESGO,
        o.COD_ACTI_BCO_CENTRAL,
        o.COD_PRODUCTO,
        o.MTO_SALDO_ORIGEN,
        o.MTO_SALDO_MN,
        o.MTO_SALDO_ME,
        o.MTO_SALDO_MO,
        o.MTO_INTERES,
        o.MTO_PREVISIONES,
        cal.IND_DIA_HABIL,
        CASE WHEN cal.FECHA_PROCESO = cal.FECHA_ORIGEN THEN 'ORIGINAL' ELSE 'COPIA_HABIL' END AS TIPO_ORIGEN,
        o.FUENTE,
        o.FECHA_CARGA,
        o.BATCH_ID
    FROM TMP_CALENDARIO_DIAS_HABILES cal
    JOIN TMP_BASE_MESES_COMPLETOS o
        ON o.FECHA_PROCESO = cal.FECHA_ORIGEN;

    CREATE INDEX ix_tmp_base_rango_fechas
        ON TMP_BASE_RANGO_FECHAS (ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO);

    -- =====================================================================
    -- PASO 6. Separar operaciones vigentes (existen hoy en BDS_OPERACIONES)
    --   de operaciones históricas (ya canceladas/no vigentes), y unirlas.
    --   (antes: TABLA_REGISTROS_MISMO_DIA_OPERACIONES /
    --           TABLA_REGISTROS_OPERACIONES_ANTERIORES / UNION_ALL_TABLE)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_OPERACIONES_VIGENTES;
    CREATE TEMPORARY TABLE TMP_OPERACIONES_VIGENTES AS
    SELECT bho.*
    FROM TMP_BASE_RANGO_FECHAS bho
    WHERE bho.ID_OPERACION_CIERRE IN (SELECT ID_OPERACION FROM BDS_OPERACIONES);

    DROP TEMPORARY TABLE IF EXISTS TMP_OPERACIONES_HISTORICAS;
    CREATE TEMPORARY TABLE TMP_OPERACIONES_HISTORICAS AS
    SELECT bho.*
    FROM TMP_BASE_RANGO_FECHAS bho
    WHERE bho.ID_OPERACION_CIERRE NOT IN (SELECT ID_OPERACION FROM BDS_OPERACIONES);

    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_OPERACIONES;
    CREATE TEMPORARY TABLE TMP_UNION_OPERACIONES AS
    SELECT * FROM TMP_OPERACIONES_VIGENTES
    UNION ALL
    SELECT * FROM TMP_OPERACIONES_HISTORICAS;

    -- =====================================================================
    -- PASO 7. Deduplicar por operación+fecha+atributos, quedándose con la
    --   carga más reciente (FECHA_CARGA DESC).
    --   (antes: UNION_ALL_TABLE_1)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_OPERACIONES_DEDUP;
    CREATE TEMPORARY TABLE TMP_UNION_OPERACIONES_DEDUP AS
    SELECT t.*
    FROM (
        SELECT
            u.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
                    COD_RUBRO, COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION,
                    COD_SUB_OPERACION, COD_TIPO_OPERACION, COD_MODULO,
                    FEC_VENCIMIENTO, FEC_VALOR, IND_CATEGORIA_RIESGO,
                    COD_ACTI_BCO_CENTRAL, COD_PRODUCTO, MTO_SALDO_ORIGEN,
                    MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO, MTO_INTERES,
                    MTO_PREVISIONES
                ORDER BY FECHA_CARGA DESC, BATCH_ID DESC
            ) AS RN
        FROM TMP_UNION_OPERACIONES u
    ) t
    WHERE t.RN = 1;

    CREATE INDEX ix_tmp_union_dedup
        ON TMP_UNION_OPERACIONES_DEDUP (ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO);

    -- Sincroniza IND_DIA_HABIL con el calendario definitivo.
    UPDATE TMP_UNION_OPERACIONES_DEDUP u
    INNER JOIN TMP_CALENDARIO_DIAS_HABILES t
        ON t.FECHA_PROCESO = u.FECHA_PROCESO
    SET u.IND_DIA_HABIL = t.IND_DIA_HABIL;

    -- =====================================================================
    -- PASO 8. Relleno de huecos (fin de mes, tramos intermedios, inicio de
    --   mes) para que cada operación tenga un registro por cada día del
    --   mes, en saldo cero, cuando no hubo carga real ese día.
    --   (antes: DATAHUB_NULL_FIN_MES / TEMP_AUXILIAR_FIN_MES,
    --           DATAHUB_NULL_INTERMEDIO / TEMP_AUXILIAR_INTERMEDIO,
    --           DATAHUB_NULL_INICIO_MES / TEMP_AUXILIAR_INICIO_MES)
    -- =====================================================================

    -- 8a. Huecos de fin de mes: la última fecha registrada no llega al
    --     último día del mes.
    DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_FIN_MES;
    CREATE TEMPORARY TABLE TMP_BRECHAS_FIN_MES AS
    SELECT
        ID_OPERACION_CIERRE, COD_RUBRO,
        YEAR(FECHA_PROCESO) ANIO, MONTH(FECHA_PROCESO) MES,
        MAX(FECHA_PROCESO) ULTIMA_FECHA_REGISTRADA,
        LAST_DAY(MAX(FECHA_PROCESO)) FIN_DE_MES
    FROM TMP_UNION_OPERACIONES_DEDUP
    GROUP BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO), COD_RUBRO
    HAVING MAX(FECHA_PROCESO) <> LAST_DAY(MAX(FECHA_PROCESO));

    DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_FIN_MES;
    CREATE TEMPORARY TABLE TMP_RELLENO_FIN_MES AS
    WITH RECURSIVE fechas_faltantes AS (
        SELECT
            nfm.ID_OPERACION_CIERRE, nfm.COD_RUBRO, nfm.ANIO, nfm.MES,
            DATE_ADD(nfm.ULTIMA_FECHA_REGISTRADA, INTERVAL 1 DAY) AS FECHA_PROCESO,
            nfm.ULTIMA_FECHA_REGISTRADA, nfm.FIN_DE_MES
        FROM TMP_BRECHAS_FIN_MES nfm
        UNION ALL
        SELECT
            ff.ID_OPERACION_CIERRE, ff.COD_RUBRO, ff.ANIO, ff.MES,
            DATE_ADD(ff.FECHA_PROCESO, INTERVAL 1 DAY),
            ff.ULTIMA_FECHA_REGISTRADA, ff.FIN_DE_MES
        FROM fechas_faltantes ff
        WHERE ff.FECHA_PROCESO < ff.FIN_DE_MES
    )
    SELECT
        urt.ID_OPERACION_CIERRE, ff.FECHA_PROCESO,
        urt.COD_EMPRESA, urt.COD_SUCURSAL, urt.COD_RUBRO, urt.COD_MONEDA,
        urt.COD_PAPEL, urt.NUM_CUENTA_BT, urt.COD_OPERACION, urt.COD_SUB_OPERACION,
        urt.COD_TIPO_OPERACION, urt.COD_MODULO, urt.FEC_VENCIMIENTO, urt.FEC_VALOR,
        urt.IND_CATEGORIA_RIESGO, urt.COD_ACTI_BCO_CENTRAL, urt.COD_PRODUCTO,
        0 AS MTO_SALDO_ORIGEN, 0 AS MTO_SALDO_MN, 0 AS MTO_SALDO_ME,
        0 AS MTO_SALDO_MO, 0 AS MTO_INTERES, 0 AS MTO_PREVISIONES,
        'S' AS IND_DIA_HABIL, urt.FUENTE, urt.FECHA_CARGA, urt.BATCH_ID
    FROM fechas_faltantes ff
    INNER JOIN TMP_UNION_OPERACIONES_DEDUP urt
        ON urt.ID_OPERACION_CIERRE = ff.ID_OPERACION_CIERRE
       AND urt.FECHA_PROCESO = ff.ULTIMA_FECHA_REGISTRADA
       AND urt.COD_RUBRO = ff.COD_RUBRO
       AND urt.TIPO_ORIGEN <> 'COPIA_HABIL'
    WHERE ff.FECHA_PROCESO < v_fecha_fin;

    -- 8b. Huecos intermedios: meses completos sin ningún registro para la
    --     operación/rubro.
    DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_INTERMEDIAS;
    CREATE TEMPORARY TABLE TMP_BRECHAS_INTERMEDIAS AS
    WITH RECURSIVE calendario AS (
        SELECT
            ID_OPERACION_CIERRE, COD_RUBRO,
            YEAR(MIN(FECHA_PROCESO)) ANIO, MONTH(MIN(FECHA_PROCESO)) MES,
            MIN(FECHA_PROCESO) FECHA_PROCESO, MAX(FECHA_PROCESO) FECHA_FIN
        FROM TMP_UNION_OPERACIONES_DEDUP
        GROUP BY ID_OPERACION_CIERRE, COD_RUBRO, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
        UNION ALL
        SELECT
            ID_OPERACION_CIERRE, COD_RUBRO, ANIO, MES,
            DATE_ADD(FECHA_PROCESO, INTERVAL 1 DAY), FECHA_FIN
        FROM calendario
        WHERE FECHA_PROCESO < FECHA_FIN
    )
    SELECT c.ID_OPERACION_CIERRE, c.COD_RUBRO, c.ANIO, c.MES, c.FECHA_PROCESO
    FROM calendario c
    LEFT JOIN TMP_UNION_OPERACIONES_DEDUP t
        ON t.ID_OPERACION_CIERRE = c.ID_OPERACION_CIERRE
       AND t.COD_RUBRO = c.COD_RUBRO
       AND t.FECHA_PROCESO = c.FECHA_PROCESO
    WHERE t.ID_OPERACION_CIERRE IS NULL;

    DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_INTERMEDIO;
    CREATE TEMPORARY TABLE TMP_RELLENO_INTERMEDIO AS
    SELECT
        a.ID_OPERACION_CIERRE, a.FECHA_PROCESO,
        b.COD_EMPRESA, b.COD_SUCURSAL, b.COD_RUBRO, b.COD_MONEDA, b.COD_PAPEL,
        b.NUM_CUENTA_BT, b.COD_OPERACION, b.COD_SUB_OPERACION, b.COD_TIPO_OPERACION,
        b.COD_MODULO, b.FEC_VENCIMIENTO, b.FEC_VALOR, b.IND_CATEGORIA_RIESGO,
        b.COD_ACTI_BCO_CENTRAL, b.COD_PRODUCTO,
        0 AS MTO_SALDO_ORIGEN, 0 AS MTO_SALDO_MN, 0 AS MTO_SALDO_ME,
        0 AS MTO_SALDO_MO, 0 AS MTO_INTERES, 0 AS MTO_PREVISIONES,
        'S' AS IND_DIA_HABIL, b.FUENTE, b.FECHA_CARGA, b.BATCH_ID
    FROM TMP_BRECHAS_INTERMEDIAS a
    JOIN TMP_UNION_OPERACIONES_DEDUP b
        ON b.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
       AND b.COD_RUBRO = a.COD_RUBRO
       AND b.FECHA_PROCESO = (
            SELECT MAX(c.FECHA_PROCESO)
            FROM TMP_UNION_OPERACIONES_DEDUP c
            WHERE c.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
              AND c.COD_RUBRO = a.COD_RUBRO
              AND c.FECHA_PROCESO < a.FECHA_PROCESO
              AND c.TIPO_ORIGEN = 'ORIGINAL'
       );

    -- 8c. Huecos de inicio de mes: el primer registro del mes no coincide
    --     con el día 1.
    DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_INICIO_MES;
    CREATE TEMPORARY TABLE TMP_BRECHAS_INICIO_MES AS
    SELECT
        ID_OPERACION_CIERRE, COD_RUBRO,
        YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
        MIN(FECHA_PROCESO) AS PRIMERA_FECHA_REGISTRADA,
        DATE_FORMAT(MIN(FECHA_PROCESO), '%Y-%m-01') AS INICIO_MES
    FROM TMP_UNION_OPERACIONES_DEDUP
    GROUP BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO), COD_RUBRO
    HAVING MIN(FECHA_PROCESO) <> DATE_FORMAT(MIN(FECHA_PROCESO), '%Y-%m-01');

    DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_INICIO_MES;
    CREATE TEMPORARY TABLE TMP_RELLENO_INICIO_MES AS
    WITH RECURSIVE fechas_faltantes AS (
        SELECT
            ID_OPERACION_CIERRE, COD_RUBRO, ANIO, MES,
            INICIO_MES AS FECHA_PROCESO, PRIMERA_FECHA_REGISTRADA
        FROM TMP_BRECHAS_INICIO_MES
        UNION ALL
        SELECT
            ID_OPERACION_CIERRE, COD_RUBRO, ANIO, MES,
            DATE_ADD(FECHA_PROCESO, INTERVAL 1 DAY), PRIMERA_FECHA_REGISTRADA
        FROM fechas_faltantes
        WHERE DATE_ADD(FECHA_PROCESO, INTERVAL 1 DAY) < PRIMERA_FECHA_REGISTRADA
    )
    SELECT
        x.ID_OPERACION_CIERRE, ff.FECHA_PROCESO,
        x.COD_EMPRESA, x.COD_SUCURSAL, x.COD_RUBRO, x.COD_MONEDA, x.COD_PAPEL,
        x.NUM_CUENTA_BT, x.COD_OPERACION, x.COD_SUB_OPERACION, x.COD_TIPO_OPERACION,
        x.COD_MODULO, x.FEC_VENCIMIENTO, x.FEC_VALOR, x.IND_CATEGORIA_RIESGO,
        x.COD_ACTI_BCO_CENTRAL, x.COD_PRODUCTO,
        0 AS MTO_SALDO_ORIGEN, 0 AS MTO_SALDO_MN, 0 AS MTO_SALDO_ME,
        0 AS MTO_SALDO_MO, 0 AS MTO_INTERES, 0 AS MTO_PREVISIONES,
        'S' AS IND_DIA_HABIL, x.FUENTE, x.FECHA_CARGA, x.BATCH_ID
    FROM fechas_faltantes ff
    INNER JOIN TMP_UNION_OPERACIONES_DEDUP x
        ON x.ID_OPERACION_CIERRE = ff.ID_OPERACION_CIERRE
       AND x.COD_RUBRO = ff.COD_RUBRO
       AND x.FECHA_PROCESO = ff.PRIMERA_FECHA_REGISTRADA
       AND x.TIPO_ORIGEN <> 'COPIA_HABIL';

    -- Incorporar los tres rellenos a la tabla dedup principal, evitando
    -- duplicados y respetando el límite inferior del periodo.
    INSERT INTO TMP_UNION_OPERACIONES_DEDUP (
        ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO,
        COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION,
        COD_TIPO_OPERACION, COD_MODULO, FEC_VENCIMIENTO, FEC_VALOR,
        IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
        MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
        MTO_INTERES, MTO_PREVISIONES, IND_DIA_HABIL, FUENTE, FECHA_CARGA, BATCH_ID
    )
    SELECT t.*
    FROM TMP_RELLENO_INICIO_MES t
    WHERE t.FECHA_PROCESO >= v_fecha_inicio_habil
      AND NOT EXISTS (
          SELECT 1 FROM TMP_UNION_OPERACIONES_DEDUP u
          WHERE u.ID_OPERACION_CIERRE = t.ID_OPERACION_CIERRE
            AND u.COD_RUBRO = t.COD_RUBRO
            AND u.FECHA_PROCESO = t.FECHA_PROCESO
      );

    INSERT INTO TMP_UNION_OPERACIONES_DEDUP (
        ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO,
        COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION,
        COD_TIPO_OPERACION, COD_MODULO, FEC_VENCIMIENTO, FEC_VALOR,
        IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
        MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
        MTO_INTERES, MTO_PREVISIONES, IND_DIA_HABIL, FUENTE, FECHA_CARGA, BATCH_ID
    )
    SELECT t.*
    FROM TMP_RELLENO_INTERMEDIO t
    WHERE t.FECHA_PROCESO >= v_fecha_inicio_habil
      AND NOT EXISTS (
          SELECT 1 FROM TMP_UNION_OPERACIONES_DEDUP u
          WHERE u.ID_OPERACION_CIERRE = t.ID_OPERACION_CIERRE
            AND u.COD_RUBRO = t.COD_RUBRO
            AND u.FECHA_PROCESO = t.FECHA_PROCESO
      );

    INSERT INTO TMP_UNION_OPERACIONES_DEDUP (
        ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO,
        COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION,
        COD_TIPO_OPERACION, COD_MODULO, FEC_VENCIMIENTO, FEC_VALOR,
        IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
        MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
        MTO_INTERES, MTO_PREVISIONES, IND_DIA_HABIL, FUENTE, FECHA_CARGA, BATCH_ID
    )
    SELECT t.*
    FROM TMP_RELLENO_FIN_MES t
    WHERE t.FECHA_PROCESO >= v_fecha_inicio_habil
      AND NOT EXISTS (
          SELECT 1 FROM TMP_UNION_OPERACIONES_DEDUP u
          WHERE u.ID_OPERACION_CIERRE = t.ID_OPERACION_CIERRE
            AND u.COD_RUBRO = t.COD_RUBRO
            AND u.FECHA_PROCESO = t.FECHA_PROCESO
      );

    -- Re-sincroniza IND_DIA_HABIL tras las inserciones de relleno.
    UPDATE TMP_UNION_OPERACIONES_DEDUP u
    INNER JOIN TMP_CALENDARIO_DIAS_HABILES t
        ON t.FECHA_PROCESO = u.FECHA_PROCESO
    SET u.IND_DIA_HABIL = t.IND_DIA_HABIL;

    -- =====================================================================
    -- PASO 9. Desglose de ACTIVOS por categoría (vigente, reestructurado,
    --   refinanciado, vencido, judicial) a partir del prefijo de COD_RUBRO.
    --   NOTA: los prefijos '1401'..'1426' están hardcodeados igual que en
    --   el script original. Se recomienda moverlos a una tabla de
    --   parámetros (p.ej. PARAM_RUBROS_ACTIVOS) para no tener que editar
    --   este procedimiento cuando cambie el plan de cuentas.
    --   (antes: TABLA_FINAL_SIN_NOMBRE)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_ACTIVOS_POR_CATEGORIA;
    CREATE TEMPORARY TABLE TMP_ACTIVOS_POR_CATEGORIA AS
    WITH base_vigente AS (
        SELECT ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO,
               MTO_SALDO_MO AS MTO_SALDO_VIGENTE_MO, MTO_SALDO_MN AS MTO_SALDO_VIGENTE_MN
        FROM TMP_UNION_OPERACIONES_DEDUP
        WHERE SUBSTR(COD_RUBRO,1,4) LIKE '14_1'
        GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO
    ),
    base_reestructurado AS (
        SELECT ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO,
               MTO_SALDO_MO AS MTO_SALDO_RESTRUCTURADO_MO, MTO_SALDO_MN AS MTO_SALDO_RESTRUCTURADO_MN
        FROM TMP_UNION_OPERACIONES_DEDUP
        WHERE SUBSTR(COD_RUBRO,1,4) LIKE '14_3'
        GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO
    ),
    base_refinanciado AS (
        SELECT ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO,
               MTO_SALDO_MO AS MTO_SALDO_REFINANCIADO_MO, MTO_SALDO_MN AS MTO_SALDO_REFINANCIADO_MN
        FROM TMP_UNION_OPERACIONES_DEDUP
        WHERE SUBSTR(COD_RUBRO,1,4) LIKE '14_4'
        GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO
    ),
    base_vencido AS (
        SELECT ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO,
               MTO_SALDO_MO AS MTO_SALDO_VENCIDO_MO, MTO_SALDO_MN AS MTO_SALDO_VENCIDO_MN
        FROM TMP_UNION_OPERACIONES_DEDUP
        WHERE SUBSTR(COD_RUBRO,1,4) LIKE '14_5'
        GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO
    ),
    base_judicial AS (
        SELECT ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO,
               MTO_SALDO_MO AS MTO_SALDO_JUDIAL_MO, MTO_SALDO_MN AS MTO_SALDO_JUDICIAL_MN
        FROM TMP_UNION_OPERACIONES_DEDUP
        WHERE SUBSTR(COD_RUBRO,1,4) LIKE '14_6'
        GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO
    ),
    base AS (
        SELECT DISTINCT bt.*
        FROM TMP_UNION_OPERACIONES_DEDUP bt
        WHERE LEFT(bt.COD_RUBRO, 4) IN (
            '1401','1403','1404','1405','1406',
            '1411','1413','1414','1415','1416',
            '1421','1423','1424','1425','1426'
        )
    )
    SELECT
        bt.ID_OPERACION_CIERRE, bt.FECHA_PROCESO, bt.COD_EMPRESA, bt.COD_SUCURSAL,
        bt.COD_RUBRO, bt.COD_MONEDA, bt.COD_PAPEL, bt.NUM_CUENTA_BT, bt.COD_OPERACION,
        bt.COD_SUB_OPERACION, bt.COD_TIPO_OPERACION, bt.COD_MODULO, bt.FEC_VENCIMIENTO,
        bt.FEC_VALOR, bt.IND_CATEGORIA_RIESGO, bt.COD_ACTI_BCO_CENTRAL, bt.COD_PRODUCTO,
        bt.MTO_SALDO_ORIGEN, bt.MTO_SALDO_MN, bt.MTO_SALDO_ME, bt.MTO_SALDO_MO,
        bt.MTO_INTERES, bt.MTO_PREVISIONES,
        COALESCE(bv.MTO_SALDO_VIGENTE_MO, 0)        AS MTO_SALDO_VIGENTE_MO,
        COALESCE(br.MTO_SALDO_RESTRUCTURADO_MO, 0)  AS MTO_SALDO_RESTRUCTURADO_MO,
        COALESCE(bf.MTO_SALDO_REFINANCIADO_MO, 0)   AS MTO_SALDO_REFINANCIADO_MO,
        COALESCE(bven.MTO_SALDO_VENCIDO_MO, 0)      AS MTO_SALDO_VENCIDO_MO,
        COALESCE(bj.MTO_SALDO_JUDIAL_MO, 0)         AS MTO_SALDO_JUDIAL_MO,
        COALESCE(bv.MTO_SALDO_VIGENTE_MN, 0)        AS MTO_SALDO_VIGENTE_MN,
        COALESCE(br.MTO_SALDO_RESTRUCTURADO_MN, 0)  AS MTO_SALDO_RESTRUCTURADO_MN,
        COALESCE(bf.MTO_SALDO_REFINANCIADO_MN, 0)   AS MTO_SALDO_REFINANCIADO_MN,
        COALESCE(bven.MTO_SALDO_VENCIDO_MN, 0)      AS MTO_SALDO_VENCIDO_MN,
        COALESCE(bj.MTO_SALDO_JUDICIAL_MN, 0)       AS MTO_SALDO_JUDICIAL_MN,
        bt.FUENTE, bt.IND_DIA_HABIL, bt.FECHA_CARGA, bt.BATCH_ID
    FROM base bt
    LEFT JOIN base_vigente bv        ON bt.ID_OPERACION_CIERRE = bv.ID_OPERACION_CIERRE   AND bt.FECHA_PROCESO = bv.FECHA_PROCESO   AND bt.COD_RUBRO = bv.COD_RUBRO
    LEFT JOIN base_reestructurado br ON bt.ID_OPERACION_CIERRE = br.ID_OPERACION_CIERRE   AND bt.FECHA_PROCESO = br.FECHA_PROCESO   AND bt.COD_RUBRO = br.COD_RUBRO
    LEFT JOIN base_refinanciado bf   ON bt.ID_OPERACION_CIERRE = bf.ID_OPERACION_CIERRE   AND bt.FECHA_PROCESO = bf.FECHA_PROCESO   AND bt.COD_RUBRO = bf.COD_RUBRO
    LEFT JOIN base_vencido bven      ON bt.ID_OPERACION_CIERRE = bven.ID_OPERACION_CIERRE AND bt.FECHA_PROCESO = bven.FECHA_PROCESO AND bt.COD_RUBRO = bven.COD_RUBRO
    LEFT JOIN base_judicial bj       ON bt.ID_OPERACION_CIERRE = bj.ID_OPERACION_CIERRE   AND bt.FECHA_PROCESO = bj.FECHA_PROCESO   AND bt.COD_RUBRO = bj.COD_RUBRO;

    -- Deduplicación determinística (reemplaza el GROUP BY * original).
    DROP TEMPORARY TABLE IF EXISTS TMP_ACTIVOS_DEDUP;
    CREATE TEMPORARY TABLE TMP_ACTIVOS_DEDUP AS
    SELECT t.* EXCEPT (RN) FROM (
        SELECT a.*,
               ROW_NUMBER() OVER (
                   PARTITION BY ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO
                   ORDER BY FECHA_CARGA DESC, BATCH_ID DESC
               ) AS RN
        FROM TMP_ACTIVOS_POR_CATEGORIA a
    ) t
    WHERE t.RN = 1;

    -- =====================================================================
    -- PASO 10. PASIVOS: operaciones cuyo COD_MODULO corresponde a módulos
    --   de pasivo. Igual que en el paso anterior, esta lista debería vivir
    --   en una tabla de parámetros (p.ej. PARAM_MODULOS_PASIVOS).
    --   (antes: TABLA_FINAL_SIN_NOMBRE_PASIVOS / SIN_DUPLICADO_PASIVOS)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_PASIVOS;
    CREATE TEMPORARY TABLE TMP_PASIVOS AS
    SELECT *
    FROM TMP_UNION_OPERACIONES_DEDUP
    WHERE COD_MODULO IN (20,120,321,22,185,184,162,155,21);

    DROP TEMPORARY TABLE IF EXISTS TMP_PASIVOS_DEDUP;
    CREATE TEMPORARY TABLE TMP_PASIVOS_DEDUP AS
    SELECT t.* EXCEPT (RN) FROM (
        SELECT p.*,
               ROW_NUMBER() OVER (
                   PARTITION BY ID_OPERACION_CIERRE, COD_RUBRO, FECHA_PROCESO
                   ORDER BY FECHA_CARGA DESC, BATCH_ID DESC
               ) AS RN
        FROM TMP_PASIVOS p
    ) t
    WHERE t.RN = 1;

    -- =====================================================================
    -- PASO 11. Unión de pasivos y activos en un esquema común (los
    --   pasivos no tienen desglose por categoría, se llenan en 0).
    --   (antes: UNION_PASIVOS_ACTIVOS)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS;
    CREATE TEMPORARY TABLE TMP_UNION_PASIVOS_ACTIVOS AS
    SELECT
        ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO,
        COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION,
        COD_TIPO_OPERACION, COD_MODULO, FEC_VENCIMIENTO, FEC_VALOR,
        IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, COD_PRODUCTO,
        MTO_SALDO_ORIGEN, MTO_SALDO_MN, MTO_SALDO_ME, MTO_SALDO_MO,
        MTO_INTERES, MTO_PREVISIONES,
        0 AS MTO_SALDO_VIGENTE_MO, 0 AS MTO_SALDO_RESTRUCTURADO_MO,
        0 AS MTO_SALDO_REFINANCIADO_MO, 0 AS MTO_SALDO_VENCIDO_MO, 0 AS MTO_SALDO_JUDIAL_MO,
        0 AS MTO_SALDO_VIGENTE_MN, 0 AS MTO_SALDO_RESTRUCTURADO_MN,
        0 AS MTO_SALDO_REFINANCIADO_MN, 0 AS MTO_SALDO_VENCIDO_MN, 0 AS MTO_SALDO_JUDIAL_MN,
        FUENTE, IND_DIA_HABIL, FECHA_CARGA, BATCH_ID
    FROM TMP_PASIVOS_DEDUP
    UNION ALL
    SELECT * FROM TMP_ACTIVOS_DEDUP;

    -- Deduplicación final: se conserva la carga más reciente por
    -- operación/fecha/rubro; ante duplicados con saldo cero e igual
    -- FECHA_CARGA se prioriza el registro con saldo distinto de cero
    -- (misma regla de negocio que el script original).
    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS_DEDUP;
    CREATE TEMPORARY TABLE TMP_UNION_PASIVOS_ACTIVOS_DEDUP AS
    SELECT t.* EXCEPT (RN) FROM (
        SELECT a.*,
               ROW_NUMBER() OVER (
                   PARTITION BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO
                   ORDER BY FECHA_CARGA DESC, BATCH_ID DESC
               ) AS RN
        FROM TMP_UNION_PASIVOS_ACTIVOS a
    ) t
    WHERE t.RN = 1;

    CREATE INDEX ix_tmp_union_pa_dedup
        ON TMP_UNION_PASIVOS_ACTIVOS_DEDUP (ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO);

    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS_DEPURADA;
    CREATE TEMPORARY TABLE TMP_UNION_PASIVOS_ACTIVOS_DEPURADA AS
    WITH conteo AS (
        SELECT a.*,
               COUNT(*) OVER (PARTITION BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO) AS CNT
        FROM TMP_UNION_PASIVOS_ACTIVOS_DEDUP a
    )
    SELECT * FROM conteo
    WHERE CNT = 1
       OR (
            CNT > 1
            AND (
                COALESCE(MTO_SALDO_ORIGEN,0) <> 0
             OR COALESCE(MTO_SALDO_MN,0) <> 0
             OR COALESCE(MTO_SALDO_ME,0) <> 0
             OR COALESCE(MTO_SALDO_MO,0) <> 0
            )
       );

    CREATE INDEX ix_tmp_union_pa_depurada
        ON TMP_UNION_PASIVOS_ACTIVOS_DEPURADA (ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO);

    -- =====================================================================
    -- PASO 12. Atributos "base" por operación/fecha (sin COD_RUBRO), un
    --   registro por operación y día, elegido de forma determinística.
    --   (antes: TEMPORAL_TABLE, corrige el GROUP BY * original)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_ATRIBUTOS_BASE;
    CREATE TEMPORARY TABLE TMP_ATRIBUTOS_BASE AS
    SELECT t.* EXCEPT (RN) FROM (
        SELECT
            ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL,
            COD_MONEDA, COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION,
            COD_TIPO_OPERACION, COD_MODULO, FEC_VENCIMIENTO, FEC_VALOR,
            IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL, FUENTE, IND_DIA_HABIL,
            FECHA_CARGA, BATCH_ID,
            ROW_NUMBER() OVER (
                PARTITION BY ID_OPERACION_CIERRE, FECHA_PROCESO
                ORDER BY FECHA_CARGA DESC, BATCH_ID DESC
            ) AS RN
        FROM TMP_UNION_PASIVOS_ACTIVOS_DEPURADA
    ) t
    WHERE t.RN = 1;

    CREATE INDEX ix_tmp_atributos_base
        ON TMP_ATRIBUTOS_BASE (ID_OPERACION_CIERRE, FECHA_PROCESO);

    -- =====================================================================
    -- PASO 13. Totales diarios por operación (suma de todos los rubros).
    --   (antes: TABLA_1_HAMER)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_TOTALES_DIARIOS;
    CREATE TEMPORARY TABLE TMP_TOTALES_DIARIOS AS
    SELECT
        ID_OPERACION_CIERRE,
        FECHA_PROCESO,
        DAY(FECHA_PROCESO) AS NUM_DIA_MES,
        DAY(LAST_DAY(FECHA_PROCESO)) AS TOTAL_DIAS_MES,
        SUM(MTO_SALDO_ORIGEN) AS TOTAL_DIA_SALDO_ORIGEN,
        SUM(MTO_SALDO_MN)     AS TOTAL_DIA_SALDO_MN,
        SUM(MTO_SALDO_ME)     AS TOTAL_DIA_SALDO_ME,
        SUM(MTO_SALDO_MO)     AS TOTAL_DIA_SALDO_MO,
        SUM(MTO_INTERES)      AS TOTAL_DIA_INTERES,
        SUM(MTO_PREVISIONES)  AS TOTAL_DIA_PREVISIONES,
        SUM(MTO_SALDO_VIGENTE_MO)       AS TOTAL_DIA_SALDO_VIGENTE_MO,
        SUM(MTO_SALDO_RESTRUCTURADO_MO) AS TOTAL_DIA_SALDO_RESTRUCTURADO_MO,
        SUM(MTO_SALDO_REFINANCIADO_MO)  AS TOTAL_DIA_SALDO_REFINANCIADO_MO,
        SUM(MTO_SALDO_VENCIDO_MO)       AS TOTAL_DIA_SALDO_VENCIDO_MO,
        SUM(MTO_SALDO_JUDIAL_MO)        AS TOTAL_DIA_SALDO_JUDIAL_MO,
        SUM(MTO_SALDO_VIGENTE_MN)       AS TOTAL_DIA_SALDO_VIGENTE_MN,
        SUM(MTO_SALDO_RESTRUCTURADO_MN) AS TOTAL_DIA_SALDO_RESTRUCTURADO_MN,
        SUM(MTO_SALDO_REFINANCIADO_MN)  AS TOTAL_DIA_SALDO_REFINANCIADO_MN,
        SUM(MTO_SALDO_VENCIDO_MN)       AS TOTAL_DIA_SALDO_VENCIDO_MN,
        SUM(MTO_SALDO_JUDIAL_MN)        AS TOTAL_DIA_SALDO_JUDIAL_MN
    FROM TMP_UNION_PASIVOS_ACTIVOS_DEPURADA
    GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO;

    CREATE INDEX ix_tmp_totales_diarios
        ON TMP_TOTALES_DIARIOS (ID_OPERACION_CIERRE, FECHA_PROCESO);

    -- =====================================================================
    -- PASO 14. Saldos promedio mensuales (ver nota de negocio pendiente de
    --   validar en el encabezado del procedimiento).
    --   (antes: TABLA_2_HAMER_V2)
    -- =====================================================================
    DROP TEMPORARY TABLE IF EXISTS TMP_SALDOS_PROMEDIOS;
    CREATE TEMPORARY TABLE TMP_SALDOS_PROMEDIOS AS
    SELECT
        ID_OPERACION_CIERRE, FECHA_PROCESO, NUM_DIA_MES, TOTAL_DIAS_MES,
        TOTAL_DIA_SALDO_ORIGEN, TOTAL_DIA_SALDO_MN, TOTAL_DIA_SALDO_ME, TOTAL_DIA_SALDO_MO,
        TOTAL_DIA_INTERES, TOTAL_DIA_PREVISIONES,
        TOTAL_DIA_SALDO_VIGENTE_MO, TOTAL_DIA_SALDO_RESTRUCTURADO_MO,
        TOTAL_DIA_SALDO_REFINANCIADO_MO, TOTAL_DIA_SALDO_VENCIDO_MO, TOTAL_DIA_SALDO_JUDIAL_MO,
        TOTAL_DIA_SALDO_VIGENTE_MN, TOTAL_DIA_SALDO_RESTRUCTURADO_MN,
        TOTAL_DIA_SALDO_REFINANCIADO_MN, TOTAL_DIA_SALDO_VENCIDO_MN, TOTAL_DIA_SALDO_JUDIAL_MN,
        CAST(SUM(TOTAL_DIA_SALDO_ORIGEN) OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_ORIGEN,
        CAST(SUM(TOTAL_DIA_SALDO_MN)     OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_MN,
        CAST(SUM(TOTAL_DIA_SALDO_ME)     OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_ME,
        CAST(SUM(TOTAL_DIA_SALDO_MO)     OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_MO,
        CAST(SUM(TOTAL_DIA_INTERES)      OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_INTERES,
        CAST(SUM(TOTAL_DIA_PREVISIONES)  OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_PREVISIONES,
        CAST(SUM(TOTAL_DIA_SALDO_VIGENTE_MO)       OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_VIGENTE_MO,
        CAST(SUM(TOTAL_DIA_SALDO_RESTRUCTURADO_MO) OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_RESTRUCTURADO_MO,
        CAST(SUM(TOTAL_DIA_SALDO_REFINANCIADO_MO)  OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_REFINANCIADO_MO,
        CAST(SUM(TOTAL_DIA_SALDO_VENCIDO_MO)       OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_VENCIDO_MO,
        CAST(SUM(TOTAL_DIA_SALDO_JUDIAL_MO)        OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_JUDIAL_MO,
        CAST(SUM(TOTAL_DIA_SALDO_VIGENTE_MN)       OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_VIGENTE_MN,
        CAST(SUM(TOTAL_DIA_SALDO_RESTRUCTURADO_MN) OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_RESTRUCTURADO_MN,
        CAST(SUM(TOTAL_DIA_SALDO_REFINANCIADO_MN)  OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_REFINANCIADO_MN,
        CAST(SUM(TOTAL_DIA_SALDO_VENCIDO_MN)       OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_VENCIDO_MN,
        CAST(SUM(TOTAL_DIA_SALDO_JUDIAL_MN)        OVER (PARTITION BY ID_OPERACION_CIERRE, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO) ORDER BY FECHA_PROCESO) / TOTAL_DIAS_MES AS DECIMAL(18,4)) AS AVG_SALDO_JUDIAL_MN
    FROM TMP_TOTALES_DIARIOS;

    CREATE INDEX ix_tmp_saldos_promedios
        ON TMP_SALDOS_PROMEDIOS (ID_OPERACION_CIERRE, FECHA_PROCESO);

    -- =====================================================================
    -- PASO 15. Carga final en BDS_SALDOS_OPERATIVOS_HB (atómica: DELETE +
    --   INSERT dentro de la misma transacción del procedimiento).
    --   CORRECCIÓN: el DELETE ahora usa siempre v_fecha_inicio_mes, nunca
    --   una fecha literal.
    -- =====================================================================
    DELETE FROM BDS_SALDOS_OPERATIVOS_HB
    WHERE FECHA_PROCESO >= v_fecha_inicio_mes
      AND FECHA_PROCESO <= v_fecha_fin;

    INSERT INTO BDS_SALDOS_OPERATIVOS_HB (
        ID_OPERACION_CIERRE, FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_MONEDA,
        COD_PAPEL, NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION, COD_TIPO_OPERACION,
        COD_MODULO, FEC_VENCIMIENTO, FEC_VALOR, IND_CATEGORIA_RIESGO, COD_ACTI_BCO_CENTRAL,
        TOTAL_DIAS_MES, TOTAL_DIA_SALDO_ORIGEN, TOTAL_DIA_SALDO_MN, TOTAL_DIA_SALDO_ME,
        TOTAL_DIA_SALDO_MO, TOTAL_DIA_INTERES, TOTAL_DIA_PREVISIONES,
        TOTAL_DIA_SALDO_VIGENTE_MO, TOTAL_DIA_SALDO_RESTRUCTURADO_MO,
        TOTAL_DIA_SALDO_REFINANCIADO_MO, TOTAL_DIA_SALDO_VENCIDO_MO, TOTAL_DIA_SALDO_JUDIAL_MO,
        TOTAL_DIA_SALDO_VIGENTE_MN, TOTAL_DIA_SALDO_RESTRUCTURADO_MN,
        TOTAL_DIA_SALDO_REFINANCIADO_MN, TOTAL_DIA_SALDO_VENCIDO_MN, TOTAL_DIA_SALDO_JUDIAL_MN,
        AVG_SALDO_ORIGEN, AVG_SALDO_MN, AVG_SALDO_ME, AVG_SALDO_MO, AVG_INTERES, AVG_PREVISIONES,
        AVG_SALDO_VIGENTE_MO, AVG_SALDO_RESTRUCTURADO_MO, AVG_SALDO_REFINANCIADO_MO,
        AVG_SALDO_VENCIDO_MO, AVG_SALDO_JUDIAL_MO, AVG_SALDO_VIGENTE_MN,
        AVG_SALDO_RESTRUCTURADO_MN, AVG_SALDO_REFINANCIADO_MN, AVG_SALDO_VENCIDO_MN,
        AVG_SALDO_JUDIAL_MN, FUENTE, IND_DIA_HABIL, FECHA_CARGA, BATCH_ID
    )
    SELECT
        a.ID_OPERACION_CIERRE, a.FECHA_PROCESO, a.COD_EMPRESA, a.COD_SUCURSAL, a.COD_MONEDA,
        a.COD_PAPEL, a.NUM_CUENTA_BT, a.COD_OPERACION, a.COD_SUB_OPERACION, a.COD_TIPO_OPERACION,
        a.COD_MODULO, a.FEC_VENCIMIENTO, a.FEC_VALOR, a.IND_CATEGORIA_RIESGO, a.COD_ACTI_BCO_CENTRAL,
        b.TOTAL_DIAS_MES, b.TOTAL_DIA_SALDO_ORIGEN, b.TOTAL_DIA_SALDO_MN, b.TOTAL_DIA_SALDO_ME,
        b.TOTAL_DIA_SALDO_MO, b.TOTAL_DIA_INTERES, b.TOTAL_DIA_PREVISIONES,
        b.TOTAL_DIA_SALDO_VIGENTE_MO, b.TOTAL_DIA_SALDO_RESTRUCTURADO_MO,
        b.TOTAL_DIA_SALDO_REFINANCIADO_MO, b.TOTAL_DIA_SALDO_VENCIDO_MO, b.TOTAL_DIA_SALDO_JUDIAL_MO,
        b.TOTAL_DIA_SALDO_VIGENTE_MN, b.TOTAL_DIA_SALDO_RESTRUCTURADO_MN,
        b.TOTAL_DIA_SALDO_REFINANCIADO_MN, b.TOTAL_DIA_SALDO_VENCIDO_MN, b.TOTAL_DIA_SALDO_JUDIAL_MN,
        b.AVG_SALDO_ORIGEN, b.AVG_SALDO_MN, b.AVG_SALDO_ME, b.AVG_SALDO_MO, b.AVG_INTERES, b.AVG_PREVISIONES,
        b.AVG_SALDO_VIGENTE_MO, b.AVG_SALDO_RESTRUCTURADO_MO, b.AVG_SALDO_REFINANCIADO_MO,
        b.AVG_SALDO_VENCIDO_MO, b.AVG_SALDO_JUDIAL_MO, b.AVG_SALDO_VIGENTE_MN,
        b.AVG_SALDO_RESTRUCTURADO_MN, b.AVG_SALDO_REFINANCIADO_MN, b.AVG_SALDO_VENCIDO_MN,
        b.AVG_SALDO_JUDIAL_MN, a.FUENTE, a.IND_DIA_HABIL, a.FECHA_CARGA, a.BATCH_ID
    FROM TMP_ATRIBUTOS_BASE a
    INNER JOIN TMP_SALDOS_PROMEDIOS b
        ON a.ID_OPERACION_CIERRE = b.ID_OPERACION_CIERRE
       AND a.FECHA_PROCESO = b.FECHA_PROCESO
    WHERE a.FECHA_PROCESO >= v_fecha_inicio_mes
      AND a.FECHA_PROCESO <= v_fecha_fin;

    COMMIT;

    -- -------------------------------------------------------------------
    -- Limpieza de temporales tras un cierre exitoso.
    -- -------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS TMP_BASE_PERIODO_HABIL;
    DROP TEMPORARY TABLE IF EXISTS TMP_BASE_MESES_COMPLETOS;
    DROP TEMPORARY TABLE IF EXISTS TMP_CALENDARIO_DIAS_HABILES;
    DROP TEMPORARY TABLE IF EXISTS TMP_BASE_RANGO_FECHAS;
    DROP TEMPORARY TABLE IF EXISTS TMP_OPERACIONES_VIGENTES;
    DROP TEMPORARY TABLE IF EXISTS TMP_OPERACIONES_HISTORICAS;
    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_OPERACIONES;
    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_OPERACIONES_DEDUP;
    DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_FIN_MES;
    DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_FIN_MES;
    DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_INTERMEDIAS;
    DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_INTERMEDIO;
    DROP TEMPORARY TABLE IF EXISTS TMP_BRECHAS_INICIO_MES;
    DROP TEMPORARY TABLE IF EXISTS TMP_RELLENO_INICIO_MES;
    DROP TEMPORARY TABLE IF EXISTS TMP_ACTIVOS_POR_CATEGORIA;
    DROP TEMPORARY TABLE IF EXISTS TMP_ACTIVOS_DEDUP;
    DROP TEMPORARY TABLE IF EXISTS TMP_PASIVOS;
    DROP TEMPORARY TABLE IF EXISTS TMP_PASIVOS_DEDUP;
    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS;
    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS_DEDUP;
    DROP TEMPORARY TABLE IF EXISTS TMP_UNION_PASIVOS_ACTIVOS_DEPURADA;
    DROP TEMPORARY TABLE IF EXISTS TMP_ATRIBUTOS_BASE;
    DROP TEMPORARY TABLE IF EXISTS TMP_TOTALES_DIARIOS;
    DROP TEMPORARY TABLE IF EXISTS TMP_SALDOS_PROMEDIOS;

END proc_body$$

DELIMITER ;

-- =============================================================================
-- EJEMPLO DE INVOCACIÓN
--   CALL sp_calcular_saldos_operativos_hb('2026-04-03', 33);
-- =============================================================================

-- =============================================================================
-- BLOQUE OPCIONAL DE VALIDACIÓN POST-EJECUCIÓN (fuera del procedimiento,
-- no forma parte de la transacción; ejecutar manualmente para auditar)
-- =============================================================================
-- SELECT ID_OPERACION_CIERRE, COUNT(*)
-- FROM BDS_SALDOS_OPERATIVOS_HB
-- WHERE FECHA_PROCESO BETWEEN '2026-02-01' AND '2026-02-28'
-- GROUP BY ID_OPERACION_CIERRE
-- ORDER BY COUNT(*) ASC;
--
-- SELECT ID_OPERACION_CIERRE, FECHA_PROCESO, COUNT(*)
-- FROM BDS_SALDOS_OPERATIVOS_HB
-- GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO
-- HAVING COUNT(*) > 1;

-- =============================================================================
-- RECOMENDACIONES DE SEGUIMIENTO (no aplicadas aquí por requerir cambios de
-- esquema fuera del alcance de este SP)
-- =============================================================================
-- 1. Crear PARAM_RUBROS_ACTIVOS (prefijo_rubro, categoria) y
--    PARAM_MODULOS_PASIVOS (cod_modulo) para eliminar las listas
--    hardcodeadas de los pasos 9 y 10.
-- 2. Agregar una tabla de auditoría (LOG_PROCESOS_BATCH) con
--    fecha_inicio, fecha_fin, filas_insertadas, estado y mensaje_error,
--    e insertar en ella al inicio/fin del procedimiento y dentro del
--    manejador de excepción.
-- 3. Confirmar con negocio la fórmula de AVG_* documentada en el
--    encabezado (promedio prorrateado a fin de mes vs. promedio a la
--    fecha).
-- =============================================================================