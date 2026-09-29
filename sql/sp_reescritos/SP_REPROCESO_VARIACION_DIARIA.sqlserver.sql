-- ============================================================================
--  SP_REPROCESO_VARIACION_DIARIA   ::   SQL Server 2022
-- ----------------------------------------------------------------------------
--  Puerto a T-SQL de la version reescrita. La logica, el orden de los pasos y
--  los nombres de las CTE son los mismos que en
--  SP_REPROCESO_VARIACION_DIARIA.singlestore.sql; lea esa cabecera para el
--  detalle de por que el bucle sobraba, por que los tres INSERT son uno, y por
--  que el cero se decide por la EXISTENCIA de la fila y no con COALESCE sobre
--  el importe (ese ultimo punto es el que mas facil se hace mal).
--
--  Aqui solo va lo propio de SQL Server.
--
--  EQUIVALENCIAS
--      DATE_ADD(d, INTERVAL 1 DAY)   ->   DATEADD(day, 1, d)
--      LAST_DAY(d)                   ->   EOMONTH(d)
--      RAISE USER_EXCEPTION(msg)     ->   THROW 50000, @msg, 1
--      CREATE TEMPORARY TABLE t AS   ->   SELECT ... INTO #t
--      IF ... THEN ... END IF        ->   IF ... BEGIN ... END
--
--  EJECUCION
--      EXEC dbo.SP_REPROCESO_VARIACION_DIARIA '2026-01-01', '2026-01-31';
--      EXEC dbo.SP_REPROCESO_VARIACION_DIARIA '2026-01-01', '2026-01-31', 3;
-- ============================================================================

USE GNBPE_DATAHUB;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE dbo.SP_REPROCESO_VARIACION_DIARIA
    @P_FECHA_INICIO DATE,
    @P_FECHA_FIN    DATE,
    @P_OPCION       INT = 4   -- 4 es lo que pasaba el bucle original
AS
BEGIN
    SET NOCOUNT ON;

    -- Sin XACT_ABORT, un error en tiempo de ejecucion aborta la SENTENCIA y
    -- sigue con la siguiente: el DELETE se quedaria hecho y el INSERT no.
    SET XACT_ABORT ON;

    DECLARE @V_SIN_PAREJA BIGINT,
            @V_PRIMERA    DATE,
            @V_MSG        NVARCHAR(2000);

    IF @P_FECHA_INICIO IS NULL OR @P_FECHA_FIN IS NULL
        THROW 50010, 'P_FECHA_INICIO y P_FECHA_FIN son obligatorias.', 1;

    IF @P_FECHA_INICIO > @P_FECHA_FIN
    BEGIN
        SET @V_MSG = CONCAT('El rango esta invertido: inicio=',
                            CONVERT(varchar(10), @P_FECHA_INICIO, 23),
                            ' fin=', CONVERT(varchar(10), @P_FECHA_FIN, 23),
                            '. No se procesa nada.');
        THROW 50011, @V_MSG, 1;
    END

    IF @P_OPCION NOT IN (2, 3, 4)
    BEGIN
        SET @V_MSG = CONCAT('P_OPCION=', @P_OPCION, ' no existe. Use 2 (cierre ',
                            'mensual), 3 (diario) o 4 (diario vs cierre mensual).');
        THROW 50012, @V_MSG, 1;
    END

    -- ========================================================================
    -- 1. LA FECHA DE VARIACION DE CADA DIA
    -- ------------------------------------------------------------------------
    -- Lo que el bucle calculaba una vez por vuelta, para todos los dias de
    -- golpe. Solo entran los dias que de verdad tienen datos.
    --
    -- DIFERENCIA CON SINGLESTORE
    -- OUTER APPLY en vez de subconsulta correlacionada en el SELECT: misma
    -- semantica, pero SQL Server lo resuelve con un TOP 1 y un seek sobre el
    -- indice de FECHA_PROCESO en lugar de un agregado por cada dia.
    -- ========================================================================
    DROP TABLE IF EXISTS #VAR_PAREJAS;

    SELECT d.FECHA_PROCESO,
           v.FECHA_VARIACION
    INTO #VAR_PAREJAS
    FROM (
        SELECT DISTINCT FECHA_PROCESO
        FROM dbo.BDS_SALDOS_CIERRE_JARED
        WHERE FECHA_PROCESO BETWEEN @P_FECHA_INICIO AND @P_FECHA_FIN
    ) d
    OUTER APPLY (
        SELECT TOP (1) c.FECHA_PROCESO AS FECHA_VARIACION
        FROM dbo.BDS_SALDOS_CIERRE_JARED c
        WHERE c.FECHA_PROCESO < d.FECHA_PROCESO
          -- Opciones 2 y 4 comparan contra un FIN DE MES; la 3, contra el dia
          -- anterior con datos, sea cual sea.
          AND (@P_OPCION = 3 OR c.FECHA_PROCESO = EOMONTH(c.FECHA_PROCESO))
        ORDER BY c.FECHA_PROCESO DESC
    ) v;

    -- ========================================================================
    -- 2. AVISAR DE LOS DIAS SIN PAREJA, ANTES DE TOCAR NADA
    -- ------------------------------------------------------------------------
    -- El original hacia un "RAISE" pelado, sin mensaje: moria en el primer dia
    -- sin pareja, el log no decia cual ni por que, y los dias ya procesados
    -- quedaban escritos con el rango a medias.
    -- ========================================================================
    SELECT @V_SIN_PAREJA = COUNT_BIG(*), @V_PRIMERA = MIN(FECHA_PROCESO)
      FROM #VAR_PAREJAS WHERE FECHA_VARIACION IS NULL;

    IF @V_SIN_PAREJA > 0
    BEGIN
        SET @V_MSG = CONCAT(
            @V_SIN_PAREJA, ' dia(s) del rango no tienen fecha de comparacion. ',
            'El primero es ', CONVERT(varchar(10), @V_PRIMERA, 23), '. ',
            CASE WHEN @P_OPCION = 3
                 THEN 'Con opcion 3 hace falta un dia anterior con datos en BDS_SALDOS_CIERRE_JARED.'
                 ELSE 'Con opcion 2 o 4 hace falta un CIERRE DE FIN DE MES anterior en BDS_SALDOS_CIERRE_JARED.'
            END,
            ' No se ha escrito nada.');
        DROP TABLE IF EXISTS #VAR_PAREJAS;
        THROW 50013, @V_MSG, 1;
    END

    -- ========================================================================
    -- 3 y 4. REESCRIBIR LOS DIAS DEL RANGO, EN UNA TRANSACCION
    -- ------------------------------------------------------------------------
    -- Quien consulte la tabla mientras corre ve los dias completos, viejos o
    -- nuevos, nunca a medias.
    --
    -- Se borran solo los dias que se van a reescribir, no el rango entero: si
    -- un dia del medio no tenia datos, lo que hubiera en destino para ese dia
    -- se respeta, igual que hacia el bucle original.
    -- ========================================================================
    BEGIN TRANSACTION;

    DELETE v
      FROM dbo.BDS_SALDOS_CIERRE_VARIACION v
      JOIN #VAR_PAREJAS p ON p.FECHA_PROCESO = v.FECHA_PROCESO;

    WITH CLAVES AS (
        -- Todas las operaciones que aparecen en cualquiera de las dos fechas
        -- de cada pareja: el FULL OUTER JOIN hecho a mano.
        SELECT p.FECHA_PROCESO, p.FECHA_VARIACION,
               c.COD_EMPRESA, c.COD_SUCURSAL, c.COD_RUBRO, c.COD_MONEDA,
               c.COD_PAPEL, c.NUM_CUENTA_BT, c.COD_OPERACION,
               c.COD_SUB_OPERACION, c.COD_TIPO_OPERACION
        FROM #VAR_PAREJAS p
        JOIN dbo.BDS_SALDOS_CIERRE_JARED c
          ON c.FECHA_PROCESO IN (p.FECHA_PROCESO, p.FECHA_VARIACION)
        GROUP BY p.FECHA_PROCESO, p.FECHA_VARIACION,
                 c.COD_EMPRESA, c.COD_SUCURSAL, c.COD_RUBRO, c.COD_MONEDA,
                 c.COD_PAPEL, c.NUM_CUENTA_BT, c.COD_OPERACION,
                 c.COD_SUB_OPERACION, c.COD_TIPO_OPERACION
    ),
    EMPAREJADO AS (
        SELECT
            k.FECHA_PROCESO, k.FECHA_VARIACION,
            k.COD_EMPRESA, k.COD_SUCURSAL, k.COD_RUBRO, k.NUM_CUENTA_BT,
            -- Los testigos de existencia son act.COD_EMPRESA y ant.COD_EMPRESA,
            -- NO los importes. Como son columnas de igualdad del join, solo
            -- pueden ser NULL si no hubo emparejamiento. Un COALESCE sobre el
            -- importe confundiria "no hay fila" con "hay fila con importe NULL",
            -- y el original distingue los dos casos.
            act.COD_EMPRESA AS EXISTE_ACTUAL,
            ant.COD_EMPRESA AS EXISTE_ANTERIOR,
            COALESCE(act.ID_OPERACION, ant.ID_OPERACION) AS ID_OPERACION,
            act.MTO_SALDO_MN AS ACT_MN, act.MTO_SALDO_MO AS ACT_MO,
            ant.MTO_SALDO_MN AS ANT_MN, ant.MTO_SALDO_MO AS ANT_MO,
            COALESCE(act.COD_TIPO_OPERACION_ORIGEN, ant.COD_TIPO_OPERACION_ORIGEN) AS COD_TIPO_OPERACION_ORIGEN,
            COALESCE(act.COD_MODULO_PRODUCTO,       ant.COD_MODULO_PRODUCTO)       AS COD_MODULO_PRODUCTO
        FROM CLAVES k
        LEFT JOIN dbo.BDS_SALDOS_CIERRE_JARED act
               ON act.FECHA_PROCESO      = k.FECHA_PROCESO
              AND act.COD_EMPRESA        = k.COD_EMPRESA
              AND act.COD_SUCURSAL       = k.COD_SUCURSAL
              AND act.COD_RUBRO          = k.COD_RUBRO
              AND act.COD_MONEDA         = k.COD_MONEDA
              AND act.COD_PAPEL          = k.COD_PAPEL
              AND act.NUM_CUENTA_BT      = k.NUM_CUENTA_BT
              AND act.COD_OPERACION      = k.COD_OPERACION
              AND act.COD_SUB_OPERACION  = k.COD_SUB_OPERACION
              AND act.COD_TIPO_OPERACION = k.COD_TIPO_OPERACION
        LEFT JOIN dbo.BDS_SALDOS_CIERRE_JARED ant
               ON ant.FECHA_PROCESO      = k.FECHA_VARIACION
              AND ant.COD_EMPRESA        = k.COD_EMPRESA
              AND ant.COD_SUCURSAL       = k.COD_SUCURSAL
              AND ant.COD_RUBRO          = k.COD_RUBRO
              AND ant.COD_MONEDA         = k.COD_MONEDA
              AND ant.COD_PAPEL          = k.COD_PAPEL
              AND ant.NUM_CUENTA_BT      = k.NUM_CUENTA_BT
              AND ant.COD_OPERACION      = k.COD_OPERACION
              AND ant.COD_SUB_OPERACION  = k.COD_SUB_OPERACION
              AND ant.COD_TIPO_OPERACION = k.COD_TIPO_OPERACION
    )
    INSERT INTO dbo.BDS_SALDOS_CIERRE_VARIACION (
        ID_OPERACION, FECHA_PROCESO, FECHA_PROCESO_MA,
        COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, NUM_CUENTA_BT,
        SALDO_MN_MA, SALDO_MN_ACTUAL, SALDO_MN_VARIACION,
        SALDO_MO_MA, SALDO_MO_ACTUAL, SALDO_MO_VARIACION,
        COD_TIPO_OPERACION_ORIGEN, COD_MODULO_PRODUCTO, FLG_PRODUCTO_GASTO
    )
    SELECT
        ID_OPERACION,
        FECHA_PROCESO,
        -- NULL cuando la operacion no existia en la fecha de variacion: es el
        -- caso 3 del original.
        CASE WHEN EXISTE_ANTERIOR IS NOT NULL THEN FECHA_VARIACION END,

        COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, NUM_CUENTA_BT,

        CASE WHEN EXISTE_ANTERIOR IS NOT NULL THEN ANT_MN ELSE 0 END,
        CASE WHEN EXISTE_ACTUAL   IS NOT NULL THEN ACT_MN ELSE 0 END,
          (CASE WHEN EXISTE_ACTUAL   IS NOT NULL THEN ACT_MN ELSE 0 END)
        - (CASE WHEN EXISTE_ANTERIOR IS NOT NULL THEN ANT_MN ELSE 0 END),

        CASE WHEN EXISTE_ANTERIOR IS NOT NULL THEN ANT_MO ELSE 0 END,
        CASE WHEN EXISTE_ACTUAL   IS NOT NULL THEN ACT_MO ELSE 0 END,
          (CASE WHEN EXISTE_ACTUAL   IS NOT NULL THEN ACT_MO ELSE 0 END)
        - (CASE WHEN EXISTE_ANTERIOR IS NOT NULL THEN ANT_MO ELSE 0 END),

        COD_TIPO_OPERACION_ORIGEN,
        COD_MODULO_PRODUCTO,

        -- El mismo CASE del original, escrito UNA vez en vez de tres. Las
        -- reglas no cambian; deja de haber tres copias que pueden divergir sin
        -- que nada avise.
        CASE
            WHEN COD_MODULO_PRODUCTO = 20  THEN CASE WHEN COD_TIPO_OPERACION_ORIGEN = 8  THEN 10 ELSE 7 END
            WHEN COD_MODULO_PRODUCTO = 21  THEN 8
            WHEN COD_MODULO_PRODUCTO = 22  THEN CASE WHEN COD_TIPO_OPERACION_ORIGEN = 10 THEN 11 ELSE 9 END
            WHEN COD_MODULO_PRODUCTO = 120 THEN CASE WHEN COD_TIPO_OPERACION_ORIGEN = 30 THEN 4  ELSE 3 END
            WHEN COD_MODULO_PRODUCTO = 155 THEN 10
            WHEN COD_MODULO_PRODUCTO = 185 THEN CASE WHEN COD_TIPO_OPERACION_ORIGEN = 0  THEN 5
                                                    WHEN COD_TIPO_OPERACION_ORIGEN = 15 THEN 3
                                                    WHEN COD_TIPO_OPERACION_ORIGEN = 3  THEN 10 ELSE 2 END
            WHEN COD_MODULO_PRODUCTO = 321 THEN 6
            ELSE 0
        END
    FROM EMPAREJADO;

    COMMIT TRANSACTION;

    DROP TABLE IF EXISTS #VAR_PAREJAS;
END
GO
