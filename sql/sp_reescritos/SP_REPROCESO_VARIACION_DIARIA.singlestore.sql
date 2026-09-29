-- ============================================================================
--  SP_REPROCESO_VARIACION_DIARIA   ::   SingleStore
-- ----------------------------------------------------------------------------
--  Reescritura de SP_REPROCESO_VARIACION_DIARIA.sql.
--
--      bucle WHILE dia a dia   ->   una pasada sobre todo el rango
--      3 INSERT por dia        ->   1 INSERT en total
--
--  Para un mes: de 124 sentencias (31 DELETE + 93 INSERT) a 2.
--
-- ============================================================================
--  POR QUE EL BUCLE SOBRABA
-- ----------------------------------------------------------------------------
--  El original era esto:
--
--      V_FECHA = P_FECHA_INICIO;
--      WHILE V_FECHA <= P_FECHA_FIN LOOP
--          CALL SP_CREAR_VARIACION_CONTABLE(4, V_FECHA);
--          V_FECHA = DATE_ADD(V_FECHA, INTERVAL 1 DAY);
--      END LOOP;
--
--  Y cada una de esas llamadas hacia, por dentro, un DELETE y tres INSERT.
--
--  En un motor distribuido como SingleStore eso es lo mas caro que se puede
--  hacer: cada sentencia se planifica, se reparte entre los nodos, espera a
--  que todos terminen y se consolida. Treinta y una veces, en serie, cuando
--  los dias no dependen unos de otros y podrian resolverse todos a la vez.
--
--  Lo unico que cambia entre un dia y otro es su FECHA_VARIACION, y eso es un
--  dato, no una razon para repetir la sentencia: se calcula para todos los
--  dias de golpe en una CTE y se une por ahi.
--
-- ============================================================================
--  POR QUE LOS TRES INSERT SON UNO
-- ----------------------------------------------------------------------------
--  SP_CREAR_VARIACION_CONTABLE escribe tres INSERT contra la misma tabla:
--
--      1. la operacion existe en las DOS fechas      -> INNER JOIN
--      2. existia en la fecha de variacion y ya no   -> LEFT ANTI JOIN
--      3. existe ahora y no existia antes            -> RIGHT ANTI JOIN
--
--  Esos tres casos, juntos, son exactamente un FULL OUTER JOIN. Aqui se
--  resuelven con las claves unidas y dos LEFT JOIN, que hace lo mismo y es
--  portable a cualquier motor.
--
--  Ademas, el CASE de FLG_PRODUCTO_GASTO -catorce lineas- estaba copiado tres
--  veces, una por INSERT. Cualquier cambio en las reglas habia que hacerlo en
--  los tres sitios, y olvidarse de uno no da ningun error: solo deja un
--  tercio de las filas mal clasificadas.
--
-- ============================================================================
--  UN DETALLE QUE COSTO UN FALLO EN LA PRUEBA
-- ----------------------------------------------------------------------------
--  Al unificar, la tentacion es escribir COALESCE(ant.MTO_SALDO_MN, 0). Es
--  incorrecto, y de una forma que no se ve: confunde dos cosas distintas.
--
--      la fila NO EXISTE                 -> el saldo anterior es 0
--      la fila existe con importe NULL   -> el saldo anterior es NULL
--
--  El original distingue los dos casos: en el segundo, la resta da NULL y la
--  variacion queda en NULL. Con COALESCE saldria una variacion de 80 donde
--  antes salia NULL, y nadie lo notaria hasta cuadrar un reporte.
--
--  Por eso el cero se decide por la EXISTENCIA de la fila, mirando una columna
--  de igualdad del join (COD_EMPRESA), que solo puede ser NULL si no hubo
--  emparejamiento. La equivalencia esta verificada fila por fila en
--  scripts/verificar_variacion_sp.py.
--
-- ============================================================================
--  EJECUCION
-- ----------------------------------------------------------------------------
--      CALL SP_REPROCESO_VARIACION_DIARIA('2026-01-01', '2026-01-31');
--      CALL SP_REPROCESO_VARIACION_DIARIA('2026-01-01', '2026-01-31', 3);
--
--  P_OPCION, igual que en SP_CREAR_VARIACION_CONTABLE:
--      2  cierre mensual              contra el ultimo fin de mes anterior
--      3  diario                      contra el dia anterior con datos
--      4  diario vs cierre mensual    contra el ultimo fin de mes anterior
--
--  Es idempotente: reejecutar el mismo rango da el mismo resultado.
--  El original tambien lo era, porque cada CALL borraba su dia.
-- ============================================================================

DELIMITER //

CREATE OR REPLACE PROCEDURE `DATAHUB`.`SP_REPROCESO_VARIACION_DIARIA`(
    P_FECHA_INICIO DATE NULL,
    P_FECHA_FIN    DATE NULL,
    P_OPCION       INT  NULL
) RETURNS void AS

DECLARE
    V_OPCION      INT;
    V_SIN_PAREJA  BIGINT;
    V_PRIMERA     DATE;

BEGIN

    -- 4 es lo que pasaba el bucle original, asi que es el valor por defecto.
    V_OPCION = COALESCE(P_OPCION, 4);

    IF P_FECHA_INICIO IS NULL OR P_FECHA_FIN IS NULL THEN
        RAISE USER_EXCEPTION('P_FECHA_INICIO y P_FECHA_FIN son obligatorias.');
    END IF;

    IF P_FECHA_INICIO > P_FECHA_FIN THEN
        RAISE USER_EXCEPTION(CONCAT(
            'El rango esta invertido: inicio=', P_FECHA_INICIO,
            ' fin=', P_FECHA_FIN, '. No se procesa nada.'));
    END IF;

    IF V_OPCION NOT IN (2, 3, 4) THEN
        RAISE USER_EXCEPTION(CONCAT(
            'P_OPCION=', V_OPCION, ' no existe. Use 2 (cierre mensual), ',
            '3 (diario) o 4 (diario vs cierre mensual).'));
    END IF;

    -- ========================================================================
    -- 1. LA FECHA DE VARIACION DE CADA DIA
    -- ------------------------------------------------------------------------
    -- Esto es lo que el bucle calculaba una vez por vuelta. Aqui sale para
    -- todos los dias del rango de una sola vez.
    --
    -- Solo entran los dias que DE VERDAD tienen datos: si el 5 de enero no hay
    -- nada en BDS_SALDOS_CIERRE_JARED, no tiene sentido calcularle variacion.
    -- El bucle original tampoco lo hacia bien: llamaba igual para ese dia y el
    -- resultado era un DELETE de un dia vacio y tres INSERT de cero filas.
    -- ========================================================================
    DROP TABLE IF EXISTS TMP_VAR_PAREJAS;

    CREATE TEMPORARY TABLE TMP_VAR_PAREJAS AS
    WITH DIAS AS (
        SELECT DISTINCT FECHA_PROCESO
        FROM BDS_SALDOS_CIERRE_JARED
        WHERE FECHA_PROCESO BETWEEN P_FECHA_INICIO AND P_FECHA_FIN
    )
    SELECT
        d.FECHA_PROCESO,
        (SELECT MAX(v.FECHA_PROCESO)
           FROM BDS_SALDOS_CIERRE_JARED v
          WHERE v.FECHA_PROCESO < d.FECHA_PROCESO
            -- Opciones 2 y 4 comparan contra un FIN DE MES; la 3, contra el
            -- dia anterior con datos, sea cual sea.
            AND (V_OPCION = 3
                 OR DAY(v.FECHA_PROCESO) = DAY(LAST_DAY(v.FECHA_PROCESO)))
        ) AS FECHA_VARIACION
    FROM DIAS d;

    -- ========================================================================
    -- 2. AVISAR DE LOS DIAS QUE NO TIENEN CONTRA QUE COMPARARSE
    -- ------------------------------------------------------------------------
    -- El original hacia "IF V_FECHA_VARIACION IS NULL THEN RAISE; END IF", un
    -- RAISE pelado sin mensaje: el bucle moria en el primer dia sin pareja y
    -- el log decia unicamente que algo habia fallado, sin decir que dia ni por
    -- que. Y los dias ya procesados quedaban escritos, asi que el rango
    -- quedaba a medias.
    --
    -- Aqui se dice CUANTOS dias y CUAL es el primero, y se para ANTES de tocar
    -- la tabla destino.
    -- ========================================================================
    SELECT COUNT(*), MIN(FECHA_PROCESO)
      INTO V_SIN_PAREJA, V_PRIMERA
      FROM TMP_VAR_PAREJAS
     WHERE FECHA_VARIACION IS NULL;

    IF V_SIN_PAREJA > 0 THEN
        DROP TABLE IF EXISTS TMP_VAR_PAREJAS;
        RAISE USER_EXCEPTION(CONCAT(
            V_SIN_PAREJA, ' dia(s) del rango no tienen fecha de comparacion. ',
            'El primero es ', V_PRIMERA, '. ',
            CASE WHEN V_OPCION = 3
                 THEN 'Con opcion 3 hace falta que exista un dia anterior con datos en BDS_SALDOS_CIERRE_JARED.'
                 ELSE 'Con opcion 2 o 4 hace falta un CIERRE DE FIN DE MES anterior en BDS_SALDOS_CIERRE_JARED.'
            END,
            ' No se ha escrito nada.'));
    END IF;

    -- ========================================================================
    -- 3. LIMPIAR EL RANGO
    -- ------------------------------------------------------------------------
    -- Un solo DELETE del rango entero, en vez de uno por dia dentro del bucle.
    -- Se borran solo los dias que se van a reescribir, no el rango completo:
    -- si un dia del medio no tenia datos, lo que hubiera en destino para ese
    -- dia se respeta, igual que hacia el original.
    -- ========================================================================
    DELETE v
      FROM BDS_SALDOS_CIERRE_VARIACION v
      JOIN TMP_VAR_PAREJAS p ON p.FECHA_PROCESO = v.FECHA_PROCESO;

    -- ========================================================================
    -- 4. LOS TRES CASOS, EN UN SOLO INSERT
    -- ========================================================================
    INSERT INTO BDS_SALDOS_CIERRE_VARIACION (
        ID_OPERACION, FECHA_PROCESO, FECHA_PROCESO_MA,
        COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, NUM_CUENTA_BT,
        SALDO_MN_MA, SALDO_MN_ACTUAL, SALDO_MN_VARIACION,
        SALDO_MO_MA, SALDO_MO_ACTUAL, SALDO_MO_VARIACION,
        COD_TIPO_OPERACION_ORIGEN, COD_MODULO_PRODUCTO, FLG_PRODUCTO_GASTO
    )
    WITH CLAVES AS (
        -- Todas las operaciones que aparecen en cualquiera de las dos fechas
        -- de cada pareja. Es el "FULL OUTER JOIN" hecho a mano.
        SELECT p.FECHA_PROCESO, p.FECHA_VARIACION,
               c.COD_EMPRESA, c.COD_SUCURSAL, c.COD_RUBRO, c.COD_MONEDA,
               c.COD_PAPEL, c.NUM_CUENTA_BT, c.COD_OPERACION,
               c.COD_SUB_OPERACION, c.COD_TIPO_OPERACION
        FROM TMP_VAR_PAREJAS p
        JOIN BDS_SALDOS_CIERRE_JARED c
          ON c.FECHA_PROCESO IN (p.FECHA_PROCESO, p.FECHA_VARIACION)
        GROUP BY p.FECHA_PROCESO, p.FECHA_VARIACION,
                 c.COD_EMPRESA, c.COD_SUCURSAL, c.COD_RUBRO, c.COD_MONEDA,
                 c.COD_PAPEL, c.NUM_CUENTA_BT, c.COD_OPERACION,
                 c.COD_SUB_OPERACION, c.COD_TIPO_OPERACION
    ),
    EMPAREJADO AS (
        SELECT
            k.FECHA_PROCESO,
            k.FECHA_VARIACION,
            k.COD_EMPRESA, k.COD_SUCURSAL, k.COD_RUBRO, k.NUM_CUENTA_BT,
            -- ATENCION: los testigos de existencia son act.COD_EMPRESA y
            -- ant.COD_EMPRESA, NO los importes. La diferencia importa cuando
            -- la fila existe con importe NULL; vea la cabecera.
            act.COD_EMPRESA AS EXISTE_ACTUAL,
            ant.COD_EMPRESA AS EXISTE_ANTERIOR,
            COALESCE(act.ID_OPERACION, ant.ID_OPERACION) AS ID_OPERACION,
            act.MTO_SALDO_MN AS ACT_MN, act.MTO_SALDO_MO AS ACT_MO,
            ant.MTO_SALDO_MN AS ANT_MN, ant.MTO_SALDO_MO AS ANT_MO,
            COALESCE(act.COD_TIPO_OPERACION_ORIGEN, ant.COD_TIPO_OPERACION_ORIGEN) AS COD_TIPO_OPERACION_ORIGEN,
            COALESCE(act.COD_MODULO_PRODUCTO,       ant.COD_MODULO_PRODUCTO)       AS COD_MODULO_PRODUCTO
        FROM CLAVES k
        LEFT JOIN BDS_SALDOS_CIERRE_JARED act
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
        LEFT JOIN BDS_SALDOS_CIERRE_JARED ant
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
    SELECT
        ID_OPERACION,
        FECHA_PROCESO,
        -- NULL cuando la operacion no existia en la fecha de variacion, que es
        -- el caso 3 del original.
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

        -- El mismo CASE que el original, pero escrito UNA vez en vez de tres.
        -- Las reglas no cambian; solo deja de haber tres copias que pueden
        -- divergir sin que nada avise.
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

    DROP TABLE IF EXISTS TMP_VAR_PAREJAS;

END //

DELIMITER ;
