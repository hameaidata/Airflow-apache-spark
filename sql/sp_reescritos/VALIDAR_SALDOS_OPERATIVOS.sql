/* ===========================================================================
   VALIDAR_SALDOS_OPERATIVOS   ::   SingleStore
   ---------------------------------------------------------------------------
   Son LAS MISMAS comprobaciones que estan intercaladas en el script original,
   con sus mismos criterios, apuntadas a la salida del procedimiento.

   COMO SE EJECUTA
   ---------------
   Las comprobaciones 1 a 4 necesitan el grano (operacion, rubro, dia), y ese
   grano SOLO existe en TMP_SO_COMPLETO, que es TEMPORARY. Eso significa que
   hay que correrlas EN LA MISMA SESION que el CALL y ANTES de que el
   procedimiento haga su DROP final:

       CALL SP_BDS_SALDOS_OPERATIVOS('2026-07-01', 30);
       -- ...y aqui, en la misma conexion, los SELECT 1 a 4

   Si prefiere correrlas despues, comente los dos DROP del paso 7 del
   procedimiento mientras valida. Las comprobaciones 5 a 8 van contra
   BDS_SALDOS_OPERATIVOS y se pueden correr cuando sea.

   Ajuste estas dos variables al rango que acaba de procesar.
=========================================================================== */

SET @v_fecha_inicio_mes = '2026-06-01';
SET @v_fecha_fin        = '2026-06-30';


/* ===========================================================================
   1. VALIDACION DE DOMINGOS            >>> DEBE SALIR VACIO <<<
   ---------------------------------------------------------------------------
   Es su comprobacion, tal cual. El domingo copia del sabado, asi que los dos
   saldos tienen que ser identicos.

   Sigue siendo valida con la logica nueva, y conviene saber por que: si el
   sabado NO es habil, sabado y domingo copian los dos del viernes. Si el
   sabado SI es habil y tiene dato, el domingo copia de el. Y si el sabado es
   habil pero la operacion no tiene dato ese dia, los dos salen por relleno en
   cero. En los tres casos coinciden.
=========================================================================== */
SELECT
    D.ID_OPERACION_CIERRE,
    D.FECHA_PROCESO                              AS FECHA_DOMINGO,
    DATE_SUB(D.FECHA_PROCESO, INTERVAL 1 DAY)    AS FECHA_SABADO,
    D.MTO_SALDO_MN                               AS SALDO_DOMINGO,
    S.MTO_SALDO_MN                               AS SALDO_SABADO
FROM TMP_SO_COMPLETO D
INNER JOIN TMP_SO_COMPLETO S
    ON  S.ID_OPERACION_CIERRE = D.ID_OPERACION_CIERRE
    AND S.COD_RUBRO           = D.COD_RUBRO
    AND S.FECHA_PROCESO       = DATE_SUB(D.FECHA_PROCESO, INTERVAL 1 DAY)
WHERE DAYOFWEEK(D.FECHA_PROCESO) = 1
  AND COALESCE(D.MTO_SALDO_MN, 0) <> COALESCE(S.MTO_SALDO_MN, 0);

-- El conteo de la misma comprobacion: tiene que dar 0.
SELECT COUNT(*) AS DIFERENCIAS_DOMINGO
FROM TMP_SO_COMPLETO D
INNER JOIN TMP_SO_COMPLETO S
    ON  S.ID_OPERACION_CIERRE = D.ID_OPERACION_CIERRE
    AND S.COD_RUBRO           = D.COD_RUBRO
    AND S.FECHA_PROCESO       = DATE_SUB(D.FECHA_PROCESO, INTERVAL 1 DAY)
WHERE DAYOFWEEK(D.FECHA_PROCESO) = 1
  AND COALESCE(D.MTO_SALDO_MN, 0) <> COALESCE(S.MTO_SALDO_MN, 0);


/* ===========================================================================
   2. DIAS POR MES            >>> CONTEO = DIAS DE ESE MES, SIEMPRE <<<
   ---------------------------------------------------------------------------
   EL INVARIANTE VA POR MES Y POR RUBRO.

   El grano de TMP_SO_COMPLETO es (operacion, rubro, dia). Por eso el GROUP BY
   lleva el rubro y el mes: cada combinacion tiene que tener EXACTAMENTE los
   dias de su mes. Marzo 31, febrero 28, abril 30.

   Ojo al agrupar solo por ID_OPERACION_CIERRE: una operacion con dos rubros da
   62 en marzo, y eso NO es un duplicado -son 2 rubros x 31 dias, con 31 en cada
   uno-. Para contar dias hay que incluir el rubro, o usar la comprobacion 6,
   que va contra el destino, donde los rubros ya estan sumados.

   UNICA EXCEPCION: el mes donde cae @v_fecha_fin. Si proceso hasta el dia 15,
   ese mes tendra 15 dias. Los dias que todavia no existen no se inventan, y el
   HAVING ya lo descuenta.
=========================================================================== */
SELECT
    ID_OPERACION_CIERRE,
    COD_RUBRO,
    YEAR(FECHA_PROCESO)               AS ANIO,
    MONTH(FECHA_PROCESO)              AS MES,
    COUNT(*)                          AS CONTEO,
    DAY(LAST_DAY(MAX(FECHA_PROCESO))) AS DIAS_DEL_MES
FROM TMP_SO_COMPLETO
WHERE FECHA_PROCESO >= @v_fecha_inicio_mes
  AND FECHA_PROCESO <= @v_fecha_fin
GROUP BY ID_OPERACION_CIERRE, COD_RUBRO,
         YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
HAVING COUNT(*) <> DAY(LAST_DAY(MAX(FECHA_PROCESO)))
   AND MAX(FECHA_PROCESO) <> @v_fecha_fin
ORDER BY COUNT(*) ASC
LIMIT 50;


/* ===========================================================================
   3. UNA FILA POR FECHA      >>> TODO 1, SIN EXCEPCION <<<
   ---------------------------------------------------------------------------
   Su consulta. En el GROUP BY por (operacion, fecha, rubro) el COUNT tiene que
   ser 1 en cada fecha. Cualquier valor mayor es un duplicado.

   Con el arreglo esto se cumple por construccion: TMP_SO_DIARIO agrupa por esa
   misma clave y SUMA los montos de las filas repetidas en vez de descartar una,
   y la espina toma el calendario con DISTINCT para que un dia repetido en
   BDS_CALENDARIOS no duplique la fila de relleno.
=========================================================================== */
SELECT
    ID_OPERACION_CIERRE,
    FECHA_PROCESO,
    COD_RUBRO,
    COUNT(*) AS VECES
FROM TMP_SO_COMPLETO
GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO
HAVING COUNT(*) <> 1
ORDER BY COUNT(*) DESC
LIMIT 50;


/* ===========================================================================
   4. RANGO DE FECHAS CUBIERTO
   ---------------------------------------------------------------------------
   Su MIN/MAX. El minimo tiene que ser el dia 1 del mes de inicio y el maximo
   la fecha de proceso.
=========================================================================== */
SELECT
    MIN(FECHA_PROCESO) AS DESDE,
    MAX(FECHA_PROCESO) AS HASTA,
    COUNT(*)           AS FILAS
FROM TMP_SO_COMPLETO;


/* ===========================================================================
   5. OPERACIONES QUE SE PIERDEN EN EL JOIN FINAL   >>> DEBE SALIR 0 <<<
   ---------------------------------------------------------------------------
   Esta sale de su propia observacion. En el script original:

       SELECT COUNT(DISTINCT ID_OPERACION_CIERRE) FROM TEMPORAL_TABLE     -- 598778
       SELECT COUNT(DISTINCT ID_OPERACION_CIERRE) FROM TABLA_2_HAMER_V2   -- 606229

   Son 7451 operaciones de diferencia, y el INSERT final las unia con INNER
   JOIN. Es decir: 7451 operaciones que tenian promedios calculados NO llegaban
   al destino, en silencio.

   Aqui se comprueba contra el origen: cada operacion con dias en el rango tiene
   que estar en la salida.
=========================================================================== */
SELECT COUNT(*) AS OPERACIONES_PERDIDAS
FROM (
    SELECT DISTINCT ID_OPERACION_CIERRE
    FROM TMP_SO_COMPLETO
    WHERE FECHA_PROCESO >= @v_fecha_inicio_mes
      AND FECHA_PROCESO <= @v_fecha_fin
) o
LEFT JOIN (
    SELECT DISTINCT ID_OPERACION_CIERRE
    FROM BDS_SALDOS_OPERATIVOS
    WHERE FECHA_PROCESO >= @v_fecha_inicio_mes
      AND FECHA_PROCESO <= @v_fecha_fin
) d ON d.ID_OPERACION_CIERRE = o.ID_OPERACION_CIERRE
WHERE d.ID_OPERACION_CIERRE IS NULL;


/* ===========================================================================
   6. DIAS POR OPERACION EN EL DESTINO  >>> EL MINIMO = DIAS DEL MES <<<
   ---------------------------------------------------------------------------
   La misma comprobacion que la 2, pero en BDS_SALDOS_OPERATIVOS, donde el
   grano ya es (operacion, dia): el rubro esta agregado.
=========================================================================== */
SELECT
    ID_OPERACION_CIERRE,
    COUNT(*) AS DIAS
FROM BDS_SALDOS_OPERATIVOS
WHERE FECHA_PROCESO >= @v_fecha_inicio_mes
  AND FECHA_PROCESO <= @v_fecha_fin
GROUP BY ID_OPERACION_CIERRE
ORDER BY COUNT(*) ASC
LIMIT 50;


/* ===========================================================================
   7. DUPLICADOS EN EL DESTINO          >>> DEBE SALIR VACIO <<<
=========================================================================== */
SELECT
    ID_OPERACION_CIERRE,
    FECHA_PROCESO,
    COUNT(*) AS VECES
FROM BDS_SALDOS_OPERATIVOS
GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 50;


/* ===========================================================================
   8. EL PROMEDIO CUADRA                >>> DEBE SALIR VACIO <<<
   ---------------------------------------------------------------------------
   Su comprobacion final, la que compara el promedio guardado el ultimo dia del
   mes contra el que se recalcula sumando los dias y dividiendo entre los dias
   del mes.

   Se mantiene su ROUND a 3 decimales y su ORDER BY por la diferencia
   descendente, para que lo peor salga arriba.
=========================================================================== */
WITH BASE AS (
    SELECT
        ID_OPERACION_CIERRE,
        MAX(FECHA_PROCESO) AS FECHA_PROCESO,
        SUM(TOTAL_DIA_SALDO_MN) / DAY(LAST_DAY(MAX(FECHA_PROCESO))) AS TOTAL_AVG_SALDO_MN
    FROM BDS_SALDOS_OPERATIVOS
    WHERE FECHA_PROCESO >= @v_fecha_inicio_mes
      AND FECHA_PROCESO <= @v_fecha_fin
    GROUP BY YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO), ID_OPERACION_CIERRE
),
BASE_2 AS (
    SELECT
        ID_OPERACION_CIERRE,
        FECHA_PROCESO,
        TOTAL_AVG_SALDO_MN
    FROM BDS_SALDOS_OPERATIVOS
    WHERE FECHA_PROCESO = LAST_DAY(FECHA_PROCESO)
      AND FECHA_PROCESO >= @v_fecha_inicio_mes
      AND FECHA_PROCESO <= @v_fecha_fin
)
SELECT
    A.ID_OPERACION_CIERRE,
    A.FECHA_PROCESO,
    A.TOTAL_AVG_SALDO_MN AS TOTAL_AVG_RECALCULADO,
    B.TOTAL_AVG_SALDO_MN AS TOTAL_AVG_GUARDADO,
    ABS(ROUND(A.TOTAL_AVG_SALDO_MN, 3) - ROUND(B.TOTAL_AVG_SALDO_MN, 3)) AS DIFERENCIA
FROM BASE A
INNER JOIN BASE_2 B
    ON  A.ID_OPERACION_CIERRE = B.ID_OPERACION_CIERRE
    AND A.FECHA_PROCESO       = B.FECHA_PROCESO
WHERE IFNULL(ROUND(A.TOTAL_AVG_SALDO_MN, 3), 0)
   <> IFNULL(ROUND(B.TOTAL_AVG_SALDO_MN, 3), 0)
ORDER BY ABS(ROUND(A.TOTAL_AVG_SALDO_MN, 3) - ROUND(B.TOTAL_AVG_SALDO_MN, 3)) DESC
LIMIT 50;


/* ===========================================================================
   RESUMEN: las seis que tienen veredicto. Todas deben dar 0.
=========================================================================== */
SELECT 'domingos distintos del sabado' AS COMPROBACION,
       (SELECT COUNT(*) FROM TMP_SO_COMPLETO D
          INNER JOIN TMP_SO_COMPLETO S
             ON S.ID_OPERACION_CIERRE = D.ID_OPERACION_CIERRE
            AND S.COD_RUBRO = D.COD_RUBRO
            AND S.FECHA_PROCESO = DATE_SUB(D.FECHA_PROCESO, INTERVAL 1 DAY)
         WHERE DAYOFWEEK(D.FECHA_PROCESO) = 1
           AND COALESCE(D.MTO_SALDO_MN,0) <> COALESCE(S.MTO_SALDO_MN,0)) AS CUANTOS
UNION ALL
SELECT 'duplicados por operacion+rubro+dia',
       (SELECT COUNT(*) FROM (
           SELECT 1 FROM TMP_SO_COMPLETO
            GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO
           HAVING COUNT(*) > 1) z)
UNION ALL
SELECT 'duplicados en el destino',
       (SELECT COUNT(*) FROM (
           SELECT 1 FROM BDS_SALDOS_OPERATIVOS
            WHERE FECHA_PROCESO >= @v_fecha_inicio_mes
              AND FECHA_PROCESO <= @v_fecha_fin
            GROUP BY ID_OPERACION_CIERRE, FECHA_PROCESO
           HAVING COUNT(*) > 1) z)
UNION ALL
SELECT 'meses incompletos (por operacion+rubro+mes)',
       (SELECT COUNT(*) FROM (
           SELECT 1 FROM TMP_SO_COMPLETO
            WHERE FECHA_PROCESO >= @v_fecha_inicio_mes
              AND FECHA_PROCESO <= @v_fecha_fin
            GROUP BY ID_OPERACION_CIERRE, COD_RUBRO,
                     YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
           HAVING COUNT(*) <> DAY(LAST_DAY(MAX(FECHA_PROCESO)))
              AND MAX(FECHA_PROCESO) <> @v_fecha_fin) z);


/* ===========================================================================
   MES A MES: el desglose, para ver cual mes falla si alguno falla.
=========================================================================== */
SELECT
    YEAR(FECHA_PROCESO)                       AS ANIO,
    MONTH(FECHA_PROCESO)                      AS MES,
    DAY(LAST_DAY(MAX(FECHA_PROCESO)))         AS DIAS_DEL_MES,
    COUNT(DISTINCT FECHA_PROCESO)             AS DIAS_PRESENTES,
    COUNT(DISTINCT CONCAT(ID_OPERACION_CIERRE, '|', COD_RUBRO)) AS LLAVES,
    COUNT(*)                                  AS FILAS,
    COUNT(DISTINCT CONCAT(ID_OPERACION_CIERRE, '|', COD_RUBRO))
      * DAY(LAST_DAY(MAX(FECHA_PROCESO)))     AS FILAS_ESPERADAS
FROM TMP_SO_COMPLETO
WHERE FECHA_PROCESO >= @v_fecha_inicio_mes
  AND FECHA_PROCESO <= @v_fecha_fin
GROUP BY YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
ORDER BY 1, 2;
