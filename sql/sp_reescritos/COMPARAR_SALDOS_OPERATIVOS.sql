-- ============================================================================
--  COMPARADOR: version actual vs version reescrita
-- ----------------------------------------------------------------------------
--  PARA QUE SIRVE
--
--  Yo no puedo probar la reescritura: no tengo acceso a la base ni a los datos.
--  Lo unico que esta verificado por mi parte es la equivalencia del relleno de
--  huecos y de los tres recortes grandes, sobre datos sinteticos, en
--  scripts/verificar_relleno_sp.py y scripts/verificar_equivalencias_sp.py.
--
--  Eso demuestra que el ALGORITMO es equivalente. NO demuestra que sobre sus
--  datos reales de siete millones de operaciones salga lo mismo, porque ahi
--  aparecen casos que un dato sintetico no tiene: nulos donde no se esperan,
--  rubros fuera de catalogo, operaciones que cambian de modulo a mitad de mes.
--
--  Este script cierra esa distancia. Compara fila por fila la salida que ya
--  tiene en BDS_SALDOS_OPERATIVOS -producida por el script manual- contra la
--  que produce el procedimiento nuevo.
--
--  NO REEMPLACE EL PROCESO HASTA QUE EL PASO 6 DE CERO DIFERENCIAS.
--
--  UNA ADVERTENCIA SOBRE LAS DIFERENCIAS QUE VA A ENCONTRAR
--  --------------------------------------------------------
--  Es posible que salgan algunas, y que la version NUEVA sea la correcta. El
--  script original tenia tres puntos no deterministas -tres GROUP BY sin
--  agregar- donde el motor elegia una fila arbitraria de cada grupo. Eso
--  significa que el original no siempre daba el mismo resultado ni consigo
--  mismo.
--
--  Si aparecen diferencias, el paso 7 ayuda a separar las que vienen de ahi
--  (atributos distintos, importes iguales) de las que serian un error de
--  verdad (importes distintos).
--
--  DIALECTO
--  --------
--  Escrito en T-SQL. Para SingleStore: cambie EOMONTH(x) por LAST_DAY(x),
--  DATEFROMPARTS(YEAR(x),MONTH(x),1) por DATE_FORMAT(x,'%Y-%m-01'),
--  "SELECT ... INTO #t" por "CREATE TEMPORARY TABLE t AS SELECT ...", y
--  FORMAT(n,'N0') por el numero pelado.
-- ============================================================================

USE GNBPE_DATAHUB;
GO

-- Parametros de la comparacion. Use el mismo rango con el que corrio el
-- proceso original, o la comparacion no significa nada.
DECLARE @FECHA_PROCESO DATE = '2026-07-01';
DECLARE @DIAS          INT  = 30;

DECLARE @FECHA_INICIO_MES DATE =
    DATEFROMPARTS(YEAR(DATEADD(day, -@DIAS, @FECHA_PROCESO)),
                  MONTH(DATEADD(day, -@DIAS, @FECHA_PROCESO)), 1);


-- ============================================================================
-- 1. GUARDAR LA SALIDA ACTUAL
-- ----------------------------------------------------------------------------
-- Una copia fisica, no una vista: el procedimiento nuevo va a BORRAR el rango
-- de BDS_SALDOS_OPERATIVOS, asi que si esto fuera una vista se quedaria sin
-- nada que comparar justo cuando hace falta.
--
-- Se llama _ANTES y se deja en el esquema a proposito. Si el paso 6 da
-- diferencias, sirve para investigarlas con calma; y si hay que volver atras,
-- es la copia desde la que se restaura.
-- ============================================================================
DROP TABLE IF EXISTS dbo.BDS_SALDOS_OPERATIVOS_ANTES;

SELECT *
INTO dbo.BDS_SALDOS_OPERATIVOS_ANTES
FROM dbo.BDS_SALDOS_OPERATIVOS
WHERE FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO;

PRINT CONCAT('Copia de seguridad: ',
             (SELECT FORMAT(COUNT_BIG(*), 'N0') FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES),
             ' filas en BDS_SALDOS_OPERATIVOS_ANTES');
GO


-- ============================================================================
-- 2. EJECUTAR EL PROCEDIMIENTO NUEVO
-- ============================================================================
DECLARE @T0 DATETIME2 = SYSDATETIME();

EXEC dbo.SP_BDS_SALDOS_OPERATIVOS @P_FECHA_PROCESO = '2026-07-01', @P_DIAS = 30;

PRINT CONCAT('Duracion del proceso nuevo: ',
             DATEDIFF(second, @T0, SYSDATETIME()), ' segundos');
GO


-- ============================================================================
-- 3. CONTEO DE FILAS
-- ----------------------------------------------------------------------------
-- Lo primero y lo mas barato. Si los conteos no cuadran, no hace falta mirar
-- los importes todavia: sobran o faltan filas, y eso se diagnostica en el
-- paso 5, que dice CUALES.
-- ============================================================================
DECLARE @FECHA_PROCESO DATE = '2026-07-01';
DECLARE @DIAS          INT  = 30;
DECLARE @FECHA_INICIO_MES DATE =
    DATEFROMPARTS(YEAR(DATEADD(day, -@DIAS, @FECHA_PROCESO)),
                  MONTH(DATEADD(day, -@DIAS, @FECHA_PROCESO)), 1);

SELECT 'CONTEO' AS PRUEBA,
       (SELECT COUNT_BIG(*) FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES) AS ANTES,
       (SELECT COUNT_BIG(*) FROM dbo.BDS_SALDOS_OPERATIVOS
         WHERE FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO) AS DESPUES,
       (SELECT COUNT_BIG(*) FROM dbo.BDS_SALDOS_OPERATIVOS
         WHERE FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO)
     - (SELECT COUNT_BIG(*) FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES) AS DIFERENCIA;
GO


-- ============================================================================
-- 4. TOTALES POR DIA
-- ----------------------------------------------------------------------------
-- La prueba que de verdad importa para contabilidad. Si los totales diarios
-- cuadran al centimo, las diferencias que queden son de atributos, no de
-- dinero, y eso es mucho menos grave.
--
-- Se comparan con "<>" y no con una tolerancia porque las dos versiones usan
-- la MISMA expresion aritmetica, con el mismo orden de division y CAST. Si
-- aparece cualquier diferencia, por pequena que sea, es que algo cambio de
-- verdad y hay que mirarlo, no redondearlo.
-- ============================================================================
DECLARE @FECHA_PROCESO DATE = '2026-07-01';
DECLARE @DIAS          INT  = 30;
DECLARE @FECHA_INICIO_MES DATE =
    DATEFROMPARTS(YEAR(DATEADD(day, -@DIAS, @FECHA_PROCESO)),
                  MONTH(DATEADD(day, -@DIAS, @FECHA_PROCESO)), 1);

WITH A AS (
    SELECT FECHA_PROCESO,
           COUNT_BIG(*)                  AS FILAS,
           SUM(TOTAL_DIA_SALDO_MN)       AS SALDO_MN,
           SUM(TOTAL_DIA_SALDO_MO)       AS SALDO_MO,
           SUM(TOTAL_DIA_INTERES)        AS INTERES,
           SUM(TOTAL_AVG_SALDO_MN)       AS AVG_MN
    FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES
    GROUP BY FECHA_PROCESO
),
B AS (
    SELECT FECHA_PROCESO,
           COUNT_BIG(*)                  AS FILAS,
           SUM(TOTAL_DIA_SALDO_MN)       AS SALDO_MN,
           SUM(TOTAL_DIA_SALDO_MO)       AS SALDO_MO,
           SUM(TOTAL_DIA_INTERES)        AS INTERES,
           SUM(TOTAL_AVG_SALDO_MN)       AS AVG_MN
    FROM dbo.BDS_SALDOS_OPERATIVOS
    WHERE FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO
    GROUP BY FECHA_PROCESO
)
SELECT COALESCE(A.FECHA_PROCESO, B.FECHA_PROCESO) AS FECHA_PROCESO,
       A.FILAS AS FILAS_ANTES,  B.FILAS AS FILAS_DESPUES,
       B.FILAS - A.FILAS        AS DIF_FILAS,
       A.SALDO_MN - B.SALDO_MN  AS DIF_SALDO_MN,
       A.SALDO_MO - B.SALDO_MO  AS DIF_SALDO_MO,
       A.INTERES  - B.INTERES   AS DIF_INTERES,
       A.AVG_MN   - B.AVG_MN    AS DIF_AVG_MN
FROM A
FULL OUTER JOIN B ON A.FECHA_PROCESO = B.FECHA_PROCESO
WHERE A.FECHA_PROCESO IS NULL
   OR B.FECHA_PROCESO IS NULL
   OR A.FILAS    <> B.FILAS
   OR ISNULL(A.SALDO_MN,0) <> ISNULL(B.SALDO_MN,0)
   OR ISNULL(A.SALDO_MO,0) <> ISNULL(B.SALDO_MO,0)
   OR ISNULL(A.INTERES ,0) <> ISNULL(B.INTERES ,0)
   OR ISNULL(A.AVG_MN  ,0) <> ISNULL(B.AVG_MN  ,0)
ORDER BY 1;

PRINT 'Si no salio ninguna fila, los totales diarios son identicos.';
GO


-- ============================================================================
-- 5. FILAS QUE SOBRAN O FALTAN
-- ----------------------------------------------------------------------------
-- Solo las CLAVES, no las columnas. Lo que interesa aqui es que operaciones y
-- que dias aparecen en una version y no en la otra, porque eso apunta directo
-- a cual de los cambios lo provoco.
--
-- Como leer el resultado:
--
--   ORIGEN = 'FALTA_EN_NUEVO'   la version nueva perdio filas. Es lo grave.
--                               Normalmente significa que el ancla del relleno
--                               de huecos no encontro de donde heredar.
--
--   ORIGEN = 'SOBRA_EN_NUEVO'   la version nueva genero filas de mas. Casi
--                               siempre son las que el original PERDIA por el
--                               NOT IN con nulos, o dias que el original no
--                               rellenaba. Revise una a mano antes de darla
--                               por mala: puede que la nueva tenga razon.
-- ============================================================================
DECLARE @FECHA_PROCESO DATE = '2026-07-01';
DECLARE @DIAS          INT  = 30;
DECLARE @FECHA_INICIO_MES DATE =
    DATEFROMPARTS(YEAR(DATEADD(day, -@DIAS, @FECHA_PROCESO)),
                  MONTH(DATEADD(day, -@DIAS, @FECHA_PROCESO)), 1);

WITH NUEVO AS (
    SELECT ID_OPERACION_CIERRE, FECHA_PROCESO
    FROM dbo.BDS_SALDOS_OPERATIVOS
    WHERE FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO
)
SELECT TOP (200) 'FALTA_EN_NUEVO' AS ORIGEN, a.ID_OPERACION_CIERRE, a.FECHA_PROCESO
FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES a
WHERE NOT EXISTS (SELECT 1 FROM NUEVO n
                   WHERE n.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
                     AND n.FECHA_PROCESO       = a.FECHA_PROCESO)

UNION ALL

SELECT TOP (200) 'SOBRA_EN_NUEVO', n.ID_OPERACION_CIERRE, n.FECHA_PROCESO
FROM dbo.BDS_SALDOS_OPERATIVOS n
WHERE n.FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO
  AND NOT EXISTS (SELECT 1 FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES a
                   WHERE a.ID_OPERACION_CIERRE = n.ID_OPERACION_CIERRE
                     AND a.FECHA_PROCESO       = n.FECHA_PROCESO)
ORDER BY 1, 2, 3;
GO


-- ============================================================================
-- 6. LA PRUEBA DEFINITIVA: importes fila por fila
-- ----------------------------------------------------------------------------
-- Compara las filas que existen en las DOS versiones, columna a columna, en
-- los importes que se usan aguas abajo.
--
-- ESTE ES EL QUE TIENE QUE DAR CERO ANTES DE REEMPLAZAR NADA.
-- ============================================================================
DECLARE @FECHA_PROCESO DATE = '2026-07-01';
DECLARE @DIAS          INT  = 30;
DECLARE @FECHA_INICIO_MES DATE =
    DATEFROMPARTS(YEAR(DATEADD(day, -@DIAS, @FECHA_PROCESO)),
                  MONTH(DATEADD(day, -@DIAS, @FECHA_PROCESO)), 1);

SELECT TOP (500)
    a.ID_OPERACION_CIERRE,
    a.FECHA_PROCESO,
    a.TOTAL_DIA_SALDO_MN  AS MN_ANTES,   n.TOTAL_DIA_SALDO_MN  AS MN_NUEVO,
    a.TOTAL_DIA_SALDO_MO  AS MO_ANTES,   n.TOTAL_DIA_SALDO_MO  AS MO_NUEVO,
    a.TOTAL_AVG_SALDO_MN  AS AVG_ANTES,  n.TOTAL_AVG_SALDO_MN  AS AVG_NUEVO,
    a.TOTAL_DIAS_MES      AS DIAS_ANTES, n.TOTAL_DIAS_MES      AS DIAS_NUEVO,
    -- Que columna concreta difiere, para no tener que compararlas a ojo
    CONCAT(
        CASE WHEN ISNULL(a.TOTAL_DIA_SALDO_MN,0)  <> ISNULL(n.TOTAL_DIA_SALDO_MN,0)  THEN 'SALDO_MN '   ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_DIA_SALDO_MO,0)  <> ISNULL(n.TOTAL_DIA_SALDO_MO,0)  THEN 'SALDO_MO '   ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_DIA_SALDO_ME,0)  <> ISNULL(n.TOTAL_DIA_SALDO_ME,0)  THEN 'SALDO_ME '   ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_DIA_INTERES,0)   <> ISNULL(n.TOTAL_DIA_INTERES,0)   THEN 'INTERES '    ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_AVG_SALDO_MN,0)  <> ISNULL(n.TOTAL_AVG_SALDO_MN,0)  THEN 'AVG_MN '     ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_AVG_SALDO_MO,0)  <> ISNULL(n.TOTAL_AVG_SALDO_MO,0)  THEN 'AVG_MO '     ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_DIAS_MES,0)      <> ISNULL(n.TOTAL_DIAS_MES,0)      THEN 'DIAS_MES '   ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_DIA_SALDO_VIGENTE_MO,0) <> ISNULL(n.TOTAL_DIA_SALDO_VIGENTE_MO,0) THEN 'VIGENTE_MO ' ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_DIA_SALDO_VENCIDO_MO,0) <> ISNULL(n.TOTAL_DIA_SALDO_VENCIDO_MO,0) THEN 'VENCIDO_MO ' ELSE '' END,
        CASE WHEN ISNULL(a.TOTAL_DIA_SALDO_JUDIAL_MO,0)  <> ISNULL(n.TOTAL_DIA_SALDO_JUDIAL_MO,0)  THEN 'JUDICIAL_MO ' ELSE '' END
    ) AS COLUMNAS_QUE_DIFIEREN
FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES a
JOIN dbo.BDS_SALDOS_OPERATIVOS n
  ON n.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
 AND n.FECHA_PROCESO       = a.FECHA_PROCESO
WHERE n.FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO
  AND (   ISNULL(a.TOTAL_DIA_SALDO_MN,0)  <> ISNULL(n.TOTAL_DIA_SALDO_MN,0)
       OR ISNULL(a.TOTAL_DIA_SALDO_MO,0)  <> ISNULL(n.TOTAL_DIA_SALDO_MO,0)
       OR ISNULL(a.TOTAL_DIA_SALDO_ME,0)  <> ISNULL(n.TOTAL_DIA_SALDO_ME,0)
       OR ISNULL(a.TOTAL_DIA_INTERES,0)   <> ISNULL(n.TOTAL_DIA_INTERES,0)
       OR ISNULL(a.TOTAL_AVG_SALDO_MN,0)  <> ISNULL(n.TOTAL_AVG_SALDO_MN,0)
       OR ISNULL(a.TOTAL_AVG_SALDO_MO,0)  <> ISNULL(n.TOTAL_AVG_SALDO_MO,0)
       OR ISNULL(a.TOTAL_DIAS_MES,0)      <> ISNULL(n.TOTAL_DIAS_MES,0)
       OR ISNULL(a.TOTAL_DIA_SALDO_VIGENTE_MO,0) <> ISNULL(n.TOTAL_DIA_SALDO_VIGENTE_MO,0)
       OR ISNULL(a.TOTAL_DIA_SALDO_VENCIDO_MO,0) <> ISNULL(n.TOTAL_DIA_SALDO_VENCIDO_MO,0)
       OR ISNULL(a.TOTAL_DIA_SALDO_JUDIAL_MO,0)  <> ISNULL(n.TOTAL_DIA_SALDO_JUDIAL_MO,0))
ORDER BY ABS(ISNULL(a.TOTAL_DIA_SALDO_MN,0) - ISNULL(n.TOTAL_DIA_SALDO_MN,0)) DESC;

PRINT 'Si no salio ninguna fila: las dos versiones son equivalentes. Puede reemplazar.';
GO


-- ============================================================================
-- 7. SEPARAR LAS DIFERENCIAS "DE DINERO" DE LAS "DE ATRIBUTO"
-- ----------------------------------------------------------------------------
-- Si el paso 6 devolvio filas, este las clasifica. Es la diferencia entre un
-- problema de contabilidad y una consecuencia esperada de haber hecho
-- deterministas los tres GROUP BY arbitrarios del original.
--
--   IMPORTES_DISTINTOS   hay que investigarlo. No reemplace.
--   SOLO_ATRIBUTOS       los importes cuadran y lo que cambia es que fila gano
--                        el desempate. Es EXACTAMENTE lo que se esperaba al
--                        quitar los GROUP BY no deterministas, y de hecho es
--                        una mejora: antes el resultado podia cambiar entre dos
--                        corridas identicas.
-- ============================================================================
DECLARE @FECHA_PROCESO DATE = '2026-07-01';
DECLARE @DIAS          INT  = 30;
DECLARE @FECHA_INICIO_MES DATE =
    DATEFROMPARTS(YEAR(DATEADD(day, -@DIAS, @FECHA_PROCESO)),
                  MONTH(DATEADD(day, -@DIAS, @FECHA_PROCESO)), 1);

SELECT CASE WHEN ISNULL(a.TOTAL_DIA_SALDO_MN,0) <> ISNULL(n.TOTAL_DIA_SALDO_MN,0)
              OR ISNULL(a.TOTAL_DIA_SALDO_MO,0) <> ISNULL(n.TOTAL_DIA_SALDO_MO,0)
              OR ISNULL(a.TOTAL_AVG_SALDO_MN,0) <> ISNULL(n.TOTAL_AVG_SALDO_MN,0)
            THEN 'IMPORTES_DISTINTOS  -> investigar, NO reemplazar'
            ELSE 'SOLO_ATRIBUTOS      -> esperado, es el desempate determinista'
       END AS TIPO_DE_DIFERENCIA,
       COUNT_BIG(*) AS FILAS
FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES a
JOIN dbo.BDS_SALDOS_OPERATIVOS n
  ON n.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
 AND n.FECHA_PROCESO       = a.FECHA_PROCESO
WHERE n.FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO
  AND (   ISNULL(a.TOTAL_DIA_SALDO_MN,0)      <> ISNULL(n.TOTAL_DIA_SALDO_MN,0)
       OR ISNULL(a.TOTAL_DIA_SALDO_MO,0)      <> ISNULL(n.TOTAL_DIA_SALDO_MO,0)
       OR ISNULL(a.TOTAL_AVG_SALDO_MN,0)      <> ISNULL(n.TOTAL_AVG_SALDO_MN,0)
       OR ISNULL(a.COD_MODULO,-1)             <> ISNULL(n.COD_MODULO,-1)
       OR ISNULL(a.IND_CATEGORIA_RIESGO,'')   <> ISNULL(n.IND_CATEGORIA_RIESGO,'')
       OR ISNULL(a.COD_ACTI_BCO_CENTRAL,'')   <> ISNULL(n.COD_ACTI_BCO_CENTRAL,''))
GROUP BY CASE WHEN ISNULL(a.TOTAL_DIA_SALDO_MN,0) <> ISNULL(n.TOTAL_DIA_SALDO_MN,0)
                OR ISNULL(a.TOTAL_DIA_SALDO_MO,0) <> ISNULL(n.TOTAL_DIA_SALDO_MO,0)
                OR ISNULL(a.TOTAL_AVG_SALDO_MN,0) <> ISNULL(n.TOTAL_AVG_SALDO_MN,0)
              THEN 'IMPORTES_DISTINTOS  -> investigar, NO reemplazar'
              ELSE 'SOLO_ATRIBUTOS      -> esperado, es el desempate determinista'
         END;
GO


-- ============================================================================
-- 8. VOLVER ATRAS, SI HACE FALTA
-- ----------------------------------------------------------------------------
-- Descomentar solo si decide descartar la version nueva.
-- ============================================================================
/*
DECLARE @FECHA_PROCESO DATE = '2026-07-01';
DECLARE @DIAS          INT  = 30;
DECLARE @FECHA_INICIO_MES DATE =
    DATEFROMPARTS(YEAR(DATEADD(day, -@DIAS, @FECHA_PROCESO)),
                  MONTH(DATEADD(day, -@DIAS, @FECHA_PROCESO)), 1);

BEGIN TRANSACTION;
    DELETE FROM dbo.BDS_SALDOS_OPERATIVOS
     WHERE FECHA_PROCESO BETWEEN @FECHA_INICIO_MES AND @FECHA_PROCESO;
    INSERT INTO dbo.BDS_SALDOS_OPERATIVOS
    SELECT * FROM dbo.BDS_SALDOS_OPERATIVOS_ANTES;
COMMIT TRANSACTION;
*/
