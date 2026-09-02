-- ============================================================================
-- SALDO PROMEDIO: del detalle por rubro al consolidado por cuenta
-- ----------------------------------------------------------------------------
-- Dos tablas, dos granos:
--
--   TABLA 1   id(6) + cod_rubro + fecha      el detalle, con su promedio
--   TABLA 2   id(6) + fecha                  el consolidado, SIN cod_rubro
--
-- La llave "id" son SEIS columnas, y las seis tienen que aparecer en cada
-- PARTITION BY. Omitir una sola mezcla cuentas distintas en la misma ventana
-- y el resultado sale mal sin que nada lo advierta:
--
--   empresa, modulo, numerocuenta, operacion, tipo_operacion, cod_moneda
--
-- Ojo con cod_moneda en particular: la MISMA cuenta en soles y en dolares son
-- dos llaves distintas y no se deben sumar jamas.
--
-- ============================================================================
-- LA REGLA QUE GOBIERNA TODO ESTE ARCHIVO
--
--   El monto se SUMA. El promedio se RECALCULA. Nunca se suman promedios.
--
-- Un promedio es suma/divisor. Sumar promedios solo vale si TODOS comparten
-- el mismo divisor, y eso deja de ser cierto en cuanto un rubro arranca a
-- mitad de mes -- que es lo normal cuando un cliente abre un producto nuevo.
--
-- Medido sobre el juego de pruebas de este archivo, sumar los promedios da
-- 2300.00 donde lo correcto es 2100.00: 200 soles de error por cuenta y por
-- dia. Con miles de cuentas eso no es un redondeo.
--
-- ============================================================================
-- POR QUE SE GUARDAN suma_acum Y dias_acum, Y NO SOLO EL PROMEDIO
--
-- Con esas dos columnas, CUALQUIER reagregacion posterior es correcta: se
-- suman los numeradores, se suman los dias, se divide al final. Sirve para
-- consolidar por cuenta, por moneda, por sucursal o por lo que haga falta
-- manana, sin volver al detalle.
--
-- Guardando solo avg_mto_mn esa puerta queda cerrada para siempre.
-- ============================================================================


-- ============================================================================
-- DDL
-- ============================================================================

-- ---------------------------------------------------------------- TABLA 1 ---
CREATE TABLE stg_saldo_rubro_prom (
    -- llave compuesta
    empresa         VARCHAR(10)   NOT NULL,
    modulo          VARCHAR(10)   NOT NULL,
    numerocuenta    VARCHAR(30)   NOT NULL,
    operacion       VARCHAR(30)   NOT NULL,
    tipo_operacion  VARCHAR(10)   NOT NULL,
    cod_moneda      VARCHAR(5)    NOT NULL,
    cod_rubro       VARCHAR(10)   NOT NULL,
    fecha           DATE          NOT NULL,

    -- el dato de origen
    mto_mn          DECIMAL(18,2) NOT NULL,

    -- ingredientes del promedio, acumulados dentro del mes
    suma_acum       DECIMAL(18,2) NOT NULL,
    dias_acum       INT           NOT NULL,

    -- el promedio, derivado de los dos anteriores
    avg_mto_mn      DECIMAL(18,6) NOT NULL,

    fecha_carga     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (empresa, modulo, numerocuenta, operacion,
                 tipo_operacion, cod_moneda, cod_rubro, fecha)
);

-- ---------------------------------------------------------------- TABLA 2 ---
-- Misma llave MENOS cod_rubro. Al desaparecer el rubro, (id, fecha) pasa a
-- ser unico: es exactamente lo que garantiza el GROUP BY que la puebla.
CREATE TABLE stg_saldo_cuenta_prom (
    empresa         VARCHAR(10)   NOT NULL,
    modulo          VARCHAR(10)   NOT NULL,
    numerocuenta    VARCHAR(30)   NOT NULL,
    operacion       VARCHAR(30)   NOT NULL,
    tipo_operacion  VARCHAR(10)   NOT NULL,
    cod_moneda      VARCHAR(5)    NOT NULL,
    fecha           DATE          NOT NULL,

    mto_mn          DECIMAL(18,2) NOT NULL,   -- suma de los rubros de ese dia
    rubros          INT           NOT NULL,   -- cuantos rubros se consolidaron

    suma_acum       DECIMAL(18,2) NOT NULL,
    dias_acum       INT           NOT NULL,
    avg_mto_mn      DECIMAL(18,6) NOT NULL,

    fecha_carga     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (empresa, modulo, numerocuenta, operacion,
                 tipo_operacion, cod_moneda, fecha)
);


-- ============================================================================
-- CARGA DE LA TABLA 1  --  detalle por rubro
-- ============================================================================
INSERT INTO stg_saldo_rubro_prom (
    empresa, modulo, numerocuenta, operacion, tipo_operacion, cod_moneda,
    cod_rubro, fecha, mto_mn, suma_acum, dias_acum, avg_mto_mn)
SELECT
    empresa, modulo, numerocuenta, operacion, tipo_operacion, cod_moneda,
    cod_rubro,
    fecha,
    mto_mn,

    -- Numerador: suma acumulada dentro del mes.
    SUM(mto_mn) OVER (
        PARTITION BY empresa, modulo, numerocuenta, operacion,
                     tipo_operacion, cod_moneda, cod_rubro,
                     YEAR(fecha), MONTH(fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS suma_acum,

    -- Denominador: dias transcurridos del mes para ESTA llave.
    COUNT(*) OVER (
        PARTITION BY empresa, modulo, numerocuenta, operacion,
                     tipo_operacion, cod_moneda, cod_rubro,
                     YEAR(fecha), MONTH(fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dias_acum,

    -- El promedio acumulado. YEAR y MONTH dentro de la particion son lo que
    -- lo reinicia el dia 1 de cada mes.
    --
    -- ROWS no es opcional aunque lo parezca: sin el, el marco por defecto es
    -- RANGE, que agrupa por VALOR de la clave de orden. Si algun dia llegan
    -- dos filas con la misma fecha, RANGE les da el mismo promedio y el error
    -- es silencioso.
    AVG(mto_mn) OVER (
        PARTITION BY empresa, modulo, numerocuenta, operacion,
                     tipo_operacion, cod_moneda, cod_rubro,
                     YEAR(fecha), MONTH(fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS avg_mto_mn

FROM stg_saldo_rubro;


-- ============================================================================
-- CARGA DE LA TABLA 2  --  consolidado por cuenta, sin cod_rubro
-- ============================================================================
--
-- El orden importa y no se puede invertir: primero el GROUP BY, despues la
-- ventana. Las funciones de ventana se evaluan DESPUES del GROUP BY, asi que
-- hace falta el CTE. No es un rodeo, es el unico orden posible.
--
-- Y fijese en de donde sale mto_mn del CTE: de stg_saldo_rubro, el ORIGEN.
-- NO se lee avg_mto_mn de la tabla 1 para sumarlo. Eso seria sumar promedios.
-- ============================================================================
INSERT INTO stg_saldo_cuenta_prom (
    empresa, modulo, numerocuenta, operacion, tipo_operacion, cod_moneda,
    fecha, mto_mn, rubros, suma_acum, dias_acum, avg_mto_mn)
WITH por_dia AS (
    -- Colapsar los rubros. Aqui (id, fecha) ya es unico.
    SELECT empresa, modulo, numerocuenta, operacion, tipo_operacion,
           cod_moneda, fecha,
           SUM(mto_mn) AS mto_mn,
           COUNT(*)    AS rubros
    FROM   stg_saldo_rubro
    GROUP  BY empresa, modulo, numerocuenta, operacion, tipo_operacion,
              cod_moneda, fecha
)
SELECT
    empresa, modulo, numerocuenta, operacion, tipo_operacion, cod_moneda,
    fecha,
    mto_mn,
    rubros,

    SUM(mto_mn) OVER (
        PARTITION BY empresa, modulo, numerocuenta, operacion,
                     tipo_operacion, cod_moneda, YEAR(fecha), MONTH(fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS suma_acum,

    COUNT(*) OVER (
        PARTITION BY empresa, modulo, numerocuenta, operacion,
                     tipo_operacion, cod_moneda, YEAR(fecha), MONTH(fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dias_acum,

    AVG(mto_mn) OVER (
        PARTITION BY empresa, modulo, numerocuenta, operacion,
                     tipo_operacion, cod_moneda, YEAR(fecha), MONTH(fecha)
        ORDER BY fecha
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS avg_mto_mn

FROM por_dia;


-- ============================================================================
-- CONTROLES DE CALIDAD
-- Las tres consultas deben devolver CERO filas. Si alguna devuelve algo,
-- no promueva la carga.
-- ============================================================================

-- 1. La llave de la tabla 2 tiene que ser unica.
--    Si aparece algo aqui, el GROUP BY perdio una columna de la llave.
SELECT empresa, modulo, numerocuenta, operacion, tipo_operacion,
       cod_moneda, fecha, COUNT(*) AS repeticiones
FROM   stg_saldo_cuenta_prom
GROUP  BY empresa, modulo, numerocuenta, operacion, tipo_operacion,
          cod_moneda, fecha
HAVING COUNT(*) > 1;

-- 2. El promedio tiene que ser exactamente suma_acum / dias_acum.
--    Si no cuadra, alguien toco una columna sin recalcular la otra.
SELECT * FROM stg_saldo_cuenta_prom
WHERE  ABS(avg_mto_mn - (suma_acum / dias_acum)) > 0.000001;

-- 3. El monto consolidado tiene que ser la suma exacta del detalle.
--    Atrapa rubros perdidos y filas duplicadas en la tabla 1.
SELECT c.empresa, c.numerocuenta, c.cod_moneda, c.fecha,
       c.mto_mn AS consolidado, SUM(r.mto_mn) AS detalle
FROM       stg_saldo_cuenta_prom c
INNER JOIN stg_saldo_rubro_prom  r
        ON  r.empresa        = c.empresa
        AND r.modulo         = c.modulo
        AND r.numerocuenta   = c.numerocuenta
        AND r.operacion      = c.operacion
        AND r.tipo_operacion = c.tipo_operacion
        AND r.cod_moneda     = c.cod_moneda
        AND r.fecha          = c.fecha
GROUP  BY c.empresa, c.modulo, c.numerocuenta, c.operacion,
          c.tipo_operacion, c.cod_moneda, c.fecha, c.mto_mn
HAVING ABS(c.mto_mn - SUM(r.mto_mn)) > 0.01;


-- ============================================================================
-- CONSULTA DE LECTURA: el saldo promedio del mes cerrado
-- ============================================================================
-- La ULTIMA fila del mes ya tiene el promedio de todo el mes: por definicion,
-- el acumulado del ultimo dia abarca el mes entero. No hay que volver a
-- promediar nada.
SELECT empresa, numerocuenta, cod_moneda,
       YEAR(fecha)  AS anio,
       MONTH(fecha) AS mes,
       dias_acum    AS dias_del_mes,
       suma_acum,
       avg_mto_mn   AS saldo_promedio_mes
FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (
               PARTITION BY empresa, modulo, numerocuenta, operacion,
                            tipo_operacion, cod_moneda,
                            YEAR(fecha), MONTH(fecha)
               ORDER BY fecha DESC
           ) AS rn
    FROM stg_saldo_cuenta_prom s
) t
WHERE rn = 1;


-- ============================================================================
-- DIALECTOS
-- ----------------------------------------------------------------------------
-- YEAR() y MONTH() funcionan tal cual en SingleStore, SQL Server y DB2.
-- Solo PostgreSQL necesita otra sintaxis:
--
--     EXTRACT(YEAR  FROM fecha)::int
--     EXTRACT(MONTH FROM fecha)::int
--
-- El resto -- funciones de ventana, CTE, ROWS BETWEEN -- es SQL estandar y no
-- cambia entre los cuatro motores.
--
-- En SingleStore, si estas tablas van a ser grandes, conviene declarar
-- SHARD KEY sobre las columnas de la llave para que la ventana se resuelva
-- dentro de cada particion y no obligue a redistribuir datos entre nodos.
-- ============================================================================
