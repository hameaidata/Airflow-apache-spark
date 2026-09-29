"""
Verifica que colapsar los TRES INSERT de SP_CREAR_VARIACION_CONTABLE en UNO
solo da exactamente el mismo resultado.

El original escribe tres INSERT contra la misma tabla destino:

    1. la operacion existe en las DOS fechas          -> INNER JOIN
    2. existia en la fecha de variacion y ya no       -> LEFT ANTI JOIN
    3. existe ahora y no existia antes                -> RIGHT ANTI JOIN

Esos tres casos juntos son, exactamente, un FULL OUTER JOIN. Aqui se comprueba
que la version unificada -claves unidas + dos LEFT JOIN, que es portable a
cualquier motor- produce las mismas filas y los mismos importes.

Los casos de prueba cubren las tres ramas y los borde que suelen romper este
tipo de unificacion: importes en cero, nulos, y una operacion que cambia de
rubro entre las dos fechas (que segun la clave del join son dos operaciones
distintas, no una que cambio).
"""

import duckdb

con = duckdb.connect()

ACTUAL = "2026-04-15"
VARIACION = "2026-03-31"

con.execute(f"""
CREATE TABLE CIERRE AS SELECT * FROM (VALUES
    -- (fecha, empresa, sucursal, rubro, moneda, papel, cuenta, oper, subop, tipo,
    --  id_operacion, saldo_mn, saldo_mo, tipo_oper_origen, modulo)

    -- CASO 1: en las dos fechas
    ('{VARIACION}', 1, 1, 'R1', 1, 0, 'C1', 10, 0, 0, 'ID_A', 100.0, 30.0,  8, 20),
    ('{ACTUAL}',    1, 1, 'R1', 1, 0, 'C1', 10, 0, 0, 'ID_A', 175.0, 55.0,  8, 20),

    -- CASO 1 con importes iguales (variacion cero: tiene que salir igual)
    ('{VARIACION}', 1, 1, 'R2', 1, 0, 'C2', 11, 0, 0, 'ID_B',  50.0, 10.0, 10, 22),
    ('{ACTUAL}',    1, 1, 'R2', 1, 0, 'C2', 11, 0, 0, 'ID_B',  50.0, 10.0, 10, 22),

    -- CASO 2: existia antes, ya no
    ('{VARIACION}', 1, 2, 'R3', 1, 0, 'C3', 12, 0, 0, 'ID_C', 200.0, 60.0, 30, 120),

    -- CASO 3: existe ahora, no existia
    ('{ACTUAL}',    1, 2, 'R4', 2, 0, 'C4', 13, 0, 0, 'ID_D', 300.0, 90.0,  0, 185),

    -- CASO 3 con saldo cero
    ('{ACTUAL}',    1, 3, 'R5', 1, 0, 'C5', 14, 0, 0, 'ID_E',   0.0,  0.0, 15, 185),

    -- CASO 1 con NULL en un importe
    ('{VARIACION}', 1, 3, 'R6', 1, 0, 'C6', 15, 0, 0, 'ID_F',  NULL, 20.0,  3, 185),
    ('{ACTUAL}',    1, 3, 'R6', 1, 0, 'C6', 15, 0, 0, 'ID_F',  80.0, 25.0,  3, 185),

    -- La MISMA cuenta con rubro distinto en cada fecha: segun la clave del
    -- join son dos operaciones distintas, asi que tiene que salir un CASO 2 y
    -- un CASO 3, no un CASO 1.
    ('{VARIACION}', 1, 4, 'R7', 1, 0, 'C7', 16, 0, 0, 'ID_G', 400.0, 40.0,  0, 321),
    ('{ACTUAL}',    1, 4, 'R8', 1, 0, 'C7', 16, 0, 0, 'ID_G', 450.0, 45.0,  0, 321)
) t(FECHA_PROCESO, COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
    NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION, COD_TIPO_OPERACION,
    ID_OPERACION, MTO_SALDO_MN, MTO_SALDO_MO, COD_TIPO_OPERACION_ORIGEN,
    COD_MODULO_PRODUCTO);
""")

CLAVE = """a.COD_EMPRESA=b.COD_EMPRESA AND a.COD_SUCURSAL=b.COD_SUCURSAL
       AND a.COD_RUBRO=b.COD_RUBRO AND a.COD_MONEDA=b.COD_MONEDA
       AND a.COD_PAPEL=b.COD_PAPEL AND a.NUM_CUENTA_BT=b.NUM_CUENTA_BT
       AND a.COD_OPERACION=b.COD_OPERACION
       AND a.COD_SUB_OPERACION=b.COD_SUB_OPERACION
       AND a.COD_TIPO_OPERACION=b.COD_TIPO_OPERACION"""

# ---------------------------------------------------------------------------
# A. EL ORIGINAL: tres INSERT
# ---------------------------------------------------------------------------
ORIGINAL = f"""
-- 1. existe en las dos
SELECT a.ID_OPERACION, DATE '{VARIACION}' AS FECHA_MA,
       b.MTO_SALDO_MN AS MN_MA, a.MTO_SALDO_MN AS MN_ACT,
       a.MTO_SALDO_MN - b.MTO_SALDO_MN AS MN_VAR
FROM CIERRE a INNER JOIN CIERRE b ON {CLAVE}
WHERE a.FECHA_PROCESO = DATE '{ACTUAL}' AND b.FECHA_PROCESO = DATE '{VARIACION}'

UNION ALL
-- 2. existia y ya no
SELECT a.ID_OPERACION, DATE '{VARIACION}',
       a.MTO_SALDO_MN, 0, -a.MTO_SALDO_MN
FROM CIERRE a LEFT JOIN CIERRE b ON {CLAVE} AND b.FECHA_PROCESO = DATE '{ACTUAL}'
WHERE a.FECHA_PROCESO = DATE '{VARIACION}' AND b.ID_OPERACION IS NULL

UNION ALL
-- 3. existe ahora y no existia
SELECT a.ID_OPERACION, NULL,
       0, a.MTO_SALDO_MN, a.MTO_SALDO_MN
FROM CIERRE a LEFT JOIN CIERRE b ON {CLAVE} AND b.FECHA_PROCESO = DATE '{VARIACION}'
WHERE a.FECHA_PROCESO = DATE '{ACTUAL}' AND b.ID_OPERACION IS NULL
ORDER BY 1
"""

# ---------------------------------------------------------------------------
# B. LA REESCRITURA: claves unidas + dos LEFT JOIN, en una sola pasada
# ---------------------------------------------------------------------------
NUEVO = f"""
WITH CLAVES AS (
    SELECT COD_EMPRESA, COD_SUCURSAL, COD_RUBRO, COD_MONEDA, COD_PAPEL,
           NUM_CUENTA_BT, COD_OPERACION, COD_SUB_OPERACION, COD_TIPO_OPERACION
    FROM CIERRE WHERE FECHA_PROCESO IN (DATE '{ACTUAL}', DATE '{VARIACION}')
    GROUP BY 1,2,3,4,5,6,7,8,9
)
-- OJO: el cero se pone segun EXISTA LA FILA, no segun el importe sea NULL.
-- Un coalesce(ant.MTO_SALDO_MN, 0) convertiria en 0 tanto "no hay fila" como
-- "hay fila con importe NULL", y el original distingue los dos casos: cuando
-- la fila existe con importe NULL, la variacion sale NULL.
--
-- Como COD_EMPRESA es columna de igualdad del join, solo puede ser NULL si no
-- hubo emparejamiento. Por eso sirve de testigo de existencia.
SELECT
    coalesce(act.ID_OPERACION, ant.ID_OPERACION) AS ID_OPERACION,
    CASE WHEN ant.COD_EMPRESA IS NOT NULL THEN DATE '{VARIACION}' END AS FECHA_MA,
    CASE WHEN ant.COD_EMPRESA IS NOT NULL THEN ant.MTO_SALDO_MN ELSE 0 END AS MN_MA,
    CASE WHEN act.COD_EMPRESA IS NOT NULL THEN act.MTO_SALDO_MN ELSE 0 END AS MN_ACT,
      (CASE WHEN act.COD_EMPRESA IS NOT NULL THEN act.MTO_SALDO_MN ELSE 0 END)
    - (CASE WHEN ant.COD_EMPRESA IS NOT NULL THEN ant.MTO_SALDO_MN ELSE 0 END) AS MN_VAR
FROM CLAVES k
LEFT JOIN CIERRE act
       ON act.COD_EMPRESA=k.COD_EMPRESA AND act.COD_SUCURSAL=k.COD_SUCURSAL
      AND act.COD_RUBRO=k.COD_RUBRO AND act.COD_MONEDA=k.COD_MONEDA
      AND act.COD_PAPEL=k.COD_PAPEL AND act.NUM_CUENTA_BT=k.NUM_CUENTA_BT
      AND act.COD_OPERACION=k.COD_OPERACION
      AND act.COD_SUB_OPERACION=k.COD_SUB_OPERACION
      AND act.COD_TIPO_OPERACION=k.COD_TIPO_OPERACION
      AND act.FECHA_PROCESO = DATE '{ACTUAL}'
LEFT JOIN CIERRE ant
       ON ant.COD_EMPRESA=k.COD_EMPRESA AND ant.COD_SUCURSAL=k.COD_SUCURSAL
      AND ant.COD_RUBRO=k.COD_RUBRO AND ant.COD_MONEDA=k.COD_MONEDA
      AND ant.COD_PAPEL=k.COD_PAPEL AND ant.NUM_CUENTA_BT=k.NUM_CUENTA_BT
      AND ant.COD_OPERACION=k.COD_OPERACION
      AND ant.COD_SUB_OPERACION=k.COD_SUB_OPERACION
      AND ant.COD_TIPO_OPERACION=k.COD_TIPO_OPERACION
      AND ant.FECHA_PROCESO = DATE '{VARIACION}'
ORDER BY 1
"""

a = con.execute(ORIGINAL).fetchall()
b = con.execute(NUEVO).fetchall()

print(f"  original (3 INSERT) : {len(a)} filas")
print(f"  reescritura (1)     : {len(b)} filas")
print()

for f in a:
    print("   orig ", f)
print()
for f in b:
    print("   nuevo", f)
print()

if a == b:
    print("  IDENTICAS: mismas filas, mismo orden, mismos importes.")
    raise SystemExit(0)

print("  DIFIEREN")
for x in a:
    if x not in b:
        print("    falta en la nueva:", x)
for x in b:
    if x not in a:
        print("    sobra en la nueva:", x)
raise SystemExit(1)
