"""
Verifica que el relleno de huecos en UNA pasada (ventanas) da exactamente el
mismo resultado que las TRES CTE recursivas del script original.

Se comparan sobre datos sinteticos que cubren los tres casos que el original
trataba por separado, mas los casos borde que suelen romper este tipo de
reescrituras:

    - hueco al FINAL del mes      (TEMP_AUXILIAR_FIN_MES)
    - hueco EN MEDIO del mes      (TEMP_AUXILIAR_INTERMEDIO)
    - hueco al PRINCIPIO del mes  (TEMP_AUXILIAR_INICIO_MES)
    - los tres a la vez en la misma operacion
    - un mes con un unico dia con datos
    - un mes completo sin huecos
    - dos rubros de la misma operacion con huecos distintos
    - un mes de 28 dias (febrero) y uno de 31
"""

import duckdb

con = duckdb.connect()

# ---------------------------------------------------------------------------
# Datos
# ---------------------------------------------------------------------------
con.execute("""
CREATE TABLE CALENDARIO AS
SELECT d::DATE AS FEC_CALENDARIO,
       CASE WHEN dayofweek(d) IN (0, 6) THEN 'N' ELSE 'S' END AS IND_DIA_HABIL
FROM generate_series(DATE '2026-01-01', DATE '2026-03-31', INTERVAL 1 DAY) t(d);
""")

# (operacion, rubro, fecha, saldo). Solo los dias que SI tienen dato.
filas = []

def add(op, rubro, dias, mes="2026-02"):
    for d in dias:
        filas.append((op, rubro, f"{mes}-{d:02d}", float(d)))

add("OP_FIN",        "R1", [1, 2, 3, 4, 5])                 # falta del 6 al 28
add("OP_MEDIO",      "R1", [1, 2, 3, 20, 27, 28])           # hueco del 4 al 19 y 21-26
add("OP_INICIO",     "R1", list(range(20, 29)))             # falta del 1 al 19
add("OP_TODOS",      "R1", [5, 6, 15, 16, 25])              # principio, medio y final
add("OP_UN_DIA",     "R1", [14])                            # un solo dia
add("OP_COMPLETO",   "R1", list(range(1, 29)))              # sin huecos
add("OP_2RUBROS",    "R1", [1, 10, 28])
add("OP_2RUBROS",    "R2", [14])                            # otro rubro, otro hueco
add("OP_ENERO",      "R1", [3, 17], mes="2026-01")          # mes de 31 dias

con.execute("CREATE TABLE BASE (ID_OP VARCHAR, COD_RUBRO VARCHAR, FECHA_PROCESO DATE, SALDO DOUBLE)")
con.executemany("INSERT INTO BASE VALUES (?, ?, ?::DATE, ?)", filas)

FECHA_FIN = "2026-03-31"

# ---------------------------------------------------------------------------
# A. EL ORIGINAL: tres CTE recursivas, una por tipo de hueco
# ---------------------------------------------------------------------------
ORIGINAL = f"""
WITH RECURSIVE
LLAVES AS (
    SELECT ID_OP, COD_RUBRO,
           year(FECHA_PROCESO) ANIO, month(FECHA_PROCESO) MES,
           min(FECHA_PROCESO) PRIMERA, max(FECHA_PROCESO) ULTIMA,
           last_day(max(FECHA_PROCESO)) FIN_MES,
           date_trunc('month', min(FECHA_PROCESO))::DATE INICIO_MES
    FROM BASE
    GROUP BY 1,2,3,4
),

-- 1. FIN DE MES: desde el dia siguiente al ultimo registrado hasta fin de mes
FIN AS (
    SELECT ID_OP, COD_RUBRO, ULTIMA + INTERVAL 1 DAY AS FECHA_PROCESO, ULTIMA, FIN_MES
    FROM LLAVES WHERE ULTIMA <> FIN_MES
    UNION ALL
    SELECT ID_OP, COD_RUBRO, FECHA_PROCESO + INTERVAL 1 DAY, ULTIMA, FIN_MES
    FROM FIN WHERE FECHA_PROCESO < FIN_MES
),
FIN_R AS (
    SELECT f.ID_OP, f.COD_RUBRO, f.FECHA_PROCESO::DATE AS FECHA_PROCESO, f.ULTIMA AS ANCLA
    FROM FIN f WHERE f.FECHA_PROCESO <= DATE '{FECHA_FIN}'
),

-- 2. INTERMEDIO: dias entre el primero y el ultimo que no existen
CAL AS (
    SELECT ID_OP, COD_RUBRO, ANIO, MES, PRIMERA AS FECHA_PROCESO, ULTIMA AS FIN
    FROM LLAVES
    UNION ALL
    SELECT ID_OP, COD_RUBRO, ANIO, MES, FECHA_PROCESO + INTERVAL 1 DAY, FIN
    FROM CAL WHERE FECHA_PROCESO < FIN
),
MEDIO_R AS (
    SELECT c.ID_OP, c.COD_RUBRO, c.FECHA_PROCESO::DATE AS FECHA_PROCESO,
           (SELECT max(b.FECHA_PROCESO) FROM BASE b
             WHERE b.ID_OP = c.ID_OP AND b.COD_RUBRO = c.COD_RUBRO
               AND b.FECHA_PROCESO < c.FECHA_PROCESO) AS ANCLA
    FROM CAL c
    LEFT JOIN BASE t ON t.ID_OP = c.ID_OP AND t.COD_RUBRO = c.COD_RUBRO
                    AND t.FECHA_PROCESO = c.FECHA_PROCESO
    WHERE t.ID_OP IS NULL
),

-- 3. INICIO DE MES: desde el dia 1 hasta el primero registrado
INI AS (
    SELECT ID_OP, COD_RUBRO, INICIO_MES AS FECHA_PROCESO, PRIMERA
    FROM LLAVES WHERE PRIMERA <> INICIO_MES
    UNION ALL
    SELECT ID_OP, COD_RUBRO, FECHA_PROCESO + INTERVAL 1 DAY, PRIMERA
    FROM INI WHERE FECHA_PROCESO + INTERVAL 1 DAY < PRIMERA
),
INI_R AS (
    SELECT ID_OP, COD_RUBRO, FECHA_PROCESO::DATE AS FECHA_PROCESO, PRIMERA AS ANCLA
    FROM INI
)
SELECT DISTINCT ID_OP, COD_RUBRO, FECHA_PROCESO, ANCLA FROM (
    SELECT * FROM FIN_R
    UNION ALL SELECT * FROM MEDIO_R
    UNION ALL SELECT * FROM INI_R
) u
WHERE ANCLA IS NOT NULL
ORDER BY 1,2,3
"""

# ---------------------------------------------------------------------------
# B. LA REESCRITURA: una pasada, calendario + dos ventanas
# ---------------------------------------------------------------------------
NUEVO = f"""
WITH LLAVES AS (
    SELECT ID_OP, COD_RUBRO,
           year(FECHA_PROCESO) ANIO, month(FECHA_PROCESO) MES
    FROM BASE GROUP BY 1,2,3,4
),
ESPINA AS (
    SELECT k.ID_OP, k.COD_RUBRO, k.ANIO, k.MES, c.FEC_CALENDARIO AS FECHA_PROCESO
    FROM LLAVES k
    JOIN CALENDARIO c
      ON year(c.FEC_CALENDARIO) = k.ANIO AND month(c.FEC_CALENDARIO) = k.MES
     AND c.FEC_CALENDARIO <= DATE '{FECHA_FIN}'
),
CON_ANCLA AS (
    SELECT e.*, b.ID_OP AS EXISTE,
           max(CASE WHEN b.ID_OP IS NOT NULL THEN e.FECHA_PROCESO END)
               OVER (PARTITION BY e.ID_OP, e.COD_RUBRO, e.ANIO, e.MES
                     ORDER BY e.FECHA_PROCESO
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ANCLA_PREV,
           min(CASE WHEN b.ID_OP IS NOT NULL THEN e.FECHA_PROCESO END)
               OVER (PARTITION BY e.ID_OP, e.COD_RUBRO, e.ANIO, e.MES
                     ORDER BY e.FECHA_PROCESO
                     ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ANCLA_NEXT
    FROM ESPINA e
    LEFT JOIN BASE b ON b.ID_OP = e.ID_OP AND b.COD_RUBRO = e.COD_RUBRO
                    AND b.FECHA_PROCESO = e.FECHA_PROCESO
)
SELECT ID_OP, COD_RUBRO, FECHA_PROCESO,
       coalesce(ANCLA_PREV, ANCLA_NEXT) AS ANCLA
FROM CON_ANCLA
WHERE EXISTE IS NULL AND coalesce(ANCLA_PREV, ANCLA_NEXT) IS NOT NULL
ORDER BY 1,2,3
"""

a = con.execute(ORIGINAL).fetchall()
b = con.execute(NUEVO).fetchall()

print(f"  original (3 CTE recursivas) : {len(a):>4} filas generadas")
print(f"  reescritura (1 pasada)      : {len(b):>4} filas generadas")
print()

sa, sb = set(a), set(b)
solo_a = sorted(sa - sb)
solo_b = sorted(sb - sa)

if not solo_a and not solo_b:
    print("  IDENTICAS: mismas filas y mismas anclas, fila por fila.")
else:
    print(f"  DIFIEREN. solo en el original: {len(solo_a)} | solo en la nueva: {len(solo_b)}")
    for f in solo_a[:12]:
        print("    falta en la nueva :", f)
    for f in solo_b[:12]:
        print("    sobra en la nueva :", f)

print()
print("  Desglose por operacion:")
for op, rubro in sorted({(x[0], x[1]) for x in sa | sb}):
    na = sum(1 for x in a if x[0] == op and x[1] == rubro)
    nb = sum(1 for x in b if x[0] == op and x[1] == rubro)
    marca = "ok " if na == nb else "DIF"
    print(f"    {marca} {op:<12} {rubro}  original={na:>3}  nueva={nb:>3}")

raise SystemExit(0 if (not solo_a and not solo_b) else 1)
