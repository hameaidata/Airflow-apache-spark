"""
Verifica las tres afirmaciones que justifican los recortes mas grandes de la
reescritura. Si alguna fuera falsa, la reescritura cambiaria resultados.

  1. Los cinco LEFT JOIN de buckets son auto-joins -> equivalen a cinco CASE
  2. El split por BDS_OPERACIONES + UNION ALL es un no-op
     ... salvo que haya un NULL, y entonces PIERDE DATOS en silencio
  3. El filtro "CNT = 1 OR (CNT > 1 AND ...)" no descarta nada
"""

import duckdb

con = duckdb.connect()

con.execute("""
CREATE TABLE T AS SELECT * FROM (VALUES
    ('OP1', '1401000000', DATE '2026-02-01', 100.0, 10.0),
    ('OP1', '1413000000', DATE '2026-02-01', 200.0, 20.0),
    ('OP1', '1424000000', DATE '2026-02-01', 300.0, 30.0),
    ('OP2', '1415000000', DATE '2026-02-01', 400.0, 40.0),
    ('OP2', '1426000000', DATE '2026-02-01', 500.0, 50.0),
    ('OP3', '2101000000', DATE '2026-02-01', 600.0, 60.0)
) t(ID_OP, COD_RUBRO, FECHA_PROCESO, MTO_SALDO_MO, MTO_SALDO_MN);
""")

print("=" * 74)
print("1. LOS CINCO LEFT JOIN DE BUCKETS SON AUTO-JOINS")
print("=" * 74)

CON_JOINS = """
WITH B1 AS (SELECT ID_OP, COD_RUBRO, FECHA_PROCESO, MTO_SALDO_MO AS V
            FROM T WHERE substr(COD_RUBRO,1,4) LIKE '14_1'
            GROUP BY ID_OP, FECHA_PROCESO, COD_RUBRO, MTO_SALDO_MO),
     B3 AS (SELECT ID_OP, COD_RUBRO, FECHA_PROCESO, MTO_SALDO_MO AS V
            FROM T WHERE substr(COD_RUBRO,1,4) LIKE '14_3'
            GROUP BY ID_OP, FECHA_PROCESO, COD_RUBRO, MTO_SALDO_MO),
     B4 AS (SELECT ID_OP, COD_RUBRO, FECHA_PROCESO, MTO_SALDO_MO AS V
            FROM T WHERE substr(COD_RUBRO,1,4) LIKE '14_4'
            GROUP BY ID_OP, FECHA_PROCESO, COD_RUBRO, MTO_SALDO_MO),
     B5 AS (SELECT ID_OP, COD_RUBRO, FECHA_PROCESO, MTO_SALDO_MO AS V
            FROM T WHERE substr(COD_RUBRO,1,4) LIKE '14_5'
            GROUP BY ID_OP, FECHA_PROCESO, COD_RUBRO, MTO_SALDO_MO),
     B6 AS (SELECT ID_OP, COD_RUBRO, FECHA_PROCESO, MTO_SALDO_MO AS V
            FROM T WHERE substr(COD_RUBRO,1,4) LIKE '14_6'
            GROUP BY ID_OP, FECHA_PROCESO, COD_RUBRO, MTO_SALDO_MO)
SELECT t.ID_OP, t.COD_RUBRO,
       coalesce(B1.V,0), coalesce(B3.V,0), coalesce(B4.V,0),
       coalesce(B5.V,0), coalesce(B6.V,0)
FROM T t
LEFT JOIN B1 ON t.ID_OP=B1.ID_OP AND t.FECHA_PROCESO=B1.FECHA_PROCESO AND t.COD_RUBRO=B1.COD_RUBRO
LEFT JOIN B3 ON t.ID_OP=B3.ID_OP AND t.FECHA_PROCESO=B3.FECHA_PROCESO AND t.COD_RUBRO=B3.COD_RUBRO
LEFT JOIN B4 ON t.ID_OP=B4.ID_OP AND t.FECHA_PROCESO=B4.FECHA_PROCESO AND t.COD_RUBRO=B4.COD_RUBRO
LEFT JOIN B5 ON t.ID_OP=B5.ID_OP AND t.FECHA_PROCESO=B5.FECHA_PROCESO AND t.COD_RUBRO=B5.COD_RUBRO
LEFT JOIN B6 ON t.ID_OP=B6.ID_OP AND t.FECHA_PROCESO=B6.FECHA_PROCESO AND t.COD_RUBRO=B6.COD_RUBRO
ORDER BY 1,2
"""

CON_CASE = """
SELECT ID_OP, COD_RUBRO,
       CASE WHEN substr(COD_RUBRO,1,4) LIKE '14_1' THEN MTO_SALDO_MO ELSE 0 END,
       CASE WHEN substr(COD_RUBRO,1,4) LIKE '14_3' THEN MTO_SALDO_MO ELSE 0 END,
       CASE WHEN substr(COD_RUBRO,1,4) LIKE '14_4' THEN MTO_SALDO_MO ELSE 0 END,
       CASE WHEN substr(COD_RUBRO,1,4) LIKE '14_5' THEN MTO_SALDO_MO ELSE 0 END,
       CASE WHEN substr(COD_RUBRO,1,4) LIKE '14_6' THEN MTO_SALDO_MO ELSE 0 END
FROM T ORDER BY 1,2
"""

a, b = con.execute(CON_JOINS).fetchall(), con.execute(CON_CASE).fetchall()
print(f"   con 5 LEFT JOIN : {len(a)} filas")
print(f"   con 5 CASE      : {len(b)} filas")
print("   ->", "IDENTICO" if a == b else f"DIFIERE\n   {a}\n   {b}")

print()
print("=" * 74)
print("2. EL SPLIT + UNION ALL ES UN NO-OP  (y con un NULL, pierde datos)")
print("=" * 74)

for etiqueta, valores in (("sin NULL", "('OP1'),('OP2')"),
                          ("con un NULL", "('OP1'),('OP2'),(NULL)")):
    con.execute("DROP TABLE IF EXISTS OPS")
    con.execute(f"CREATE TABLE OPS AS SELECT * FROM (VALUES {valores}) v(ID_OPERACION)")
    total = con.execute("SELECT count(*) FROM T").fetchone()[0]
    split = con.execute("""
        SELECT count(*) FROM (
            SELECT * FROM T WHERE ID_OP IN (SELECT ID_OPERACION FROM OPS)
            UNION ALL
            SELECT * FROM T WHERE ID_OP NOT IN (SELECT ID_OPERACION FROM OPS)
        ) x
    """).fetchone()[0]
    veredicto = "no-op, correcto" if split == total else f"PIERDE {total - split} filas"
    print(f"   {etiqueta:<12}  tabla={total}  tras el split={split}   -> {veredicto}")

print()
print("=" * 74)
print("3. EL FILTRO DE 'DEPURACION' NO DESCARTA NADA")
print("=" * 74)

n = con.execute("""
WITH SIN_DUP AS (
    SELECT *, row_number() OVER (PARTITION BY ID_OP, FECHA_PROCESO, COD_RUBRO
                                 ORDER BY FECHA_PROCESO DESC) RN
    FROM T
),
BASE AS (
    SELECT a.*, count(*) OVER (PARTITION BY ID_OP, FECHA_PROCESO, COD_RUBRO) AS CNT
    FROM SIN_DUP a WHERE RN = 1
)
SELECT count(*) FILTER (WHERE CNT = 1),
       count(*) FILTER (WHERE CNT > 1),
       count(*)
FROM BASE
""").fetchone()
print(f"   filas con CNT=1 : {n[0]}")
print(f"   filas con CNT>1 : {n[1]}   <- la rama OR del filtro nunca se evalua")
print(f"   total           : {n[2]}")
print("   ->", "el filtro es inerte" if n[1] == 0 else "el filtro SI descarta")
