"""
Reproduce el RELLENO DE HUECOS de SP_BDS_SALDOS_OPERATIVOS.singlestore.sql
(pasos 3 y 4) sobre DuckDB, con datos pequenos y controlados, para ver
exactamente cuantos dias sale cada ID_OPERACION_CIERRE en un mes.

El SQL es una traduccion literal del procedimiento; lo unico que cambia es
DATE_FORMAT/YEAR/MONTH por sus equivalentes de DuckDB.
"""
import duckdb

con = duckdb.connect()

# ---------------------------------------------------------------- CALENDARIO
# Septiembre 2026 completo. Sabados y domingos NO habiles.
# Ademas: 2026-09-01 (martes) feriado, y 2026-09-30 (miercoles) feriado.
con.execute("""
CREATE TABLE BDS_CALENDARIOS AS
SELECT 1 AS COD_CALENDARIO,
       d::DATE AS FEC_CALENDARIO,
       CASE
         WHEN dayofweek(d) IN (0, 6) THEN 'N'      -- domingo / sabado
         WHEN d::DATE IN (DATE '2026-09-01', DATE '2026-09-30') THEN 'N'
         ELSE 'S'
       END AS IND_DIA_HABIL
FROM generate_series(DATE '2026-08-01', DATE '2026-10-31', INTERVAL 1 DAY) t(d);
""")

# ------------------------------------------------------------ BDS_SALDOS_CIERRE
# Cada caso es una operacion distinta, para poder leer el resultado de un vistazo.
con.execute("""
CREATE TABLE BDS_SALDOS_CIERRE (
    COD_EMPRESA        INT,
    COD_MODULO         INT,
    COD_SUCURSAL       INT,
    COD_MONEDA         INT,
    COD_PAPEL          INT,
    NUM_CUENTA_BT      VARCHAR,
    COD_OPERACION      INT,
    COD_SUB_OPERACION  INT,
    COD_TIPO_OPERACION INT,
    COD_RUBRO          VARCHAR,
    FECHA_PROCESO      DATE,
    MTO_SALDO_MN       DECIMAL(18,2),
    FECHA_CARGA        TIMESTAMP,
    BATCH_ID           VARCHAR
);
""")


def sembrar(cuenta, rubro, fechas, monto=100):
    for f in fechas:
        con.execute(
            "INSERT INTO BDS_SALDOS_CIERRE VALUES "
            "(1,20,1,1,0,?,1,0,1,?,?,?, TIMESTAMP '2026-09-30 02:00:00','B1')",
            [cuenta, rubro, f, monto])


# CASO A - operacion normal: dato todos los dias habiles de septiembre
sembrar("CTA-A", "R1", [f"2026-09-{d:02d}" for d in range(1, 31)
                        if con.execute(
                            "SELECT IND_DIA_HABIL FROM BDS_CALENDARIOS "
                            "WHERE FEC_CALENDARIO = ?", [f"2026-09-{d:02d}"]
                        ).fetchone()[0] == 'S'])

# CASO B - la operacion nace a mitad de mes (primer dato el 15)
sembrar("CTA-B", "R1", ["2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"])

# CASO C - la operacion muere a mitad de mes (ultimo dato el 10)
sembrar("CTA-C", "R1", ["2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10"])

# CASO D - hueco en medio: dias 2,3 y luego 21,22
sembrar("CTA-D", "R1", ["2026-09-02", "2026-09-03", "2026-09-21", "2026-09-22"])

# CASO E - el ULTIMO dato esta en AGOSTO. En septiembre solo hay COPIA_HABIL.
sembrar("CTA-E", "R1", ["2026-08-31"])

# CASO F - dos rubros para la MISMA operacion (mismo ID_OPERACION_CIERRE)
sembrar("CTA-F", "R1", ["2026-09-02", "2026-09-03"])
sembrar("CTA-F", "R2", ["2026-09-02", "2026-09-03"])

V_FECHA_INICIO = "2026-08-31"   # ultimo habil <= (fin - P_DIAS)
V_FECHA_FIN    = "2026-09-30"

SQL = f"""
WITH CALENDARIO AS (
    SELECT c.FEC_CALENDARIO AS FECHA_PROCESO,
           c.IND_DIA_HABIL,
           (SELECT MAX(h.FEC_CALENDARIO) FROM BDS_CALENDARIOS h
             WHERE h.COD_CALENDARIO = 1 AND h.IND_DIA_HABIL = 'S'
               AND h.FEC_CALENDARIO <= c.FEC_CALENDARIO) AS FECHA_ORIGEN
    FROM BDS_CALENDARIOS c
    WHERE c.COD_CALENDARIO = 1
      AND c.FEC_CALENDARIO BETWEEN DATE '{V_FECHA_INICIO}' AND DATE '{V_FECHA_FIN}'
),
ORIGEN AS (
    SELECT * FROM BDS_SALDOS_CIERRE
    WHERE FECHA_PROCESO BETWEEN DATE '{V_FECHA_INICIO}' AND DATE '{V_FECHA_FIN}'
),
PROPAGADO AS (
    SELECT
        concat_ws('|', o.COD_EMPRESA, o.COD_MODULO, o.COD_SUCURSAL, o.COD_MONEDA,
                  o.COD_PAPEL, o.NUM_CUENTA_BT, o.COD_OPERACION,
                  o.COD_SUB_OPERACION, o.COD_TIPO_OPERACION) AS ID_OPERACION_CIERRE,
        cal.FECHA_PROCESO, o.COD_RUBRO, o.MTO_SALDO_MN,
        cal.IND_DIA_HABIL,
        CASE WHEN cal.FECHA_PROCESO = cal.FECHA_ORIGEN
             THEN 'ORIGINAL' ELSE 'COPIA_HABIL' END AS TIPO_ORIGEN,
        o.FECHA_CARGA, o.BATCH_ID
    FROM CALENDARIO cal
    JOIN ORIGEN o ON o.FECHA_PROCESO = cal.FECHA_ORIGEN
),
TMP_SO_DIARIO AS (
    SELECT * EXCLUDE (RN) FROM (
        SELECT p.*, ROW_NUMBER() OVER (
                   PARTITION BY p.ID_OPERACION_CIERRE, p.COD_RUBRO, p.FECHA_PROCESO
                   ORDER BY p.FECHA_CARGA DESC,
                            COALESCE(p.MTO_SALDO_MN,0) DESC, p.BATCH_ID DESC) AS RN
        FROM PROPAGADO p) x
    WHERE RN = 1
),
LLAVES AS (
    SELECT ID_OPERACION_CIERRE, COD_RUBRO,
           YEAR(FECHA_PROCESO) AS ANIO, MONTH(FECHA_PROCESO) AS MES,
           MIN(FECHA_PROCESO) AS PRIMERA_FECHA, MAX(FECHA_PROCESO) AS ULTIMA_FECHA
    FROM TMP_SO_DIARIO
    GROUP BY ID_OPERACION_CIERRE, COD_RUBRO, YEAR(FECHA_PROCESO), MONTH(FECHA_PROCESO)
),
ESPINA AS (
    SELECT k.ID_OPERACION_CIERRE, k.COD_RUBRO, k.ANIO, k.MES,
           c.FEC_CALENDARIO AS FECHA_PROCESO, c.IND_DIA_HABIL,
           k.PRIMERA_FECHA, k.ULTIMA_FECHA
    FROM LLAVES k
    JOIN BDS_CALENDARIOS c
      ON c.COD_CALENDARIO = 1
     AND YEAR(c.FEC_CALENDARIO) = k.ANIO
     AND MONTH(c.FEC_CALENDARIO) = k.MES
     AND c.FEC_CALENDARIO <= DATE '{V_FECHA_FIN}'
),
CON_ANCLA AS (
    SELECT e.*,
           d.ID_OPERACION_CIERRE AS EXISTE,
           MAX(CASE WHEN d.ID_OPERACION_CIERRE IS NOT NULL
                     AND d.TIPO_ORIGEN = 'ORIGINAL' THEN e.FECHA_PROCESO END)
               OVER (PARTITION BY e.ID_OPERACION_CIERRE, e.COD_RUBRO, e.ANIO, e.MES
                     ORDER BY e.FECHA_PROCESO
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ANCLA_PREV,
           MIN(CASE WHEN d.ID_OPERACION_CIERRE IS NOT NULL
                     AND d.TIPO_ORIGEN = 'ORIGINAL' THEN e.FECHA_PROCESO END)
               OVER (PARTITION BY e.ID_OPERACION_CIERRE, e.COD_RUBRO, e.ANIO, e.MES
                     ORDER BY e.FECHA_PROCESO
                     ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS ANCLA_NEXT
    FROM ESPINA e
    LEFT JOIN TMP_SO_DIARIO d
           ON d.ID_OPERACION_CIERRE = e.ID_OPERACION_CIERRE
          AND d.COD_RUBRO = e.COD_RUBRO
          AND d.FECHA_PROCESO = e.FECHA_PROCESO
),
COMPLETO AS (
    SELECT ID_OPERACION_CIERRE, FECHA_PROCESO, COD_RUBRO, MTO_SALDO_MN,
           IND_DIA_HABIL, TIPO_ORIGEN AS PROCEDENCIA
    FROM TMP_SO_DIARIO
    UNION ALL
    SELECT a.ID_OPERACION_CIERRE, a.FECHA_PROCESO, a.COD_RUBRO, 0,
           a.IND_DIA_HABIL, 'RELLENO'
    FROM CON_ANCLA a
    JOIN TMP_SO_DIARIO b
      ON b.ID_OPERACION_CIERRE = a.ID_OPERACION_CIERRE
     AND b.COD_RUBRO = a.COD_RUBRO
     AND b.FECHA_PROCESO = COALESCE(a.ANCLA_PREV, a.ANCLA_NEXT)
    WHERE a.EXISTE IS NULL
      AND COALESCE(a.ANCLA_PREV, a.ANCLA_NEXT) IS NOT NULL
)
SELECT * FROM COMPLETO
"""

con.execute(f"CREATE TABLE COMPLETO AS {SQL}")

print("=" * 78)
print("DIAS DE SEPTIEMBRE POR OPERACION Y RUBRO   (septiembre tiene 30 dias)")
print("=" * 78)
print(con.execute("""
SELECT regexp_extract(ID_OPERACION_CIERRE, 'CTA-[A-Z]') AS CUENTA,
       COD_RUBRO,
       COUNT(*)                                   AS DIAS,
       MIN(FECHA_PROCESO)                         AS DESDE,
       MAX(FECHA_PROCESO)                         AS HASTA,
       SUM(CASE WHEN PROCEDENCIA='ORIGINAL'    THEN 1 ELSE 0 END) AS ORIG,
       SUM(CASE WHEN PROCEDENCIA='COPIA_HABIL' THEN 1 ELSE 0 END) AS COPIA,
       SUM(CASE WHEN PROCEDENCIA='RELLENO'     THEN 1 ELSE 0 END) AS RELLENO
FROM COMPLETO
WHERE MONTH(FECHA_PROCESO)=9
GROUP BY 1,2 ORDER BY 1,2
""").df().to_string(index=False))

print()
print("=" * 78)
print("AGRUPANDO SOLO POR ID_OPERACION_CIERRE  (como lo hace usted)")
print("=" * 78)
print(con.execute("""
SELECT regexp_extract(ID_OPERACION_CIERRE, 'CTA-[A-Z]') AS CUENTA,
       COUNT(*)                        AS FILAS,
       COUNT(DISTINCT FECHA_PROCESO)   AS DIAS_DISTINTOS,
       COUNT(DISTINCT COD_RUBRO)       AS RUBROS
FROM COMPLETO
WHERE MONTH(FECHA_PROCESO)=9
GROUP BY 1 ORDER BY 1
""").df().to_string(index=False))

print()
print("=" * 78)
print("DIAS QUE FALTAN DE SEPTIEMBRE, POR OPERACION+RUBRO")
print("=" * 78)
print(con.execute("""
WITH TODOS AS (
  SELECT FEC_CALENDARIO FROM BDS_CALENDARIOS
  WHERE COD_CALENDARIO=1 AND MONTH(FEC_CALENDARIO)=9 AND YEAR(FEC_CALENDARIO)=2026
),
LLAVES AS (SELECT DISTINCT ID_OPERACION_CIERRE, COD_RUBRO FROM COMPLETO)
SELECT regexp_extract(l.ID_OPERACION_CIERRE,'CTA-[A-Z]') AS CUENTA, l.COD_RUBRO,
       COUNT(*) AS DIAS_FALTANTES,
       string_agg(strftime(t.FEC_CALENDARIO,'%d'), ',' ORDER BY t.FEC_CALENDARIO) AS QUE_DIAS
FROM LLAVES l CROSS JOIN TODOS t
LEFT JOIN COMPLETO c
       ON c.ID_OPERACION_CIERRE=l.ID_OPERACION_CIERRE
      AND c.COD_RUBRO=l.COD_RUBRO AND c.FECHA_PROCESO=t.FEC_CALENDARIO
WHERE c.ID_OPERACION_CIERRE IS NULL
GROUP BY 1,2 ORDER BY 1,2
""").df().to_string(index=False))

print()
print("=" * 78)
print("CASO E AL DETALLE  (ultimo dato real: 31-ago)")
print("=" * 78)
print(con.execute("""
SELECT FECHA_PROCESO, IND_DIA_HABIL, PROCEDENCIA, MTO_SALDO_MN
FROM COMPLETO WHERE ID_OPERACION_CIERRE LIKE '%CTA-E%'
ORDER BY FECHA_PROCESO
""").df().to_string(index=False))
