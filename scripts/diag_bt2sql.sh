#!/bin/sh
# ---------------------------------------------------------------------------
# diag_bt2sql.sh v2 - Diagnostico de SOLO LECTURA de STG_BT2SQL_CARGA_SPARK
#
#   docker compose -f docker-compose.windows.yml exec -T airflow-worker \
#       sh /opt/airflow/scripts/diag_bt2sql.sh
#
# No escribe, no borra, no modifica ninguna tabla ni variable.
#
# v2: lo que importa ahora no es si el parquet esta en disco -ya se vio que
# si-, sino DOS cosas:
#   a) el grafo real que tiene Airflow en memoria, para saber si la compuerta
#      de verdad cuelga de la extraccion o corre en paralelo con ella;
#   b) si la consulta de la compuerta, corrida AHORA contra el mismo batch,
#      devuelve la fila. Si la devuelve, la consulta nunca estuvo mal: lo que
#      estuvo mal fue el momento en que se hizo.
# ---------------------------------------------------------------------------

DAG=STG_BT2SQL_CARGA_SPARK
RAIZ_LOGS=/opt/airflow/logs/dag_id=$DAG
BATCH=${1:-20261011214211}

echo "############################################################"
echo "# 1. EL GRAFO REAL (que tarea espera a cual)"
echo "############################################################"
echo "Si 'verificar_parquet' NO aparece debajo de 'extraccion_completa',"
echo "el encadenamiento del DAG no es el que dice el codigo."
echo
airflow tasks list "$DAG" --tree 2>&1 | head -60

echo
echo "############################################################"
echo "# 2. TIEMPOS REALES DE CADA TAREA EN LA ULTIMA CORRIDA"
echo "############################################################"
# El nombre de la carpeta de log es run_id=<run_id>
ULTIMA=$(ls -1t "$RAIZ_LOGS" 2>/dev/null | head -1)
RUN_ID=$(echo "$ULTIMA" | sed 's/^run_id=//')
echo "run_id: $RUN_ID"
echo
echo "Si el inicio de verificar_parquet es ANTERIOR al fin de las tareas"
echo "stg_*, la compuerta pregunto antes de que hubiera algo que encontrar."
echo
airflow tasks states-for-dag-run "$DAG" "$RUN_ID" 2>&1 | head -40

echo
echo "############################################################"
echo "# 3. LA CONSULTA DE LA COMPUERTA, CORRIDA AHORA"
echo "############################################################"
echo "batch: $BATCH"
echo
python - "$BATCH" <<'FINPY' 2>&1
import sys
sys.path.insert(0, "/opt/airflow/dags/production")

batch = sys.argv[1]

from airflow.models import Variable
import json

# La misma configuracion que lee el DAG, por el mismo camino.
try:
    doc = json.loads(Variable.get("BT2SQL_SPARK"))
except Exception as exc:
    print(f"no se pudo leer la Variable BT2SQL_SPARK: {exc}")
    raise SystemExit(1)

carga = doc["carga"]
sql = carga["sql_lotes"]
print("SQL tal como esta en la Variable:")
print("  " + sql)
print()

from etl_bt2sql.bt2sql_comun import conexion_sqlserver

conn = conexion_sqlserver({"conn_id_destino": carga.get("conn_id_destino",
                                                        "CONEXION_SQLSERVER")})
try:
    cur = conn.cursor()

    # 3a. La consulta EXACTA de la compuerta.
    cur.execute(sql.replace("{BATCH_ID}", "?"), (batch,))
    cols = [str(d[0]).lower() for d in cur.description]
    filas = cur.fetchall()
    print(f"--- 3a. consulta de la compuerta -> {len(filas)} fila(s) ---")
    for f in filas:
        print("   " + "  |  ".join(f"{c}={v}" for c, v in zip(cols, f)))
    if not filas:
        print("   (vacia: por esto la compuerta salto la carga)")
    print()

    # 3b. SIN el filtro de estado: para ver en que estado esta de verdad.
    # SELECT * y no una lista de columnas: no conozco de memoria el nombre
    # exacto de las columnas de fecha de esta tabla, y adivinarlo ya costo un
    # "Invalid column name 'fecha_inicio'" que aborto el resto del diagnostico.
    cur.execute(
        "SELECT * FROM " + carga["tabla_control_extraccion"] +
        " WHERE batch_id = ? ORDER BY id_log", (batch,))
    cols = [str(d[0]).lower() for d in cur.description]
    filas = cur.fetchall()
    print(f"--- 3b. la misma fila SIN filtrar por estado -> {len(filas)} fila(s) ---")
    for f in filas:
        for c, v in zip(cols, f):
            print(f"   {c:20} = {v}")
        print()
    if not filas:
        print("   (ni una fila con ese batch_id: entonces el batch_id no cuadra)")
    print()

    # 3c. Los ultimos batches registrados, para comparar formato de batch_id.
    cur.execute(
        "SELECT TOP 5 id_log, batch_id, estado, tabla_origen "
        "FROM   " + carga["tabla_control_extraccion"] +
        " ORDER BY id_log DESC")
    cols = [str(d[0]).lower() for d in cur.description]
    print("--- 3c. ultimos 5 registros de la bitacora de extraccion ---")
    for f in cur.fetchall():
        print("   " + "  |  ".join(f"{c}={v}" for c, v in zip(cols, f)))
    print()

    # 3d. La tabla de control de la carga.
    cur.execute("SELECT COUNT(*) FROM " + carga["tabla_control"])
    print(f"--- 3d. filas en {carga['tabla_control']}: {cur.fetchall()[0][0]} ---")
finally:
    conn.close()
FINPY

echo
echo "############################################################"
echo "# 4. LOG DE cargar_stg_spark"
echo "############################################################"
G=$(find "$RAIZ_LOGS" -path '*task_id=cargar_stg*' -name '*.log' 2>/dev/null | head -1)
if [ -n "$G" ]; then
    echo "archivo: $G"
    tail -n 30 "$G" 2>&1
else
    echo "SIN LOG: cargar_stg_spark nunca se ejecuto (la compuerta lo salto)."
fi

echo
echo "############################################################"
echo "# 5. FIN"
echo "############################################################"
