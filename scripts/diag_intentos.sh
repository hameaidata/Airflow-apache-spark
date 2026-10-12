#!/bin/sh
# ---------------------------------------------------------------------------
# diag_intentos.sh - SOLO LECTURA. Un intento por linea, en orden de reloj.
#
#   docker compose -f docker-compose.windows.yml exec -T airflow-worker \
#       sh /opt/airflow/scripts/diag_intentos.sh
#
# Para que sirve: si una tarea stg_* tiene attempt=1.log Y attempt=2.log, hubo
# un reintento, y entonces verificar_parquet no corrio "en paralelo" con la
# extraccion: corrio entre el intento que fallo y el que funciono. Son dos
# causas distintas y dos arreglos distintos.
#
# El orden cronologico es lo que importa, no el alfabetico: muestra la
# secuencia real de la corrida.
# ---------------------------------------------------------------------------

DAG=STG_BT2SQL_CARGA_SPARK
RAIZ=/opt/airflow/logs/dag_id=$DAG

ULTIMA=$(ls -1t "$RAIZ" 2>/dev/null | head -1)
echo "corrida: $ULTIMA"
echo
echo "=== TODOS LOS INTENTOS, EN ORDEN DE RELOJ ==================="
echo "fecha-hora(UTC)      bytes  tarea / intento"
echo "-------------------------------------------------------------"
find "$RAIZ/$ULTIMA" -name '*.log' -printf '%TY-%Tm-%Td %TH:%TM:%TS  %6s  %p\n' 2>/dev/null \
  | sed "s#$RAIZ/$ULTIMA/##" \
  | sed 's/task_id=//' \
  | sort

echo
echo "=== CUANTOS INTENTOS POR TAREA =============================="
for d in "$RAIZ/$ULTIMA"/task_id=*; do
    [ -d "$d" ] || continue
    n=$(ls -1 "$d"/*.log 2>/dev/null | wc -l)
    nombre=$(basename "$d" | sed 's/task_id=//')
    marca=""
    [ "$n" -gt 1 ] && marca="   <<< REINTENTO"
    printf "  %-44s %s intento(s)%s\n" "$nombre" "$n" "$marca"
done

echo
echo "=== SI HUBO UN INTENTO 1 QUE FALLO, SU ERROR ================"
hubo=0
for f in $(find "$RAIZ/$ULTIMA" -name 'attempt=1.log' 2>/dev/null); do
    tarea=$(echo "$f" | sed "s#$RAIZ/$ULTIMA/##" | sed 's#/attempt=1.log##' | sed 's/task_id=//')
    # Solo las tareas que tienen un intento 2: esas son las que fallaron.
    dir=$(dirname "$f")
    [ -f "$dir/attempt=2.log" ] || continue
    hubo=1
    echo
    echo "--- $tarea  (intento 1, el que fallo) ---"
    grep -E 'ERROR|Exception|Traceback|FAIL|returned non-zero|Task failed|Marking task' "$f" 2>/dev/null | head -20
done
[ "$hubo" -eq 0 ] && echo "  Ninguna tarea tuvo un segundo intento."

echo
echo "=== FIN ====================================================="
