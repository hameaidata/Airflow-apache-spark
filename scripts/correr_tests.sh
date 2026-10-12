#!/bin/sh
# ---------------------------------------------------------------------------
# correr_tests.sh - La suite completa + prueba de que los tests nuevos muerden
#
#   docker compose -f docker-compose.windows.yml exec -T airflow-worker \
#       sh /opt/airflow/scripts/correr_tests.sh
#
# No modifica ningun archivo del proyecto.
# ---------------------------------------------------------------------------
set -u
export PYTHONWARNINGS=ignore
cd /opt/airflow

echo "############################################################"
echo "# 1. LA SUITE COMPLETA"
echo "############################################################"
python -m pytest tests/unit -q -p no:cacheprovider 2>&1 | tail -40

echo
echo "############################################################"
echo "# 2. LOS DOS TESTS NUEVOS, CON DETALLE"
echo "############################################################"
python -m pytest tests/unit/test_bt2sql_spark.py -q -p no:cacheprovider \
    -k "espera_a_TODAS or no_arrastra_a_los_niveles" -v 2>&1 | tail -25

echo
echo "############################################################"
echo "# 3. TIENEN DIENTES? (dos DAG de juguete, nada del proyecto)"
echo "############################################################"
python - <<'FINPY' 2>&1
# Se construyen DOS grafos de juguete con la misma forma que el real -dos
# niveles de prioridad encadenados dentro de un TaskGroup- y se aplica a los
# dos la MISMA afirmacion del test. Si la afirmacion no distingue el cableado
# viejo del nuevo, el test no vale nada y aqui se ve.
import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

def construir(cableado):
    with DAG(f"juguete_{cableado}", start_date=datetime.datetime(2026, 1, 1),
             schedule=None) as dag:
        union = EmptyOperator(task_id="extraccion_completa",
                              trigger_rule="all_done")
        todas = []
        with TaskGroup(group_id="extraccion") as grupo:
            anterior = None
            for nivel in (1, 2):
                with TaskGroup(group_id=f"prioridad_{nivel}") as gp:
                    for n in ("a", "b"):
                        todas.append(EmptyOperator(task_id=f"t{nivel}{n}"))
                if anterior is not None:
                    anterior >> gp
                anterior = gp
        if cableado == "viejo":
            grupo >> union                     # por las HOJAS del grupo
        else:
            for t in todas:                    # abanico explicito
                t >> union
    return dag

def afirmacion(dag):
    """La misma que el test: cada tarea de extraccion, padre DIRECTO."""
    union = dag.get_task("extraccion_completa")
    padres = {t.task_id for t in union.upstream_list}
    extraccion = {t.task_id for t in dag.tasks
                  if t.task_id.startswith("extraccion.")}
    return extraccion - padres          # vacio = pasa

falta_viejo = afirmacion(construir("viejo"))
falta_nuevo = afirmacion(construir("nuevo"))

print(f"  cableado VIEJO  -> no esperadas: {sorted(falta_viejo) or 'ninguna'}")
print(f"  cableado NUEVO  -> no esperadas: {sorted(falta_nuevo) or 'ninguna'}")
print()
if falta_viejo and not falta_nuevo:
    print("  OK: el test FALLA con el cableado viejo y PASA con el nuevo.")
    print("      Es decir, muerde.")
elif not falta_viejo:
    print("  PROBLEMA: el test pasaria tambien con el bug puesto. No sirve.")
else:
    print("  PROBLEMA: el test falla con el cableado nuevo. Revisar.")
FINPY

echo
echo "############################################################"
echo "# FIN"
echo "############################################################"
