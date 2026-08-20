"""
DAG canario para verificar el auto-discovery.

Sirve para una sola cosa: comprobar que el scheduler esta leyendo la carpeta
dags/ y que los workers de Celery estan recogiendo tareas.

No tiene dependencias externas (ni Spark, ni base de datos, ni providers),
asi que si ESTE falla el problema es de infraestructura, no de tu codigo.

Como usarlo:
  1. El archivo ya esta en airflow/dags/examples/
  2. Espera <= 30 segundos (DAG_DIR_LIST_INTERVAL)
  3. Aparece en http://localhost:8080 como "canary_auto_discovery"
  4. Despausalo y dale a Trigger
  5. Si las 3 tareas quedan en verde, el stack funciona
"""

from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def report_worker_identity(**context) -> dict:
    """Muestra QUE worker ejecuto la tarea.

    Al escalar (--scale airflow-worker=5) y disparar el DAG varias veces,
    veras hostnames distintos aqui. Esa es la prueba de que Celery esta
    repartiendo el trabajo y no todo cae en el mismo contenedor.
    """
    identity = {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "executor": os.getenv("AIRFLOW__CORE__EXECUTOR", "desconocido"),
        "queue": context["task_instance"].queue,
        "try_number": context["task_instance"].try_number,
    }

    print("=" * 60)
    print("TAREA EJECUTADA POR:")
    for key, value in identity.items():
        print(f"  {key:12} = {value}")
    print("=" * 60)

    return identity


def verify_connectivity(**context) -> dict:
    """Comprueba que el worker alcanza a Postgres, Redis y el Spark master.

    Si alguna falla, el mensaje dice exactamente cual, para no tener que
    adivinar revisando cinco contenedores.
    """
    targets = {
        "postgres": ("postgres", 5432),
        "redis": ("redis", 6379),
        "spark-master": ("spark-master", 7077),
    }

    results = {}
    failures = []

    for name, (host, port) in targets.items():
        try:
            with socket.create_connection((host, port), timeout=5):
                results[name] = "alcanzable"
                print(f"  [ok]    {name:14} {host}:{port}")
        except OSError as exc:
            results[name] = f"FALLO: {exc}"
            failures.append(name)
            print(f"  [FALLO] {name:14} {host}:{port} -> {exc}")

    if failures:
        raise RuntimeError(
            f"El worker no alcanza: {', '.join(failures)}. "
            "Revisa que esten en la misma red de Docker "
            "(docker network inspect airflow-spark_network)."
        )

    return results


default_args = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=5),
}

# La variable de nivel de modulo es lo que el scheduler busca.
# Puede llamarse como quieras; lo que importa es que sea un objeto DAG
# accesible en el ambito global del archivo.
dag = DAG(
    dag_id="canary_auto_discovery",
    description="Verifica auto-discovery, Celery y conectividad de red",
    default_args=default_args,
    schedule=None,          # solo manual; no queremos ruido en el scheduler
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["platform", "smoke-test"],
)

t1 = PythonOperator(
    task_id="quien_me_ejecuta",
    python_callable=report_worker_identity,
    dag=dag,
)

t2 = PythonOperator(
    task_id="verificar_conectividad",
    python_callable=verify_connectivity,
    dag=dag,
)

t3 = BashOperator(
    task_id="entorno_del_worker",
    bash_command=(
        'echo "hostname : $(hostname)" && '
        'echo "airflow  : $(airflow version)" && '
        'echo "python   : $(python --version)" && '
        'echo "dags_dir : ${AIRFLOW__CORE__DAGS_FOLDER:-/opt/airflow/dags}" && '
        'echo "--- archivos vistos por el worker ---" && '
        'find /opt/airflow/dags -name "*.py" -type f | head -20'
    ),
    dag=dag,
)

t1 >> t2 >> t3
