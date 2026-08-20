"""
20 — Credenciales: Variables vs Connections

Lo primero que hay que aclarar, porque cambia todo lo demas:

  VARIABLES    -> configuracion (rutas, banderas, nombres de tabla, umbrales)
  CONNECTIONS  -> credenciales (host, puerto, usuario, contrasena, esquema)

Las dos se cifran con la Fernet key en la base de datos. Pero las Variables:
  - se ven en texto plano en la UI (Admin > Variables) para cualquiera con permiso
  - aparecen en los logs cuando las renderiza Jinja
  - no tienen el concepto de host/puerto/esquema, asi que acabas parseando strings

Airflow enmascara el valor de una Variable en los logs solo si su nombre
contiene "password", "secret", "passwd", "authorization", "api_key", "apikey"
o "token". Una Variable llamada "usuario_bd_produccion" NO se enmascara.

Para banca: las credenciales van en Connections. Punto.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


# =============================================================================
# EL ERROR MAS CARO: llamar a Variable.get() en el nivel superior del archivo
# =============================================================================
#
# NO HAGAS ESTO:
#
#     RUTA = Variable.get("ruta_datos")        # <-- fuera de cualquier funcion
#
#     dag = DAG(...)
#
# Por que duele: el scheduler re-parsea CADA archivo de dags/ cada 30 segundos.
# Esa linea se ejecuta en cada parseo. Con 50 DAGs son 100 consultas por minuto
# a la base de datos de Airflow solo para leer configuracion. Con el tiempo el
# scheduler se vuelve lento y nadie entiende por que.
#
# Ademas: si la Variable no existe, el archivo entero falla al importarse y el
# DAG desaparece de la UI sin mensaje visible.
#
# Las tres formas correctas estan abajo.


def forma_1_dentro_de_la_tarea(**context):
    """Forma 1 — leer la Variable dentro de la funcion.

    Se ejecuta solo cuando la tarea corre, no en cada parseo.
    Es la opcion por defecto: simple y siempre correcta.
    """
    ruta_base = Variable.get("ruta_datos", default_var="/opt/airflow/data")
    lote = Variable.get("tamano_lote", default_var="10000")

    # deserialize_json para valores estructurados
    config = Variable.get(
        "config_etl",
        default_var={"reintentos": 3, "timeout": 300},
        deserialize_json=True,
    )

    log.info("ruta_base = %s", ruta_base)
    log.info("tamano_lote = %s", lote)
    log.info("config = %s", json.dumps(config))
    return {"ruta": ruta_base, "lote": int(lote)}


def forma_3_credenciales_bien_hechas(**context):
    """Forma 3 — credenciales desde una Connection, no desde Variables.

    BaseHook.get_connection() devuelve un objeto con los campos separados.
    No hay que parsear nada a mano.
    """
    conn = BaseHook.get_connection("sqlserver_core")

    # Campos disponibles:
    log.info("host   = %s", conn.host)
    log.info("puerto = %s", conn.port)
    log.info("login  = %s", conn.login)
    log.info("schema = %s", conn.schema)      # aqui va el nombre de la BD
    log.info("extra  = %s", conn.extra_dejson)

    # conn.password existe pero NUNCA lo escribas en un log.
    # Airflow enmascara automaticamente el valor de conn.password si aparece
    # en la salida, pero no dependas de eso: simplemente no lo imprimas.

    # Para armar una URL de conexion (p.ej. para SQLAlchemy o Spark):
    jdbc_url = (
        f"jdbc:sqlserver://{conn.host}:{conn.port or 1433};"
        f"databaseName={conn.schema};encrypt=true;trustServerCertificate=true"
    )
    log.info("jdbc_url (sin credenciales) = %s", jdbc_url)

    return {"host": conn.host, "base_datos": conn.schema}


default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ej20_credenciales",
    description="Variables vs Connections: como leerlas sin romper el scheduler",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ejemplo", "fundamentos"],
) as dag:

    t1 = PythonOperator(
        task_id="forma_1_dentro_de_la_tarea",
        python_callable=forma_1_dentro_de_la_tarea,
    )

    # -------------------------------------------------------------------------
    # Forma 2 — plantilla Jinja.
    # Airflow resuelve {{ var.value.x }} en tiempo de EJECUCION, no de parseo.
    # Es la forma mas barata: ni siquiera abre conexion a la BD durante el parseo.
    # Solo funciona en campos que Airflow declara como "templated"
    # (bash_command, sql, op_kwargs, application_args...).
    # -------------------------------------------------------------------------
    t2 = PythonOperator(
        task_id="forma_2_plantilla_jinja",
        python_callable=lambda **kw: log.info("recibido: %s", kw["op_kwargs_valor"]),
        op_kwargs={
            "op_kwargs_valor": "{{ var.value.get('ruta_datos', '/opt/airflow/data') }}",
            # Para JSON:  {{ var.json.config_etl.reintentos }}
        },
    )

    t3 = PythonOperator(
        task_id="forma_3_credenciales_bien_hechas",
        python_callable=forma_3_credenciales_bien_hechas,
    )

    t1 >> t2 >> t3


# =============================================================================
# COMO CREAR LAS CONNECTIONS
# =============================================================================
#
# Opcion A — UI:  Admin > Connections > +
#
# Opcion B — CLI (util para automatizar despliegues):
#
#   docker compose exec airflow-scheduler airflow connections add sqlserver_core \
#     --conn-type mssql \
#     --conn-host 10.20.30.40 \
#     --conn-port 1433 \
#     --conn-login usuario_etl \
#     --conn-password 'LaClave' \
#     --conn-schema BD_NEGOCIO \
#     --conn-extra '{"encrypt": "yes", "trustServerCertificate": "yes"}'
#
# Opcion C — variable de entorno (no toca la base de datos, ideal para CI):
#
#   AIRFLOW_CONN_SQLSERVER_CORE='mssql://usuario:clave@10.20.30.40:1433/BD_NEGOCIO'
#
#   El nombre de la variable es AIRFLOW_CONN_ + el conn_id en MAYUSCULAS.
#   Precedencia: variable de entorno > secrets backend > base de datos.
#
# Opcion D — Vault / AWS Secrets Manager (lo correcto en produccion):
#   Se configura un secrets backend y Airflow busca ahi primero.
#   Las credenciales dejan de vivir en la base de datos de Airflow.
#
# Verificar que una connection funciona sin escribir un DAG:
#
#   docker compose exec airflow-scheduler airflow connections test sqlserver_core
