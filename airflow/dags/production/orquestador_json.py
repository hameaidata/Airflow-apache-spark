"""
ORQUESTADOR DIRIGIDO POR JSON

Lee un manifiesto en formato JSON y construye el flujo a partir de el. Cada
paso del manifiesto declara COMO debe ejecutarse:

    "tipo": "sql"     ->  Airflow abre la conexion y ejecuta CALL sp() ahi mismo
    "tipo": "spark"   ->  Airflow envia el trabajo al cluster de Spark

En ambos casos, cada paso registra su ejecucion en una tabla de bitacora
dentro de la BASE DE DESTINO, no en la base de Airflow.

===========================================================================
DE DONDE SALE EL MANIFIESTO

De la Variable de Airflow `orquestador_manifiesto`, que admite dos formas:

  a) Apuntar a un archivo:
         {"archivo": "/opt/airflow/config/manifiestos/carga_diaria.json"}

  b) Traer el manifiesto completo dentro de la propia Variable:
         {"nombre": "...", "pasos": [ ... ]}

La forma (a) es preferible: el manifiesto queda versionado en git, con
historial de quien cambio que. Un JSON grande dentro de una Variable no tiene
control de cambios.

===========================================================================
AVISO SOBRE EL COSTO DE ESTE PATRON

El manifiesto se lee al ANALIZAR el archivo, no al ejecutarlo. Es inevitable:
para dibujar una tarea por paso, Airflow necesita conocer los pasos antes de
ejecutar nada.

Eso significa una lectura de la Variable cada 30 segundos, en cada analisis.
Con un manifiesto es despreciable. Con veinte DAGs asi, empieza a notarse en
el planificador. Si llega a ese punto, la salida es usar la forma (a) y leer
solo del archivo, sin pasar por la Variable.
===========================================================================
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.sp_operator import EjecutarSPOperator

log = logging.getLogger(__name__)

# El proveedor de Spark no viene en la imagen oficial de Airflow: lo anade el
# Dockerfile propio. Si falta, se importa un sustituto que falla con un mensaje
# claro. Sin este try, un ImportError dejaria el DAG ENTERO fuera de la
# interfaz, y el sintoma no diria nada sobre Spark.
try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    SPARK_DISPONIBLE = True
except ImportError:
    SPARK_DISPONIBLE = False


VARIABLE_MANIFIESTO = "orquestador_manifiesto"
MANIFIESTO_POR_DEFECTO = "/opt/airflow/config/manifiestos/carga_diaria.json"


# ===========================================================================
# LECTURA DEL MANIFIESTO
# ===========================================================================

def cargar_manifiesto() -> dict:
    """Resuelve el manifiesto desde la Variable, o desde el archivo por defecto.

    Tolera que la Variable no exista todavia: cae al archivo. Asi el DAG se
    puede ver en la interfaz antes de haber configurado nada.
    """
    crudo = None
    try:
        crudo = Variable.get(VARIABLE_MANIFIESTO, deserialize_json=True)
    except Exception:
        pass

    if isinstance(crudo, dict) and "archivo" in crudo:
        ruta = crudo["archivo"]
    elif isinstance(crudo, dict) and "pasos" in crudo:
        return crudo                      # manifiesto completo en la Variable
    else:
        ruta = MANIFIESTO_POR_DEFECTO

    if not os.path.exists(ruta):
        # Un manifiesto vacio hace que el DAG aparezca sin pasos, en vez de
        # desaparecer de la interfaz por un error de importacion.
        return {"nombre": "sin_manifiesto", "pasos": [],
                "_error": f"No se encontro el manifiesto en {ruta}"}

    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


MANIFIESTO = cargar_manifiesto()
PASOS = [p for p in MANIFIESTO.get("pasos", []) if not p.get("desactivado")]
CONN_BITACORA = MANIFIESTO.get("conexion_bitacora", "bd_negocio")
TABLA_BITACORA = MANIFIESTO.get("tabla_bitacora", "airflow_bitacora_procesos")


# ===========================================================================
# TRADUCCION DE UNA CONNECTION DE AIRFLOW A PARAMETROS JDBC
# ===========================================================================

DRIVERS = {
    "postgres": ("org.postgresql.Driver", "/opt/airflow/jars/postgresql-jdbc.jar"),
    "mssql":    ("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/opt/airflow/jars/mssql-jdbc.jar"),
    "odbc":     ("com.microsoft.sqlserver.jdbc.SQLServerDriver", "/opt/airflow/jars/mssql-jdbc.jar"),
    "jdbc":     ("com.ibm.db2.jcc.DB2Driver", "/opt/airflow/jars/db2-jcc.jar"),
    "oracle":   ("oracle.jdbc.OracleDriver", "/opt/airflow/jars/ojdbc.jar"),
}


def url_jdbc(conn) -> str:
    """Arma la URL JDBC segun el motor. Cada uno tiene su propia forma."""
    if conn.conn_type == "postgres":
        return f"jdbc:postgresql://{conn.host}:{conn.port or 5432}/{conn.schema}"
    if conn.conn_type in ("mssql", "odbc"):
        return (f"jdbc:sqlserver://{conn.host}:{conn.port or 1433};"
                f"databaseName={conn.schema};encrypt=true;trustServerCertificate=true")
    if conn.conn_type == "jdbc":
        # En una Connection de tipo jdbc, el host YA es la URL completa
        return conn.host
    if conn.conn_type == "oracle":
        return f"jdbc:oracle:thin:@{conn.host}:{conn.port or 1521}/{conn.schema}"
    raise ValueError(f"No se como armar la URL JDBC para el motor '{conn.conn_type}'")


def preparar_credenciales(**context) -> dict:
    """Deja en XCom lo que el job de Spark necesita para conectarse.

    La contrasena viaja por XCom, que esta cifrado en la base de metadatos.
    No es lo ideal: lo limpio seria un gestor de secretos que el propio job
    consultara. Queda anotado como pendiente.
    """
    conn = BaseHook.get_connection(CONN_BITACORA)
    driver, jar = DRIVERS.get(conn.conn_type, (None, None))
    if driver is None:
        raise ValueError(f"Motor '{conn.conn_type}' sin driver JDBC configurado")

    datos = {
        "jdbc_url": url_jdbc(conn),
        "driver": driver,
        "jar": jar,
        "usuario": conn.login,
        "clave": conn.password,
        "motor": conn.conn_type,
    }
    log.info("Conexion preparada | motor=%s servidor=%s base=%s",
             conn.conn_type, conn.host, conn.schema)
    log.info("URL JDBC: %s", datos["jdbc_url"])
    return datos


def sin_spark(**context):
    """Sustituto cuando el proveedor de Spark no esta instalado."""
    raise RuntimeError(
        "Este paso requiere Apache Spark, pero el proveedor no esta instalado "
        "en la imagen actual. Construya la imagen propia:\n"
        "    docker build -t airflow-bsg:2.11.2 .\n"
        "y descomente AIRFLOW_IMAGE en el .env.\n"
        "Los pasos de tipo 'sql' del manifiesto si funcionan sin eso."
    )


def resumen(**context) -> dict:
    """Lee la bitacora de la base destino y deja el resumen de esta ejecucion."""
    hook = BaseHook.get_connection(CONN_BITACORA).get_hook()
    filas = hook.get_records(
        f"""SELECT procedimiento, estado, duracion_seg, filas_afectadas, ejecutado_por
            FROM {TABLA_BITACORA}
            WHERE run_id = %s
            ORDER BY iniciado_en""",
        parameters=(context["dag_run"].run_id,),
    )

    print("=" * 78)
    print(f"  BITACORA — {MANIFIESTO.get('nombre')} — {context['ds']}")
    print("=" * 78)
    print(f"  {'PROCEDIMIENTO':38} {'VIA':8} {'ESTADO':8} {'SEG':>8} {'FILAS':>9}")
    print("-" * 78)
    ok = err = 0
    for proc, estado, dur, fils, via in filas:
        ok, err = (ok + 1, err) if estado == "OK" else (ok, err + 1)
        print(f"  {str(proc):38} {str(via or '-'):8} {str(estado):8} "
              f"{float(dur or 0):>8.2f} {(fils if fils is not None else '-'):>9}")
    print("=" * 78)
    print(f"  {ok} correctos, {err} con error")
    print("=" * 78)

    if err:
        raise ValueError(f"{err} paso(s) registrados con error en la bitacora")
    return {"ok": ok, "error": err, "total": len(filas)}


# ===========================================================================
# CONSTRUCCION DEL DAG
# ===========================================================================

default_args = {
    "owner": "hjara",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="orquestador_json",
    description=f"Orquestador dirigido por manifiesto: {MANIFIESTO.get('nombre', '?')}",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["produccion", "orquestador", "json"],
    doc_md=__doc__,
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    t_credenciales = PythonOperator(
        task_id="preparar_credenciales",
        python_callable=preparar_credenciales,
    )

    fin = EmptyOperator(task_id="fin", trigger_rule=TriggerRule.ALL_DONE)

    t_resumen = PythonOperator(
        task_id="resumen",
        python_callable=resumen,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # --- Una tarea por paso del manifiesto -------------------------------
    tareas: dict[str, object] = {}

    for paso in PASOS:
        pid = paso["id"]
        procedimiento = f"{paso.get('esquema', '')}.{paso['procedimiento']}".lstrip(".")
        parametros = paso.get("parametros", [])

        # ---------------------------------------------------------------
        # tipo = sql  ->  Airflow conecta y ejecuta el CALL directamente
        # ---------------------------------------------------------------
        if paso["tipo"] == "sql":
            tareas[pid] = EjecutarSPOperator(
                task_id=pid,
                conn_id=paso.get("conexion", CONN_BITACORA),
                nombre_sp=procedimiento,
                parametros=parametros,
                tabla_bitacora=TABLA_BITACORA,
                doc_md=paso.get("descripcion"),
            )

        # ---------------------------------------------------------------
        # tipo = spark  ->  Airflow envia el trabajo al cluster
        #
        # El job de Spark abre una conexion JDBC desde la JVM y ejecuta el
        # procedimiento ahi. Despues lee el resultado en paralelo. Escribe
        # su propia bitacora, en la misma tabla que los pasos de tipo sql.
        # ---------------------------------------------------------------
        elif paso["tipo"] == "spark":
            if not SPARK_DISPONIBLE:
                tareas[pid] = PythonOperator(task_id=pid, python_callable=sin_spark)
            else:
                cfg = paso.get("spark", {})
                xc = "ti.xcom_pull(task_ids='preparar_credenciales')"
                args_app = [
                    "--jdbc-url", f"{{{{ {xc}['jdbc_url'] }}}}",
                    "--driver-class", f"{{{{ {xc}['driver'] }}}}",
                    "--procedimiento", procedimiento,
                    "--fecha", "{{ ds }}",
                    "--tabla-bitacora", TABLA_BITACORA,
                    "--dag-id", "{{ dag.dag_id }}",
                    "--task-id", pid,
                    "--run-id", "{{ run_id }}",
                    "--intento", "{{ ti.try_number }}",
                ]
                if parametros:
                    args_app += ["--parametros", *[str(x) for x in parametros]]
                if cfg.get("tabla_resultado"):
                    args_app += ["--tabla-resultado", cfg["tabla_resultado"]]
                if cfg.get("columna_particion"):
                    args_app += ["--columna-particion", cfg["columna_particion"],
                                 "--particiones", str(cfg.get("particiones", 4))]
                if cfg.get("salida_parquet"):
                    args_app += ["--salida-parquet", cfg["salida_parquet"]]

                tareas[pid] = SparkSubmitOperator(
                    task_id=pid,
                    conn_id="spark_default",
                    application="/opt/spark-apps/etl/ejecutar_sp_spark.py",
                    name=f"sp_{pid}_{{{{ ds_nodash }}}}",
                    jars=f"{{{{ {xc}['jar'] }}}}",
                    application_args=args_app,
                    env_vars={
                        "ORIGEN_USUARIO": f"{{{{ {xc}['usuario'] }}}}",
                        "ORIGEN_CLAVE": f"{{{{ {xc}['clave'] }}}}",
                    },
                    # executor_memory debe ser MENOR que SPARK_WORKER_MEMORY.
                    # Si pide mas de lo que el worker anuncia, el master nunca
                    # coloca el executor y la aplicacion se queda en WAITING.
                    executor_memory=cfg.get("executor_memory", "1g"),
                    executor_cores=cfg.get("executor_cores", 1),
                    num_executors=cfg.get("num_executors", 1),
                    conf={"spark.cores.max": str(cfg.get("cores_max", 2))},
                    verbose=False,
                    doc_md=paso.get("descripcion"),
                )
        else:
            raise ValueError(
                f"El paso '{pid}' tiene tipo '{paso['tipo']}'. "
                f"Solo se admiten 'sql' y 'spark'."
            )

    # --- Dependencias declaradas en el manifiesto ------------------------
    for paso in PASOS:
        pid = paso["id"]
        padres = paso.get("depende_de") or []
        if padres:
            for padre in padres:
                if padre not in tareas:
                    raise ValueError(
                        f"El paso '{pid}' depende de '{padre}', que no existe "
                        f"en el manifiesto."
                    )
                tareas[padre] >> tareas[pid]
        else:
            # Sin dependencias declaradas, arranca al principio
            t_credenciales >> tareas[pid]

        tareas[pid] >> t_resumen

    inicio >> t_credenciales

    if not PASOS:
        # Manifiesto vacio o no encontrado: el DAG aparece igual, con un aviso.
        t_credenciales >> t_resumen

    t_resumen >> fin
