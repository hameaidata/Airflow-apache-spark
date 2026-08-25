"""
DEMOSTRACION COMPLETA — todo lo que se usa a diario en Airflow

Este DAG existe para mostrar, en un solo archivo, cada mecanismo que va a
necesitar. Cada bloque esta marcado con el numero de la caracteristica.

  1  Variables            configuracion que cambia sin tocar codigo
  2  Connections          credenciales fuera del codigo
  3  Params               parametros al disparar, con formulario en la interfaz
  4  Plugins              operador propio (EjecutarSPOperator)
  5  XCom                 pasar datos entre tareas
  6  TaskGroup            agrupar visualmente
  7  Mapeo dinamico       UNA TAREA POR CADA PROCEDIMIENTO de la lista
  8  Ramificacion         decidir el camino segun el resultado
  9  Trigger rules        tareas que corren aunque algo haya fallado
 10  Callbacks            reaccionar a un fallo
 11  doc_md               esta documentacion, visible en la interfaz

Lo central de lo que pidio esta en el bloque 7: usted pasa NOMBRES de
procedimientos, y para cada uno se establece la conexion y se ejecuta ahi, en
la base de datos, dejando bitacora en esa misma base.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# --- 4. PLUGINS -----------------------------------------------------------
# Airflow anade $AIRFLOW_HOME/plugins al sys.path, por eso el import es
# directo. El operador vive en airflow/plugins/operators/sp_operator.py
from operators.sp_operator import ConsultarBitacoraOperator, EjecutarSPOperator

log = logging.getLogger(__name__)


DOCUMENTACION = """
### Demostracion completa

Ejecuta una lista de procedimientos almacenados contra una base externa,
dejando bitacora **en esa misma base**.

#### Como dispararlo con sus propios procedimientos

1. **Trigger DAG w/ config** (la flecha junto al boton de disparar)
2. Aparece un formulario. Ponga los procedimientos que quiera ejecutar
3. Disparar

Airflow crea **una tarea por cada procedimiento** de la lista.

#### Que necesita antes

| Cosa | Donde |
|---|---|
| Connection `bd_negocio` | Admin -> Connections |
| Variable `demo_tabla_bitacora` | Admin -> Variables (opcional) |

#### La bitacora

Se crea sola en la base de destino. Para consultarla:

```sql
SELECT * FROM airflow_bitacora_sp ORDER BY iniciado_en DESC;
```
"""

CONN_ID = "bd_negocio"


# ===========================================================================
# 10. CALLBACK — se dispara cuando una tarea falla tras agotar reintentos
# ===========================================================================

def al_fallar(context) -> None:
    ti = context["task_instance"]
    log.error(
        "FALLO dag=%s tarea=%s intento=%s fecha=%s",
        ti.dag_id, ti.task_id, ti.try_number, context["ds"],
    )
    # Aqui iria el envio de correo o la llamada al sistema de alertas.


# ===========================================================================
# TAREAS
# ===========================================================================

def leer_configuracion(**context) -> dict:
    """1. VARIABLES — configuracion que se cambia sin tocar el codigo.

    Se leen DENTRO de la funcion, nunca en el nivel superior del archivo. En
    el nivel superior se ejecutarian cada 30 segundos, en cada analisis del
    planificador, golpeando la base de metadatos sin necesidad.
    """
    config = {
        "tabla_bitacora": Variable.get("demo_tabla_bitacora",
                                       default_var="airflow_bitacora_sp"),
        "umbral_seg": int(Variable.get("demo_umbral_segundos", default_var="300")),
        "fecha": context["ds"],
    }
    log.info("Configuracion: %s", config)
    return config          # 5. XCOM — lo devuelto queda disponible


def verificar_conexion(**context) -> dict:
    """2. CONNECTIONS — credenciales fuera del codigo.

    Solo el NOMBRE de la conexion viaja en el DAG. El servidor, el usuario y
    la contrasena viven cifrados en la base de metadatos y se resuelven aqui.
    """
    conn = BaseHook.get_connection(CONN_ID)

    log.info("Conexion '%s'", CONN_ID)
    log.info("   motor    : %s", conn.conn_type)
    log.info("   servidor : %s:%s", conn.host, conn.port)
    log.info("   base     : %s", conn.schema)
    log.info("   usuario  : %s", conn.login)
    # La contrasena NO se registra. Nunca.

    hook = conn.get_hook()
    hook.get_records("SELECT 1")      # prueba real de conectividad
    log.info("Conexion verificada")

    return {"motor": conn.conn_type, "servidor": conn.host, "base": conn.schema}


def preparar_lista(**context) -> list[dict]:
    """3 + 7. Convierte los parametros en la lista que se va a mapear.

    Devuelve una lista de diccionarios. Airflow crea DESPUES una tarea por
    cada elemento, con esos valores como argumentos del operador.
    """
    procedimientos = context["params"]["procedimientos"]
    config = context["ti"].xcom_pull(task_ids="preparacion.leer_configuracion")
    fecha = context["ds"]

    if not procedimientos:
        raise ValueError("No se indico ningun procedimiento a ejecutar")

    tareas = [
        {
            "nombre_sp": sp,
            "parametros": [fecha],          # todos reciben la fecha del proceso
            "tabla_bitacora": config["tabla_bitacora"],
        }
        for sp in procedimientos
    ]

    log.info("Se ejecutaran %d procedimientos:", len(tareas))
    for t in tareas:
        log.info("   %s(%s)", t["nombre_sp"], t["parametros"])

    return tareas


def decidir_camino(**context) -> str:
    """8. RAMIFICACION — devuelve el task_id de la rama a seguir.

    Las ramas no elegidas quedan como 'skipped', no como fallidas.
    """
    ti = context["ti"]
    resultados = ti.xcom_pull(task_ids="ejecutar_procedimientos") or []
    if isinstance(resultados, dict):
        resultados = [resultados]

    con_error = [r for r in resultados if r and r.get("estado") != "OK"]

    if con_error:
        log.warning("%d procedimiento(s) con error", len(con_error))
        return "hubo_errores"

    log.info("Los %d procedimientos terminaron bien", len(resultados))
    return "todo_correcto"


def resumen_final(**context) -> dict:
    """9. TRIGGER RULE — corre siempre, haya fallado lo que haya fallado."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="preparacion.leer_configuracion") or {}
    conexion = ti.xcom_pull(task_ids="preparacion.verificar_conexion") or {}
    bitacora = ti.xcom_pull(task_ids="consultar_bitacora") or {}

    resumen = {
        "fecha": context["ds"],
        "run_id": context["dag_run"].run_id,
        "base_destino": f"{conexion.get('servidor')}/{conexion.get('base')}",
        "tabla_bitacora": config.get("tabla_bitacora"),
        "procedimientos_ok": bitacora.get("ok", 0),
        "procedimientos_error": bitacora.get("error", 0),
    }
    log.warning("RESUMEN %s", resumen)
    return resumen


# ===========================================================================
# DEFINICION DEL DAG
# ===========================================================================

default_args = {
    "owner": "hjara",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="demo_completo",
    description="Ejecuta procedimientos almacenados con bitacora en la base de destino",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,                 # solo manual: es una demostracion
    catchup=False,
    max_active_runs=1,
    tags=["demo", "referencia", "procedimientos"],
    on_failure_callback=al_fallar,
    doc_md=DOCUMENTACION,          # 11. se ve en la interfaz, pestana Docs
    default_view="graph",

    # --- 3. PARAMS -------------------------------------------------------
    # Generan un formulario al usar "Trigger DAG w/ config". Es la forma de
    # pasar los nombres de los procedimientos sin tocar el codigo.
    params={
        "procedimientos": Param(
            default=["public.sp_demo_ventas", "public.sp_demo_clientes"],
            type="array",
            title="Procedimientos a ejecutar",
            description="Uno por linea, con esquema. Ej: dbo.sp_cargar_ventas",
        ),
        "detener_al_primer_error": Param(
            default=False,
            type="boolean",
            title="Detener al primer error",
            description="Si no, intenta todos y reporta al final",
        ),
    },
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    # --- 6. TASKGROUP — agrupa visualmente en la vista Graph --------------
    with TaskGroup(group_id="preparacion") as preparacion:
        t_config = PythonOperator(
            task_id="leer_configuracion",
            python_callable=leer_configuracion,
        )
        t_conexion = PythonOperator(
            task_id="verificar_conexion",
            python_callable=verificar_conexion,
        )
        # Sin dependencia entre ellas: corren en paralelo
        [t_config, t_conexion]

    t_lista = PythonOperator(
        task_id="preparar_lista",
        python_callable=preparar_lista,
    )

    # =====================================================================
    # 7. MAPEO DINAMICO — el nucleo de lo que pidio
    # =====================================================================
    # partial() fija lo que es igual para todas las tareas.
    # expand_kwargs() crea UNA TAREA POR ELEMENTO de la lista.
    #
    # Si preparar_lista devuelve 5 procedimientos, aqui aparecen 5 tareas,
    # cada una con su propio registro, su propio log y su propio reintento.
    # Si una falla, las otras siguen.
    #
    # Dentro de cada tarea, el operador del plugin:
    #   1. establece la conexion usando conn_id
    #   2. crea la tabla de bitacora si no existe
    #   3. registra el inicio
    #   4. ejecuta el procedimiento
    #   5. registra el fin con duracion y filas afectadas
    t_ejecutar = EjecutarSPOperator.partial(
        task_id="ejecutar_procedimientos",
        conn_id=CONN_ID,
        max_active_tis_per_dag=3,      # como maximo 3 a la vez contra la base
    ).expand_kwargs(t_lista.output)

    # --- 8. RAMIFICACION -------------------------------------------------
    t_decidir = BranchPythonOperator(
        task_id="decidir_camino",
        python_callable=decidir_camino,
        trigger_rule=TriggerRule.ALL_DONE,   # decide aunque alguno haya fallado
    )

    t_ok = EmptyOperator(task_id="todo_correcto")
    t_error = EmptyOperator(task_id="hubo_errores")

    # --- Consulta de la bitacora en la base externa -----------------------
    t_bitacora = ConsultarBitacoraOperator(
        task_id="consultar_bitacora",
        conn_id=CONN_ID,
        tabla_bitacora="{{ ti.xcom_pull(task_ids='preparacion.leer_configuracion')['tabla_bitacora'] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # --- 9. TRIGGER RULE — el resumen sale pase lo que pase ---------------
    t_resumen = PythonOperator(
        task_id="resumen_final",
        python_callable=resumen_final,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fin = EmptyOperator(task_id="fin", trigger_rule=TriggerRule.ALL_DONE)

    # --- Dependencias ----------------------------------------------------
    inicio >> preparacion >> t_lista >> t_ejecutar >> t_decidir
    t_decidir >> [t_ok, t_error] >> t_bitacora >> t_resumen >> fin
