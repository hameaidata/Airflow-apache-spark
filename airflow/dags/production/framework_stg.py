"""
FRAMEWORK DE CARGA A STG  —  orquestacion de los dos scripts compartidos.

===========================================================================
QUE HACE ESTE DAG, Y QUE NO HACE

NO conoce los parametros de ningun proceso. Ni la tabla origen, ni el filtro,
ni la ruta de los parquet. Nada de eso.

Sus dos scripts reciben un id y leen ellos mismos su fila en la tabla de
parametros de SingleStore. Eso deja a Airflow un trabajo muy pequeno y muy
estable: saber QUE ids ejecutar y EN QUE ORDEN. Nada mas.

La consecuencia practica: cuando alguien anada un proceso nuevo a la tabla de
parametros, este DAG lo recoge en la siguiente ejecucion. Sin tocar codigo,
sin desplegar, sin reiniciar el scheduler.

    tabla de parametros (SingleStore)
              |
              v
      listar_procesos           <- una consulta, una vez por ejecucion
              |
              v
      por cada id, en paralelo e independientes entre si:
              |
              +-- extraer        spark-submit extractor.py --id-proceso N
              +-- cargar_stg     spark-submit cargador.py  --id-proceso N
              +-- ejecutar_sp    el SP en SingleStore

===========================================================================
POR QUE LA CONSULTA VA EN UNA TAREA Y NO EN EL CUERPO DEL ARCHIVO

Seria mas corto escribir el SELECT arriba del todo y construir las tareas con
un bucle. Seria tambien un error caro, y conviene entender por que.

El scheduler de Airflow RE-EJECUTA este archivo entero cada pocos segundos
para detectar cambios (min_file_process_interval, 30 s por defecto). Un SELECT
en el cuerpo del archivo se convierte en una consulta a SingleStore cada 30
segundos, por archivo, para siempre. Y si SingleStore no responde, el archivo
falla al parsearse y el DAG DESAPARECE de la interfaz — justo cuando mas falta
hace verlo.

Poniendo la consulta dentro de una tarea, se ejecuta UNA VEZ por corrida.
El precio es que el grafo no muestra los procesos hasta que la corrida
empieza. Es un precio que vale la pena pagar.

===========================================================================
EL PATRON DE MAPEO, Y SUS DOS TRAMPAS

Se usa expand_kwargs() sobre un @task_group. Cada fila de la lista produce una
copia completa del grupo, y las copias avanzan INDEPENDIENTES: el proceso 102
puede estar cargando mientras el 101 sigue extrayendo, y si el 103 falla al
extraer, no detiene a los demas.

Dos cosas que se comprobaron ejecutando el DAG de verdad, no leyendo la
documentacion:

  1. El objeto que recibe el grupo es un MappedArgument, y NO SE PUEDE INDEXAR.
     Escribir  proceso["id_proceso"]  dentro del grupo revienta al parsear con
     "'MappedArgument' object is not subscriptable". Por eso el grupo recibe
     un parametro por columna y nunca un diccionario.

  2. Los valores deben ser TEXTO. Un entero en un argumento de operador falla
     en ejecucion con "TypeError: expected str, bytes or os.PathLike object,
     not int" — un error que no menciona el mapeo por ningun lado y cuesta
     encontrar. Por eso listar_procesos convierte todo con str().

===========================================================================
CONFIGURACION — todo por Variables de Airflow, nada quemado aqui
===========================================================================
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

try:
    from operators.sp_operator import EjecutarSPOperator
    SP_DISPONIBLE = True
except ImportError:
    # El plugin aparece tras reiniciar webserver y scheduler. Sin el, el DAG
    # sigue siendo visible y los dos pasos de Spark funcionan; solo se omite
    # el paso del procedimiento. Un DAG que no se puede ni ver es peor que uno
    # incompleto.
    SP_DISPONIBLE = False


# --- Variables de Airflow y sus valores por defecto --------------------------
# Se leen con default_var para que el DAG se pueda VER antes de configurar
# nada. Un DAG invisible no se puede diagnosticar.

CONN_PARAMETROS = Variable.get("framework_conn_parametros", default_var="singlestore_bsg")
CONN_STG        = Variable.get("framework_conn_stg",        default_var="singlestore_bsg")

SCRIPT_EXTRACTOR = Variable.get(
    "framework_script_extractor",
    default_var="/opt/spark-apps/framework/extractor.py")
SCRIPT_CARGADOR = Variable.get(
    "framework_script_cargador",
    default_var="/opt/spark-apps/framework/cargador_stg.py")

# La consulta de control. Es LO UNICO que hay que adaptar a su esquema real.
# Debe devolver, con estos nombres exactos, las tres columnas de CLAVES.
CONSULTA_POR_DEFECTO = """
    SELECT  id_proceso   AS id_proceso,
            nombre       AS nombre,
            sp_destino   AS sp_destino
    FROM    parametros_procesos
    WHERE   activo = 1
    ORDER BY orden, id_proceso
"""
CONSULTA = Variable.get("framework_consulta_procesos", default_var=CONSULTA_POR_DEFECTO)

# Como se llama el argumento con el que sus scripts reciben el id. Es lo unico
# que hay que ajustar si su convencion no coincide: --proceso, --id, -p, lo que
# sea. Va en una Variable para no tener que editar este archivo por un guion.
ARG_ID = Variable.get("framework_arg_id", default_var="--id-proceso")

# Cuantas extracciones simultaneas como maximo. Es el freno que protege a la
# base origen: cuarenta procesos lanzados a la vez contra DB2 la tumban, y el
# cuello de botella pasa a ser suya, no nuestra.
MAX_EXTRACCIONES = int(Variable.get("framework_max_extracciones", default_var="4"))
MAX_CARGAS       = int(Variable.get("framework_max_cargas",       default_var="4"))

# Contrato de columnas. El grupo mapeado tiene un parametro por cada una.
CLAVES = ("id_proceso", "nombre", "sp_destino")


# =============================================================================

@dag(
    dag_id="framework_stg",
    description="Extrae a parquet y carga a STG, un proceso por fila de la tabla de parametros",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Lima"),
    catchup=False,
    max_active_runs=1,          # dos corridas a la vez escribirian el mismo STG
    tags=["framework", "stg", "produccion"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    doc_md=__doc__,
)
def framework_stg():

    # -------------------------------------------------------------------------
    @task
    def listar_procesos() -> list[dict]:
        """Lee de SingleStore QUE procesos hay que correr. Solo eso.

        No trae los parametros de cada proceso: de eso se encargan los scripts.
        Trae identificadores. Cuanto menos sepa Airflow, menos se rompe cuando
        la tabla de parametros cambie de forma.
        """
        conexion = BaseHook.get_connection(CONN_PARAMETROS)
        # get_hook() resuelve el hook desde el conn_type. Asi este codigo no
        # sabe que hay un SingleStore al otro lado, y si manana lo cambian por
        # MySQL o SQL Server no hay nada que tocar aqui.
        hook = conexion.get_hook()

        filas = hook.get_records(CONSULTA)
        if not filas:
            raise AirflowException(
                f"La consulta de control no devolvio ninguna fila.\n"
                f"Conexion: {CONN_PARAMETROS}\n"
                f"Revise la Variable 'framework_consulta_procesos' y que haya "
                f"procesos con activo = 1.")

        # get_records devuelve tuplas posicionales, no diccionarios: hay que
        # saber el orden. Es el de CONSULTA, y por eso la consulta declara
        # alias explicitos aunque parezcan redundantes.
        procesos: list[dict] = []
        for fila in filas:
            if len(fila) < len(CLAVES):
                raise AirflowException(
                    f"La consulta devolvio {len(fila)} columnas y se esperaban "
                    f"{len(CLAVES)}: {', '.join(CLAVES)}.\n"
                    f"Corrija la Variable 'framework_consulta_procesos'.")
            # str() obligatorio: un entero aqui falla despues con un
            # "expected str ... not int" que no menciona el mapeo.
            # None -> "" para que el corto-circuito del SP lo detecte.
            procesos.append({
                clave: ("" if valor is None else str(valor))
                for clave, valor in zip(CLAVES, fila)
            })

        nombres = ", ".join(p["nombre"] for p in procesos[:10])
        print(f"[framework] {len(procesos)} procesos activos: {nombres}"
              f"{' ...' if len(procesos) > 10 else ''}")
        return procesos

    # -------------------------------------------------------------------------
    @task_group(group_id="proceso")
    def procesar(id_proceso: str, nombre: str, sp_destino: str):
        """Una copia de este grupo por cada fila. Avanzan independientes.

        Los tres parametros son MappedArgument. Se pasan ENTEROS a los
        operadores; cualquier intento de operar con ellos aqui (indexar,
        concatenar, comparar) falla al parsear, porque en tiempo de parseo
        todavia no tienen valor.
        """

        # --- 1. Extraer del origen a parquet ---------------------------------
        extraer = SparkSubmitOperator(
            task_id="extraer",
            conn_id="spark_default",
            application=SCRIPT_EXTRACTOR,
            # El nombre aparece en la interfaz de Spark. Con el id dentro, se
            # sabe cual es cual cuando hay veinte corriendo.
            name="extraer_{{ ds_nodash }}",
            application_args=[ARG_ID, id_proceso],
            # Los jars ya estan en la imagen. Se pasan explicitos porque los
            # executors de Spark no heredan el classpath del driver.
            jars=("/opt/airflow/jars/db2-jcc.jar,"
                  "/opt/airflow/jars/singlestore-jdbc.jar"),
            # executor_memory debe ser MENOR que SPARK_WORKER_MEMORY. Si pide
            # mas de lo que el worker anuncia, el master nunca coloca el
            # executor y la aplicacion se queda en WAITING para siempre, sin
            # ningun mensaje de error.
            executor_memory="1g",
            executor_cores=1,
            # El freno a la base origen.
            max_active_tis_per_dag=MAX_EXTRACCIONES,
        )

        # --- 2. Cargar los parquet a STG -------------------------------------
        cargar_stg = SparkSubmitOperator(
            task_id="cargar_stg",
            conn_id="spark_default",
            application=SCRIPT_CARGADOR,
            name="cargar_{{ ds_nodash }}",
            application_args=[ARG_ID, id_proceso],
            jars="/opt/airflow/jars/singlestore-jdbc.jar",
            executor_memory="1g",
            executor_cores=1,
            max_active_tis_per_dag=MAX_CARGAS,
        )

        extraer >> cargar_stg

        # --- 3. El procedimiento en STG, si esta fila tiene uno ---------------
        if SP_DISPONIBLE:
            # No todos los procesos tienen SP. short_circuit salta el resto de
            # la rama cuando devuelve False, sin marcar fallo: el proceso
            # termina en verde con el SP omitido, que es lo correcto.
            @task.short_circuit(task_id="tiene_sp")
            def tiene_sp(nombre_sp: str) -> bool:
                if nombre_sp and nombre_sp.strip():
                    return True
                print("[framework] Sin SP configurado; se omite este paso.")
                return False

            ejecutar_sp = EjecutarSPOperator(
                task_id="ejecutar_sp",
                conn_id=CONN_STG,
                nombre_sp=sp_destino,
                tabla_bitacora="airflow_bitacora_procesos",
            )

            cargar_stg >> tiene_sp(sp_destino) >> ejecutar_sp

    # -------------------------------------------------------------------------
    # expand_kwargs y no expand: cada CLAVE del diccionario se convierte en un
    # parametro distinto del grupo. Con expand() el grupo recibiria el
    # diccionario entero como un solo MappedArgument, que luego no se puede
    # indexar — el error del que habla la cabecera.
    procesar.expand_kwargs(listar_procesos())


dag_framework_stg = framework_stg()
