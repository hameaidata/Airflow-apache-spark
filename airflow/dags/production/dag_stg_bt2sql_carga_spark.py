"""
STG_BT2SQL_CARGA_SPARK - Bantotal (IBM i) -> Parquet -> STG en SQL Server, con Spark.

GEMELO DE STG_BT2SQL_CARGA. NO LO REEMPLAZA.
--------------------------------------------
Mismas cuatro etapas, mismo contrato, mismas tablas de control. Lo unico que
cambia es quien lee y quien escribe: aqui el core se lee con varias conexiones
en paralelo y la staging se escribe con varios executores.

Los dos DAGs pueden correr el mismo dia sin pisarse:

    STG_BT2SQL_CARGA        /data/bt2sql/<fecha>/<tabla>/<tabla>.parquet
    STG_BT2SQL_CARGA_SPARK  /data/bt2sql/spark/<fecha>/<tabla>/     (carpeta)

y se distinguen en la bitacora por nom_proceso. Esa convivencia es a proposito:
la forma de dar por bueno el pipeline Spark es correr los dos sobre el mismo
dia y comparar fila a fila, no confiar en que la traduccion salio bien.

DONDE GANA SPARK, DICHO SIN ADORNOS
-----------------------------------
Gana en dos sitios y en ninguno mas:

  - la LECTURA del core, si la tabla declara columna de particion. Sin eso,
    Spark lee con UNA conexion y hace el mismo trabajo que pandas arrastrando
    un cluster. El job lo avisa en el log.
  - la ESCRITURA a la staging, que va en paralelo.

El catalogo, la bitacora y el TRUNCATE+INSERT final siguen siendo sentencias
sueltas ejecutadas desde el driver. Mandarlas por el cluster solo agregaria
coordinacion.

QUE HACE FALTA ANTES DE LA PRIMERA CORRIDA
------------------------------------------
  1. La Variable BT2SQL_SPARK, con scripts/sync_variables.py
  2. Las Connections CONEXION_BANTOTAL y CONEXION_SQLSERVER
  3. Los dos jobs en /opt/spark-apps/etl/ (los monta el compose desde spark/jobs)
  4. El volumen spark_data montado, que es el puente para el JSON de runtime
"""

from __future__ import annotations

import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

logger = logging.getLogger(__name__)

NOMBRE = "STG_BT2SQL_CARGA_SPARK"
VARIABLE = "BT2SQL_SPARK"

# Puente entre Airflow y Spark. El job no puede leer Variables -no tiene la
# metadata database-, asi que el DAG las materializa aqui y le pasa la ruta.
# El volumen spark_data lo montan los dos lados.
RUNTIME_JSON = "/opt/spark-data/runtime/bt2sql_spark.json"
APPS = "/opt/spark-apps/etl"

POOL_SQLSERVER = os.environ.get("AIRFLOW_POOL_SQLSERVER", "default_pool")

# Nombres por defecto, y son SOLO el respaldo. Los de verdad salen de la
# configuracion publicada, igual que todo lo demas.
CONN_ORIGEN_DEFECTO = "CONEXION_BANTOTAL"
CONN_DESTINO_DEFECTO = "CONEXION_SQLSERVER"


def _conn_ids() -> tuple[str, str]:
    """Lee los conn_id de la configuracion publicada, con respaldo.

    POR QUE SE LEE EN TIEMPO DE PARSEO Y POR QUE NO SE FALLA SI NO SE PUEDE
    ----------------------------------------------------------------------
    El operador de Spark necesita el conn_id para CONSTRUIRSE, asi que esto no
    se puede dejar para la ejecucion de la tarea: hay que resolverlo mientras el
    scheduler parsea el archivo.

    Y si el JSON no existe todavia -primera instalacion, o nadie ha publicado
    aun- se usan los nombres por defecto en vez de lanzar una excepcion. La
    razon es practica: un DAG que no aparece en la interfaz porque revento al
    parsearse es muchisimo mas dificil de diagnosticar que uno que aparece y
    falla en su primera tarea con un mensaje que dice exactamente que pasa.
    Esa primera tarea es verificar_origen_destino, que comprueba las dos
    conexiones por su nombre.

    Se lee del archivo del volumen y NO de Variable.get() a proposito: esto
    corre en cada pasada del parser, cada 30 segundos, y un Variable.get() ahi
    es una consulta a la base de metadatos por DAG y por pasada.
    """
    try:
        import json

        doc = json.loads(Path(RUNTIME_JSON).read_text(encoding="utf-8"))
        ext = doc.get("extraccion", {})
        origen = str(ext.get("conn_id_origen") or "").strip() or CONN_ORIGEN_DEFECTO
        destino = str(ext.get("conn_id_destino") or "").strip() or CONN_DESTINO_DEFECTO
        return origen, destino
    except Exception as exc:                                     # noqa: BLE001
        logger.warning(
            "No se pudieron leer los conn_id de %s (%s). Se usan los nombres "
            "por defecto: %s y %s. Si sus Connections se llaman de otra forma, "
            "declarelas en conn_id_origen y conn_id_destino de la Variable %s "
            "y publique con publicar_config='si'.",
            RUNTIME_JSON, exc, CONN_ORIGEN_DEFECTO, CONN_DESTINO_DEFECTO, VARIABLE)
        return CONN_ORIGEN_DEFECTO, CONN_DESTINO_DEFECTO


CONN_ORIGEN, CONN_DESTINO = _conn_ids()

# Acota cuantos spark-submit corren a la vez. Cada tarea de tabla levanta su
# propio driver, y cuatro drivers peleando por los mismos executors tardan mas
# que dos. Declare el pool en Admin > Pools con el numero de slots que aguante
# su cluster; sin pool declarado, Airflow no limita nada.
POOL_SPARK = os.environ.get("AIRFLOW_POOL_SPARK", "default_pool")


def _slug(texto: str) -> str:
    """Nombre valido para task_id y group_id de Airflow.

    Airflow valida los dos con reglas distintas y el group_id es el estricto:
    no admite el punto, que usa para separar la jerarquia.
    """
    limpio = re.sub(r"[^0-9A-Za-z_-]+", "_", str(texto).strip())
    return re.sub(r"_+", "_", limpio).strip("_").lower() or "sin_nombre"


def _plan_de_tablas() -> list[dict]:
    """La lista con la que se DIBUJA el grafo, leida en tiempo de parseo.

    POR QUE AQUI Y NO EN UNA TAREA
    ------------------------------
    Para que cada proceso sea una tarea visible, el grafo tiene que conocer la
    lista antes de que exista una DagRun. Es el mismo patron que usa
    BDS_DATAHUB_PROCESOS, que arma sus TaskGroups desde DAG_ODS_TABLAS y
    DAG_BDS_TABLAS.

    EL HUEVO Y LA GALLINA, Y COMO SE RESUELVE
    -----------------------------------------
    La lista buena es la del JSON publicado en el volumen, porque es la que
    Spark va a leer. Pero ese archivo lo escribe preparar_config DURANTE la
    corrida, asi que en una instalacion nueva todavia no existe y el grafo se
    quedaria sin tareas para siempre.

    Por eso: primero el archivo -lectura de disco, barata, en cada pasada del
    parser- y SOLO si no existe se cae a Variable.get(), que es una consulta a
    la base de metadatos. Despues de la primera corrida el archivo existe y esa
    consulta no se vuelve a hacer.

    Se devuelven TODAS las tablas declaradas, activas o no. Las que hoy no
    entran se dibujan igual y se saltan en ejecucion: un grafo que cambia de
    forma segun el dia es mucho mas dificil de leer que uno estable con tareas
    en rosa.
    """
    import json

    doc, origen = None, ""
    try:
        doc = json.loads(Path(RUNTIME_JSON).read_text(encoding="utf-8"))
        origen = "el JSON publicado"
    except Exception:                                            # noqa: BLE001
        try:
            from airflow.models import Variable

            doc = Variable.get(VARIABLE, deserialize_json=True)
            origen = f"la Variable {VARIABLE} (el JSON del volumen aun no existe)"
        except Exception as exc:                                 # noqa: BLE001
            logger.warning(
                "No se pudo leer ni %s ni la Variable %s (%s). El DAG se "
                "publica con una sola tarea que lo explica.",
                RUNTIME_JSON, VARIABLE, exc)
            return []

    tablas = doc.get("tablas") or []
    procesos = {
        (str(p.get("nombre_proceso", "")).strip().upper(),
         str(p.get("nombre_esquema", "")).strip().upper()): p
        for p in (doc.get("extraccion", {}).get("procesos") or [])
    }

    plan = []
    for e in tablas:
        nombre = str(e.get("nombre_proceso") or e.get("tabla", "")).strip()
        esquema = str(e.get("esquema", "")).strip()
        proc = procesos.get((nombre.upper(), esquema.upper()))
        plan.append({
            "proceso": nombre,
            "esquema": esquema,
            "tabla": str(e.get("tabla", "")).strip(),
            "destino": e.get("tabla_destino") or "",
            "prioridad": int((proc or {}).get("prioridad", 99)),
            "declarado_activo": str(e.get("activo", "S")).strip().upper() == "S",
            "tiene_proceso": proc is not None,
        })

    plan.sort(key=lambda x: (x["prioridad"], x["tabla"]))
    if plan:
        logger.info("Grafo de %s armado desde %s: %s proceso(s)",
                    NOMBRE, origen, len(plan))
    return plan


PLAN = _plan_de_tablas()

VARIABLE_SPARK = "CONFIGURACION_SPARK"


# ============================================================================
# RECURSOS DEL CLUSTER
# ----------------------------------------------------------------------------
# SIN ESTO EL JOB CORRE CON UN SOLO EXECUTOR.
#
# BsgSparkSubmitOperator trae de fabrica executor_memory=1g, executor_cores=1,
# num_executors=1 y cores_max=2. Son defaults prudentes para que algo arranque
# recien instalado, no para procesar el core. Si el operador no recibe estos
# cuatro valores, Spark sale al cluster y usa UN slot: el paralelismo de la
# lectura particionada no se nota porque no hay donde repartirlo.
#
# DRIVER_MEMORY importa aqui mas que en otros jobs: en client mode el driver
# corre dentro del worker de Airflow, y es quien hace el .count() y mantiene
# las conexiones de catalogo y bitacora.
# ============================================================================
RECURSOS_DEFAULT = {
    "EXECUTOR_MEMORY": "1g",
    "EXECUTOR_CORES": 1,
    "NUM_EXECUTORS": 1,
    "CORES_MAX": 2,
    "DRIVER_MEMORY": "1g",
}


def cargar_recursos() -> dict:
    """Lee CONFIGURACION_SPARK en tiempo de parseo.

    Si falta o esta mal, NO tumba el archivo: cae a los defaults y deja un
    WARNING. Ese aviso es importante -la version de este patron en el otro DAG
    se traga la excepcion en silencio, y un recurso que no se aplica se parece
    mucho a un cluster lento.
    """
    from airflow.models import Variable

    try:
        config = Variable.get(VARIABLE_SPARK, deserialize_json=True) or {}
    except Exception as exc:                                    # noqa: BLE001
        logger.warning(
            "No se pudo leer la Variable %s (%s). Se usan los recursos por "
            "defecto: %s. El job correra con UN executor.",
            VARIABLE_SPARK, exc, RECURSOS_DEFAULT)
        return dict(RECURSOS_DEFAULT)

    recursos = {**RECURSOS_DEFAULT, **{k: v for k, v in config.items()
                                       if k in RECURSOS_DEFAULT}}
    if recursos == RECURSOS_DEFAULT:
        logger.warning(
            "%s no define ninguno de %s. El job correra con UN executor.",
            VARIABLE_SPARK, ", ".join(RECURSOS_DEFAULT))
    return recursos


RECURSOS = cargar_recursos()

# ----------------------------------------------------------------------------
# EL EVENT LOG YA NO SE DESACTIVA.
#
# Hasta el 2026-10-07 aqui habia un spark.eventLog.enabled=false, y era un
# parche: spark-defaults.conf lo activa apuntando a /opt/spark-events, pero el
# volumen spark_events no estaba montado en los servicios de Airflow. En client
# mode el driver vive en el worker de Airflow, asi que al construir el
# SparkContext buscaba ese directorio, no lo encontraba, y el job moria en el
# segundo 1 con un FileNotFoundException que no mencionaba ni a Airflow ni al
# eventLog. Al escalar workers el sintoma era "al worker nuevo le falta una
# carpeta", con el cluster de Spark viendose perfecto -porque los servicios
# spark-* si montaban el volumen y los de Airflow no-.
#
# Arreglado en los dos docker-compose: spark_events va en x-airflow-common.
# Con eso el History Server por fin muestra los jobs que lanza Airflow.
# ----------------------------------------------------------------------------
SPARK_CONF = {
    "spark.sql.adaptive.enabled": "true",
}


# ============================================================================
# OPERADOR LOCAL: dos conexiones en un mismo job
# ----------------------------------------------------------------------------
# BsgSparkJdbcOperator inyecta UNA Connection como ORIGEN_USUARIO/ORIGEN_CLAVE.
# La extraccion necesita dos: Bantotal para los datos y SQL Server para el
# catalogo y la bitacora. Esta subclase vive aqui y no toca el plugin, que ya
# esta en produccion sirviendo a otro DAG.
#
# Las credenciales del segundo destino van por VARIABLES DE ENTORNO y no por
# --conf ni por argumento: lo que entra en spark.* aparece en la pestana
# Environment de la interfaz de Spark, en la salida de ps dentro del
# contenedor, y -con spark.eventLog.enabled activo- queda escrito en disco
# para que lo lea el History Server.
# ============================================================================
def _operador_spark(**kwargs):
    """Importa el plugin DENTRO de la funcion.

    A nivel de modulo obligaria al scheduler a resolver el plugin en cada
    pasada del parser, y si el plugin falla se cae el archivo entero con un
    Broken DAG en vez de fallar solo la tarea.
    """
    from operators.spark_operator import BsgSparkJdbcOperator
    return BsgSparkJdbcOperator(**kwargs)


class _SparkDosConexiones:
    """Fabrica del operador de extraccion, que necesita las dos Connections."""

    @staticmethod
    def crear(**kwargs):
        from airflow.hooks.base import BaseHook
        from operators.spark_operator import BsgSparkJdbcOperator
        from utils.spark_config import driver_for_conn, jdbc_url

        class ConDestino(BsgSparkJdbcOperator):
            #: nombre_proceso de esta tarea; None = corrida de todo el catalogo
            proceso_de_la_tarea: str | None = None

            def execute(self, context):
                # La doble llave se comprueba AQUI, contra la configuracion que
                # se acaba de publicar, y no contra el plan con el que se
                # dibujo el grafo: entre un dibujo y una corrida puede haber
                # pasado una publicacion. Saltar es lo correcto, no fallar: que
                # una tabla no entre hoy es el funcionamiento normal del
                # calendario, y una tarea en rojo por eso seria ruido diario.
                if self.proceso_de_la_tarea:
                    tipo = (context.get("params") or {}).get("tipo_ejecucion", "diario")
                    entra, motivo = _proceso_entra_hoy(self.proceso_de_la_tarea, tipo)
                    if not entra:
                        from airflow.exceptions import AirflowSkipException

                        logger.info("%s no entra en una corrida %r: %s",
                                    self.proceso_de_la_tarea, tipo, motivo)
                        raise AirflowSkipException(
                            f"{self.proceso_de_la_tarea}: {motivo}")

                conn = BaseHook.get_connection(CONN_DESTINO)
                self.env_vars = {
                    **dict(self.env_vars or {}),
                    "DESTINO_JDBC_URL": jdbc_url(conn),
                    "DESTINO_DRIVER_CLASS": driver_for_conn(conn).driver_class,
                    "DESTINO_USUARIO": conn.login,
                    "DESTINO_CLAVE": conn.password,
                }
                return super().execute(context)

        proceso = kwargs.pop("proceso_de_la_tarea", None)
        tarea = ConDestino(**kwargs)
        tarea.proceso_de_la_tarea = proceso
        return tarea


# ============================================================================
# TAREAS DE PYTHON
# ============================================================================
def preparar_config(tipo_ejecucion: str = "diario",
                    publicar_config: str = "auto") -> str:
    """Genera el batch_id y pone al dia la configuracion que Spark va a leer.

    EL PROBLEMA, Y POR QUE EL VALOR POR DEFECTO ES 'auto'
    -----------------------------------------------------
    El job de Spark no puede leer Variables de Airflow: corre en otro
    contenedor, sin la base de metadatos. El puente es este JSON en el volumen
    compartido, asi que lo que esta en la Variable no le llega al job hasta que
    alguien lo copia aqui.

    Al principio esta tarea reescribia el archivo en CADA corrida, sin
    condicion, y eso tenia tres problemas: una edicion a medias entraba en
    produccion sola, relanzar una corrida vieja usaba la configuracion de hoy,
    y no quedaba rastro de con que configuracion corrio cada ejecucion.

    El primer intento de arreglarlo fue exigir publicar_config='si' a mano, con
    'no' por defecto. Era peor: olvidarse del parametro -que es lo normal-
    produce justo el sintoma mas desconcertante de todos. Se edita la Variable,
    se sincroniza, se lanza el DAG, y no pasa nada. Sin error. El pipeline corre
    con la configuracion anterior y no hay nada en la interfaz que lo diga.

    Asi que el valor por defecto compara y decide:

        'auto'  (POR DEFECTO)
            Compara el hash de la Variable con el del archivo publicado.
            Iguales -> no toca nada y lo dice.
            Distintos -> respalda el anterior y publica, con los dos hashes en
            el log. Editar la Variable y lanzar el DAG basta: no hay ritual que
            recordar, y el rastro queda igual.

        'no'
            No mira la Variable. Usa el archivo que ya esta en el volumen.
            Para esto sirve: relanzar una corrida vieja con EXACTAMENTE la
            configuracion con la que corrio, sin que una edicion posterior se
            cuele.

        'si'
            Publica aunque los hashes coincidan. Util si alguien edito el
            archivo del volumen a mano y hay que devolverlo a lo que dice la
            Variable.

    El riesgo que 'auto' deja abierto -una edicion a medias entrando sola- en
    realidad ya tiene su compuerta mas arriba: la Variable no se edita a mano en
    la interfaz, se sincroniza desde un JSON versionado con
    scripts/sync_variables.py. Ese es el acto deliberado. Y si aun asi entra
    algo equivocado, el respaldo fechado y los hashes del log permiten volver.
    """
    import hashlib
    import json
    import shutil

    from airflow.models import Variable

    destino = Path(RUNTIME_JSON)
    modo = str(publicar_config or "auto").strip().lower()
    if modo in ("si", "sí", "yes", "true", "1"):
        modo = "si"
    elif modo in ("no", "false", "0"):
        modo = "no"
    elif modo != "auto":
        raise AirflowException(
            f"publicar_config={publicar_config!r} no es valido. "
            f"Use 'auto' (lo normal), 'no' o 'si'.")

    def huella_texto(texto: str) -> str:
        return hashlib.sha256(texto.encode("utf-8")).hexdigest()[:12]

    def huella(ruta: Path) -> str:
        return hashlib.sha256(ruta.read_bytes()).hexdigest()[:12]

    def describir_publicado() -> None:
        config = json.loads(destino.read_text(encoding="utf-8"))
        logger.info("  archivo : %s", RUNTIME_JSON)
        logger.info("  hash    : %s", huella(destino))
        logger.info("  fechado : %s",
                    datetime.fromtimestamp(destino.stat().st_mtime).isoformat(timespec="seconds"))
        logger.info("  tablas  : %s declarada(s), %s en el calendario",
                    len(config.get("tablas", [])),
                    len(config.get("extraccion", {}).get("procesos", [])))

    # --- 'no': el archivo del volumen manda y no se mira la Variable -------
    if modo == "no":
        if not destino.exists():
            raise AirflowException(
                f"publicar_config='no' pero {RUNTIME_JSON} no existe todavia, "
                f"asi que no hay nada que reutilizar.\n"
                f"Lance sin parametros -publicar_config vale 'auto' por "
                f"defecto- y se publicara desde la Variable {VARIABLE}.")
        logger.info("CONFIGURACION FIJADA (publicar_config=no): no se mira la "
                    "Variable, se usa el archivo tal como esta.")
        describir_publicado()

    else:
        # --- 'auto' y 'si' necesitan la Variable ---------------------------
        try:
            config = Variable.get(VARIABLE, deserialize_json=True)
        except Exception as exc:                                # noqa: BLE001
            raise AirflowException(
                f"Falta la Variable {VARIABLE} o no es JSON valido ({exc}). "
                f"Cargala con  python scripts/sync_variables.py --solo {VARIABLE}"
            ) from exc

        for seccion in ("extraccion", "carga"):
            if seccion not in config:
                raise AirflowException(
                    f"La Variable {VARIABLE} no tiene la seccion {seccion!r}. "
                    f"Debe traer 'extraccion' y 'carga'.")

        texto = json.dumps(config, ensure_ascii=False, indent=2)
        nueva = huella_texto(texto)
        anterior = huella(destino) if destino.exists() else None

        if modo == "auto" and anterior == nueva:
            logger.info("CONFIGURACION SIN CAMBIOS: la Variable %s y el archivo "
                        "publicado son identicos (hash %s). No se toca nada.",
                        VARIABLE, nueva)
            describir_publicado()
        else:
            destino.parent.mkdir(parents=True, exist_ok=True)

            # Copia fechada ANTES de sobrescribir. Sin esto, publicar una
            # configuracion equivocada es irreversible: la version anterior
            # solo existia en ese archivo.
            if destino.exists():
                respaldo = destino.with_name(
                    f"{destino.stem}.{datetime.now():%Y%m%d%H%M%S}{destino.suffix}")
                shutil.copy2(destino, respaldo)
                logger.info("respaldo de la configuracion anterior en %s", respaldo)

            destino.write_text(texto, encoding="utf-8")
            # El contenedor de Spark corre con otro usuario que el de Airflow.
            os.chmod(destino, 0o644)

            motivo = ("la Variable cambio" if modo == "auto"
                      else "publicar_config='si', se publica aunque no haya cambiado")
            logger.info("CONFIGURACION PUBLICADA desde la Variable %s (%s)",
                        VARIABLE, motivo)
            logger.info("  antes : %s", anterior or "(no existia)")
            logger.info("  ahora : %s", nueva)
            describir_publicado()

    # tipo_ejecucion viaja por argumento del job, NO dentro del JSON: es una
    # decision de ESTA corrida, no parte de la configuracion publicada.
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    logger.info("batch_id de esta corrida: %s  (tipo_ejecucion=%s)", batch_id, tipo_ejecucion)
    return batch_id


def _sin_configuracion() -> None:
    """Falla explicando por que el grafo salio sin tareas de extraccion."""
    raise AirflowException(
        f"Este DAG dibuja una tarea por proceso, y para eso lee la lista de "
        f"tablas en tiempo de parseo: primero {RUNTIME_JSON}, y si no existe, "
        f"la Variable {VARIABLE}. No se pudo leer ninguno de los dos, asi que "
        f"no hay procesos que dibujar.\n\n"
        f"Lo mas probable, en orden:\n"
        f"  1. La Variable {VARIABLE} no esta cargada. Cargala con\n"
        f"     python scripts/sync_variables.py --solo {VARIABLE}\n"
        f"  2. La Variable esta pero no trae la clave 'tablas' de nivel "
        f"superior; es una version anterior de la configuracion.\n"
        f"  3. El volumen spark_data no esta montado en este worker.\n\n"
        f"Despues de arreglarlo, el scheduler redibuja el grafo en la "
        f"siguiente pasada del parser, en unos 30 segundos.")


def _proceso_entra_hoy(proceso: str, tipo_ejecucion: str) -> tuple[bool, str]:
    """La doble llave, para un solo proceso. Devuelve (entra, motivo si no)."""
    try:
        doc = _config_publicada()
    except Exception as exc:                                     # noqa: BLE001
        return False, f"no se pudo leer la configuracion publicada ({exc})"

    flag = {"diario": "estado_diario",
            "semanal": "estado_semanal",
            "mensual": "estado_mensual"}.get(str(tipo_ejecucion).strip().lower())
    if flag is None:
        return False, f"tipo_ejecucion={tipo_ejecucion!r} no soportado"

    clave = str(proceso).strip().upper()
    entrada = next((e for e in (doc.get("tablas") or [])
                    if str(e.get("nombre_proceso") or e.get("tabla", "")).strip().upper() == clave),
                   None)
    if entrada is None:
        return False, "ya no esta declarado en 'tablas' de la configuracion publicada"
    if str(entrada.get("activo", "S")).strip().upper() != "S":
        return False, "activo='N' en tablas"

    proc = next((p for p in (doc.get("extraccion", {}).get("procesos") or [])
                 if str(p.get("nombre_proceso", "")).strip().upper() == clave), None)
    if proc is None:
        return False, "sin entrada en extraccion.procesos"
    if int(proc.get("estado", 0)) != 1:
        return False, "estado=0 en procesos"
    if int(proc.get(flag, 0)) != 1:
        return False, f"{flag}=0 en procesos"
    return True, ""


def _config_publicada() -> dict:
    """Lee el JSON que Spark VA A USAR, no la Variable.

    Importa la diferencia. Con publicar_config='no' la Variable puede tener ya
    la edicion de manana mientras el volumen sigue con la de hoy. Si el DAG
    inventariara y validara contra la Variable, estaria dando por bueno algo
    distinto de lo que el job ejecuta: el inventario diria cinco tablas y Spark
    extraeria cuatro, y el log de Airflow mentiria sin que nada fallara.
    """
    import json

    ruta = Path(RUNTIME_JSON)
    if not ruta.exists():
        raise AirflowException(
            f"{RUNTIME_JSON} no existe. Lance el DAG con publicar_config='si' "
            f"una vez para publicar la configuracion.")
    return json.loads(ruta.read_text(encoding="utf-8"))


def _clave(esquema, tabla) -> tuple[str, str]:
    """Normaliza (esquema, tabla) para comparar. El core devuelve mayusculas y
    una Variable escrita a mano casi nunca."""
    return (str(esquema or "").strip().upper(), str(tabla or "").strip().upper())


# ============================================================================
# COMPUERTA CERO: responden el origen y el destino?
# ============================================================================
def verificar_origen_destino() -> dict:
    """Comprueba Bantotal y SQL Server ANTES de mover un solo byte.

    Esto no es ceremonia. Sin esta tarea, un core caido o una credencial
    rotada se descubre dentro del job de Spark: el error llega envuelto en una
    traza de la JVM, despues de haber levantado un cluster, y lo que se lee en
    la interfaz es un spark-submit con codigo de salida 1. Aqui falla en
    segundos, en Python, diciendo que conexion es y que paso.

    Falla en rojo a proposito, no se salta: que el core no responda no es "hoy
    no habia nada que hacer".
    """
    import jaydebeapi  # noqa: F401

    from etl_bt2sql.bt2sql_comun import conexion_bantotal, conexion_sqlserver

    doc = _config_publicada()
    ext, car = doc["extraccion"], doc["carga"]
    resultado = {}

    # --- ORIGEN: Bantotal -----------------------------------------------
    # Se usa sql_fecha, que es la consulta mas barata que ya existe y que el
    # job necesita de todas formas. Si responde, el core esta arriba, la
    # credencial sirve y la biblioteca del Extra resuelve.
    try:
        conn = conexion_bantotal({"conn_id_origen": CONN_ORIGEN})
    except Exception as exc:                                     # noqa: BLE001
        raise AirflowException(
            f"No se pudo conectar al core con la Connection {CONN_ORIGEN!r}: {exc}\n"
            f"Revise en Admin > Connections: host, puerto, usuario, clave, y que "
            f"el Extra declare el motor y la biblioteca, "
            f'por ejemplo {{"motor": "as400", "libraries": "GPPPBTDB"}}.') from exc
    try:
        cur = conn.cursor()
        cur.execute(ext["sql_fecha"])
        fila = cur.fetchone()
        if not fila:
            raise AirflowException(
                f"sql_fecha no devolvio ninguna fila: {ext['sql_fecha']}\n"
                f"Sin fecha de proceso del core no se puede nombrar la carpeta "
                f"del dia ni registrar la bitacora.")
        resultado["fecha_core"] = str(fila[0]).strip().split(".")[0]
        logger.info("ORIGEN   %-22s OK   fecha de proceso del core: %s",
                    CONN_ORIGEN, resultado["fecha_core"])
    finally:
        conn.close()

    # --- DESTINO: SQL Server y sus tablas de control --------------------
    try:
        conn = conexion_sqlserver({"conn_id_destino": CONN_DESTINO})
    except Exception as exc:                                     # noqa: BLE001
        raise AirflowException(
            f"No se pudo conectar al destino con la Connection {CONN_DESTINO!r}: {exc}\n"
            f'Si es de tipo Generic, el Extra debe declarar {{"motor": "mssql"}}.'
        ) from exc

    # Las tres tablas sin las que el pipeline no puede ni empezar. Se
    # comprueban aqui y no al vuelo porque un "Invalid object name" a mitad de
    # la carga deja la bitacora a medias.
    requeridas = {
        "catalogo":           "CTL_PARAMETROS_PARQUET",
        "bitacora extraccion": ext["tb_proceso_parquet"],
        "bitacora carga":      car["tabla_control"],
    }
    faltan = []
    try:
        cur = conn.cursor()
        for papel, tabla in requeridas.items():
            try:
                cur.execute(f"SELECT COUNT(*) FROM {tabla}")
                logger.info("DESTINO  %-22s OK   %s = %s fila(s)",
                            CONN_DESTINO, f"{tabla} ({papel})", cur.fetchone()[0])
            except Exception as exc:                             # noqa: BLE001
                faltan.append(f"{tabla} ({papel}): {str(exc).splitlines()[0]}")
    finally:
        conn.close()

    if faltan:
        raise AirflowException(
            "Faltan tablas de control en SQL Server:\n  " + "\n  ".join(faltan) +
            "\nCrearlas con los scripts de sql/bt2sql/ "
            "(20_bt2sql_control_sqlserver.sql y 21_bt2sql_catalogo_ejemplo.sql).")

    # --- LA RESOLUCION DEL MOTOR, QUE ES LO QUE USA SPARK ----------------
    # Las dos comprobaciones de arriba NO pasan por aqui: conexion_bantotal y
    # conexion_sqlserver arman su URL a mano, cada una para su motor. El
    # operador de Spark, en cambio, deduce motor, driver y URL desde la
    # Connection. Asi que una Connection Generic sin el motor declarado pasaria
    # las dos pruebas de arriba y moriria dentro del spark-submit.
    #
    # Ejercitarla aqui es la diferencia entre un mensaje que dice que poner en
    # el campo Extra y un codigo de salida 1 de spark-submit.
    from airflow.hooks.base import BaseHook

    from utils.spark_config import driver_for_conn, jdbc_url, motor_de_conexion

    for papel, conn_id in (("origen", CONN_ORIGEN), ("destino", CONN_DESTINO)):
        conexion = BaseHook.get_connection(conn_id)
        try:
            motor = motor_de_conexion(conexion)
            driver = driver_for_conn(conexion)
            url = jdbc_url(conexion)
        except Exception as exc:                                 # noqa: BLE001
            raise AirflowException(
                f"La Connection {conn_id!r} ({papel}) no resuelve a un motor "
                f"JDBC utilizable por Spark:\n  {exc}") from exc

        # El jar tiene que existir en el worker: spark-submit lo pasa con
        # --jars y si no esta, el executor falla con un ClassNotFoundException
        # que no nombra el archivo.
        if not os.path.isfile(driver.jar):
            raise AirflowException(
                f"La Connection {conn_id!r} resuelve al motor {motor!r}, que "
                f"necesita {driver.jar}, y ese jar no existe en este worker.\n"
                f"Reconstruya la imagen de Airflow Y la de Spark: el jar tiene "
                f"que estar en las dos o el job falla en el executor.")

        # La URL lleva credenciales en algunos motores: se recorta.
        logger.info("SPARK    %-22s motor=%-9s driver=%-24s jar=OK",
                    conn_id, motor, driver.driver_class.rsplit(".", 1)[-1])
        logger.info("         url=%s", url.split(";")[0])
        resultado[f"motor_{papel}"] = motor

    resultado["ok"] = True
    return resultado


# ============================================================================
# INVENTARIO: QUE TABLAS SE VAN A EXTRAER, Y QUE PASA CON LAS QUE NO
# ============================================================================
def inventario_tablas(tipo_ejecucion: str = "diario") -> dict:
    """Publica en el log los PROCESOS y las TABLAS de la configuracion.

    LA DOBLE LLAVE, QUE ES LO QUE AQUI SE HACE VISIBLE
    --------------------------------------------------
    Una tabla se extrae solo si pasa DOS filtros, y estan en sitios distintos
    de la misma configuracion:

        tablas[]              activo == 'S'
        extraccion.procesos[] mismo nombre_proceso + nombre_esquema,
                              estado == 1, y el flag del dia == 1
                              (estado_diario / semanal / mensual segun
                              tipo_ejecucion)

    Y prioridad, del proceso, decide el ORDEN de extraccion.

    Las dos listas se imprimen enteras, con todos sus campos, porque los dos
    huecos posibles son silenciosos: una tabla sin proceso no se extrae y no da
    error, y un proceso sin tabla no hace nada y tampoco avisa.
    """
    doc = _config_publicada()
    tablas = doc.get("tablas") or []
    procesos = doc["extraccion"].get("procesos") or []

    flag = {"diario": "estado_diario",
            "semanal": "estado_semanal",
            "mensual": "estado_mensual"}.get(str(tipo_ejecucion).strip().lower())
    if flag is None:
        raise AirflowException(
            f"tipo_ejecucion={tipo_ejecucion!r} no soportado. "
            f"Use diario, semanal o mensual.")

    if not tablas:
        raise AirflowException(
            "La configuracion publicada no declara ninguna tabla en 'tablas'. "
            "Agreguelas en airflow/config/json/BT2SQL_SPARK.json, sincronice "
            "con scripts/sync_variables.py y relance el DAG.")
    if not procesos:
        raise AirflowException(
            "La configuracion publicada no declara 'extraccion.procesos', asi "
            "que no hay calendario que aplicar y no se extraeria nada.")

    por_nombre = {}
    for p in procesos:
        clave = (str(p.get("nombre_proceso", "")).strip().upper(),
                 str(p.get("nombre_esquema", "")).strip().upper())
        por_nombre[clave] = p

    # ---------------- 1. LOS PROCESOS, TAL COMO ESTAN DECLARADOS -----------
    tablas_por_proceso = {
        (str(e.get("nombre_proceso") or e.get("tabla", "")).strip().upper(),
         str(e.get("esquema", "")).strip().upper()): e
        for e in tablas
    }

    logger.info("")
    logger.info("PROCESOS DECLARADOS (extraccion.procesos): %s", len(procesos))
    cab = (f"  {'NOMBRE_PROCESO':<16} {'ESQUEMA':<10} {'ESTADO':>6} {'PRIOR':>5} "
           f"{'DIARIO':>6} {'SEMANAL':>7} {'MENSUAL':>7}  ENTRA HOY / OBSERVACION")
    logger.info(cab)
    logger.info("  " + "-" * (len(cab) - 2))

    activos_hoy, sin_tabla = [], []
    for p in sorted(procesos, key=lambda x: (int(x.get("prioridad", 99)),
                                             str(x.get("nombre_proceso", "")))):
        nombre = str(p.get("nombre_proceso", "")).strip()
        esquema = str(p.get("nombre_esquema", "")).strip()
        clave = (nombre.upper(), esquema.upper())
        estado = int(p.get("estado", 0))
        d, s, m = (int(p.get("estado_diario", 0)), int(p.get("estado_semanal", 0)),
                   int(p.get("estado_mensual", 0)))

        if estado != 1:
            obs = "no  -- estado=0"
        elif int(p.get(flag, 0)) != 1:
            obs = f"no  -- {flag}=0"
        elif clave not in tablas_por_proceso:
            obs = "no  -- SIN entrada en 'tablas'"
            sin_tabla.append(nombre)
        else:
            obs = "SI"
            activos_hoy.append(nombre)

        logger.info("  %-16s %-10s %6s %5s %6s %7s %7s  %s",
                    nombre, esquema, estado, p.get("prioridad", "-"), d, s, m, obs)

    # ---------------- 2. LAS TABLAS, Y SU VEREDICTO ------------------------
    filas, se_extraen = [], []
    for e in tablas:
        nombre = str(e.get("nombre_proceso") or e.get("tabla", "")).strip()
        clave = (nombre.upper(), str(e.get("esquema", "")).strip().upper())
        activo = str(e.get("activo", "S")).strip().upper() == "S"
        proc = por_nombre.get(clave)

        if not activo:
            motivo = "activo='N' en tablas"
        elif proc is None:
            motivo = "SIN entrada en extraccion.procesos"
        elif int(proc.get("estado", 0)) != 1:
            motivo = "estado=0 en procesos"
        elif int(proc.get(flag, 0)) != 1:
            motivo = f"{flag}=0 en procesos"
        else:
            motivo = ""

        filas.append({
            "proceso": nombre,
            "prioridad": (proc or {}).get("prioridad", "-"),
            "origen": f"{e.get('esquema', '?')}.{e.get('tabla', '?')}",
            "destino": e.get("tabla_destino") or "(SIN DESTINO)",
            "estado": "ACTIVA" if not motivo else "inactiva",
            "motivo": motivo,
            "particion": (e.get("particion") or {}).get("columna") or "-",
            "tipos": "si" if (e.get("tipos") or "").strip() else "destino",
        })
        if not motivo:
            se_extraen.append(nombre)

    logger.info("")
    logger.info("TABLAS DECLARADAS: %s  |  se extraeran hoy: %s  |  no: %s  "
                "(tipo_ejecucion=%s)",
                len(filas), len(se_extraen), len(filas) - len(se_extraen), tipo_ejecucion)
    cab = (f"  {'PRIOR':>5} {'PROCESO':<16} {'ORIGEN':<20} {'DESTINO':<16} "
           f"{'ESTADO':<9} {'PARTICION':<10} {'TIPOS':<7} MOTIVO SI NO SE EXTRAE")
    logger.info(cab)
    logger.info("  " + "-" * (len(cab) - 2))
    for f in sorted(filas, key=lambda x: (x["estado"] != "ACTIVA",
                                          x["prioridad"] if isinstance(x["prioridad"], int) else 99,
                                          x["proceso"])):
        logger.info("  %5s %-16s %-20s %-16s %-9s %-10s %-7s %s",
                    f["prioridad"], f["proceso"], f["origen"], f["destino"],
                    f["estado"], f["particion"], f["tipos"], f["motivo"])
    logger.info("")

    # ---------------- 3. LO QUE CORTA LA CORRIDA ---------------------------
    if sin_tabla:
        logger.warning(
            "Proceso(s) en 'procesos' sin entrada en 'tablas': %s. No hacen "
            "nada: el calendario los habilita y no hay tabla que extraer. "
            "Declarelos en 'tablas' o quitelos de 'procesos'.", sin_tabla)

    sin_destino = [f["proceso"] for f in filas
                   if f["estado"] == "ACTIVA" and f["destino"] == "(SIN DESTINO)"]
    if sin_destino:
        raise AirflowException(
            f"Tabla(s) activas sin 'tabla_destino': {', '.join(sin_destino)}. "
            f"Se extraerian y la carga no sabria donde ponerlas.")

    if not se_extraen:
        raise AirflowException(
            f"Ninguna de las tablas declaradas pasa los dos filtros para "
            f"tipo_ejecucion={tipo_ejecucion!r}, asi que esta corrida no "
            f"extraeria nada. La columna MOTIVO de arriba dice por que se cae "
            f"cada una.")

    return {"procesos": len(procesos), "declaradas": len(filas),
            "se_extraen": se_extraen, "sin_tabla": sin_tabla, "detalle": filas}


# ============================================================================
# VALIDACION CONTRA EL CATALOGO DE SQL SERVER
# ============================================================================
def validar_catalogo() -> dict:
    """Compara la lista de la Variable con CTL_PARAMETROS_PARQUET.

    POR QUE SE VALIDA SI LA VARIABLE YA MANDA
    -----------------------------------------
    La Variable decide que se extrae; eso no cambia. Lo que esta tarea busca es
    la DERIVA entre las dos listas, porque las dos existen y las mantiene gente
    distinta: la Variable va en el repositorio y se revisa en un diff, el
    catalogo vive en SQL Server y lo edita quien opere la base. Mientras el
    pipeline de pandas siga leyendo el catalogo y este el Variable, una tabla
    puede entrar en uno y no en el otro, y los dos pipelines dejarian de ser
    comparables sin que nada falle.

    Las tres situaciones, y por que cada una se trata distinto:

      declarada y NO en el catalogo  -> ERROR. El catalogo es el registro de
          lo que esta configurado; extraer algo que no figura ahi es trabajar
          fuera de registro.
      en el catalogo activo y NO declarada -> AVISO. El pipeline de pandas la
          extrae y el de Spark no. No es un fallo de esta corrida, pero es
          exactamente la diferencia que invalidaria una comparacion.
      destino distinto entre los dos -> ERROR. Dos pipelines escribiendo la
          misma tabla de origen en destinos distintos es ambiguedad real.
    """
    import jaydebeapi  # noqa: F401

    from etl_bt2sql.bt2sql_comun import conexion_sqlserver

    doc = _config_publicada()
    modo = str(doc.get("validacion_catalogo", "estricto")).strip().lower()
    if modo == "ninguna":
        logger.warning("validacion_catalogo='ninguna': no se compara contra "
                       "CTL_PARAMETROS_PARQUET. La deriva entre las dos listas "
                       "deja de vigilarse.")
        return {"modo": modo}

    declaradas = {_clave(e.get("esquema"), e.get("tabla")): e
                  for e in (doc.get("tablas") or [])
                  if str(e.get("activo", "S")).strip().upper() == "S"}

    conn = conexion_sqlserver({"conn_id_destino": CONN_DESTINO})
    try:
        cur = conn.cursor()
        cur.execute(doc["extraccion"]["sql_parametros_parquet"])
        columnas = [d[0].upper() for d in cur.description]
        catalogo = {}
        for fila in cur.fetchall():
            r = dict(zip(columnas, fila))
            catalogo[_clave(r.get("ESQUEMA"), r.get("TABLA"))] = r
    finally:
        conn.close()

    solo_variable = sorted(declaradas.keys() - catalogo.keys())
    en_ambas = sorted(declaradas.keys() & catalogo.keys())

    destinos_distintos = []
    for k in en_ambas:
        dv = str(declaradas[k].get("tabla_destino") or "").strip().upper()
        dc = str(catalogo[k].get("TABLA_DESTINO") or "").strip().upper()
        if dv and dc and dv != dc:
            destinos_distintos.append(f"{k[0]}.{k[1]}: Variable={dv} catalogo={dc}")

    # SOLO SE REPORTA LO QUE DECLARA LA VARIABLE.
    # El catalogo puede tener cien tablas de otros pipelines; listarlas aqui
    # convertiria el log del DAG en un volcado de CTL_PARAMETROS_PARQUET y
    # taparia lo unico que importa, que es si lo declarado esta registrado.
    # Por eso la comparacion es en UNA direccion: de la Variable al catalogo,
    # nunca al reves. Del catalogo solo sale un conteo, sin nombres.
    logger.info("Tablas declaradas en la Variable: %s  |  registradas en el "
                "catalogo: %s  |  el catalogo tiene %s fila(s) activas en total",
                len(declaradas), len(en_ambas), len(catalogo))
    for k in sorted(declaradas):
        marca = "registrada" if k in catalogo else "NO REGISTRADA"
        logger.info("  %-14s %s.%s", marca, k[0], k[1])

    problemas = []
    if solo_variable:
        problemas.append(
            "Declaradas en la Variable y NO registradas en "
            "CTL_PARAMETROS_PARQUET: " +
            ", ".join(f"{a}.{b}" for a, b in solo_variable) +
            ". Registrelas (sql/bt2sql/21_bt2sql_catalogo_ejemplo.sql) o "
            "pongalas con activo='N'.")
    if destinos_distintos:
        problemas.append("Destino distinto entre la Variable y el catalogo:\n    " +
                         "\n    ".join(destinos_distintos))

    if problemas:
        mensaje = "La Variable y el catalogo de SQL Server no coinciden:\n  " + \
                  "\n  ".join(problemas)
        if modo == "aviso":
            logger.warning("%s\n(validacion_catalogo='aviso': se continua)", mensaje)
        else:
            raise AirflowException(
                mensaje + "\n\nPara dejar pasar esto temporalmente, ponga "
                "\"validacion_catalogo\": \"aviso\" en la Variable "
                f"{VARIABLE} y republique.")

    logger.info("Variable y catalogo coinciden en las %s tabla(s) activas.", len(en_ambas))
    return {"modo": modo, "declaradas": len(declaradas),
            "registradas": len(en_ambas), "sin_registrar": len(solo_variable)}


def verificar_parquet(batch_id: str | None = None) -> bool:
    """Compuerta: hay salida del lote EN DISCO?

    Comprueba CARPETAS y no archivos: Spark escribe un directorio con varios
    part-*.parquet dentro, no un archivo suelto. Esa es la unica diferencia
    real con la compuerta del pipeline de pandas, y es la que haria fallar un
    copiar y pegar de aquella.
    """
    import json

    from airflow.hooks.base import BaseHook

    doc = _config_publicada()
    config = doc["carga"]
    # Destino por (esquema, tabla): desde el 2026-10-07 la consulta de lotes ya
    # no lo trae -lo trae la Variable- asi que hay que cruzarlo aqui igual que
    # lo hace el job de carga.
    destinos = {_clave(e.get("esquema"), e.get("tabla")): e.get("tabla_destino")
                for e in (doc.get("tablas") or [])}
    batch = str(batch_id or "").strip()
    if not batch or batch.lower() in ("none", "null"):
        raise AirflowException(
            "verificar_parquet no recibio batch_id. Sin el no se sabe que "
            "lote mirar.")

    import jaydebeapi  # noqa: F401  -- se usa a traves del helper comun

    from etl_bt2sql.bt2sql_comun import conexion_sqlserver

    conn = conexion_sqlserver({"conn_id_destino": CONN_DESTINO})
    try:
        cur = conn.cursor()
        cur.execute(config["sql_lotes"].replace("{BATCH_ID}", "?"), (batch,))
        columnas = [d[0].lower() for d in cur.description]
        jobs = [dict(zip(columnas, f)) for f in cur.fetchall()]
    finally:
        conn.close()

    if not jobs:
        logger.info("No hay archivos registrados para el batch %s. La carga se "
                    "salta: no es un error, es que no habia nada que extraer.", batch)
        return False

    existen, faltan = [], []
    for fila in jobs:
        ruta = str(fila["archivo_parquet"])
        destino = destinos.get(_clave(fila.get("esquema"), fila.get("tabla_origen"))) \
                  or "(no declarada en la Variable)"
        # isdir, no isfile: la salida de Spark es una carpeta.
        (existen if os.path.isdir(ruta) else faltan).append((ruta, destino))

    for ruta, destino in existen:
        partes = len([f for f in os.listdir(ruta) if f.endswith(".parquet")])
        logger.info("  OK     %-34s -> %-28s  %s archivo(s)", ruta, destino, partes)
    for ruta, destino in faltan:
        logger.error("  FALTA  %-34s -> %-28s", ruta, destino)

    if not existen:
        raise AirflowException(
            f"La bitacora registra {len(jobs)} salida(s) del batch {batch} pero "
            f"NINGUNA esta en disco. Lo mas habitual es que la carpeta de parquet "
            f"no este montada en los contenedores de Spark, solo en los de Airflow.")
    if faltan:
        raise AirflowException(
            f"Faltan {len(faltan)} de {len(jobs)} salidas del batch {batch}. "
            f"Cargar solo una parte dejaria el dia incompleto sin que nada lo "
            f"advirtiera.")

    logger.info("Las %s salidas del batch estan en disco. Se procede a cargar.", len(existen))
    return True


def limpiar_parquet() -> dict:
    """Borra las carpetas de dias anteriores a la retencion.

    Solo toca carpetas cuyo nombre son ocho digitos y parsean como fecha: asi
    una carpeta puesta a mano no se borra por accidente. Con retencion_dias en
    0 no borra nada.
    """
    import json
    import shutil

    config = json.loads(Path(RUNTIME_JSON).read_text(encoding="utf-8"))["extraccion"]
    raiz = Path(config["output_dir"])
    dias = int(config.get("retencion_dias", 0))
    if dias <= 0 or not raiz.exists():
        logger.info("retencion_dias=%s: no se borra nada", dias)
        return {"borradas": 0}

    corte = datetime.now().date() - timedelta(days=dias)
    borradas = []
    for carpeta in sorted(raiz.iterdir()):
        if not carpeta.is_dir() or not (len(carpeta.name) == 8 and carpeta.name.isdigit()):
            continue
        try:
            fecha = datetime.strptime(carpeta.name, "%Y%m%d").date()
        except ValueError:
            continue
        if fecha < corte:
            shutil.rmtree(carpeta, ignore_errors=True)
            borradas.append(carpeta.name)

    logger.info("retencion %s dias: %s carpeta(s) borradas %s",
                dias, len(borradas), borradas or "")
    return {"borradas": len(borradas), "carpetas": borradas}


def alertar_fallo(context) -> None:
    ti = context.get("task_instance")
    logger.error("FALLO_%s tarea=%s intento=%s run=%s",
                 NOMBRE, getattr(ti, "task_id", "?"),
                 getattr(ti, "try_number", "?"), context.get("run_id"))


# ============================================================================
# EL DAG
# ============================================================================
default_args = {
    "owner": "datahub",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alertar_fallo,
}

with DAG(
    dag_id=NOMBRE,
    description="Gemelo Spark de STG_BT2SQL_CARGA: Bantotal a Parquet y carga a STG",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Con max_active_runs=1 una corrida atascada bloquea todas las siguientes, y
    # execution_timeout no cubre el tiempo en queued esperando un slot del
    # cluster ni el de up_for_retry.
    dagrun_timeout=timedelta(hours=10),
    # Sin esto, "{{ ti.xcom_pull(...) }}" entrega la CADENA 'None' cuando no hay
    # XCom, y un 'if batch_id:' la da por buena.
    render_template_as_native_obj=True,
    default_args=default_args,
    doc_md=__doc__,
    params={
        "tipo_ejecucion": Param(
            default="diario",
            type="string",
            enum=["diario", "reproceso"],
            description="diario usa la fecha del core; reproceso recarga el dia indicado.",
        ),
        "publicar_config": Param(
            default="auto",
            type="string",
            enum=["auto", "no", "si"],
            description=(
                "auto (por defecto): publica solo si la Variable BT2SQL_SPARK "
                "cambio respecto del archivo del volumen; editar la Variable y "
                "lanzar el DAG basta. no: ignora la Variable y usa el archivo "
                "tal como esta, para relanzar una corrida vieja con su "
                "configuracion exacta. si: publica aunque no haya cambiado."
            ),
        ),
    },
    tags=["manual", "spark", "bt2sql", "bantotal", "sqlserver", "stg", "produccion"],
) as dag:

    tarea_preflight = PythonOperator(
        task_id="verificar_origen_destino",
        python_callable=verificar_origen_destino,
        execution_timeout=timedelta(minutes=10),
        doc_md=(
            "Compuerta cero. Comprueba que Bantotal responde a `sql_fecha` y "
            "que SQL Server tiene el catalogo y las dos bitacoras, antes de "
            "levantar un solo executor. Falla en rojo: un core caido no es "
            "'hoy no habia nada que hacer'."
        ),
    )

    tarea_config = PythonOperator(
        task_id="preparar_config",
        python_callable=preparar_config,
        op_kwargs={
            "tipo_ejecucion": "{{ params.tipo_ejecucion }}",
            "publicar_config": "{{ params.publicar_config }}",
        },
        execution_timeout=timedelta(minutes=5),
        doc_md=(
            "Genera el batch_id y pone al dia el JSON que Spark lee del "
            "volumen. Con `publicar_config=auto` (por defecto) compara hashes y "
            "publica solo si la Variable cambio. El hash de la configuracion "
            "usada queda en el log de **toda** corrida, publique o no."
        ),
    )

    tarea_inventario = PythonOperator(
        task_id="inventario_tablas",
        python_callable=inventario_tablas,
        op_kwargs={"tipo_ejecucion": "{{ params.tipo_ejecucion }}"},
        execution_timeout=timedelta(minutes=5),
        doc_md=(
            "Imprime las dos listas enteras: **procesos** (estado, prioridad, "
            "los tres flags del calendario) y **tablas** (origen, destino, "
            "particion, tipos). Los dos huecos posibles son silenciosos -una "
            "tabla sin proceso no se extrae, un proceso sin tabla no hace "
            "nada- y aqui los dos salen con nombre."
        ),
    )

    tarea_validar = PythonOperator(
        task_id="validar_catalogo",
        python_callable=validar_catalogo,
        execution_timeout=timedelta(minutes=10),
        doc_md=(
            "Compara la lista de la Variable con `CTL_PARAMETROS_PARQUET` de "
            "SQL Server. Una tabla declarada y no registrada es error; una "
            "registrada y no declarada es aviso; un destino distinto entre las "
            "dos es error. Se relaja con "
            "`\"validacion_catalogo\": \"aviso\"` en la Variable."
        ),
    )

    # ------------------------------------------------------------------
    # LA EXTRACCION: UN GRUPO POR PRIORIDAD, UNA TAREA POR PROCESO
    # ------------------------------------------------------------------
    # Misma forma que BDS_DATAHUB_PROCESOS, y por el mismo motivo: con una sola
    # tarea "extraer_parquet_spark" la interfaz no dice que tabla tardo, cual
    # fallo, ni cual se salto, y reintentar una obliga a reintentarlas todas.
    #
    # Los grupos de prioridad van ENCADENADOS, no en paralelo: prioridad 1
    # entera antes que prioridad 2, que es lo que significa el campo y lo que
    # hace el pipeline de pandas (ejecutar_lote("prioridad_1") y despues
    # "prioridad_2"). Dentro de un grupo las tablas si van en paralelo.
    #
    # EL COSTE, DICHO CLARO: cada tarea es un spark-submit, y arrancar la JVM
    # cuesta entre 15 y 30 segundos. Con una sola tarea ese arranque se pagaba
    # una vez y las tablas se recorrian en serie dentro del job. Ahora se paga
    # por tabla, pero en paralelo, asi que en la practica se gana tiempo en
    # cuanto hay mas de dos tablas; lo que hay que vigilar es no lanzar mas
    # drivers de los que el cluster aguanta, y para eso esta POOL_SPARK.
    extraer_tareas: list = []
    niveles: dict[int, list] = defaultdict(list)
    for entrada in PLAN:
        niveles[entrada["prioridad"]].append(entrada)

    with TaskGroup(group_id="extraccion",
                   tooltip="Bantotal -> parquet, un proceso por tarea") as grupo_extraccion:
        if not PLAN:
            # Mismo criterio que BDS_DATAHUB_PROCESOS con su tarea
            # 'configuracion_invalida': el DAG NO desaparece de la interfaz
            # cuando no se puede leer la configuracion. Aparece con una sola
            # tarea que falla diciendo que hacer. Un DAG ausente no se
            # diagnostica; uno en rojo con un mensaje, si.
            PythonOperator(
                task_id="configuracion_no_disponible",
                python_callable=_sin_configuracion,
                retries=0,
                execution_timeout=timedelta(minutes=1),
                doc_md=(
                    "No se pudo leer la lista de tablas al dibujar el grafo, ni "
                    "del JSON del volumen ni de la Variable. Vea el mensaje de "
                    "esta tarea."
                ),
            )
        grupo_anterior = None
        for prioridad in sorted(niveles):
            etiqueta = (f"prioridad_{prioridad}" if prioridad != 99
                        else "sin_prioridad_declarada")
            with TaskGroup(
                group_id=etiqueta,
                tooltip=(f"{len(niveles[prioridad])} proceso(s) de prioridad "
                         f"{prioridad}, en paralelo entre si"),
            ) as grupo_prioridad:
                for entrada in niveles[prioridad]:
                    tarea = _SparkDosConexiones.crear(
                        task_id=_slug(entrada["proceso"]),
                        proceso_de_la_tarea=entrada["proceso"],
                        jdbc_conn_id=CONN_ORIGEN,
                        application=f"{APPS}/bt2sql_extraccion_spark.py",
                        application_args=[
                            "--config", RUNTIME_JSON,
                            "--batch-id", "{{ ti.xcom_pull(task_ids='preparar_config') }}",
                            "--tipo-ejecucion", "{{ params.tipo_ejecucion }}",
                            "--proceso", entrada["proceso"],
                        ],
                        conf=SPARK_CONF,
                        executor_memory=RECURSOS["EXECUTOR_MEMORY"],
                        executor_cores=int(RECURSOS["EXECUTOR_CORES"]),
                        num_executors=int(RECURSOS["NUM_EXECUTORS"]),
                        cores_max=int(RECURSOS["CORES_MAX"]),
                        driver_memory=RECURSOS["DRIVER_MEMORY"],
                        pool=POOL_SPARK,
                        execution_timeout=timedelta(hours=4),
                        verbose=False,
                        doc_md=(
                            f"**{entrada['proceso']}**\n\n"
                            f"- origen: `{entrada['esquema']}.{entrada['tabla']}`\n"
                            f"- destino: `{entrada['destino'] or '(sin declarar)'}`\n"
                            f"- prioridad: {entrada['prioridad']}\n\n"
                            f"Se salta, en rosa, cuando el calendario del dia no "
                            f"lo incluye. El motivo exacto queda en su log y en "
                            f"la tabla de `inventario_tablas`."
                        ),
                    )
                    extraer_tareas.append(tarea)
            if grupo_anterior is not None:
                grupo_anterior >> grupo_prioridad
            grupo_anterior = grupo_prioridad

    # Punto de union, como ods_completo en BDS_DATAHUB_PROCESOS. Sin el, la
    # carga colgaria de cada tarea por separado y el grafo seria una maraña.
    #
    # all_done y no all_success: lo normal es que varias tablas se salten por
    # calendario, y con la regla por defecto una sola saltada dejaria la carga
    # sin ejecutar. Que una tabla haya FALLADO no se pierde: verificar_parquet
    # mira lo que de verdad quedo en disco para este batch.
    tarea_extraccion_completa = EmptyOperator(
        task_id="extraccion_completa",
        trigger_rule="all_done",
        doc_md=(
            "Punto de union de la extraccion. `all_done` porque saltarse "
            "tablas por calendario es el funcionamiento normal; lo que decide "
            "si hay algo que cargar es `verificar_parquet`, que mira el disco."
        ),
    )
    if extraer_tareas:
        grupo_extraccion >> tarea_extraccion_completa

    tarea_verificar = ShortCircuitOperator(
        task_id="verificar_parquet",
        python_callable=verificar_parquet,
        op_kwargs={"batch_id": "{{ ti.xcom_pull(task_ids='preparar_config') }}"},
        # Sin esto el corto-circuito salta tambien la limpieza, que lleva
        # trigger_rule="all_done" justamente para correr siempre.
        ignore_downstream_trigger_rules=False,
        pool=POOL_SQLSERVER,
        execution_timeout=timedelta(minutes=15),
        doc_md="Compuerta: si no hay salida en disco, la carga queda en SKIPPED, no en failed.",
    )

    tarea_cargar = _operador_spark(
        task_id="cargar_stg_spark",
        jdbc_conn_id=CONN_DESTINO,
        application=f"{APPS}/bt2sql_carga_spark.py",
        application_args=[
            "--config", RUNTIME_JSON,
            "--batch-id", "{{ ti.xcom_pull(task_ids='preparar_config') }}",
        ],
        conf=SPARK_CONF,
        executor_memory=RECURSOS["EXECUTOR_MEMORY"],
        executor_cores=int(RECURSOS["EXECUTOR_CORES"]),
        num_executors=int(RECURSOS["NUM_EXECUTORS"]),
        cores_max=int(RECURSOS["CORES_MAX"]),
        driver_memory=RECURSOS["DRIVER_MEMORY"],
        pool=POOL_SQLSERVER,
        execution_timeout=timedelta(hours=4),
        verbose=False,
        doc_md=(
            "Staging con los tipos de la destino, escritura en paralelo, y "
            "TRUNCATE+INSERT en una transaccion desde el driver."
        ),
    )

    tarea_limpiar = PythonOperator(
        task_id="limpiar_parquet",
        python_callable=limpiar_parquet,
        # all_done: la limpieza corre aunque la carga falle. Si no, los parquet
        # de un dia fallido se quedan para siempre y el disco crece sin tope.
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=30),
        doc_md="Borra las carpetas anteriores a retencion_dias. Corre siempre.",
    )

    (
        tarea_preflight
        >> tarea_config
        >> tarea_inventario
        >> tarea_validar
        >> grupo_extraccion
        >> tarea_extraccion_completa
        >> tarea_verificar
        >> tarea_cargar
        >> tarea_limpiar
    )
