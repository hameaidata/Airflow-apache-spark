"""
Hace cumplir las convenciones de docs/CONVENCIONES.md.

COMO FUNCIONA LA LISTA DE EXCEPCIONES

Los DAGs que ya existian cuando se escribio la convencion estan en LEGADO, con
la fecha en que se agregaron. Esa lista esta para ENCOGERSE: cada vez que se
migra un DAG, se quita de ahi. Lo que no se admite es que crezca, y hay un test
que lo comprueba.

La alternativa -escribir tests que fallen desde el primer dia- termina con todo
el mundo ignorando la suite, que es peor que no tenerla.

Ejecucion:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/unit/test_convenciones.py -v
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest


RAIZ = Path(__file__).resolve().parents[3]
DIR_RAIZ_DAGS = RAIZ / "airflow" / "dags"
DIR_DAGS = DIR_RAIZ_DAGS / "production"

# ============================================================================
# LISTA DE EXCEPCIONES
# ----------------------------------------------------------------------------
# Agregados el 2026-09-30, al escribir la convencion. Quitar segun se migren.
#
# Salieron cinco el mismo dia, y no por haberse renombrado: Extraer_datos_bt,
# carga_parquet_singlestore, VALIDAR_CONEXIONES, ODS_PROCESOS_DATAHUB y
# BDS_PROCESOS_DATAHUB venian de examples/ y templates/, que ahora estan en
# .airflowignore. El scheduler ya no los registra, asi que no son DAGs de
# nadie: eran cuatro entradas de mas en la interfaz.
# ============================================================================
LEGADO_NOMBRE = {
    # Del pipeline parquet -> SingleStore
    "etl_bt_parquet_singlestore",       # -> STG_BT_PARQUET
    "etl_bt_parquet_singlestore_spark", # -> STG_BT_PARQUET_SPARK
    # Utilidades
    "orquestador_json",                 # -> UTIL_ORQUESTADOR_JSON
    "auditoria_roles",                  # -> UTIL_AUDITORIA_ROLES
    "bt_odbc_test",                     # -> LAB_BT_ODBC
    # Declaran su dag_id como constante de modulo
    "BT_DATAHUB",                       # -> ODS_DATAHUB_PROCESOS / BDS_...
    "S2SQL_EXPORT",                     # -> EXP_S2SQL_CARGA
}

# Archivos que todavia no declaran su cadencia con etiquetas.
LEGADO_CADENCIA: set[str] = set()

# Scripts sueltos que hay que sacar de dags/production/. Ver el test que los
# vigila al final de este archivo: no son una excepcion tolerada, son deuda
# con fecha.
SCRIPTS_A_MOVER = {
    "test_bt_dev.py",
    "test_bt_preproduccion.py",
    "test_sqlserver.py",
}

CAPAS = ("STG", "ODS", "BDS", "CU", "EXP", "UTIL", "LAB")
CADENCIAS = {"continuo", "manual", "disparado"}

PESADOS = ("jaydebeapi", "jpype", "pyodbc", "pyarrow", "singlestoredb")


def patrones_ignorados() -> list[re.Pattern]:
    """Los patrones de .airflowignore, ya compilados.

    Son expresiones regulares -no comodines de shell- y se evaluan contra la
    ruta relativa a airflow/dags/. Asi las interpreta Airflow, y asi hay que
    interpretarlas aqui: si el test mira archivos que el scheduler no parsea,
    reporta problemas que no existen; si mira menos, calla los que si.
    """
    archivo = DIR_RAIZ_DAGS / ".airflowignore"
    if not archivo.exists():
        return []
    patrones = []
    for linea in archivo.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#"):
            patrones.append(re.compile(linea))
    return patrones


def archivos_de_dag() -> list[Path]:
    """Exactamente lo que el DagBag parsea: todo airflow/dags/ de forma
    recursiva, menos lo que .airflowignore excluye, y de eso solo lo que
    define al menos un DAG.

    ANTES ESTE ALCANCE ESTABA MAL: recorria unicamente production/*.py. Con
    eso, examples/ y templates/ -que el scheduler SI importaba- quedaban fuera
    del test, y sus cuatro DAGs registrados no los veia nadie. El test pasaba
    por no mirar.
    """
    ignorados = patrones_ignorados()
    salida = []
    for p in sorted(DIR_RAIZ_DAGS.rglob("*.py")):
        relativa = p.relative_to(DIR_RAIZ_DAGS).as_posix()
        if any(patron.search(relativa) for patron in ignorados):
            continue
        if "DAG(" in p.read_text(encoding="utf-8", errors="replace"):
            salida.append(p)
    return salida


def dag_ids_de(texto: str) -> list[str]:
    """Los dag_id del archivo, resolviendo la forma dag_id=CONSTANTE.

    dag_bt_datahub.py y dag_s2sql_export.py no escriben el nombre dentro de
    DAG(): lo declaran arriba como DAG_ID = "..." y lo pasan por variable.
    Buscando solo dag_id="..." esos dos DAGs eran invisibles para todos los
    tests de nombre -que es justo donde un nombre mal puesto se esconde-.
    """
    ids = re.findall(r'dag_id\s*=\s*"([^"]+)"', texto)
    constantes = dict(re.findall(r'^([A-Z_][A-Z0-9_]*)\s*=\s*"([^"]+)"', texto, re.M))
    for variable in re.findall(r'dag_id\s*=\s*([A-Z_][A-Z0-9_]*)\s*,', texto):
        if variable in constantes:
            ids.append(constantes[variable])
    return ids


def bloque_dag(texto: str) -> str:
    """Desde 'with DAG(' hasta el 'as dag:'. Basta para leer sus argumentos."""
    m = re.search(r"with DAG\((.*?)\)\s*as\s+\w+\s*:", texto, re.S)
    return m.group(1) if m else ""


# ============================================================================
# NOMBRES
# ============================================================================
def test_los_dag_id_nuevos_siguen_la_convencion():
    """<CAPA>_<PIPELINE>_<ACCION>, todo en mayusculas.

    La capa va primero porque es lo que agrupa en la lista ordenada de la
    interfaz: los STG_ juntos, los ODS_ juntos, y las utilidades al final sin
    mezclarse con produccion.
    """
    patron = re.compile(rf"^({'|'.join(CAPAS)})_[A-Z0-9]+(_[A-Z0-9]+)*$")
    malos = []
    for archivo in archivos_de_dag():
        for dag_id in dag_ids_de(archivo.read_text(encoding="utf-8")):
            if dag_id in LEGADO_NOMBRE:
                continue
            if not patron.match(dag_id):
                malos.append(f"{dag_id}  ({archivo.name})")
    assert not malos, (
        "estos dag_id no siguen <CAPA>_<PIPELINE>_<ACCION>:\n  " + "\n  ".join(malos)
        + f"\nCapas validas: {', '.join(CAPAS)}"
    )


def test_la_lista_de_legado_no_crece():
    """Si este test falla es porque alguien agrego un dag_id a LEGADO_NOMBRE en
    vez de arreglarlo. La lista esta para encoger."""
    actuales = set()
    for archivo in archivos_de_dag():
        actuales |= set(dag_ids_de(archivo.read_text(encoding="utf-8")))

    sobran = LEGADO_NOMBRE - actuales
    assert not sobran, (
        f"estos dag_id estan en LEGADO_NOMBRE pero ya no existen: {sorted(sobran)}.\n"
        f"Quitelos de la lista: ya estan migrados."
    )
    assert len(LEGADO_NOMBRE) <= 10, (
        f"LEGADO_NOMBRE tiene {len(LEGADO_NOMBRE)} entradas y el tope es 10. "
        f"La lista esta para encoger, no para crecer."
    )


def test_el_archivo_se_llama_como_el_dag():
    """dag_<dag_id en minusculas>.py.

    Cuando la interfaz muestra un DAG roto, uno quiere saber que archivo abrir
    sin buscar. Hoy Extraer_datos_bt vive en orquestador_json.py.
    """
    malos = []
    for archivo in archivos_de_dag():
        ids = dag_ids_de(archivo.read_text(encoding="utf-8"))
        if len(ids) != 1:
            continue
        if ids[0] in LEGADO_NOMBRE:
            continue
        esperado = f"dag_{ids[0].lower()}.py"
        if archivo.name != esperado:
            malos.append(f"{archivo.name} define {ids[0]}, deberia llamarse {esperado}")
    assert not malos, "\n  " + "\n  ".join(malos)


def test_un_dag_por_archivo():
    """Dos DAGs en un archivo significa que un error de sintaxis los rompe los
    dos, y que la interfaz no dice cual archivo abrir."""
    malos = [f"{a.name}: {ids}" for a in archivos_de_dag()
             if len(ids := dag_ids_de(a.read_text(encoding="utf-8"))) > 1]
    assert not malos, "archivos con mas de un DAG:\n  " + "\n  ".join(malos)


# ============================================================================
# CADENCIA: LO QUE DISTINGUE UN PROCESO CONTINUO
# ============================================================================
def test_todo_dag_declara_su_cadencia():
    """Hoy 7 de 8 tienen schedule=None y nada dice si es deliberado.

    Con la etiqueta, un schedule=None deja de ser ambiguo: o lleva 'manual' o
    lleva 'disparado'. Si no lleva ninguna, es un olvido.
    """
    malos = []
    for archivo in archivos_de_dag():
        if archivo.name in LEGADO_CADENCIA or archivo.name in SCRIPTS_A_MOVER:
            continue
        texto = archivo.read_text(encoding="utf-8")
        for m in re.finditer(r'tags\s*=\s*\[([^\]]*)\]', texto):
            etiquetas = set(re.findall(r'"([^"]+)"', m.group(1)))
            if not (etiquetas & CADENCIAS):
                malos.append(f"{archivo.name}: tags={sorted(etiquetas)}")
    assert not malos, (
        "estos DAGs no declaran cadencia:\n  " + "\n  ".join(malos)
        + f"\nAgregue una de: {sorted(CADENCIAS)}"
    )


def test_un_dag_continuo_tiene_horario_de_verdad():
    """Etiquetarlo 'continuo' y dejarlo con schedule=None es peor que no
    etiquetarlo: promete algo que no ocurre."""
    malos = []
    for archivo in archivos_de_dag():
        texto = archivo.read_text(encoding="utf-8")
        bloque = bloque_dag(texto)
        if '"continuo"' in bloque and re.search(r"schedule\s*=\s*None", bloque):
            malos.append(archivo.name)
    assert not malos, (
        "etiquetados 'continuo' pero con schedule=None:\n  " + "\n  ".join(malos)
    )


# ============================================================================
# IMPLEMENTACION
# ============================================================================
def test_toda_tarea_tiene_execution_timeout():
    """Sin el, una consulta trabada deja la tarea en running para siempre y,
    con max_active_runs=1, el DAG entero parado."""
    malos = []
    for archivo in archivos_de_dag():
        if archivo.name in SCRIPTS_A_MOVER:
            continue
        texto = archivo.read_text(encoding="utf-8")

        # Un execution_timeout en default_args lo hereda CADA tarea del DAG.
        # Contarlo como una sola cobertura era el error que hacia fallar a
        # auditoria_roles.py y orquestador_json.py, que si estaban protegidos.
        bloque = re.search(r"default_args\s*=\s*\{(.*?)\n\}", texto, re.S)
        if bloque and "execution_timeout" in bloque.group(1):
            continue

        tareas = len(re.findall(r"task_id\s*=", texto))
        # EmptyOperator no ejecuta nada: existe para dibujar dependencias y
        # termina en el mismo instante en que arranca. Exigirle un plazo es
        # ruido, y ruido es como una suite deja de leerse.
        vacias = len(re.findall(r"EmptyOperator\(\s*task_id\s*=", texto))
        tareas -= vacias

        plazos = len(re.findall(r"execution_timeout\s*=", texto))
        if tareas > 0 and plazos < tareas:
            malos.append(
                f"{archivo.name}: {tareas} tareas con trabajo real "
                f"({vacias} EmptyOperator aparte), {plazos} con execution_timeout"
            )
    assert not malos, "\n  " + "\n  ".join(malos)


def test_todo_dag_tiene_dagrun_timeout():
    """Con max_active_runs=1, una corrida atascada bloquea TODAS las
    siguientes. execution_timeout protege cada tarea pero no el tiempo en
    queued ni en up_for_retry."""
    malos = [a.name for a in archivos_de_dag()
             if a.name not in SCRIPTS_A_MOVER
             and "dagrun_timeout" not in a.read_text(encoding="utf-8")]
    assert not malos, "sin dagrun_timeout:\n  " + "\n  ".join(malos)


def test_ningun_dag_importa_nada_pesado_al_parsearse():
    """El scheduler importa cada archivo cada 30 segundos. jaydebeapi arrastra
    JPype, que levanta una JVM en cada pasada.

    Y el efecto lateral importa mas: con el import DENTRO de la funcion, un
    driver que falte en un worker hace fallar esa tarea con su traza. Con el
    import arriba, tumba el archivo entero con un Broken DAG y desaparecen
    todos sus DAGs de la interfaz.
    """
    malos = []
    for archivo in archivos_de_dag():
        if archivo.name in SCRIPTS_A_MOVER:
            continue
        try:
            arbol = ast.parse(archivo.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        for nodo in arbol.body:            # solo el nivel de modulo
            if isinstance(nodo, (ast.Import, ast.ImportFrom)):
                nombre = (nodo.module or "") if isinstance(nodo, ast.ImportFrom) \
                         else ",".join(a.name for a in nodo.names)
                for pesado in PESADOS:
                    if pesado in nombre:
                        malos.append(f"{archivo.name}:{nodo.lineno} importa {pesado}")
    assert not malos, (
        "imports pesados a nivel de modulo:\n  " + "\n  ".join(malos)
        + "\nMuevalos DENTRO de la funcion que los usa."
    )


@pytest.mark.parametrize("obligatorio", ["catchup", "tags", "description"])
def test_argumentos_obligatorios_del_dag(obligatorio):
    malos = [a.name for a in archivos_de_dag()
             if a.name not in SCRIPTS_A_MOVER
             and obligatorio not in bloque_dag(a.read_text(encoding="utf-8"))]
    assert not malos, f"sin {obligatorio}=:\n  " + "\n  ".join(malos)


def test_start_date_es_fijo():
    """Con datetime.now() el DAG cambia de fecha de inicio en cada parseo y el
    scheduler se comporta de forma impredecible."""
    malos = []
    for archivo in archivos_de_dag():
        bloque = bloque_dag(archivo.read_text(encoding="utf-8"))
        if re.search(r"start_date\s*=\s*(datetime\.now|days_ago|pendulum\.now)", bloque):
            malos.append(archivo.name)
    assert not malos, "start_date dinamico:\n  " + "\n  ".join(malos)


# ============================================================================
# CREDENCIALES
# ============================================================================
def test_ningun_dag_tiene_credenciales_en_texto_plano():
    """Un archivo versionado con la clave del core es una fuga permanente:
    queda en el historial de git aunque se borre despues."""
    patron = re.compile(r'^\s*(PASSWORD|PWD|CLAVE|PASS)\s*=\s*["\'][^"\']{3,}["\']',
                        re.M | re.I)
    malos = []
    for archivo in sorted(DIR_DAGS.rglob("*.py")):
        if patron.search(archivo.read_text(encoding="utf-8", errors="replace")):
            malos.append(archivo.name)

    pendientes = sorted(set(malos) - SCRIPTS_A_MOVER)
    assert not pendientes, (
        "credenciales en texto plano:\n  " + "\n  ".join(pendientes)
        + "\nUse una Connection de Airflow."
    )


def test_los_scripts_con_credenciales_estan_ignorados_por_el_scheduler():
    """Mientras sigan en dags/production/, el scheduler los IMPORTA cada 30
    segundos: abre sesiones de Spark y conexiones al core, con las credenciales
    que llevan dentro.

    Lo correcto es sacarlos de ahi. Mientras tanto, como minimo, que el
    scheduler no los ejecute.
    """
    presentes = {p.name for p in DIR_DAGS.glob("*.py")} & SCRIPTS_A_MOVER
    if not presentes:
        return  # ya se movieron: perfecto

    # Los patrones de .airflowignore son expresiones regulares. Buscar el
    # nombre del archivo como subcadena daba un falso positivo con
    # "production/test_.*\.py", que si los cubre a los tres.
    ignorados = patrones_ignorados()
    sin_ignorar = [n for n in sorted(presentes)
                   if not any(patron.search(f"production/{n}") for patron in ignorados)]
    assert not sin_ignorar, (
        f"estos scripts tienen credenciales, siguen en dags/production/ y el "
        f"scheduler los ejecuta cada 30 segundos: {sin_ignorar}\n"
        f"Muevalos fuera de dags/, o agreguelos a .airflowignore mientras tanto. "
        f"Y rote esas contrasenas."
    )


# ============================================================================
# CONNECTIONS
# ============================================================================
def test_las_conexiones_nuevas_declaran_quien_las_abre():
    """AF_ si la abre el worker de Airflow, SPK_ si la consume un job de Spark.

    No es burocracia: las credenciales que viajan a Spark pueden acabar en la
    pestana Environment de su interfaz y en los event logs, que con
    spark.eventLog.enabled se escriben en disco y sobreviven al job.
    """
    conocidas_legado = {
        "CONEXION_BANTOTAL", "CONEXION_SQLSERVER", "CONEXION_SINGLESTORE",
        "BT_CONEXION_PREPRODUCTION", "BT_PREPRODUCTION",
        "conexion_bitacora", "mi_base", "spark_default",
    }
    patron = re.compile(r'conn_id[^=]*=\s*"([^"]+)"')
    malas = set()
    for archivo in sorted((RAIZ / "airflow" / "dags").rglob("*.py")):
        for conn in patron.findall(archivo.read_text(encoding="utf-8", errors="replace")):
            if conn in conocidas_legado or conn.startswith(("AF_", "SPK_")):
                continue
            if "{" in conn or conn.isupper() is False and "_" not in conn:
                continue
            malas.add(conn)
    assert not malas, (
        f"estas Connections no dicen quien las abre: {sorted(malas)}\n"
        f"Use AF_<MOTOR>_<AMBIENTE> o SPK_<MOTOR>_<AMBIENTE>."
    )


def test_spark_no_recibe_credenciales_por_configuracion():
    """Meterlas en el diccionario properties de spark.read.jdbc o en un
    --conf las expone en tres sitios que nadie audita: la pestana Environment
    de la interfaz de Spark, la salida de ps dentro de los contenedores, y los
    event logs, que quedan EN DISCO para que los lea el History Server.

    La forma correcta -la que ya usa BsgSparkJdbcOperator- es pasarlas por
    variables de entorno.
    """
    malos = []
    for archivo in sorted((RAIZ / "airflow").rglob("*.py")):
        if archivo.name in SCRIPTS_A_MOVER:
            continue
        # Los propios tests nombran el patron que persiguen. Sin esta linea,
        # este test se denunciaba a si mismo por la cadena que busca.
        if "tests" in archivo.parts:
            continue
        texto = archivo.read_text(encoding="utf-8", errors="replace")
        if "read.jdbc" not in texto and "write.jdbc" not in texto:
            continue
        if re.search(r'"password"\s*:', texto):
            malos.append(f"{archivo.name}: pasa la clave en el dict de properties")
        if re.search(r'--conf[^\n]*password', texto, re.I):
            malos.append(f"{archivo.name}: pasa la clave por --conf")
    assert not malos, "\n  " + "\n  ".join(malos)
