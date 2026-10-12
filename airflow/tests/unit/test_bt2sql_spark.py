"""
Tests del pipeline BT2SQL_SPARK (Bantotal -> Parquet -> STG en SQL Server, con Spark).

QUE SE PRUEBA AQUI Y QUE NO
---------------------------
No hace falta ni Bantotal, ni SQL Server, ni un cluster de Spark, ni la JVM.
Lo que se prueba es lo que de verdad se rompe y se puede comprobar sin nada de
eso delante:

  - que una Connection de tipo Generic resuelve al motor correcto, porque con
    dos conexiones Generic -una al core y otra a SQL Server- equivocarse de
    motor es silencioso y el error sale lejisimos del motivo;
  - que el calendario de 'procesos' se aplica y que prioridad ordena, porque
    el job lo ignoraba por completo y el DAG decia una cosa mientras Spark
    hacia otra;
  - que los tipos se deducen de la tabla destino, porque un parquet con tipos
    distintos de los del destino falla el INSERT o, peor, trunca;
  - que el grafo se dibuja con una tarea por proceso y los grupos encadenados;
  - que la configuracion solo se republica cuando cambia;
  - y que el volumen spark_events esta montado en los servicios de Airflow,
    que es lo que tumbaba a los workers nuevos.

pyspark se sustituye por un doble: estos tests miran la LOGICA de seleccion y
de tipos, no la ejecucion distribuida. Lo que Spark haga con un DataFrame no es
comprobable sin un cluster, y fingirlo daria una confianza falsa.

Ejecucion:
    docker compose -f docker-compose.windows.yml exec airflow-scheduler \
        pytest /opt/airflow/tests/unit/test_bt2sql_spark.py -v

    o en local:
        pip install "apache-airflow==2.11.2" pytest pyyaml
        AIRFLOW_HOME=/tmp/af pytest airflow/tests/unit/test_bt2sql_spark.py -v

DONDE EJECUTARLOS
-----------------
La suite tiene dos clases de test y no corren igual en los dos sitios:

    pytest airflow/tests/unit -q                 <- DESDE EL REPOSITORIO: todos
    docker compose exec -T airflow-scheduler \
        pytest /opt/airflow/tests/unit -q        <- en el contenedor: los que
                                                    necesitan Airflow instalado

Los que auditan archivos de la raiz (docker-compose*.yml, setup.sh, sql/) se
SALTAN en el contenedor con un motivo explicito, porque esa carpeta no esta
montada ahi. Ver airflow/tests/unit/raiz_repo.py.
"""

from __future__ import annotations

import ast
import copy
import json
import logging
import os
import re
import sys
import types
from datetime import date
from pathlib import Path

import pytest


from raiz_repo import DIR_AIRFLOW, RAIZ, requiere_repo  # noqa: E402
DIR_PRODUCCION = DIR_AIRFLOW / "dags" / "production"
DIR_PLUGINS = DIR_AIRFLOW / "plugins"
DIR_JSON = DIR_AIRFLOW / "config" / "json"
DIR_JOBS = RAIZ / "spark" / "jobs" / "etl"

VARIABLE = "BT2SQL_SPARK"
DAG_ID = "STG_BT2SQL_CARGA_SPARK"
ARCHIVO_DAG = DIR_PRODUCCION / "dag_stg_bt2sql_carga_spark.py"

# ----------------------------------------------------------------------------
# DOS TRAMPAS DE sys.path, Y LAS DOS DAN ERRORES QUE NO SE PARECEN A SU CAUSA
# ----------------------------------------------------------------------------
# 1) La raiz del repositorio tiene una carpeta airflow/ SIN __init__.py. Si
#    pytest se lanza desde ahi, Python la importa como namespace package y tapa
#    el paquete airflow instalado. El sintoma:
#        ImportError: cannot import name 'DagBag' from 'airflow.models'
#                     (unknown location)
#    que no menciona el repositorio por ningun lado. Se quita la raiz de
#    sys.path para que gane el instalado.
for _ruta in list(sys.path):
    try:
        if Path(_ruta or ".").resolve() == RAIZ:
            sys.path.remove(_ruta)
    except OSError:                                              # pragma: no cover
        pass

# 2) plugins va al FINAL de sys.path, nunca al principio: la carpeta tiene
#    paquetes propios y ponerla delante taparia modulos de la biblioteca
#    estandar. Ver
#    test_convenciones.test_ningun_paquete_de_plugins_tapa_la_biblioteca_estandar.
if str(DIR_PLUGINS) not in sys.path:
    sys.path.append(str(DIR_PLUGINS))


# ============================================================================
# DOBLES
# ============================================================================
class ConnFalsa:
    """Connection de Airflow, lo justo para resolver motor, driver y URL."""

    def __init__(self, conn_id="X", conn_type="generic", host="h",
                 port=None, schema="db", extra=None, login="u", password="p"):
        self.conn_id, self.conn_type = conn_id, conn_type
        self.host, self.port, self.schema = host, port, schema
        self.login, self.password = login, password
        self.extra_dejson = extra or {}


class JdbcFalso:
    """Conexion JDBC cruda, como la usan los jobs (createStatement/ResultSet)."""

    def __init__(self, filas, columnas):
        self.filas, self.columnas = filas, columnas
        self.consultas = []

    def createStatement(self):  # noqa: N802  -- es la API de Java
        padre = self

        class RS:
            def __init__(self):
                self.i = -1

            def getMetaData(self):  # noqa: N802
                class Meta:
                    def getColumnCount(_):  # noqa: N802, N805
                        return len(padre.columnas)

                    def getColumnLabel(_, i):  # noqa: N802, N805
                        return padre.columnas[i - 1]

                return Meta()

            def next(self):
                self.i += 1
                return self.i < len(padre.filas)

            def getObject(self, i):  # noqa: N802
                return padre.filas[self.i][i - 1]

            def close(self):
                pass

        class St:
            def executeQuery(_, sql):  # noqa: N802, N805
                padre.consultas.append(" ".join(sql.split()))
                return RS()

            def close(_):  # noqa: N805
                pass

        return St()


def _stub_pyspark() -> None:
    """pyspark falso, suficiente para importar los jobs."""
    if "pyspark" in sys.modules:
        return
    for nombre in ("pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types"):
        sys.modules.setdefault(nombre, types.ModuleType(nombre))
    sys.modules["pyspark.sql"].SparkSession = object
    sys.modules["pyspark.sql"].functions = sys.modules["pyspark.sql.functions"]
    sys.modules["pyspark.sql"].types = sys.modules["pyspark.sql.types"]


def _cargar_job(nombre_archivo: str):
    """Importa un job de spark/jobs/etl con pyspark sustituido."""
    _stub_pyspark()
    if str(DIR_JOBS) not in sys.path:
        sys.path.append(str(DIR_JOBS))
    import importlib

    return importlib.import_module(nombre_archivo)


def _funciones_del_dag(*nombres):
    """Extrae funciones sueltas del DAG sin importarlo.

    El archivo del DAG construye el objeto DAG al importarse, lo que exige una
    base de metadatos. Para probar la logica de una funcion concreta se compila
    solo esa funcion, con sus globales inyectadas. El DAG entero se prueba
    aparte, con DagBag, en test_el_grafo_*.
    """
    from airflow.exceptions import AirflowException

    fuente = ARCHIVO_DAG.read_text(encoding="utf-8")
    arbol = ast.parse(fuente)
    espacio = {
        "AirflowException": AirflowException,
        "logger": logging.getLogger("test"),
        "Path": Path,
        "VARIABLE": VARIABLE,
        "CONN_ORIGEN_DEFECTO": "CONEXION_BANTOTAL",
        "CONN_DESTINO_DEFECTO": "CONEXION_SQLSERVER",
        "CONN_DESTINO": "CONEXION_SQLSERVER",
        "NOMBRE": DAG_ID,
        "__builtins__": __builtins__,
    }
    for nodo in arbol.body:
        if isinstance(nodo, ast.Assign) and getattr(nodo.targets[0], "id", "") in (
                "SQLSERVER_A_TIPO", "COLUMNAS_INYECTADAS"):
            exec(ast.get_source_segment(fuente, nodo), espacio)  # noqa: S102
    encontradas = {}
    for nodo in arbol.body:
        if isinstance(nodo, ast.FunctionDef) and nodo.name in nombres:
            exec(ast.get_source_segment(fuente, nodo), espacio)  # noqa: S102
            encontradas[nodo.name] = espacio[nodo.name]
    faltan = set(nombres) - set(encontradas)
    assert not faltan, f"el DAG ya no define: {sorted(faltan)}"
    return espacio


# ============================================================================
# FIXTURES
# ============================================================================
@pytest.fixture(scope="session")
def variable() -> dict:
    """La Variable BT2SQL_SPARK tal como esta versionada en el repositorio."""
    return json.loads((DIR_JSON / f"{VARIABLE}.json").read_text(encoding="utf-8"))


@pytest.fixture
def config(variable, tmp_path, monkeypatch) -> Path:
    """Escribe la Variable como si fuera el JSON publicado en el volumen."""
    ruta = tmp_path / "bt2sql_spark.json"
    ruta.write_text(json.dumps(variable, ensure_ascii=False), encoding="utf-8")
    return ruta


@pytest.fixture
def extraccion():
    return _cargar_job("bt2sql_extraccion_spark")


@pytest.fixture
def carga():
    return _cargar_job("bt2sql_carga_spark")


def cfg_de(doc: dict) -> dict:
    """Arma el cfg que recibe el job: su seccion mas lo compartido."""
    cfg = dict(doc["extraccion"])
    cfg["tablas"] = doc["tablas"]
    cfg["origen_tablas"] = doc.get("origen_tablas", "variable")
    return cfg


# ============================================================================
# 1. CONEXIONES GENERIC
# ----------------------------------------------------------------------------
# Generic no dice nada del motor. Con dos conexiones Generic, cualquier regla
# que traduzca 'generic' a un motor fijo acierta en una y se equivoca en la
# otra EN SILENCIO: arma una URL jdbc:as400:// apuntando a SQL Server y carga
# el driver de IBM i para escribir en una tabla de SQL Server.
# ============================================================================
def test_generic_no_se_traduce_a_as400_por_omision():
    from utils.spark_config import JDBC_DRIVERS

    for nombre, driver in JDBC_DRIVERS.items():
        assert "generic" not in driver.conn_types, (
            f"el driver {nombre!r} vuelve a reclamar 'generic'. Eso hace que "
            f"TODA conexion Generic resuelva a el, incluida la que no es suya."
        )


@pytest.mark.parametrize("extra,esperado", [
    ({"motor": "as400"}, "as400"),
    ({"motor": "mssql"}, "mssql"),
    ({"engine": "mssql"}, "mssql"),
    ({"MOTOR": "as400"}, None),          # las claves son sensibles a mayusculas
    ({"libraries": "GPPPBTDB"}, "as400"),
    ({"naming": "system"}, "as400"),
])
def test_el_motor_sale_del_extra(extra, esperado):
    from airflow.exceptions import AirflowException
    from utils.spark_config import motor_de_conexion

    conn = ConnFalsa(extra=extra)
    if esperado is None:
        with pytest.raises(AirflowException):
            motor_de_conexion(conn)
    else:
        assert motor_de_conexion(conn) == esperado


@pytest.mark.parametrize("puerto,esperado", [
    (1433, "mssql"), (8471, "as400"), (9471, "as400"), (446, "as400"),
    (5432, "postgres"), (1521, "oracle"), (50000, "db2"),
])
def test_sin_extra_el_motor_sale_del_puerto(puerto, esperado):
    from utils.spark_config import motor_de_conexion

    assert motor_de_conexion(ConnFalsa(port=puerto)) == esperado


def test_el_3306_no_se_adivina():
    """MySQL y SingleStore comparten el 3306. Adivinar ahi es el error que
    esta funcion existe para evitar, asi que tiene que fallar."""
    from airflow.exceptions import AirflowException
    from utils.spark_config import motor_de_conexion

    with pytest.raises(AirflowException):
        motor_de_conexion(ConnFalsa(port=3306))


def test_sin_ninguna_pista_el_error_dice_que_hacer():
    from airflow.exceptions import AirflowException
    from utils.spark_config import motor_de_conexion

    with pytest.raises(AirflowException) as exc:
        motor_de_conexion(ConnFalsa(conn_id="CONEXION_SQLSERVER"))
    mensaje = str(exc.value)
    assert "CONEXION_SQLSERVER" in mensaje
    assert "motor" in mensaje and "Extra" in mensaje, (
        "el mensaje tiene que decir QUE poner y DONDE, no solo que fallo")


def test_un_conn_type_explicito_gana_sobre_el_extra():
    from utils.spark_config import motor_de_conexion

    conn = ConnFalsa(conn_type="mssql", extra={"motor": "as400"})
    assert motor_de_conexion(conn) == "mssql"


@pytest.mark.parametrize("extra,clase,prefijo", [
    ({"motor": "as400"}, "com.ibm.as400.access.AS400JDBCDriver", "jdbc:as400://"),
    ({"motor": "mssql"}, "com.microsoft.sqlserver.jdbc.SQLServerDriver", "jdbc:sqlserver://"),
])
def test_driver_y_url_cuadran_con_el_motor(extra, clase, prefijo):
    from utils.spark_config import driver_for_conn, jdbc_url

    conn = ConnFalsa(extra=extra)
    assert driver_for_conn(conn).driver_class == clase
    assert jdbc_url(conn).startswith(prefijo)


def test_la_url_del_core_lleva_las_propiedades_de_proteccion():
    """Sin 'prompt=false' el driver de IBM i intenta abrir un dialogo grafico y
    la tarea se cuelga sin un solo mensaje util en el log."""
    from utils.spark_config import jdbc_url

    url = jdbc_url(ConnFalsa(extra={"motor": "as400", "libraries": "GPPPBTDB"}))
    for propiedad in ("prompt=false", "access=read only", "transaction isolation=none"):
        assert propiedad in url, f"falta {propiedad!r} en la URL del core"
    assert "libraries=GPPPBTDB" in url


# ============================================================================
# 2. LA VARIABLE
# ============================================================================
def test_la_variable_es_json_valido_y_trae_las_tres_secciones(variable):
    for clave in ("tablas", "extraccion", "carga"):
        assert clave in variable, f"la Variable {VARIABLE} no trae {clave!r}"
    assert variable["tablas"], "'tablas' esta vacio: el DAG no dibujaria nada"


def test_las_claves_de_control_existen_y_tienen_valores_validos(variable):
    assert variable.get("origen_tablas") in ("variable", "catalogo")
    assert variable.get("validacion_catalogo") in ("estricto", "aviso", "ninguna")
    assert isinstance(variable["extraccion"].get("tipos_desde_destino"), bool)


def test_toda_tabla_declara_esquema_tabla_y_destino(variable):
    faltan = [
        t.get("tabla", "?") for t in variable["tablas"]
        if not (t.get("esquema") and t.get("tabla") and t.get("tabla_destino"))
    ]
    assert not faltan, (
        f"tabla(s) sin esquema, tabla o tabla_destino: {faltan}. Una tabla "
        f"activa sin destino se extraeria y la carga no sabria donde ponerla.")


def test_la_doble_llave_cuadra_en_los_dos_sentidos(variable):
    """Una tabla sin proceso no se extrae y no da error; un proceso sin tabla
    no hace nada y tampoco avisa. Los dos huecos son silenciosos en produccion,
    asi que se vigilan aqui."""
    tablas = {(str(t.get("nombre_proceso") or t["tabla"]).upper(),
               str(t["esquema"]).upper()) for t in variable["tablas"]}
    procesos = {(str(p["nombre_proceso"]).upper(), str(p["nombre_esquema"]).upper())
                for p in variable["extraccion"]["procesos"]}

    assert not tablas - procesos, (
        f"tabla(s) sin entrada en 'procesos', no se extraerian nunca: "
        f"{sorted(tablas - procesos)}")
    assert not procesos - tablas, (
        f"proceso(s) sin entrada en 'tablas', no hacen nada: "
        f"{sorted(procesos - tablas)}")


def test_las_particiones_declaradas_estan_bien_formadas(variable):
    for t in variable["tablas"]:
        p = t.get("particion")
        if p is None:
            continue
        for clave in ("columna", "desde", "hasta", "particiones"):
            assert clave in p, f"{t['tabla']}: particion sin {clave!r}"
        assert int(p["desde"]) < int(p["hasta"]), (
            f"{t['tabla']}: desde >= hasta, todas las filas caerian en una "
            f"particion y las demas quedarian vacias")
        assert int(p["particiones"]) > 1, (
            f"{t['tabla']}: particiones <= 1 no aporta paralelismo")


def test_los_dos_pipelines_no_escriben_en_la_misma_carpeta(variable):
    """Los gemelos pandas y Spark tienen que poder correr el mismo dia para
    poder compararlos fila a fila. Si escribieran en la misma carpeta, el
    segundo pisaria el parquet del primero y no habria nada que comparar."""
    otra = json.loads((DIR_JSON / "BT2SQL_EXTRACCION.json").read_text(encoding="utf-8"))
    assert variable["extraccion"]["output_dir"] != otra["output_dir"]


def test_los_dos_pipelines_se_distinguen_en_la_bitacora(variable):
    """Y tienen que poder distinguirse DESPUES, en ctl_proceso_parquet.

    El nom_proceso del pipeline de pandas no sale de su Variable: esta escrito
    a mano dentro de _abrir_log(), en etl_bt2sql/bt2sql_extraccion.py. Por eso
    este test lo busca en el codigo fuente en vez de en el JSON. Que sea una
    constante en el codigo y no una clave de configuracion es, en si mismo,
    algo que conviene corregir algun dia; mientras tanto, lo que no puede pasar
    es que coincida con el de Spark."""
    fuente = (DIR_PRODUCCION / "etl_bt2sql" / "bt2sql_extraccion.py").read_text(encoding="utf-8")
    literales = re.findall(r'\(\s*"([A-Z_]+)",\s*str\(fila\["ESQUEMA"\]\)', fuente)
    assert literales, (
        "no encuentro el nom_proceso que escribe el pipeline de pandas en "
        "_abrir_log(). Si cambio la forma de esa consulta, actualice este test: "
        "sin el, los dos pipelines podrian volverse indistinguibles en la "
        "bitacora sin que nada avise.")
    assert variable["extraccion"]["nom_proceso"] not in literales, (
        f"el pipeline Spark escribe el mismo nom_proceso que el de pandas "
        f"({literales}). En ctl_proceso_parquet no se podrian separar las dos "
        f"corridas, que es justo como se valida que la traduccion salio bien.")


# ============================================================================
# 3. EL CALENDARIO Y LA PRIORIDAD
# ----------------------------------------------------------------------------
# El job los ignoraba por completo hasta el 2026-10-07: extraia todo lo que
# tuviera activo='S', en el orden del JSON. El DAG reportaba una tabla como
# inactiva y Spark la extraia a continuacion.
# ============================================================================
def test_el_job_aplica_el_calendario(extraccion, variable):
    doc = copy.deepcopy(variable)
    doc["extraccion"]["procesos"][1]["estado"] = 0
    doc["extraccion"]["procesos"][2]["estado_diario"] = 0

    salen = [f["TABLA"] for f in extraccion.catalogo_de_tablas(cfg_de(doc), None, "diario")]
    fuera = {doc["extraccion"]["procesos"][1]["nombre_proceso"],
             doc["extraccion"]["procesos"][2]["nombre_proceso"]}
    assert len(salen) == len(doc["tablas"]) - 2
    for t in doc["tablas"]:
        if t["nombre_proceso"] in fuera:
            assert t["tabla"] not in salen


def test_el_job_ordena_por_prioridad(extraccion, variable):
    sel = extraccion.catalogo_de_tablas(cfg_de(variable), None, "diario")
    prioridades = [f["_PRIORIDAD"] for f in sel]
    assert prioridades == sorted(prioridades), (
        f"las tablas no salen ordenadas por prioridad: {prioridades}")


def test_una_corrida_sin_ninguna_tabla_corta_antes_de_levantar_spark(extraccion, variable):
    with pytest.raises(SystemExit) as exc:
        extraccion.catalogo_de_tablas(cfg_de(variable), None, "semanal")
    assert "semanal" in str(exc.value)


# ============================================================================
# LAS MARCAS DEL FILTRO
# ============================================================================
# FILTRO se concatenaba tal cual, asi que una tabla filtrada por fecha
# obligaba a editar el catalogo cada dia. Con las marcas, la fecha la pone el
# job: la misma que leyo de GPPPBTDB.FST017 (PGFCIE) y que va a FECHA_PROCESO
# y a la subcarpeta del dia.
#
# Lo que estos tests protegen, por orden de lo que costaria descubrirlo:
#
#   1. que los DOS pipelines resuelvan igual. Si solo uno supiera, una tabla
#      traeria un dia distinto segun por donde pasara, y eso no se descubre
#      mirando el log: se descubre cuadrando saldos;
#   2. que las fechas de Bantotal salgan SIN comillas y las DATE CON comillas.
#      Es el error que mas cuesta ver: FSH005FEC es DECIMAL(8,0), y
#      '20261010' entre comillas o falla o -en DB2- compara como texto;
#   3. que una marca mal escrita FALLE. Dejarla pasar literal es peor que
#      fallar: en SQL Server da un error de sintaxis oscuro, y en DB2 for i
#      hay sintaxis donde una llave no revienta y el WHERE significa otra cosa;
#   4. que un literal ODBC {d '2026-01-01'} -SQL legitimo- no se toque.
# ============================================================================
FECHA_DE_PRUEBA = date(2026, 10, 10)
BATCH_DE_PRUEBA = "20261012091533"


def _pandas_sustituir():
    from etl_bt2sql.bt2sql_extraccion import sustituir_marcas
    return sustituir_marcas


@pytest.mark.parametrize("filtro,esperado", [
    ("FSH005FEC = {FECHA_AAAAMMDD}", "FSH005FEC = 20261010"),
    ("FEC_MOV >= {FECHA_ISO}", "FEC_MOV >= '2026-10-10'"),
    ("PERIODO = {FECHA_AAAAMM}", "PERIODO = 202610"),
    ("ANIO = {FECHA_AAAA}", "ANIO = 2026"),
    ("LOTE = {BATCH_ID}", "LOTE = '20261012091533'"),
    ("FSH005FEC BETWEEN {FECHA_AAAAMMDD-6} AND {FECHA_AAAAMMDD}",
     "FSH005FEC BETWEEN 20261004 AND 20261010"),
    ("FSH005FEC <= {FECHA_AAAAMMDD+1}", "FSH005FEC <= 20261011"),
])
def test_las_marcas_del_filtro_se_sustituyen(extraccion, filtro, esperado):
    assert extraccion.sustituir_marcas(
        filtro, "FSH005", FECHA_DE_PRUEBA, BATCH_DE_PRUEBA) == esperado


@pytest.mark.parametrize("filtro", [
    "",                                 # tabla sin condicion: el caso normal
    "PGCOD = 1",                        # filtro fijo, sin marcas
    "FEC = {d '2026-01-01'}",           # literal de fecha ODBC/JDBC
])
def test_un_filtro_sin_marcas_no_se_toca(extraccion, filtro):
    """No todas las tablas llevan condicion, y las que llevan una fija tienen
    que seguir funcionando igual que antes de que existieran las marcas."""
    assert extraccion.sustituir_marcas(
        filtro, "FSH005", FECHA_DE_PRUEBA, BATCH_DE_PRUEBA) == filtro


def test_la_fecha_de_bantotal_va_sin_comillas_y_la_ISO_con_comillas(extraccion):
    """El error mas dificil de ver de todos los posibles aqui.

    Las columnas de fecha de Bantotal son DECIMAL(8,0): 20261010 va sin
    comillas. Si saliera '20261010', o falla el tipo, o -peor- DB2 lo compara
    como texto y el WHERE devuelve algo que no es lo pedido sin dar error.
    """
    numerica = extraccion.sustituir_marcas(
        "{FECHA_AAAAMMDD}", "FSH005", FECHA_DE_PRUEBA)
    assert numerica == "20261010"
    assert "'" not in numerica, "la fecha de Bantotal no puede llevar comillas"

    iso = extraccion.sustituir_marcas("{FECHA_ISO}", "FSH005", FECHA_DE_PRUEBA)
    assert iso == "'2026-10-10'"
    assert iso.startswith("'") and iso.endswith("'"), (
        "una fecha para columna DATE tiene que ir entre comillas")


@pytest.mark.parametrize("filtro,en_el_mensaje", [
    ("FSH005FEC = {FECHA}", "no existe"),
    ("FSH005FEC = {FECHA_PROCESO}", "no existe"),
    ("FSH005FEC = {fecha_aaaammdd}", "MAYUSCULAS"),
    ("LOTE = {BATCH_ID-1}", "no es una fecha"),
])
def test_una_marca_mal_escrita_falla_y_dice_cuales_hay(extraccion, filtro,
                                                       en_el_mensaje):
    with pytest.raises(SystemExit) as exc:
        extraccion.sustituir_marcas(filtro, "FSH005", FECHA_DE_PRUEBA,
                                    BATCH_DE_PRUEBA)
    mensaje = str(exc.value)
    assert en_el_mensaje in mensaje
    assert "{FECHA_AAAAMMDD}" in mensaje, (
        "el error tiene que listar las marcas validas, o no se sabe que poner")


def test_una_marca_de_fecha_sin_fecha_falla_en_vez_de_inventarla(extraccion):
    with pytest.raises(SystemExit) as exc:
        extraccion.sustituir_marcas("FSH005FEC = {FECHA_AAAAMMDD}", "FSH005")
    assert "FST017" in str(exc.value), (
        "el error tiene que decir de donde sale la fecha")


def test_un_batch_id_con_comilla_no_entra_en_el_SQL(extraccion):
    """El batch_id acaba DENTRO de una cadena SQL.

    Hoy lo genera preparar_config y son 14 digitos, pero el job se puede
    invocar a mano con --batch-id, y esa es la puerta por la que entraria una
    comilla. No es una defensa teorica: es la unica parte de una marca que no
    se calcula a partir de un objeto date.
    """
    with pytest.raises(SystemExit) as exc:
        extraccion.sustituir_marcas("LOTE = {BATCH_ID}", "FSH005",
                                    FECHA_DE_PRUEBA, "abc' OR 1=1--")
    assert "caracteres" in str(exc.value)


def test_el_select_completo_lleva_el_filtro_resuelto(extraccion):
    fila = {"ESQUEMA": "GPPPBTDB", "TABLA": "FSH005",
            "COLUMNAS": "PGCOD, FSH005FEC",
            "FILTRO": "FSH005FEC = {FECHA_AAAAMMDD}"}
    sql = extraccion.construir_select(fila, FECHA_DE_PRUEBA, BATCH_DE_PRUEBA)
    assert sql == ("SELECT PGCOD, FSH005FEC FROM GPPPBTDB.FSH005 "
                   "WHERE FSH005FEC = 20261010")
    assert "{" not in sql


def test_una_tabla_sin_filtro_no_lleva_WHERE(extraccion):
    """Es el caso de las cuatro tablas de hoy, y tiene que seguir siendo
    el camino por defecto: la mayoria de las tablas no llevan condicion."""
    fila = {"ESQUEMA": "GPPPBTDB", "TABLA": "FSH005", "COLUMNAS": "", "FILTRO": ""}
    sql = extraccion.construir_select(fila, FECHA_DE_PRUEBA, BATCH_DE_PRUEBA)
    assert "WHERE" not in sql


@pytest.mark.parametrize("filtro", [
    "FSH005FEC = {FECHA_AAAAMMDD}",
    "FEC_MOV >= {FECHA_ISO}",
    "PERIODO = {FECHA_AAAAMM}",
    "ANIO = {FECHA_AAAA}",
    "LOTE = {BATCH_ID}",
    "FSH005FEC BETWEEN {FECHA_AAAAMMDD-6} AND {FECHA_AAAAMMDD}",
    "PGCOD = 1",
    "",
    "FEC = {d '2026-01-01'}",
])
@requiere_repo
def test_los_dos_pipelines_resuelven_igual_las_marcas(extraccion, filtro):
    """EL TEST QUE DE VERDAD IMPORTA DE ESTE BLOQUE.

    sustituir_marcas esta duplicada: una copia en el job de Spark y otra en
    airflow/dags/production/etl_bt2sql/bt2sql_extraccion.py, el pipeline de
    pandas. Duplicar esta bien -el job de Spark no puede importar del paquete
    de los DAG- mientras no pueda divergir en silencio, y esto es lo que lo
    impide.

    Si divergieran, el catalogo y la Variable seguirian siendo los mismos para
    los dos, asi que la MISMA tabla traeria un dia distinto segun por que
    pipeline pasara. Eso no se descubre leyendo un log: se descubre cuadrando
    saldos semanas despues.
    """
    de_spark = extraccion.sustituir_marcas(
        filtro, "FSH005", FECHA_DE_PRUEBA, BATCH_DE_PRUEBA)
    de_pandas = _pandas_sustituir()(
        filtro, "FSH005", FECHA_DE_PRUEBA, BATCH_DE_PRUEBA)
    assert de_spark == de_pandas, (
        f"los dos pipelines resuelven {filtro!r} de forma distinta:\n"
        f"  spark : {de_spark!r}\n"
        f"  pandas: {de_pandas!r}")


@requiere_repo
def test_los_dos_pipelines_declaran_las_mismas_marcas(extraccion):
    from etl_bt2sql.bt2sql_extraccion import MARCAS_VALIDAS as de_pandas
    assert tuple(extraccion.MARCAS_VALIDAS) == tuple(de_pandas), (
        "las listas de marcas validas de los dos pipelines han divergido")


def test_un_tipo_de_ejecucion_desconocido_no_pasa(extraccion, variable):
    with pytest.raises(SystemExit):
        extraccion.catalogo_de_tablas(cfg_de(variable), None, "trimestral")


def test_una_tabla_inactiva_no_entra(extraccion, variable):
    doc = copy.deepcopy(variable)
    doc["tablas"][0]["activo"] = "N"
    salen = [f["TABLA"] for f in extraccion.catalogo_de_tablas(cfg_de(doc), None, "diario")]
    assert doc["tablas"][0]["tabla"] not in salen


def test_el_calendario_tambien_se_aplica_leyendo_del_catalogo(extraccion, variable):
    """La rama origen_tablas='catalogo' tiene que filtrar igual que la otra.

    Este test nacio de una prueba de mutacion: al romper a proposito el filtro
    de esta rama, la suite seguia en verde, porque todos los demas tests usan
    origen_tablas='variable'. Un camino sin un solo test es un camino donde un
    fallo entra sin que nada avise.
    """
    doc = copy.deepcopy(variable)
    doc["origen_tablas"] = "catalogo"
    doc["extraccion"]["procesos"][0]["estado"] = 0
    fuera = doc["extraccion"]["procesos"][0]["nombre_proceso"]

    columnas = ["ESQUEMA", "TABLA", "COLUMNAS", "FILTRO", "ACTIVO",
                "NOMBRE_PARQUET", "TIPOS", "TABLA_DESTINO", "BATCH_SIZE"]
    filas = [(t["esquema"], t["tabla"], None, None, "S",
              t["nombre_proceso"], None, t["tabla_destino"], 5000)
             for t in doc["tablas"]]

    salen = [f["TABLA"] for f in
             extraccion.catalogo_de_tablas(cfg_de(doc), JdbcFalso(filas, columnas), "diario")]
    descartada = next(x["tabla"] for x in doc["tablas"] if x["nombre_proceso"] == fuera)
    assert descartada not in salen, (
        "leyendo del catalogo no se aplico el calendario: una tabla con "
        "estado=0 se habria extraido igual")
    assert len(salen) == len(doc["tablas"]) - 1


def test_un_origen_de_tablas_desconocido_no_pasa(extraccion, variable):
    doc = copy.deepcopy(variable)
    doc["origen_tablas"] = "donde_sea"
    with pytest.raises(SystemExit) as exc:
        extraccion.catalogo_de_tablas(cfg_de(doc), None, "diario")
    assert "variable" in str(exc.value) and "catalogo" in str(exc.value)


def test_sin_tablas_declaradas_el_mensaje_dice_donde_declararlas(extraccion, variable):
    doc = copy.deepcopy(variable)
    doc["tablas"] = []
    with pytest.raises(SystemExit) as exc:
        extraccion.catalogo_de_tablas(cfg_de(doc), None, "diario")
    assert "sync_variables" in str(exc.value)


# ============================================================================
# 4. LOS TIPOS DE DATO
# ----------------------------------------------------------------------------
# Si el parquet no trae los tipos que la tabla destino espera, el INSERT por
# JDBC falla -o, peor, no falla y trunca-.
# ============================================================================
@pytest.mark.parametrize("declarado,esperado", [
    ("PGCOD:DECIMAL(3,0)", {"type": "decimal", "precision": 3, "scale": 0}),
    ("TCV:DECIMAL(17,8)", {"type": "decimal", "precision": 17, "scale": 8}),
    ("NOMBRE:VARCHAR(50)", {"type": "string"}),
    ("N:BIGINT", {"type": "bigint"}),
    ("C:INT", {"type": "int"}),
    ("F:DATE", {"type": "date"}),
    ("TS:DATETIME2", {"type": "timestamp"}),
    ("IND:BIT", {"type": "int"}),
])
def test_parsear_tipos_reconoce_cada_tipo(extraccion, declarado, esperado):
    col = declarado.split(":")[0]
    assert extraccion.parsear_tipos(declarado, "T")[col] == esperado


def test_un_bit_es_entero_y_no_booleano(extraccion):
    """En el core un indicador viene como 0/1 numerico. Un BooleanType en el
    parquet obliga a convertir otra vez al escribir en un BIT de SQL Server."""
    assert extraccion.parsear_tipos("IND:BIT", "T")["IND"]["type"] == "int"


def test_un_decimal_sin_precision_no_pasa_por_bueno(extraccion):
    with pytest.raises(SystemExit):
        extraccion.parsear_tipos("X:DECIMAL", "T")


def test_tipos_mal_formado_dice_cual_es_el_formato(extraccion):
    with pytest.raises(SystemExit) as exc:
        extraccion.parsear_tipos("COLUMNA_SIN_TIPO", "T")
    assert "COLUMNA:TIPO" in str(exc.value)


def test_los_tipos_salen_de_la_tabla_destino_cuando_no_se_declaran(extraccion):
    columnas = ["COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE"]
    filas = [
        ("FECHA_PROCESO", "date", None, None),
        ("PGCOD", "decimal", 3, 0),
        ("NOMBRE", "varchar", None, None),
        ("N", "bigint", 19, 0),
        ("ALTA", "datetime2", None, None),
        ("GEO", "geography", None, None),
        ("BATCH_ID", "varchar", None, None),
    ]
    mapa = extraccion.tipos_de_destino(JdbcFalso(filas, columnas), "dbo.STG_X", "X")

    assert mapa["PGCOD"] == {"type": "decimal", "precision": 3, "scale": 0}
    assert mapa["ALTA"] == {"type": "timestamp"}
    assert "FECHA_PROCESO" not in mapa and "BATCH_ID" not in mapa, (
        "las inyecta la extraccion, no vienen del core")
    assert "GEO" not in mapa, (
        "un tipo que no se sabe traducir se deja pasar, no se fuerza a texto")


@pytest.mark.parametrize("destino", ["dbo.STG_X", "STG_X", "[dbo].[STG_X]"])
def test_el_nombre_del_destino_se_entiende_escrito_de_varias_formas(extraccion, destino):
    columnas = ["COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE"]
    mapa = extraccion.tipos_de_destino(
        JdbcFalso([("PGCOD", "decimal", 3, 0)], columnas), destino, "X")
    assert "PGCOD" in mapa


def test_un_destino_inexistente_avisa_y_no_revienta(extraccion):
    """El error de 'tabla destino inexistente' tiene que salir de la carga, que
    lo puede explicar bien, no de la deduccion de tipos."""
    columnas = ["COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE"]
    assert extraccion.tipos_de_destino(JdbcFalso([], columnas), "dbo.NO_EXISTE", "X") == {}


# ============================================================================
# 5. LA CARGA
# ============================================================================
def test_la_carga_cruza_el_lote_con_el_destino_de_la_variable(carga, variable):
    cfg = dict(variable["carga"])
    cfg["tablas"] = variable["tablas"]
    cfg["origen_tablas"] = "variable"
    primera = variable["tablas"][0]

    carga.filas_de = lambda conn, sql: [{
        "id_log": 1, "archivo_parquet": "/data/x",
        "esquema": primera["esquema"].lower(),        # el cruce ignora mayusculas
        "tabla_origen": primera["tabla"].lower(),
    }]
    lotes = carga.lotes_por_cargar(cfg, None, "B1")
    assert lotes[0]["tabla_destino"] == primera["tabla_destino"]
    assert lotes[0]["batch_size"] == primera["batch_size"]


def test_una_tabla_extraida_sin_declarar_es_error_y_no_silencio(carga, variable):
    """Antes habia un INNER JOIN contra el catalogo que la descartaba sin
    avisar: el parquet quedaba en disco y la tabla destino con los datos del
    dia anterior."""
    cfg = dict(variable["carga"])
    cfg["tablas"] = variable["tablas"]
    cfg["origen_tablas"] = "variable"
    carga.filas_de = lambda conn, sql: [{
        "id_log": 1, "archivo_parquet": "/data/x",
        "esquema": "GPPPBTDB", "tabla_origen": "TABLA_FANTASMA",
    }]
    with pytest.raises(SystemExit) as exc:
        carga.lotes_por_cargar(cfg, None, "B1")
    assert "TABLA_FANTASMA" in str(exc.value)


def test_el_batch_id_se_sustituye_en_la_consulta_de_lotes(carga, variable):
    cfg = dict(variable["carga"])
    cfg["tablas"] = variable["tablas"]
    cfg["origen_tablas"] = "variable"
    vistas = []

    def espia(conn, sql):
        vistas.append(sql)
        return []

    carga.filas_de = espia
    with pytest.raises(SystemExit):
        carga.lotes_por_cargar(cfg, None, "20261007120000")
    assert "20261007120000" in vistas[0] and "{BATCH_ID}" not in vistas[0]


# ============================================================================
# 6. LAS TAREAS DE PYTHON DEL DAG
# ============================================================================
def test_el_inventario_nombra_la_tabla_sin_proceso(config, variable, caplog):
    """Una tabla activa sin entrada en 'procesos' no se extrae y no da error.
    Antes no se veia en ningun sitio."""
    doc = copy.deepcopy(variable)
    doc["tablas"].append({
        "nombre_proceso": "STG_HUERFANA", "esquema": "GPPPBTDB", "tabla": "HUERFANA",
        "activo": "S", "columnas": "", "filtro": "", "tipos": "",
        "tabla_destino": "STG_HUERFANA", "batch_size": 5000,
        "commit_every": 10, "particion": None,
    })
    config.write_text(json.dumps(doc), encoding="utf-8")

    espacio = _funciones_del_dag("_config_publicada", "_clave", "inventario_tablas")
    espacio["RUNTIME_JSON"] = str(config)
    with caplog.at_level(logging.INFO):
        salida = espacio["inventario_tablas"]("diario")

    assert "STG_HUERFANA" not in salida["se_extraen"]
    assert "SIN entrada en extraccion.procesos" in caplog.text


def test_el_inventario_corta_si_ninguna_tabla_entra(config, variable):
    from airflow.exceptions import AirflowException

    doc = copy.deepcopy(variable)
    for t in doc["tablas"]:
        t["activo"] = "N"
    config.write_text(json.dumps(doc), encoding="utf-8")

    espacio = _funciones_del_dag("_config_publicada", "_clave", "inventario_tablas")
    espacio["RUNTIME_JSON"] = str(config)
    with pytest.raises(AirflowException) as exc:
        espacio["inventario_tablas"]("diario")
    assert "no" in str(exc.value).lower()


def test_el_inventario_corta_si_una_tabla_activa_no_tiene_destino(config, variable):
    from airflow.exceptions import AirflowException

    doc = copy.deepcopy(variable)
    doc["tablas"][0]["tabla_destino"] = ""
    config.write_text(json.dumps(doc), encoding="utf-8")

    espacio = _funciones_del_dag("_config_publicada", "_clave", "inventario_tablas")
    espacio["RUNTIME_JSON"] = str(config)
    with pytest.raises(AirflowException):
        espacio["inventario_tablas"]("diario")


def test_los_conn_id_salen_de_la_configuracion_y_no_estan_escritos_a_mano(config, variable):
    doc = copy.deepcopy(variable)
    doc["extraccion"]["conn_id_origen"] = "MI_CONEXION_BT"
    doc["extraccion"]["conn_id_destino"] = "MI_CONEXION_SQL"
    config.write_text(json.dumps(doc), encoding="utf-8")

    espacio = _funciones_del_dag("_conn_ids")
    espacio["RUNTIME_JSON"] = str(config)
    assert espacio["_conn_ids"]() == ("MI_CONEXION_BT", "MI_CONEXION_SQL")


def test_sin_configuracion_los_conn_id_caen_al_respaldo_sin_romper_el_parseo(
        tmp_path, monkeypatch):
    """Un DAG que no aparece en la interfaz porque revento al parsearse es
    mucho peor de diagnosticar que uno que aparece y falla en su primera
    tarea.

    OJO CON LO QUE PRUEBA ESTE TEST. _conn_ids tiene ahora TRES pasos: el JSON
    publicado, la Variable, y solo al final estos nombres. El ultimo escalon es
    el que se comprueba aqui, asi que hay que dejar sin leer tambien la
    Variable; si no, el test pasaria o fallaria segun lo que haya hoy en la
    base de metadatos -y de hecho fallaba, porque la Variable del repositorio
    declara CONEXION_SQLSERVER como origen para las pruebas en local-.

    Los otros dos escalones estan en
    test_los_conn_id_salen_del_json_publicado_cuando_existe y
    test_sin_json_publicado_los_conn_id_salen_de_la_variable.
    """
    import types

    sin_variable = types.ModuleType("airflow.models")

    class VariableAusente:
        @staticmethod
        def get(nombre, deserialize_json=False):
            raise KeyError(nombre)

    sin_variable.Variable = VariableAusente
    monkeypatch.setitem(sys.modules, "airflow.models", sin_variable)

    espacio = _funciones_del_dag("_conn_ids")
    espacio["RUNTIME_JSON"] = str(tmp_path / "no-existe.json")
    assert espacio["_conn_ids"]() == ("CONEXION_BANTOTAL", "CONEXION_SQLSERVER")


@pytest.mark.parametrize("cambio,entra", [
    (lambda d: None, True),
    (lambda d: d["tablas"][0].update({"activo": "N"}), False),
    (lambda d: d["extraccion"]["procesos"][0].update({"estado": 0}), False),
    (lambda d: d["extraccion"]["procesos"][0].update({"estado_diario": 0}), False),
])
def test_la_doble_llave_decide_si_una_tarea_se_salta(config, variable, cambio, entra):
    doc = copy.deepcopy(variable)
    cambio(doc)
    config.write_text(json.dumps(doc), encoding="utf-8")

    espacio = _funciones_del_dag("_config_publicada", "_proceso_entra_hoy")
    espacio["RUNTIME_JSON"] = str(config)
    resultado, motivo = espacio["_proceso_entra_hoy"](
        variable["tablas"][0]["nombre_proceso"], "diario")
    assert resultado is entra, motivo


# ============================================================================
# 7. LA PUBLICACION DE LA CONFIGURACION
# ============================================================================
def _preparar_config(espacio, variable_devuelta, monkeypatch):
    """Inyecta un Variable.get() falso en el espacio de preparar_config.

    Con monkeypatch.setitem y NO con una asignacion directa a sys.modules. La
    primera version hacia  sys.modules["airflow.models"] = falso  y no lo
    devolvia: a partir de ahi, cualquier test posterior que hiciera
    'from airflow.models import DagBag' recibia el modulo falso y moria con

        ImportError: cannot import name 'DagBag' from 'airflow.models'
                     (unknown location)

    que es exactamente el mismo sintoma que produce la carpeta airflow/ del
    repositorio tapando al paquete instalado. Dos causas muy distintas, el
    mismo mensaje: por eso conviene que los tests no ensucien estado global.
    """
    modulo = types.ModuleType("airflow.models")
    modulo.Variable = type("V", (), {
        "get": staticmethod(lambda nombre, deserialize_json=False: variable_devuelta)})
    monkeypatch.setitem(sys.modules, "airflow.models", modulo)
    return espacio["preparar_config"]


def test_auto_no_toca_el_archivo_si_la_variable_no_cambio(config, variable, caplog, monkeypatch):
    espacio = _funciones_del_dag("preparar_config")
    espacio["RUNTIME_JSON"] = str(config)
    espacio["datetime"] = __import__("datetime").datetime
    espacio["os"] = __import__("os")
    # el archivo publicado se escribe con el mismo formato que usa la tarea
    config.write_text(json.dumps(variable, ensure_ascii=False, indent=2), encoding="utf-8")
    antes = config.stat().st_mtime_ns

    with caplog.at_level(logging.INFO):
        _preparar_config(espacio, variable, monkeypatch)("diario", "auto")

    assert config.stat().st_mtime_ns == antes, "reescribio un archivo identico"
    assert "SIN CAMBIOS" in caplog.text


def test_auto_publica_y_respalda_cuando_la_variable_cambio(config, variable, caplog, monkeypatch):
    espacio = _funciones_del_dag("preparar_config")
    espacio["RUNTIME_JSON"] = str(config)
    espacio["datetime"] = __import__("datetime").datetime
    espacio["os"] = __import__("os")
    config.write_text(json.dumps(variable, ensure_ascii=False, indent=2), encoding="utf-8")

    nueva = copy.deepcopy(variable)
    nueva["extraccion"]["fetchsize"] = 12345
    with caplog.at_level(logging.INFO):
        _preparar_config(espacio, nueva, monkeypatch)("diario", "auto")

    assert json.loads(config.read_text(encoding="utf-8"))["extraccion"]["fetchsize"] == 12345
    assert "PUBLICADA" in caplog.text
    respaldos = [p for p in config.parent.glob("bt2sql_spark.*.json")]
    assert respaldos, "publicar sin respaldo hace irreversible un error"


def test_no_ignora_la_variable_y_deja_el_archivo_como_esta(config, variable, monkeypatch):
    espacio = _funciones_del_dag("preparar_config")
    espacio["RUNTIME_JSON"] = str(config)
    espacio["datetime"] = __import__("datetime").datetime
    espacio["os"] = __import__("os")
    config.write_text(json.dumps(variable, ensure_ascii=False, indent=2), encoding="utf-8")

    otra = copy.deepcopy(variable)
    otra["extraccion"]["fetchsize"] = 999
    _preparar_config(espacio, otra, monkeypatch)("diario", "no")

    assert json.loads(config.read_text(encoding="utf-8"))["extraccion"]["fetchsize"] != 999


def test_un_modo_de_publicacion_invalido_no_pasa(config, variable, monkeypatch):
    from airflow.exceptions import AirflowException

    espacio = _funciones_del_dag("preparar_config")
    espacio["RUNTIME_JSON"] = str(config)
    espacio["datetime"] = __import__("datetime").datetime
    espacio["os"] = __import__("os")
    with pytest.raises(AirflowException):
        _preparar_config(espacio, variable, monkeypatch)("diario", "quizas")


# ============================================================================
# 8. EL GRAFO
# ============================================================================
@pytest.fixture(scope="module")
def variable_publicada_en_airflow():
    """Publica la Variable del repo antes de que DagBag parsee el DAG.

    Hace falta porque este DAG dibuja una tarea por proceso y para eso lee la
    lista en tiempo de PARSEO: primero el JSON del volumen y, si no existe, la
    Variable. En una maquina de tests no hay volumen, asi que sin esto el DAG
    se dibuja con su tarea de respaldo 'configuracion_no_disponible' y los
    tests del grafo fallan por una causa que no tiene que ver con el grafo.

    Mismo patron que cargar_variables() de test_dags.py.
    """
    from airflow.models import Variable

    Variable.set(VARIABLE,
                 json.loads((DIR_JSON / f"{VARIABLE}.json").read_text(encoding="utf-8")),
                 serialize_json=True)


@pytest.fixture(scope="module")
def dag_spark(variable_publicada_en_airflow):
    """Parsea el DAG de verdad, con DagBag.

    Solo se miran los errores de ESTE archivo. DagBag recorre toda la carpeta,
    asi que un DAG ajeno roto -o una dependencia que no este instalada en la
    maquina donde corren los tests- haria fallar esta suite por algo que no
    tiene nada que ver con lo que prueba. Los errores de los demas DAGs son
    asunto de test_dags.py.
    """
    from airflow.models import DagBag

    bolsa = DagBag(str(DIR_PRODUCCION), include_examples=False)
    mios = {k: v for k, v in bolsa.import_errors.items()
            if Path(k).name == ARCHIVO_DAG.name}
    assert not mios, mios
    dag = bolsa.get_dag(DAG_ID)
    assert dag is not None, (
        f"{DAG_ID} no se registro. Errores de otros archivos de la carpeta, "
        f"por si fueran la causa: {bolsa.import_errors}")
    return dag


def test_el_grafo_tiene_una_tarea_por_proceso(dag_spark, variable):
    tareas = {t.task_id.split(".")[-1] for t in dag_spark.tasks
              if t.task_id.startswith("extraccion.")}
    esperadas = {t["nombre_proceso"].lower() for t in variable["tablas"]}
    assert tareas == esperadas, (
        "el grafo no dibuja exactamente un proceso por tabla declarada")


def test_los_grupos_son_por_prioridad(dag_spark):
    grupos = {g for g in dag_spark.task_group.get_task_group_dict() if g}
    assert "extraccion" in grupos
    assert any(g.startswith("extraccion.prioridad_") for g in grupos), (
        f"no hay grupos de prioridad: {sorted(grupos)}")


def test_las_prioridades_van_encadenadas_y_no_en_paralelo(dag_spark, variable):
    """prioridad 1 entera antes que prioridad 2: es lo que significa el campo y
    lo que hace el pipeline de pandas."""
    niveles = sorted({int(p.get("prioridad", 99))
                      for p in variable["extraccion"]["procesos"]})
    if len(niveles) < 2:
        pytest.skip("la Variable solo declara un nivel de prioridad")

    primera, segunda = niveles[0], niveles[1]
    de_la_segunda = [t for t in dag_spark.tasks
                     if t.task_id.startswith(f"extraccion.prioridad_{segunda}.")]
    assert de_la_segunda
    for tarea in de_la_segunda:
        arriba = {t.task_id for t in tarea.upstream_list}
        assert any(x.startswith(f"extraccion.prioridad_{primera}.") for x in arriba), (
            f"{tarea.task_id} no espera a la prioridad {primera}")


def test_cada_tarea_manda_su_propio_proceso_al_job(dag_spark):
    for tarea in dag_spark.tasks:
        if not tarea.task_id.startswith("extraccion."):
            continue
        args = list(getattr(tarea, "application_args", []) or [])
        assert "--proceso" in args, f"{tarea.task_id} no pasa --proceso"
        enviado = args[args.index("--proceso") + 1]
        assert enviado.lower() == tarea.task_id.split(".")[-1], (
            f"{tarea.task_id} manda --proceso {enviado!r}")


def test_el_punto_de_union_corre_aunque_haya_tablas_saltadas(dag_spark):
    """Saltarse tablas por calendario es el funcionamiento normal. Con la regla
    por defecto, una sola saltada dejaria la carga sin ejecutar."""
    union = dag_spark.get_task("extraccion_completa")
    assert str(union.trigger_rule) == "all_done"


def test_el_punto_de_union_espera_a_TODAS_las_tareas_de_extraccion(dag_spark):
    """extraccion_completa tiene que tener a CADA tarea de extraccion como
    padre DIRECTO, no alcanzarlas por transitividad.

    EL FALLO QUE ESTE TEST EXISTE PARA ATRAPAR
    ------------------------------------------
    El cableado era "grupo_extraccion >> tarea_extraccion_completa", que parece
    decir "espera al grupo entero" y no lo dice: Airflow conecta un TaskGroup
    por sus HOJAS, y como los niveles de prioridad van encadenados entre si,
    las hojas del grupo son solo las tareas del ULTIMO nivel. Las de prioridad
    1 llegaban al punto de union SOLO de forma transitiva, atravesando
    prioridad 2.

    Y una cadena transitiva se puede cortar. Corrida
    manual__2026-10-11T21:41:56:

        21:42:22.773  stg_msfd008 (prioridad 1) se SALTA: su tabla esta en "N"
        21:42:22.905  stg_fsh012 y stg_fsh031 (prioridad 2) quedan skipped con
                      duracion CERO, por propagacion de skip
        21:42:23.662  extraccion_completa ve todos sus upstream en estado
                      terminal y, con all_done, se da por satisfecha
        21:42:26      verificar_parquet consulta la bitacora y encuentra la
                      fila de FSH005 en EJECUTANDO, no en TERMINADO: devuelve
                      False y la carga queda en skipped
        21:43:03.623  stg_fsh005 termina y escribe TERMINADO, 32 s mas tarde.
                      Ya no habia nadie esperandolo

    Resultado: parquet correcto en disco, STG vacia, y NI UNA TAREA EN ROJO.
    Por eso se comprueba el padre DIRECTO: con la comprobacion transitiva este
    test pasaba con el bug puesto.
    """
    union = dag_spark.get_task("extraccion_completa")
    padres = {t.task_id for t in union.upstream_list}

    extraccion = {t.task_id for t in dag_spark.tasks
                  if t.task_id.startswith("extraccion.")}
    assert extraccion, "el grafo no dibujo ninguna tarea de extraccion"

    faltan = extraccion - padres
    assert not faltan, (
        "extraccion_completa NO espera directamente a: "
        + ", ".join(sorted(faltan))
        + ".\nSi su nivel de prioridad se salta, el punto de union se libera "
          "mientras estas tareas siguen corriendo, y verificar_parquet consulta "
          "la bitacora antes de que escriban TERMINADO.")


def test_un_salto_en_un_nivel_no_arrastra_a_los_niveles_siguientes(dag_spark, variable):
    """Las tareas de los niveles posteriores al primero van con all_done.

    El encadenamiento por prioridad expresa ORDEN, no condicion: "prioridad 2
    despues de prioridad 1", no "prioridad 2 solo si prioridad 1 salio bien".

    Con la regla por defecto (all_success), UNA sola tabla de prioridad 1
    saltada por calendario -el funcionamiento normal de casi cualquier dia-
    marcaba en skipped TODAS las de prioridad 2 sin ejecutarlas, con duracion
    cero. Se vio el 2026-10-11: stg_msfd008 se salto por activo "N" y arrastro
    a stg_fsh012 y stg_fsh031.

    No dio la cara porque esas dos tablas estaban inactivas de todas formas. En
    produccion, con todas activas, serian tablas que nadie extrajo y ninguna
    tarea en rojo que lo dijera.
    """
    niveles = sorted({int(p.get("prioridad", 99))
                      for p in variable["extraccion"]["procesos"]})
    if len(niveles) < 2:
        pytest.skip("la Variable solo declara un nivel de prioridad")

    malas = []
    for tarea in dag_spark.tasks:
        if not tarea.task_id.startswith("extraccion.prioridad_"):
            continue
        nivel = int(tarea.task_id.split(".")[1].rsplit("_", 1)[1])
        if nivel == niveles[0]:
            continue
        if str(tarea.trigger_rule) != "all_done":
            malas.append(f"{tarea.task_id} ({tarea.trigger_rule})")

    assert not malas, (
        "estas tareas se saltarian en cuanto se salte una tabla de un nivel "
        "anterior, sin llegar a ejecutarse: " + ", ".join(sorted(malas)))


def test_el_orden_de_las_cuatro_comprobaciones_previas(dag_spark):
    """Todo lo que puede fallar por configuracion falla ANTES de encender un
    executor: un error de configuracion descubierto dentro de un job de Spark
    llega envuelto en una traza de la JVM."""
    cadena = ["verificar_origen_destino", "preparar_config",
              "inventario_tablas", "validar_catalogo"]
    for arriba, abajo in zip(cadena, cadena[1:]):
        assert abajo in {t.task_id for t in dag_spark.get_task(arriba).downstream_list}

    primera_spark = [t for t in dag_spark.tasks if t.task_id.startswith("extraccion.")][0]
    anteriores = set()
    pendientes = list(primera_spark.upstream_list)
    while pendientes:
        t = pendientes.pop()
        if t.task_id in anteriores:
            continue
        anteriores.add(t.task_id)
        pendientes.extend(t.upstream_list)
    for comprobacion in cadena:
        assert comprobacion in anteriores, (
            f"{comprobacion} no corre antes de la extraccion")


def test_toda_tarea_de_spark_tiene_timeout_y_pool(dag_spark):
    for tarea in dag_spark.tasks:
        if not tarea.task_id.startswith("extraccion."):
            continue
        assert tarea.execution_timeout, f"{tarea.task_id} sin execution_timeout"
        assert tarea.pool, f"{tarea.task_id} sin pool"


def test_el_dag_no_fuerza_el_eventlog_a_false(dag_spark):
    """Era un parche mientras spark_events no estaba montado en los workers de
    Airflow. Con el volumen montado, dejarlo apagado ciega al History Server."""
    texto = ARCHIVO_DAG.read_text(encoding="utf-8")
    assert '"spark.eventLog.enabled": "false"' not in texto


# ============================================================================
# 9. EL COMPOSE
# ----------------------------------------------------------------------------
# En client mode el driver corre DENTRO del worker de Airflow, y es el driver
# quien escribe el event log. Sin el volumen montado ahi, SparkContext muere al
# crearse con un FileNotFoundException que no menciona ni a Airflow ni al
# eventLog: el sintoma era "al worker nuevo le falta una carpeta".
# ============================================================================
@pytest.mark.parametrize("archivo", ["docker-compose.windows.yml", "docker-compose.rhel.yml"])
@requiere_repo
def test_spark_events_esta_montado_en_los_servicios_de_airflow(archivo):
    yaml = pytest.importorskip("yaml")

    compose = yaml.safe_load((RAIZ / archivo).read_text(encoding="utf-8"))
    conf = (RAIZ / "spark" / "config" / "spark-defaults.conf").read_text(encoding="utf-8")
    if "spark.eventLog.enabled" not in conf or "true" not in conf.split("spark.eventLog.enabled")[1].split("\n")[0]:
        pytest.skip("el eventLog no esta activo en spark-defaults.conf")

    for servicio in ("airflow-scheduler", "airflow-worker"):
        montajes = [str(v) for v in (compose["services"][servicio].get("volumes") or [])]
        assert any("spark_events" in v for v in montajes), (
            f"{archivo}: {servicio} no monta spark_events, y spark-defaults.conf "
            f"activa el eventLog. En client mode el driver corre aqui y "
            f"SparkContext muere al crearse.")


@pytest.mark.parametrize("archivo", ["docker-compose.windows.yml", "docker-compose.rhel.yml"])
@requiere_repo
def test_el_worker_ve_los_jobs_de_spark(archivo):
    yaml = pytest.importorskip("yaml")

    compose = yaml.safe_load((RAIZ / archivo).read_text(encoding="utf-8"))
    montajes = [str(v) for v in compose["services"]["airflow-worker"]["volumes"]]
    assert any("/opt/spark-apps" in v for v in montajes), (
        "spark-submit corre desde el worker: sin este montaje no encuentra el .py")
    assert any("/opt/spark-data" in v for v in montajes), (
        "el JSON de runtime es el puente entre Airflow y Spark")


def test_sin_configuracion_el_dag_no_desaparece_de_la_interfaz(tmp_path, monkeypatch):
    """Un DAG ausente no se diagnostica; uno en rojo con un mensaje, si.

    Se comprueba con el archivo del DAG copiado a un sitio donde ni el JSON del
    volumen ni la Variable existan: tiene que registrarse igual, con una sola
    tarea que explique que revisar.
    """
    from airflow.models import DagBag

    carpeta = tmp_path / "dags"
    carpeta.mkdir()
    fuente = ARCHIVO_DAG.read_text(encoding="utf-8")
    fuente = fuente.replace(
        'RUNTIME_JSON = "/opt/spark-data/runtime/bt2sql_spark.json"',
        f'RUNTIME_JSON = "{tmp_path / "no-existe.json"}"')
    fuente = fuente.replace(f'VARIABLE = "{VARIABLE}"',
                            'VARIABLE = "NO_EXISTE_ESTA_VARIABLE"')
    (carpeta / ARCHIVO_DAG.name).write_text(fuente, encoding="utf-8")

    bolsa = DagBag(str(carpeta), include_examples=False)
    assert not bolsa.import_errors, bolsa.import_errors
    dag = bolsa.get_dag(DAG_ID)
    assert dag is not None, "el DAG desaparecio de la interfaz en vez de avisar"
    assert "configuracion_no_disponible" in {t.task_id.split(".")[-1] for t in dag.tasks}

# ---------------------------------------------------------------------------
# Escalar workers de Spark
# ---------------------------------------------------------------------------
@requiere_repo
@pytest.mark.parametrize("archivo", ["docker-compose.windows.yml", "docker-compose.rhel.yml"])
def test_cada_worker_de_spark_ve_la_carpeta_de_parquet(archivo):
    """La ruta de salida es LOCAL, no un sistema de archivos distribuido.

    `output_dir` es "/data/bt2sql/spark", sin esquema, asi que Spark la resuelve
    como `file:` -se vio literal en el error `Mkdirs failed to create
    file:/data/bt2sql/...`-. Y quien escribe los archivos `part-*` no es el
    driver, son los EXECUTORS, dentro del contenedor spark-worker.

    De ahi la consecuencia que no es evidente: cada worker de Spark tiene que
    ver esa misma carpeta, o escribe en el sistema de archivos de su propio
    contenedor y los archivos se pierden al reiniciarlo. Con `--scale` todas
    las replicas heredan los montajes del servicio, asi que basta con que el
    servicio los tenga; si alguien los quita de aqui, la carga empieza a
    fallar SOLO A VECES, segun que executor le toque la tarea de escritura.
    """
    yaml = pytest.importorskip("yaml")

    compose = yaml.safe_load((RAIZ / archivo).read_text(encoding="utf-8"))
    for servicio in ("spark-worker", "spark-master"):
        montajes = [str(v) for v in compose["services"][servicio]["volumes"]]
        assert any("BT2SQL_PARQUET_CONTAINER_DIR" in v or "/data/bt2sql" in v
                   for v in montajes), (
            f"{archivo}: {servicio} no monta la carpeta de parquet de bt2sql. "
            f"Los executors escriben los part-* ahi."
        )


@requiere_repo
@pytest.mark.parametrize("archivo", ["docker-compose.windows.yml", "docker-compose.rhel.yml"])
def test_el_worker_de_spark_escala_de_verdad(archivo):
    """Un `container_name` fijo hace que `--scale` falle con un choque de
    nombres: Docker no puede crear dos contenedores con el mismo nombre. El
    servicio tiene que quedarse SIN container_name para poder replicarse."""
    yaml = pytest.importorskip("yaml")

    compose = yaml.safe_load((RAIZ / archivo).read_text(encoding="utf-8"))
    worker = compose["services"]["spark-worker"]
    assert "container_name" not in worker, (
        f"{archivo}: spark-worker tiene container_name, asi que "
        f"`--scale spark-worker=3` falla con un nombre duplicado."
    )


@requiere_repo
@pytest.mark.parametrize("archivo", ["docker-compose.windows.yml", "docker-compose.rhel.yml"])
def test_el_worker_de_spark_espera_a_que_los_permisos_esten_puestos(archivo):
    """spark-init deja escribibles spark_data y spark_events. El worker es
    quien mas los usa, asi que tiene que esperarlo igual que el master."""
    yaml = pytest.importorskip("yaml")

    compose = yaml.safe_load((RAIZ / archivo).read_text(encoding="utf-8"))
    deps = compose["services"]["spark-worker"].get("depends_on") or {}
    assert "spark-init" in deps, (
        f"{archivo}: spark-worker no depende de spark-init. Un worker nuevo "
        f"puede arrancar antes de que los volumenes sean escribibles."
    )


@requiere_repo
def test_el_worker_remoto_de_la_documentacion_no_usa_la_imagen_oficial():
    """El `docker run` de ESCALAR_SPARK_WORKERS.md arrancaba con
    apache/spark:3.5.3, que NO trae los drivers JDBC: estan en la imagen
    propia. Un worker asi se registra bien en el master y falla dentro del
    job con "No suitable driver", que no apunta a la imagen por ningun lado."""
    doc = (RAIZ / "docs" / "ESCALAR_SPARK_WORKERS.md").read_text(encoding="utf-8")
    bloques = [b for b in doc.split("```") if "deploy.worker.Worker" in b
               and "docker run" in b]
    assert bloques, "no se encontro el ejemplo de worker remoto"
    for b in bloques:
        assert "apache/spark:" not in b, (
            "el ejemplo de worker remoto usa la imagen OFICIAL de Spark, sin "
            "los drivers JDBC. Tiene que usar la imagen propia (spark-bsg)."
        )
        assert "/data/bt2sql" in b, (
            "el ejemplo de worker remoto no monta la carpeta de parquet: sus "
            "executors escribirian dentro del contenedor y los archivos se "
            "perderian."
        )


# ----------------------------------------------------------------------------
# Probar cargar_recursos sin construir el DAG entero
# ----------------------------------------------------------------------------
def _cargar_dag_como_modulo(monkeypatch, variables: dict):
    """Extrae `cargar_recursos` del archivo del DAG y la compila sola.

    POR QUE ASI Y NO CON DagBag NI CON UN import DEL MODULO
    -------------------------------------------------------
    Lo que se prueba aqui es una funcion de veinte lineas que lee una Variable
    y decide numeros. Importar el modulo del DAG para llegar a ella construye
    el DAG completo -doce tareas, cuatro grupos, el operador de Spark, la
    lectura del JSON del volumen- y cualquier cosa que falle en ese camino
    aparece como un fallo de esta funcion. Ademas obliga a tener Airflow
    instalado y Variables publicadas solo para comprobar un diccionario.

    Con el arbol de sintaxis se saca EL CODIGO REAL del archivo -no una copia
    pegada en el test, que es lo que no detectaria nada si alguien lo cambia-
    y se ejecuta con un doble de Variable. Si se renombra la funcion o
    RECURSOS_DEFAULT, estos tests fallan al no encontrarlos, que es lo
    correcto.

    Devuelve un objeto con `cargar_recursos` y `RECURSOS_DEFAULT`, para que los
    tests se lean igual que si se hubiera importado el modulo.
    """
    import ast
    import logging
    import types

    arbol = ast.parse(ARCHIVO_DAG.read_text(encoding="utf-8"))

    def _busca_funcion(nombre):
        for n in arbol.body:
            if isinstance(n, ast.FunctionDef) and n.name == nombre:
                return n
        raise AssertionError(
            f"{ARCHIVO_DAG.name} ya no define {nombre}(). Si se renombro, hay "
            f"que actualizar este test; si se borro, revisar por que.")

    def _busca_asignacion(nombre):
        for n in arbol.body:
            if (isinstance(n, ast.Assign) and len(n.targets) == 1
                    and isinstance(n.targets[0], ast.Name)
                    and n.targets[0].id == nombre):
                return n
        raise AssertionError(f"{ARCHIVO_DAG.name} ya no define {nombre}")

    trozos = [_busca_asignacion("RECURSOS_DEFAULT"), _busca_funcion("cargar_recursos")]
    codigo = compile(ast.fix_missing_locations(ast.Module(body=trozos, type_ignores=[])),
                     str(ARCHIVO_DAG), "exec")

    class VariableFalsa:
        @staticmethod
        def get(nombre, deserialize_json=False):
            if nombre not in variables:
                raise KeyError(nombre)
            return variables[nombre]

    falso_airflow = types.ModuleType("airflow")
    falso_models = types.ModuleType("airflow.models")
    falso_models.Variable = VariableFalsa
    falso_airflow.models = falso_models
    monkeypatch.setitem(sys.modules, "airflow", falso_airflow)
    monkeypatch.setitem(sys.modules, "airflow.models", falso_models)

    # El logger del modulo: se le pasa el de este test para que caplog lo vea.
    ns = {"logger": logging.getLogger("dag_spark_bajo_prueba"),
          "os": os, "VARIABLE_SPARK": "CONFIGURACION_SPARK"}
    exec(codigo, ns)

    modulo = types.SimpleNamespace(
        cargar_recursos=ns["cargar_recursos"],
        RECURSOS_DEFAULT=ns["RECURSOS_DEFAULT"])
    return modulo


def test_los_recursos_se_leen_del_bloque_RECURSOS(monkeypatch):
    """Regresion del bug que hacia inutil toda la Variable CONFIGURACION_SPARK.

    cargar_recursos recorria config.items() -la RAIZ del JSON-, donde las
    claves son _DESCRIPCION, _ADVERTENCIA, RECURSOS, CONF... ninguna coincide
    con EXECUTOR_MEMORY. El bloque RECURSOS no se leia nunca, el job corria
    siempre con RECURSOS_DEFAULT, y agrandar el cluster no cambiaba nada.

    Los valores de este test son DISTINTOS de los defaults a proposito. El
    primer intento uso los del archivo del repositorio, y no detectaba nada:
    ese archivo esta dimensionado para el mismo worker de 2 cores que los
    defaults, asi que sus valores coinciden uno por uno y el test pasaba con
    el bug puesto.
    """
    modulo = _cargar_dag_como_modulo(monkeypatch, {"CONFIGURACION_SPARK": {
        "_DESCRIPCION": "texto que no debe confundir al lector de claves",
        "RECURSOS": {"EXECUTOR_MEMORY": "3g", "NUM_EXECUTORS": 4,
                     "CORES_MAX": 8, "EXECUTOR_CORES": 2},
        "CONF": {"spark.sql.adaptive.enabled": "true"},
    }})
    recursos = modulo.cargar_recursos()

    assert recursos["EXECUTOR_MEMORY"] == "3g"
    assert recursos["NUM_EXECUTORS"] == 4
    assert recursos["CORES_MAX"] == 8
    assert recursos["EXECUTOR_CORES"] == 2
    # Lo no declarado cae al default, no desaparece.
    assert recursos["DRIVER_MEMORY"] == modulo.RECURSOS_DEFAULT["DRIVER_MEMORY"]


@requiere_repo
def test_el_json_del_repositorio_declara_el_bloque_RECURSOS():
    """Lo que el test de arriba no puede comprobar: que el archivo real tenga
    las claves donde el codigo las busca. Es la otra mitad del contrato."""
    import json

    doc = json.loads((DIR_JSON / "CONFIGURACION_SPARK.json").read_text(encoding="utf-8"))
    assert isinstance(doc.get("RECURSOS"), dict), (
        "CONFIGURACION_SPARK.json no tiene un bloque RECURSOS; el DAG lo busca ahi")
    assert "CORES_MAX" in doc["RECURSOS"]


def test_la_forma_plana_de_la_variable_sigue_funcionando(monkeypatch):
    """Compatibilidad: quien tenga las claves en la raiz no se queda sin ellas."""
    modulo = _cargar_dag_como_modulo(
        monkeypatch,
        {"CONFIGURACION_SPARK": {"EXECUTOR_MEMORY": "7g", "CORES_MAX": 11}})
    recursos = modulo.cargar_recursos()

    assert recursos["EXECUTOR_MEMORY"] == "7g"
    assert recursos["CORES_MAX"] == 11


def test_una_variable_bien_puesta_NO_recibe_el_aviso_de_que_no_define_nada(monkeypatch, caplog):
    """El segundo bug, que estaba tapado por el primero.

    El aviso se decidia comparando el resultado con RECURSOS_DEFAULT. Como los
    valores del archivo del repositorio coinciden con los defaults, una
    Variable perfectamente puesta producia "no define ninguno de ...", que es
    lo contrario de lo que pasaba. Ahora se decide por las claves ENCONTRADAS.
    """
    import json

    doc = json.loads((DIR_JSON / "CONFIGURACION_SPARK.json").read_text(encoding="utf-8"))
    modulo = _cargar_dag_como_modulo(monkeypatch, {"CONFIGURACION_SPARK": doc})
    with caplog.at_level("WARNING"):
        modulo.cargar_recursos()

    assert "no define ninguno" not in caplog.text, (
        "la Variable declara RECURSOS y el DAG dice que no define nada:\n"
        + caplog.text)


def test_una_variable_sin_bloque_RECURSOS_si_recibe_el_aviso(monkeypatch, caplog):
    """La otra cara: el aviso tiene que seguir saliendo cuando toca."""
    modulo = _cargar_dag_como_modulo(
        monkeypatch, {"CONFIGURACION_SPARK": {"_DESCRIPCION": "solo texto", "CONF": {}}})
    with caplog.at_level("WARNING"):
        modulo.cargar_recursos()

    assert "no define ninguno" in caplog.text


def test_avisa_cuando_CORES_MAX_deja_el_cluster_sin_usar(monkeypatch, caplog):
    """El aviso que explica "agregue un worker y no pasa nada".

    CORES_MAX es un techo de la APLICACION, no del cluster: con CORES_MAX=2 y
    tres workers de 2 cores, el master le da 2 cores al job y los otros cuatro
    se quedan mirando, con los workers viendose sanos en su interfaz.
    """
    monkeypatch.setenv("SPARK_WORKER_REPLICAS", "3")
    monkeypatch.setenv("SPARK_WORKER_CORES", "2")

    modulo = _cargar_dag_como_modulo(
        monkeypatch, {"CONFIGURACION_SPARK": {"RECURSOS": {"CORES_MAX": 2}}})
    with caplog.at_level("WARNING"):
        modulo.cargar_recursos()

    assert "6 cores" in caplog.text, f"no aviso de la capacidad real:\n{caplog.text}"
    assert "4 cores de diferencia" in caplog.text


def test_no_avisa_cuando_CORES_MAX_cuadra_con_el_cluster(monkeypatch, caplog):
    """Un aviso que sale siempre deja de ser un aviso."""
    monkeypatch.setenv("SPARK_WORKER_REPLICAS", "3")
    monkeypatch.setenv("SPARK_WORKER_CORES", "2")

    modulo = _cargar_dag_como_modulo(
        monkeypatch, {"CONFIGURACION_SPARK": {"RECURSOS": {"CORES_MAX": 6}}})
    with caplog.at_level("WARNING"):
        modulo.cargar_recursos()

    assert "cores de diferencia" not in caplog.text


@requiere_repo
def test_el_compose_pasa_el_tamano_del_cluster_a_airflow():
    """Sin estas dos variables en el contenedor, el aviso de arriba compara
    contra los valores por defecto y nunca salta."""
    yaml = pytest.importorskip("yaml")

    for archivo in ("docker-compose.windows.yml", "docker-compose.rhel.yml",
                    "docker-compose.ubuntu.yml"):
        ruta = RAIZ / archivo
        if not ruta.exists():
            continue
        compose = yaml.safe_load(ruta.read_text(encoding="utf-8"))
        for servicio in ("airflow-scheduler", "airflow-worker"):
            env = compose["services"][servicio].get("environment") or {}
            claves = env if isinstance(env, dict) else {
                e.split("=")[0]: None for e in env}
            for necesaria in ("SPARK_WORKER_REPLICAS", "SPARK_WORKER_CORES"):
                assert necesaria in claves, (
                    f"{archivo}: {servicio} no recibe {necesaria}, asi que el "
                    f"DAG no puede saber de que tamano es el cluster.")


# ---------------------------------------------------------------------------
# El arranque en frio: la tarea 1 no puede exigir lo que crea la tarea 2
# ---------------------------------------------------------------------------
def test_la_compuerta_cero_no_exige_el_json_que_publica_la_tarea_siguiente():
    """Regresion de un circulo cerrado que impedia arrancar una instalacion nueva.

    verificar_origen_destino es la PRIMERA tarea del grafo; preparar_config,
    que escribe /opt/spark-data/runtime/bt2sql_spark.json, es la SEGUNDA. La
    compuerta llamaba a _config_publicada(), que exige ese archivo. En una
    instalacion nueva el DAG no arrancaba nunca, y el mensaje de error
    recomendaba relanzar con publicar_config='si' -un parametro que consume
    justamente la tarea que no llegaba a ejecutarse-.

    Se comprueba sobre el ARBOL DE SINTAXIS y no buscando texto: asi un
    comentario que mencione _config_publicada no cuenta como una llamada.
    """
    import ast

    arbol = ast.parse(ARCHIVO_DAG.read_text(encoding="utf-8"))
    compuerta = next(
        (n for n in ast.walk(arbol)
         if isinstance(n, ast.FunctionDef) and n.name == "verificar_origen_destino"),
        None)
    assert compuerta is not None, "el DAG ya no define verificar_origen_destino"

    llamadas = {n.func.id for n in ast.walk(compuerta)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert "_config_publicada" not in llamadas, (
        "verificar_origen_destino vuelve a exigir el JSON publicado, que lo "
        "escribe preparar_config DESPUES de ella. Una instalacion nueva no "
        "podra arrancar. Use _config_para_compuerta(), que cae a la Variable.")
    assert "_config_para_compuerta" in llamadas, (
        "verificar_origen_destino no lee la configuracion con "
        "_config_para_compuerta()")


def test_la_compuerta_cero_es_de_verdad_la_primera_tarea():
    """Lo que da sentido al test anterior. Si algun dia preparar_config pasara
    a ir primero, el circulo desapareceria y esa tolerancia dejaria de hacer
    falta; mientras el orden sea este, hace falta."""
    fuente = ARCHIVO_DAG.read_text(encoding="utf-8")
    i_preflight = fuente.index("tarea_preflight\n")
    i_config = fuente.index(">> tarea_config")
    assert i_preflight < i_config, (
        "preparar_config ya corre antes de la compuerta; revise si "
        "_config_para_compuerta sigue siendo necesaria")


def test_las_tareas_posteriores_SI_exigen_el_json_publicado():
    """La otra mitad del contrato, y la razon por la que no basta con relajar
    el lector en todas partes.

    inventario_tablas y validar_catalogo tienen que informar sobre lo que Spark
    VA A USAR. Con publicar_config='no' la Variable puede tener la edicion de
    manana mientras el volumen sigue con la de hoy; si leyeran la Variable, el
    inventario diria cinco tablas y Spark extraeria cuatro, y el log de Airflow
    mentiria sin que nada fallara.
    """
    import ast

    arbol = ast.parse(ARCHIVO_DAG.read_text(encoding="utf-8"))
    for nombre in ("inventario_tablas", "validar_catalogo"):
        fn = next((n for n in ast.walk(arbol)
                   if isinstance(n, ast.FunctionDef) and n.name == nombre), None)
        assert fn is not None, f"el DAG ya no define {nombre}"
        llamadas = {n.func.id for n in ast.walk(fn)
                    if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert "_config_publicada" in llamadas, (
            f"{nombre} ya no lee el JSON publicado. Corre despues de "
            f"preparar_config, asi que debe validar contra lo que Spark usara, "
            f"no contra la Variable.")
        assert "_config_para_compuerta" not in llamadas, (
            f"{nombre} usa el lector tolerante de la compuerta. Eso reintroduce "
            f"la deriva que _config_publicada existe para evitar.")


# ---------------------------------------------------------------------------
# Arranque en frio: los conn_id tambien salen de la Variable
# ---------------------------------------------------------------------------
def _cargar_conn_ids(monkeypatch, runtime_json, variable):
    """Extrae _conn_ids del DAG y la ejecuta con dobles. Mismo criterio que
    _cargar_dag_como_modulo: se prueba el codigo real, sin construir el DAG."""
    import ast
    import json
    import logging
    import types

    arbol = ast.parse(ARCHIVO_DAG.read_text(encoding="utf-8"))
    fn = next((n for n in arbol.body
               if isinstance(n, ast.FunctionDef) and n.name == "_conn_ids"), None)
    assert fn is not None, f"{ARCHIVO_DAG.name} ya no define _conn_ids()"

    codigo = compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])),
                     str(ARCHIVO_DAG), "exec")

    class VariableFalsa:
        @staticmethod
        def get(nombre, deserialize_json=False):
            if variable is None:
                raise KeyError(nombre)
            return variable

    falso = types.ModuleType("airflow")
    modelos = types.ModuleType("airflow.models")
    modelos.Variable = VariableFalsa
    falso.models = modelos
    monkeypatch.setitem(sys.modules, "airflow", falso)
    monkeypatch.setitem(sys.modules, "airflow.models", modelos)

    class RutaFalsa:
        def __init__(self, _):
            pass

        def read_text(self, encoding=None):
            if runtime_json is None:
                raise FileNotFoundError("/opt/spark-data/runtime/bt2sql_spark.json")
            return json.dumps(runtime_json)

    ns = {"logger": logging.getLogger("conn_ids_bajo_prueba"),
          "Path": RutaFalsa,
          "RUNTIME_JSON": "/opt/spark-data/runtime/bt2sql_spark.json",
          "VARIABLE": "BT2SQL_SPARK",
          "CONN_ORIGEN_DEFECTO": "CONEXION_BANTOTAL",
          "CONN_DESTINO_DEFECTO": "CONEXION_SQLSERVER"}
    exec(codigo, ns)
    return ns["_conn_ids"]()


def test_los_conn_id_salen_del_json_publicado_cuando_existe(monkeypatch):
    """Regimen normal: el archivo manda y no se consulta la base de metadatos."""
    origen, destino = _cargar_conn_ids(
        monkeypatch,
        runtime_json={"extraccion": {"conn_id_origen": "DEL_ARCHIVO_O",
                                     "conn_id_destino": "DEL_ARCHIVO_D"}},
        variable={"extraccion": {"conn_id_origen": "DE_LA_VARIABLE_O"}})
    assert (origen, destino) == ("DEL_ARCHIVO_O", "DEL_ARCHIVO_D")


def test_sin_json_publicado_los_conn_id_salen_de_la_variable(monkeypatch):
    """Regresion del arranque en frio.

    Sin el archivo, _conn_ids caia DIRECTO a los nombres por defecto. En una
    instalacion nueva nadie ha publicado nada, asi que el DAG usaba
    'CONEXION_BANTOTAL' pasara lo que pasara y la primera tarea fallaba con
    "The conn_id `CONEXION_BANTOTAL` isn't defined" mientras la Variable
    declaraba otra Connection. El aviso del log recomendaba declararla en la
    Variable, que era justo lo que ya estaba hecho.
    """
    origen, destino = _cargar_conn_ids(
        monkeypatch,
        runtime_json=None,
        variable={"extraccion": {"conn_id_origen": "CONEXION_SQLSERVER",
                                 "conn_id_destino": "CONEXION_SQLSERVER"}})
    assert origen == "CONEXION_SQLSERVER", (
        "sin JSON publicado, los conn_id tienen que salir de la Variable")
    assert destino == "CONEXION_SQLSERVER"


def test_sin_json_y_sin_variable_se_usan_los_nombres_por_defecto(monkeypatch):
    """El ultimo recurso sigue existiendo: el DAG no debe desaparecer de la
    interfaz por esto, porque un DAG ausente no se diagnostica."""
    origen, destino = _cargar_conn_ids(monkeypatch, runtime_json=None, variable=None)
    assert (origen, destino) == ("CONEXION_BANTOTAL", "CONEXION_SQLSERVER")


# ---------------------------------------------------------------------------
# El tipo, no solo el nombre
# ---------------------------------------------------------------------------
def test_la_compuerta_elige_el_conector_por_el_motor_no_por_el_nombre():
    """conexion_bantotal fija jdbc:as400:// y el driver de JTOpen en el codigo.

    Con conn_id_origen apuntando a SQL Server -como se prueba el pipeline sin
    el core a mano- armaba "jdbc:as400://host:1433" y cargaba el driver de
    IBM i contra SQL Server. El fallo que sale de ahi no menciona ni el motor
    ni el driver, asi que cuesta mucho llegar a la causa.
    """
    import ast

    arbol = ast.parse(ARCHIVO_DAG.read_text(encoding="utf-8"))
    compuerta = next((n for n in ast.walk(arbol)
                      if isinstance(n, ast.FunctionDef)
                      and n.name == "verificar_origen_destino"), None)
    assert compuerta is not None

    nombres = {n.id for n in ast.walk(compuerta) if isinstance(n, ast.Name)}
    assert "motor_de_conexion" in nombres, (
        "verificar_origen_destino no resuelve el motor del origen: volveria a "
        "asumir que es AS400 y fallaria con conn_id_origen apuntando a otro "
        "motor.")

    # Y no debe llamar a conexion_bantotal sin pasar por esa decision.
    llamadas_directas = [
        n for n in ast.walk(compuerta)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        and n.func.id == "conexion_bantotal"]
    assert not llamadas_directas, (
        "verificar_origen_destino llama a conexion_bantotal directamente. "
        "Tiene que elegir el conector segun el motor resuelto.")


def test_el_mapeo_de_motor_a_conector_cubre_los_dos_que_usa_el_proyecto():
    """mssql y odbc son los dos conn_type que spark_config trata como SQL
    Server; si la compuerta solo mirara 'mssql', una Connection declarada como
    odbc se abriria con el driver de IBM i."""
    fuente = (DIR_PLUGINS / "utils" / "spark_config.py").read_text(encoding="utf-8")
    assert 'conn_types=("mssql", "odbc")' in fuente, (
        "cambio el mapeo de SQL Server en spark_config; revise la compuerta")

    compuerta = ARCHIVO_DAG.read_text(encoding="utf-8")
    assert '("mssql", "odbc")' in compuerta, (
        "la compuerta no trata odbc como SQL Server, pero spark_config si")


# ---------------------------------------------------------------------------
# render_template_as_native_obj convierte el batch_id en int
# ---------------------------------------------------------------------------
def test_el_render_nativo_convierte_un_batch_id_en_entero():
    """La causa, demostrada con el mismo Jinja que usa Airflow.

    No es una suposicion sobre como funciona el render nativo: se ejecuta. Un
    batch_id "20261011144216" sale como ENTERO, y el provider de Spark agrega
    los application_args al comando TAL CUAL antes de hacer " ".join(...).
    """
    jinja2 = pytest.importorskip("jinja2")
    from jinja2.nativetypes import NativeEnvironment

    env = NativeEnvironment()
    valor = env.from_string("{{ b }}").render(b="20261011144216")
    assert isinstance(valor, int), (
        "si esto deja de ser int, el render nativo cambio de comportamiento y "
        "la conversion del operador quizas ya no haga falta")

    # Y el filtro | string NO lo arregla: literal_eval corre sobre el texto
    # final igual. Es la razon por la que el arreglo va en el operador.
    con_filtro = env.from_string("{{ b | string }}").render(b="20261011144216")
    assert isinstance(con_filtro, int), (
        "si | string ya bastara, el arreglo podria vivir en el DAG")


def test_el_operador_convierte_los_application_args_a_cadena():
    """El arreglo: una linea de comandos solo transporta cadenas."""
    fuente = (DIR_PLUGINS / "operators" / "spark_operator.py").read_text(encoding="utf-8")
    assert "str(a) for a in self.application_args" in fuente, (
        "BsgSparkSubmitOperator ya no convierte los application_args a cadena. "
        "Un batch_id entero -que es lo que produce el render nativo- rompe "
        "spark-submit con 'expected str instance, int found'.")


def test_los_argumentos_None_se_descartan_no_se_convierten():
    """str(None) seria 'None' en la linea de comandos, y el job lo recibiria
    como esa cadena: el mismo fallo silencioso que el render nativo existe para
    evitar, entrando por la puerta de atras."""
    fuente = (DIR_PLUGINS / "operators" / "spark_operator.py").read_text(encoding="utf-8")
    assert "if a is not None" in fuente, (
        "los application_args None tienen que descartarse, no convertirse")


def test_el_dag_conserva_el_render_nativo():
    """Lo que hace necesaria la conversion. Si alguien quita esta opcion para
    'arreglar' el int, reintroduce el fallo que documenta normalizar_batch_id:
    xcom_pull entrega la CADENA 'None' y la carga sale verde sin subir nada."""
    fuente = ARCHIVO_DAG.read_text(encoding="utf-8")
    assert "render_template_as_native_obj=True" in fuente


def test_el_log_del_operador_dice_el_motor_resuelto_no_generic():
    """Con Connections de tipo Generic, conn.conn_type es literal 'generic', y
    el log decia 'motor=generic' mientras el driver que cargaba era el de SQL
    Server. Un log que contradice al codigo cuesta mas que no tenerlo."""
    fuente = (DIR_PLUGINS / "operators" / "spark_operator.py").read_text(encoding="utf-8")
    assert "motor = motor_de_conexion(conn)" in fuente, (
        "el operador no resuelve el motor para el log")
    assert "motor_de_conexion" in fuente.split("from utils.spark_config import")[1].split(")")[0], (
        "falta importar motor_de_conexion en spark_operator.py")


# ---------------------------------------------------------------------------
# El classpath del driver en client mode
# ---------------------------------------------------------------------------
def test_el_operador_pone_los_jars_en_el_classpath_del_driver():
    """Regresion de ClassNotFoundException con el jar ya en --jars.

    Los jars JDBC se instalan en las imagenes para estar en el classpath "sin
    --jars y sin extraClassPath". Eso vale para los EXECUTORS, que corren la
    imagen de Spark. El DRIVER no: en client mode corre en airflow-worker, con
    la imagen de Airflow, donde los jars estan en /opt/airflow/jars y fuera del
    classpath de la JVM.

    Y --jars no lo cubre: la JVM del driver ya esta arrancada cuando se
    procesan, asi que jvm.Class.forName() va por el classloader del sistema.
    El log lo deja ver: "Added JAR ... mssql-jdbc.jar" y tres lineas despues
    ClassNotFoundException del driver que acaba de agregar.
    """
    fuente = (DIR_PLUGINS / "operators" / "spark_operator.py").read_text(encoding="utf-8")
    assert "spark.driver.extraClassPath" in fuente, (
        "el operador no pone los jars en el classpath del driver. Los jobs "
        "abren sus conexiones de control con jvm.Class.forName, que no ve los "
        "jars de --jars en client mode.")


def test_el_classpath_del_driver_no_pisa_lo_que_ya_hubiera():
    """Si alguien declaro su propio extraClassPath en CONFIGURACION_SPARK,
    sobreescribirlo seria peor que el problema que se arregla."""
    fuente = (DIR_PLUGINS / "operators" / "spark_operator.py").read_text(encoding="utf-8")
    bloque = fuente.split("spark.driver.extraClassPath")[0][-900:]
    assert "previo" in bloque or "get(\"spark.driver.extraClassPath\"" in fuente, (
        "el operador construye el extraClassPath sin leer el valor previo")


def test_la_tarea_de_extraccion_agrega_el_jar_del_destino_no_solo_su_clase():
    """Bug latente: la CLASE sin el JAR no sirve.

    El job de extraccion abre DOS conexiones con abrir_jdbc: el origen, cuyo
    jar agrega el operador desde jdbc_conn_id, y el control en SQL Server, cuya
    clase llega por DESTINO_DRIVER_CLASS. Sin su jar, jvm.Class.forName falla.

    No se veia porque en las pruebas origen y destino son el mismo motor. En
    produccion -origen AS400, control SQL Server- habria fallado en la primera
    conexion de control, con el mismo ClassNotFoundException y en otra linea.
    """
    import ast

    arbol = ast.parse(ARCHIVO_DAG.read_text(encoding="utf-8"))
    ejecutar = next(
        (n for n in ast.walk(arbol)
         if isinstance(n, ast.FunctionDef) and n.name == "execute"
         and any(isinstance(h, ast.Attribute) and h.attr == "env_vars"
                 for h in ast.walk(n))),
        None)
    assert ejecutar is not None, "no se encontro el execute de ConDestino"

    fuente = ast.get_source_segment(ARCHIVO_DAG.read_text(encoding="utf-8"), ejecutar) or ""
    assert "DESTINO_DRIVER_CLASS" in fuente, "cambio la forma de inyectar el destino"
    assert "merge_csv_values(self.jars" in fuente, (
        "ConDestino inyecta DESTINO_DRIVER_CLASS pero no agrega el jar del "
        "destino a self.jars. La clase sin el jar no se puede cargar.")


def test_los_dos_motores_del_pipeline_declaran_jar_distinto():
    """Lo que hace necesario el test anterior: si as400 y mssql compartieran
    jar, agregar el del destino seria redundante y nadie notaria si se quita."""
    from importlib.util import module_from_spec, spec_from_file_location

    ruta = DIR_PLUGINS / "utils" / "spark_config.py"
    fuente = ruta.read_text(encoding="utf-8")
    assert "jt400.jar" in fuente and "mssql-jdbc.jar" in fuente, (
        "los dos motores del pipeline tienen que declarar jars distintos")


# ---------------------------------------------------------------------------
# Driver y executors escriben en la misma carpeta con uid distinto
# ---------------------------------------------------------------------------
@requiere_repo
def test_el_umask_de_hadoop_permite_que_los_dos_uid_escriban():
    """Regresion del Mkdirs failed que cerro el pipeline.

    En client mode el driver corre en airflow-worker como uid 50000 y los
    executors en spark-worker como uid 185, y los dos escriben en el MISMO
    bind mount de parquet. Con el umask de Hadoop por defecto (022) el driver
    crea las carpetas del dia en 755 y dueno 50000; el executor no es dueno ni
    esta en ese grupo y no puede crear su subdirectorio de intento:

        java.io.IOException: Mkdirs failed to create
          file:/data/bt2sql/spark/.../FSH005/_temporary/0/_temporary/attempt_...

    El mensaje no menciona permisos ni uid. Y un `touch` en la raiz del
    montaje funciona -Docker la pone en 777- asi que la comprobacion obvia
    enganaba: lo que hereda el umask son las carpetas creadas DENTRO.
    """
    conf = (RAIZ / "spark" / "config" / "spark-defaults.conf").read_text(encoding="utf-8")
    lineas = [l for l in conf.splitlines()
              if l.strip() and not l.strip().startswith("#")
              and "fs.permissions.umask-mode" in l]
    assert lineas, (
        "spark-defaults.conf no declara spark.hadoop.fs.permissions.umask-mode. "
        "Sin el, el driver (uid 50000) crea las carpetas en 755 y el executor "
        "(uid 185) no puede escribir dentro.")
    valor = lineas[-1].split()[-1].strip()
    assert valor == "000", (
        f"umask-mode es {valor!r}; con cualquier valor que quite la escritura "
        f"a 'otros' el executor vuelve a fallar con Mkdirs failed.")


@requiere_repo
@pytest.mark.parametrize("archivo", ["docker-compose.windows.yml", "docker-compose.rhel.yml"])
def test_el_spark_defaults_llega_a_los_dos_lados(archivo):
    """La configuracion anterior solo sirve si la leen los DOS: el driver, que
    crea las carpetas, y los executors, que escriben dentro."""
    yaml = pytest.importorskip("yaml")

    compose = yaml.safe_load((RAIZ / archivo).read_text(encoding="utf-8"))
    for servicio in ("airflow-worker", "spark-worker"):
        montajes = [str(v) for v in compose["services"][servicio]["volumes"]]
        assert any("spark-defaults.conf" in v for v in montajes), (
            f"{archivo}: {servicio} no monta spark-defaults.conf, asi que no "
            f"ve el umask y vuelve el Mkdirs failed.")


# ---------------------------------------------------------------------------
# La retencion no puede mentir
# ---------------------------------------------------------------------------
def test_la_limpieza_comprueba_que_borro_de_verdad():
    """Regresion de un fallo silencioso que solo se nota meses despues.

    Estaba asi:
        shutil.rmtree(carpeta, ignore_errors=True)
        borradas.append(carpeta.name)

    ignore_errors se comia cualquier error y el nombre se agregaba igual, asi
    que el log decia "borradas: 3" con las tres carpetas intactas. La retencion
    dejaba de funcionar sin un solo aviso y el disco crecia.

    Y el borrado SI puede fallar: la tarea corre como uid 50000 y borra
    archivos escritos por los executors como uid 185.
    """
    import ast

    arbol = ast.parse(ARCHIVO_DAG.read_text(encoding="utf-8"))
    fn = next((n for n in ast.walk(arbol)
               if isinstance(n, ast.FunctionDef) and n.name == "limpiar_parquet"), None)
    assert fn is not None, "el DAG ya no define limpiar_parquet"

    fuente = ast.get_source_segment(ARCHIVO_DAG.read_text(encoding="utf-8"), fn) or ""
    lineas_codigo = [l for l in fuente.splitlines() if not l.strip().startswith("#")]
    codigo = "\n".join(lineas_codigo)

    assert "ignore_errors=True" not in codigo, (
        "rmtree con ignore_errors=True oculta los fallos de borrado y la "
        "retencion deja de funcionar en silencio")
    assert "except OSError" in codigo, (
        "el borrado no captura OSError: un fallo de permisos tumbaria la tarea "
        "sin decir que carpeta fue")
    assert "carpeta.exists()" in codigo, (
        "rmtree puede no lanzar y aun asi dejar la carpeta; hay que "
        "comprobarlo despues")


def test_la_limpieza_falla_en_rojo_si_no_pudo_borrar():
    """Un WARNING aqui es lo mismo que el bug: nadie lo lee. Y es la ultima
    tarea de la cadena, asi que un rojo dice "los datos se cargaron, la
    limpieza no", que es exacto y accionable."""
    import ast

    fuente_completa = ARCHIVO_DAG.read_text(encoding="utf-8")
    arbol = ast.parse(fuente_completa)
    fn = next(n for n in ast.walk(arbol)
              if isinstance(n, ast.FunctionDef) and n.name == "limpiar_parquet")

    lanza = [n for n in ast.walk(fn) if isinstance(n, ast.Raise)]
    assert lanza, (
        "limpiar_parquet no lanza ninguna excepcion cuando falla un borrado")

    fuente = ast.get_source_segment(fuente_completa, fn) or ""
    assert "umask-mode" in fuente, (
        "el mensaje de error no menciona el umask, que es la causa mas "
        "probable y la que costo una tarde encontrar")
