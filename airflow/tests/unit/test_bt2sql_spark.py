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
from pathlib import Path

import pytest


RAIZ = Path(__file__).resolve().parents[3]
DIR_PRODUCCION = RAIZ / "airflow" / "dags" / "production"
DIR_PLUGINS = RAIZ / "airflow" / "plugins"
DIR_JSON = RAIZ / "airflow" / "config" / "json"
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


def test_sin_configuracion_los_conn_id_caen_al_respaldo_sin_romper_el_parseo(tmp_path):
    """Un DAG que no aparece en la interfaz porque revento al parsearse es
    mucho peor de diagnosticar que uno que aparece y falla en su primera
    tarea."""
    espacio = _funciones_del_dag("_conn_ids")
    espacio["RUNTIME_JSON"] = str(tmp_path / "no-existe.json")
    assert espacio["_conn_ids"] == espacio["_conn_ids"]
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
