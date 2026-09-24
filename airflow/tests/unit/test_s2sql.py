"""
Tests del pipeline S2SQL (SingleStore -> Parquet -> SQL Server 2022).

No necesitan SingleStore ni SQL Server: las dos conexiones se sustituyen por
un doble que REGISTRA el SQL que se le manda. Eso permite comprobar lo unico
que de verdad hay que comprobar sin una base delante: que el SQL generado dice
lo que debe decir, y que los nombres que salen del catalogo no pueden colarse
sin validar.

El parquet si es real: se escribe y se lee con pyarrow de verdad, porque ahi es
donde aparecen los problemas de tipos.

Ejecucion:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/unit/test_s2sql.py -v

    o en local:
        pip install "apache-airflow==2.10.5" pytest pandas pyarrow
        AIRFLOW_HOME=/tmp/af pytest airflow/tests/unit/test_s2sql.py -v
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest


RAIZ = Path(__file__).resolve().parents[3]
DIR_DAGS = RAIZ / "airflow" / "dags"
DIR_PRODUCCION = DIR_DAGS / "production"
DIR_JSON = RAIZ / "airflow" / "config" / "json"

if str(DIR_PRODUCCION) not in sys.path:
    sys.path.append(str(DIR_PRODUCCION))


# ============================================================================
# DOBLES
# ============================================================================
class CursorFalso:
    """Cursor que apunta lo que se le pide y devuelve lo que se le programe."""

    def __init__(self, conexion):
        self.conexion = conexion
        self.description = None
        self.rowcount = -1
        self.fast_executemany = False

    def execute(self, sql, parametros=None):
        self.conexion.sql.append((" ".join(sql.split()), parametros))
        respuesta = self.conexion.respuestas.pop(0) if self.conexion.respuestas else None
        self._filas = respuesta if respuesta is not None else []
        self.description = [("col",)] if self._filas else None
        self.rowcount = self.conexion.rowcount
        return self

    def executemany(self, sql, filas):
        self.conexion.sql.append((" ".join(sql.split()), f"<{len(filas)} filas>"))
        self.conexion.insertadas.extend(filas)
        return self

    def fetchall(self):
        return list(self._filas)

    def fetchone(self):
        return self._filas[0] if self._filas else None

    def nextset(self):
        return False


class ConexionFalsa:
    def __init__(self, respuestas=None, rowcount=0):
        self.sql: list[tuple[str, object]] = []
        self.insertadas: list = []
        self.respuestas = list(respuestas or [])
        self.rowcount = rowcount
        self.commits = 0
        self.rollbacks = 0
        self.cerrada = False

    def cursor(self):
        return CursorFalso(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.cerrada = True

    def texto(self) -> str:
        return "\n".join(s for s, _ in self.sql)


@pytest.fixture(scope="session", autouse=True)
def variable_config():
    """Publica S2SQL_EXPORT_CONFIG desde el JSON versionado del repo."""
    from airflow.models import Variable

    archivo = DIR_JSON / "S2SQL_EXPORT_CONFIG.json"
    Variable.set("S2SQL_EXPORT_CONFIG", json.loads(archivo.read_text(encoding="utf-8")),
                 serialize_json=True)
    yield


@pytest.fixture
def config(variable_config):
    from etl_s2sql.s2sql_comun import cargar_config

    cargar_config.cache_clear()
    return cargar_config()


def trabajo_base(**extra):
    base = {
        "esquema_origen": "BDS", "tabla_origen": "CLIENTE",
        "esquema_destino": "dbo", "tabla_destino": "CLIENTE",
        "modo_carga": "REEMPLAZO", "columnas": ["ID_CLIENTE", "NOMBRE"],
        "filtro_where": "", "columna_marca": "", "claves_merge": [],
        "batch_filas": None, "batch_id": "20260924010000",
        "fecha_proceso": "2026-09-24", "marca_desde": None,
    }
    base.update(extra)
    return base


# ============================================================================
# SEGURIDAD: el catalogo es una tabla que alguien edita
# ============================================================================
@pytest.mark.parametrize("veneno", [
    "CLIENTE]; DROP TABLE dbo.CLIENTE --",
    "CLIENTE WITH (NOLOCK)",
    "dbo.CLIENTE",
    "CLIENTE'",
    "1_TABLA",
    "",
    None,
])
def test_un_nombre_de_tabla_raro_no_llega_al_sql(veneno):
    """Los nombres de objeto no admiten parametros enlazados en ningun motor:
    van concatenados. Como salen del catalogo, si no se validan, quien pueda
    editar esa tabla puede ejecutar lo que quiera con los permisos del pipeline."""
    from airflow.exceptions import AirflowException
    from etl_s2sql.s2sql_comun import validar_identificador

    with pytest.raises(AirflowException):
        validar_identificador(veneno, "prueba")


def test_un_nombre_normal_si_pasa():
    from etl_s2sql.s2sql_comun import validar_identificador

    for bueno in ("CLIENTE", "_tmp", "Tabla_2026", "ID_CLIENTE"):
        assert validar_identificador(bueno, "prueba") == bueno


# ============================================================================
# CATALOGO: las reglas de cada modo
# ============================================================================
def test_incremental_sin_columna_marca_se_rechaza():
    """Sin marca de agua, cada corrida traeria la tabla entera y la anadiria
    otra vez: el destino se duplicaria en silencio, corrida tras corrida."""
    from airflow.exceptions import AirflowException
    from etl_s2sql.s2sql_extraccion import validar_fila_catalogo

    fila = dict(esquema_origen="BDS", tabla_origen="MOV", esquema_destino="dbo",
                tabla_destino="MOV", modo_carga="INCREMENTAL", columnas=None,
                filtro_where=None, columna_marca=None, claves_merge=None, batch_filas=None)
    with pytest.raises(AirflowException, match="columna_marca"):
        validar_fila_catalogo(fila)


def test_merge_sin_claves_se_rechaza():
    from airflow.exceptions import AirflowException
    from etl_s2sql.s2sql_extraccion import validar_fila_catalogo

    fila = dict(esquema_origen="BDS", tabla_origen="CLI", esquema_destino="dbo",
                tabla_destino="CLI", modo_carga="MERGE", columnas=None,
                filtro_where=None, columna_marca=None, claves_merge=None, batch_filas=None)
    with pytest.raises(AirflowException, match="claves_merge"):
        validar_fila_catalogo(fila)


def test_merge_con_una_clave_que_no_esta_entre_las_columnas_se_rechaza():
    """El parquet solo lleva las columnas declaradas. Si la clave no esta
    entre ellas, el MERGE fallaria al no encontrarla en el origen."""
    from airflow.exceptions import AirflowException
    from etl_s2sql.s2sql_extraccion import validar_fila_catalogo

    fila = dict(esquema_origen="BDS", tabla_origen="CLI", esquema_destino="dbo",
                tabla_destino="CLI", modo_carga="MERGE", columnas="NOMBRE, SEGMENTO",
                filtro_where=None, columna_marca=None, claves_merge="ID_CLIENTE",
                batch_filas=None)
    with pytest.raises(AirflowException, match="no estan en la lista de columnas"):
        validar_fila_catalogo(fila)


def test_modo_desconocido_se_rechaza():
    from airflow.exceptions import AirflowException
    from etl_s2sql.s2sql_extraccion import validar_fila_catalogo

    fila = dict(esquema_origen="BDS", tabla_origen="X", esquema_destino="dbo",
                tabla_destino="X", modo_carga="UPSERT", columnas=None,
                filtro_where=None, columna_marca=None, claves_merge=None, batch_filas=None)
    with pytest.raises(AirflowException, match="no valido"):
        validar_fila_catalogo(fila)


# ============================================================================
# EL SELECT CONTRA EL ORIGEN
# ============================================================================
def test_el_valor_de_la_marca_va_enlazado_no_concatenado():
    """Es el unico dato de la consulta que viene de una corrida anterior.
    Concatenarlo seria meter en el SQL algo que salio de la base."""
    from etl_s2sql.s2sql_extraccion import construir_select

    sql, parametros = construir_select(trabajo_base(
        modo_carga="INCREMENTAL", columna_marca="FECHA_MOV", marca_desde="2026-09-01"))

    assert "[FECHA_MOV] > %s" in sql
    assert parametros == ["2026-09-01"]
    assert "2026-09-01" not in sql


def test_la_primera_corrida_se_trae_todo():
    from etl_s2sql.s2sql_extraccion import construir_select

    sql, parametros = construir_select(trabajo_base(
        modo_carga="INCREMENTAL", columna_marca="FECHA_MOV", marca_desde=None))
    assert "WHERE" not in sql
    assert parametros == []


def test_sin_columnas_declaradas_usa_asterisco():
    from etl_s2sql.s2sql_extraccion import construir_select

    sql, _ = construir_select(trabajo_base(columnas=[]))
    assert "SELECT * FROM [BDS].[CLIENTE]" in sql


# ============================================================================
# LOS TRES MODOS DE CARGA
# ============================================================================
def test_reemplazo_trunca_despues_de_llenar_la_staging(config):
    """El orden importa: si se truncara primero y el archivo estuviera
    corrupto, la tabla destino quedaria vacia. Aqui el TRUNCATE ocurre cuando
    los datos nuevos ya estan en la base, y ambos van en la misma transaccion."""
    from etl_s2sql.s2sql_carga import aplicar_reemplazo

    conn = ConexionFalsa()
    resultado = aplicar_reemplazo(conn, trabajo_base(), "[STG].[S2SQL_CLIENTE]",
                                  ["ID_CLIENTE", "NOMBRE"], escritas=500)
    texto = conn.texto()

    # No debe haber un BEGIN TRANSACTION explicito: la conexion JDBC va con
    # setAutoCommit(false) y el driver ya mantiene la transaccion. Los dos a la
    # vez dejan @@TRANCOUNT en 2 y producen "The COMMIT TRANSACTION request has
    # no corresponding BEGIN TRANSACTION" justo cuando algo ya fue mal.
    assert "BEGIN TRANSACTION" not in texto
    assert texto.index("TRUNCATE TABLE") < texto.index("INSERT INTO [dbo].[CLIENTE]")
    assert "FROM [STG].[S2SQL_CLIENTE]" in texto
    assert conn.commits == 1 and conn.rollbacks == 0
    # La cuenta NO sale de rowcount: con jaydebeapi vale -1 y el log mentiria.
    assert resultado == {"insertadas": 500, "actualizadas": 0}


def test_si_falla_el_insert_final_no_queda_la_tabla_truncada(config):
    from etl_s2sql.s2sql_carga import aplicar_reemplazo

    class ConexionQueFalla(ConexionFalsa):
        def cursor(self):
            cur = CursorFalso(self)
            ejecutar = cur.execute

            def execute(sql, parametros=None):
                if sql.strip().startswith("INSERT INTO [dbo]"):
                    raise RuntimeError("String or binary data would be truncated")
                return ejecutar(sql, parametros)

            cur.execute = execute
            return cur

    conn = ConexionQueFalla()
    with pytest.raises(RuntimeError):
        aplicar_reemplazo(conn, trabajo_base(), "[STG].[S2SQL_CLIENTE]", ["ID_CLIENTE"], 10)

    assert conn.rollbacks == 1, "el TRUNCATE tiene que deshacerse"
    assert conn.commits == 0


def test_incremental_no_toca_lo_que_ya_estaba(config):
    from etl_s2sql.s2sql_carga import aplicar_incremental

    conn = ConexionFalsa()
    aplicar_incremental(conn, trabajo_base(modo_carga="INCREMENTAL"),
                        "[STG].[S2SQL_CLIENTE]", ["ID_CLIENTE", "NOMBRE"], escritas=120)
    texto = conn.texto()

    assert "TRUNCATE" not in texto and "DELETE" not in texto
    assert "INSERT INTO [dbo].[CLIENTE]" in texto


def test_merge_lleva_holdlock_y_separa_insertadas_de_actualizadas(config):
    """HOLDLOCK no es opcional: sin el, MERGE tiene una condicion de carrera
    entre comprobar si la fila existe e insertarla, y dos cargas simultaneas
    violan la clave primaria."""
    from etl_s2sql.s2sql_carga import aplicar_merge

    conn = ConexionFalsa(respuestas=[
        [(0,)],    # verificar_claves_unicas: sin duplicados
        [(70,)],   # contar_coincidencias: 70 claves ya existen en el destino
    ])
    resultado = aplicar_merge(
        conn,
        trabajo_base(modo_carga="MERGE", claves_merge=["ID_CLIENTE"],
                     columnas=["ID_CLIENTE", "NOMBRE"]),
        "[STG].[S2SQL_CLIENTE]", ["ID_CLIENTE", "NOMBRE"], escritas=100)

    texto = conn.texto()
    assert "WITH (HOLDLOCK)" in texto
    assert "destino.[ID_CLIENTE] = origen.[ID_CLIENTE]" in texto
    # NOMBRE se actualiza; la clave no se toca en el SET.
    assert "UPDATE SET destino.[NOMBRE] = origen.[NOMBRE]" in texto
    # El MERGE va como UNA sentencia: nada de DECLARE + SELECT detras, porque
    # eso obligaria a recorrer varios result sets y jaydebeapi lo hace mal.
    assert "DECLARE" not in texto and "OUTPUT $action" not in texto
    # 100 filas en staging, 70 ya existian -> 30 nuevas.
    assert resultado == {"insertadas": 30, "actualizadas": 70}


def test_merge_con_la_clave_repetida_se_cancela_antes_de_tocar_nada(config):
    """Sin esta comprobacion, SQL Server aborta con el error 8672, que habla de
    'multiple source rows' y no dice ni la tabla ni la clave."""
    from airflow.exceptions import AirflowException
    from etl_s2sql.s2sql_carga import aplicar_merge

    conn = ConexionFalsa(respuestas=[[(4,)]])  # 4 claves repetidas
    with pytest.raises(AirflowException, match="repetido"):
        aplicar_merge(conn, trabajo_base(modo_carga="MERGE", claves_merge=["ID_CLIENTE"]),
                      "[STG].[S2SQL_CLIENTE]", ["ID_CLIENTE", "NOMBRE"], escritas=10)

    assert "MERGE" not in conn.texto()
    assert conn.commits == 0


# ============================================================================
# JDBC: lo que cambia respecto de ODBC
# ============================================================================
def test_se_elige_el_jar_segun_la_version_de_java(config, monkeypatch, tmp_path):
    """El sufijo jreNN del driver de Microsoft no es cosmetico: dice para que
    bytecode se compilo el jar. Cargar el equivocado da
    UnsupportedClassVersionError, cuyo mensaje habla de 'class file version
    55.0' y no menciona Java por ninguna parte."""
    from etl_s2sql import s2sql_comun

    def con_java(version_release: str):
        java_home = tmp_path / f"jdk-{version_release}"
        java_home.mkdir(exist_ok=True)
        (java_home / "release").write_text(f'JAVA_VERSION="{version_release}"\n')
        monkeypatch.setenv("JAVA_HOME", str(java_home))
        s2sql_comun.java_mayor.cache_clear()
        return s2sql_comun.java_mayor(), s2sql_comun.jar_sqlserver(config)

    mayor, jar = con_java("1.8.0_392")
    assert mayor == 8 and jar.endswith("mssql-jdbc-jre8.jar")

    mayor, jar = con_java("17.0.13")
    assert mayor == 17 and jar.endswith("mssql-jdbc.jar")

    mayor, jar = con_java("11.0.25")
    assert mayor == 11 and jar.endswith("mssql-jdbc.jar")


def test_jdbc_jar_de_la_variable_manda_sobre_la_deteccion(config, monkeypatch, tmp_path):
    from etl_s2sql import s2sql_comun

    java_home = tmp_path / "jdk8"
    java_home.mkdir()
    (java_home / "release").write_text('JAVA_VERSION="1.8.0_392"\n')
    monkeypatch.setenv("JAVA_HOME", str(java_home))
    s2sql_comun.java_mayor.cache_clear()

    cfg = {**config, "jdbc": {**config["jdbc"], "jar": "/ruta/propia/driver.jar"}}
    assert s2sql_comun.jar_sqlserver(cfg) == "/ruta/propia/driver.jar"


def test_la_url_jdbc_se_arma_con_las_propiedades_de_cifrado(config):
    from etl_s2sql.s2sql_comun import url_jdbc

    class ConnFalsa:
        host, port, schema = "10.0.0.10", 1433, "MIBASE"
        extra_dejson: dict = {}

    url = url_jdbc(ConnFalsa(), config)
    assert url.startswith("jdbc:sqlserver://10.0.0.10:1433;databaseName=MIBASE")
    # encrypt=true es el defecto desde el driver 10; contra un certificado
    # autofirmado hace falta ademas trustServerCertificate.
    assert "encrypt=true" in url and "trustServerCertificate=true" in url


def test_una_jdbc_url_completa_en_el_extra_se_usa_tal_cual(config):
    """Instancias con nombre, failover partner o Always On: la URL la da el
    area de base de datos y no hay que reconstruirla."""
    from etl_s2sql.s2sql_comun import url_jdbc

    propia = "jdbc:sqlserver://srv\\INST;databaseName=X;multiSubnetFailover=true"

    class ConnFalsa:
        host, port, schema = "otro", 1433, "otra"
        extra_dejson = {"jdbc_url": propia}

    assert url_jdbc(ConnFalsa(), config) == propia


def test_los_tipos_de_numpy_no_llegan_al_driver(config):
    """Es la trampa numero uno al pasar de ODBC a JDBC. pyodbc aceptaba un
    numpy.int64 sin rechistar; JPype no sabe convertirlo y falla con un error
    que solo nombra el tipo Java que esperaba."""
    import numpy as np
    import pandas as pd
    from etl_s2sql.s2sql_comun import filas_nativas

    df = pd.DataFrame({
        "ENTERO": np.array([1, 2], dtype="int64"),
        "DECIMAL": np.array([1.5, np.nan], dtype="float64"),
        "FECHA": pd.to_datetime(["2026-09-24", None]),
        "TEXTO": ["ana", None],
    })
    filas = filas_nativas(df)

    assert len(filas) == 2
    for fila in filas:
        for valor in fila:
            assert not isinstance(valor, np.generic), f"tipo de numpy sin convertir: {type(valor)}"
            assert not isinstance(valor, pd.Timestamp), "Timestamp sin convertir a datetime"

    assert isinstance(filas[0][0], int) and not isinstance(filas[0][0], np.generic)
    assert isinstance(filas[0][1], float)
    assert isinstance(filas[0][2], datetime)
    # Los nulos se detectan ANTES de convertir: despues un NaN es un float
    # corriente y se insertaria como tal.
    assert filas[1][1] is None and filas[1][2] is None and filas[1][3] is None


# ============================================================================
# EL PARQUET, DE VERDAD
# ============================================================================
def test_una_tabla_sin_filas_nuevas_no_trunca_el_destino(config, monkeypatch, tmp_path):
    """Es el caso que puede destruir datos. En modo REEMPLAZO, "el origen no
    cambio" y "el origen esta vacio" llevan a decisiones opuestas: la primera
    no debe tocar la tabla destino."""
    from etl_s2sql import s2sql_carga

    conn = ConexionFalsa()
    monkeypatch.setattr(s2sql_carga, "conexion_sqlserver", lambda *a, **k: conn)

    resultado = s2sql_carga.cargar_tabla(
        trabajo_base(estado="SIN_DATOS", ruta_parquet=None))

    assert resultado["estado_carga"] == "SIN_DATOS"
    assert conn.sql == [], "no se debe mandar ni una sentencia"


def test_el_parquet_se_lee_y_se_vuelca_por_lotes(config, tmp_path):
    """Ida y vuelta real con pyarrow, incluyendo NULLs: pyodbc no entiende NaN
    ni NaT y los insertaria como el texto 'nan' o fallaria en las numericas."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    from etl_s2sql.s2sql_carga import columnas_del_parquet, volcar_parquet_en_staging

    ruta = tmp_path / "CLIENTE.parquet"
    df = pd.DataFrame({
        "ID_CLIENTE": [1, 2, 3, 4, 5],
        "NOMBRE": ["ana", None, "luis", "eva", None],
        "SALDO": [10.5, 20.0, None, 40.25, 50.0],
    })
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), ruta)

    assert columnas_del_parquet(str(ruta)) == ["ID_CLIENTE", "NOMBRE", "SALDO"]

    conn = ConexionFalsa()
    escritas = volcar_parquet_en_staging(
        conn, config, trabajo_base(ruta_parquet=str(ruta), batch_filas=2),
        "[STG].[S2SQL_CLIENTE]", ["ID_CLIENTE", "NOMBRE", "SALDO"])

    assert escritas == 5
    assert len(conn.insertadas) == 5
    # Lo importante: ningun NaN/NaT llego al driver.
    for fila in conn.insertadas:
        for valor in fila:
            assert valor is None or valor == valor, f"NaN sin convertir en {fila}"
    assert conn.insertadas[1][1] is None
    assert conn.insertadas[2][2] is None


# ============================================================================
# LIMPIEZA
# ============================================================================
def test_la_limpieza_solo_toca_carpetas_con_nombre_de_fecha(config, monkeypatch, tmp_path):
    """La carpeta esta FUERA del contenedor y puede tener vecinos que no son de
    este pipeline. Borrar por 'todo lo que haya dentro' es como se borra el
    trabajo de otro equipo."""
    from etl_s2sql import s2sql_extraccion

    hoy = date.today()
    viejas = [(hoy - timedelta(days=d)).strftime("%Y%m%d") for d in (30, 20, 10)]
    nuevas = [(hoy - timedelta(days=d)).strftime("%Y%m%d") for d in (0, 1, 2)]
    intocables = ["backup", "NOTAS.txt", "2026", "logs_20260101"]

    for nombre in viejas + nuevas:
        (tmp_path / nombre).mkdir()
    for nombre in intocables:
        if "." in nombre:
            (tmp_path / nombre).write_text("no me borres")
        else:
            (tmp_path / nombre).mkdir()

    cfg = dict(config)
    cfg["parquet"] = {**config["parquet"], "directorio": str(tmp_path), "retencion_dias": 7}
    monkeypatch.setattr(s2sql_extraccion, "cargar_config", lambda: cfg)

    resumen = s2sql_extraccion.limpiar_parquet()

    assert sorted(resumen["borradas"]) == sorted(viejas)
    for nombre in nuevas + intocables:
        assert (tmp_path / nombre).exists(), f"{nombre} no se debia tocar"


def test_con_retencion_cero_no_borra_nada(config, monkeypatch, tmp_path):
    from etl_s2sql import s2sql_extraccion

    (tmp_path / "20200101").mkdir()
    cfg = dict(config)
    cfg["parquet"] = {**config["parquet"], "directorio": str(tmp_path), "retencion_dias": 0}
    monkeypatch.setattr(s2sql_extraccion, "cargar_config", lambda: cfg)

    assert s2sql_extraccion.limpiar_parquet()["borradas"] == []
    assert (tmp_path / "20200101").exists()


# ============================================================================
# EL DAG
# ============================================================================
@pytest.fixture(scope="module")
def dag_s2sql(variable_config):
    from airflow.models import DagBag

    bolsa = DagBag(str(DIR_DAGS), include_examples=False)
    assert not bolsa.import_errors, bolsa.import_errors
    return bolsa.get_dag("S2SQL_EXPORT")


def test_el_dag_se_registra_con_su_topologia(dag_s2sql):
    esperadas = {"inicio", "preparar_lote", "tabla.extraer", "tabla.cargar",
                 "cerrar_lote", "limpiar_parquet", "fin"}
    assert set(dag_s2sql.task_ids) == esperadas


def test_el_carril_por_tabla_esta_mapeado(dag_s2sql):
    """Es lo que da un par extraer/cargar por tabla, creado en ejecucion a
    partir del catalogo, sin consultar SQL Server al parsear el archivo."""
    from airflow.utils.task_group import MappedTaskGroup

    assert isinstance(dag_s2sql.task_group.get_child_by_label("tabla"), MappedTaskGroup)


def test_el_cierre_y_la_limpieza_corren_aunque_falle_una_tabla(dag_s2sql):
    for task_id in ("cerrar_lote", "limpiar_parquet", "fin"):
        assert dag_s2sql.get_task(task_id).trigger_rule == "all_done", task_id


def test_toda_tarea_tiene_timeout(dag_s2sql):
    sin_timeout = [t.task_id for t in dag_s2sql.tasks
                   if t.execution_timeout is None and t.task_id not in ("inicio", "fin")]
    assert not sin_timeout, f"sin execution_timeout: {sin_timeout}"


def test_el_render_nativo_esta_activo(dag_s2sql):
    """Sin esto, un xcom_pull vacio entrega la CADENA 'None', que es truthy.
    Ya costo un fallo silencioso en el pipeline BT."""
    assert dag_s2sql.render_template_as_native_obj is True


def test_el_dag_no_bloquea_corridas_para_siempre(dag_s2sql):
    assert dag_s2sql.max_active_runs == 1
    assert dag_s2sql.dagrun_timeout is not None


def test_toda_tarea_avisa_cuando_falla(dag_s2sql):
    sin_alerta = [t.task_id for t in dag_s2sql.tasks
                  if not t.on_failure_callback and t.task_id not in ("inicio", "fin")]
    assert not sin_alerta, f"sin on_failure_callback: {sin_alerta}"


# ============================================================================
# COHERENCIA ENTRE LA VARIABLE, EL .env Y LOS COMPOSE
# ============================================================================
def _leer_env() -> dict[str, str]:
    env, ruta = {}, RAIZ / ".env"
    if ruta.exists():
        for linea in ruta.read_text(encoding="utf-8", errors="ignore").splitlines():
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                clave, _, valor = linea.partition("=")
                env[clave.strip()] = valor.strip()
    return env


def test_la_ruta_de_parquet_dice_lo_mismo_en_la_variable_y_en_el_env(config):
    """Si no coinciden, la extraccion escribe en un sitio y la carga busca en
    otro, y el error no dice por que."""
    env = _leer_env()
    if "S2SQL_PARQUET_CONTAINER_DIR" not in env:
        pytest.skip("todavia no esta el bloque S2SQL en .env")
    assert config["parquet"]["directorio"] == env["S2SQL_PARQUET_CONTAINER_DIR"]


def test_una_sola_ruta_de_host_activa():
    ruta = RAIZ / ".env"
    if not ruta.exists():
        pytest.skip("no hay .env en el repo")
    activas = [l.strip() for l in ruta.read_text(encoding="utf-8", errors="ignore").splitlines()
               if l.strip().startswith("S2SQL_PARQUET_HOST_DIR=")]
    if not activas:
        pytest.skip("todavia no esta el bloque S2SQL en .env")
    assert len(activas) == 1, f"solo puede haber una sin comentar: {activas}"


def test_el_ancho_del_mensaje_de_error_coincide_con_el_ddl(config):
    """El codigo recorta a limites.mensaje_error. Si el DDL declarara menos,
    el INSERT del log fallaria y se perderia el error original."""
    ddl = (RAIZ / "sql" / "s2sql" / "10_s2sql_control_sqlserver.sql")
    if not ddl.exists():
        pytest.skip("falta el DDL")
    assert f"NVARCHAR({config['limites']['mensaje_error']})" in ddl.read_text(encoding="utf-8")
