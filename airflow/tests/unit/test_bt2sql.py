"""
Tests del pipeline BT2SQL (Bantotal -> Parquet -> STG en SQL Server 2022).

No hace falta ni Bantotal ni SQL Server: las dos conexiones se sustituyen por
un doble que REGISTRA el SQL que se le manda. Con eso se comprueba lo unico
que se puede comprobar sin una base delante, que es justo lo que mas se rompe:
que el SQL generado dice lo que debe decir, que las rutas caen donde deben, y
que nada que salga del catalogo llega concatenado sin validar.

El parquet SI es real: se escribe y se lee con pyarrow de verdad, porque los
problemas de tipos solo aparecen escribiendo el archivo.

Tampoco hace falta la JVM. atar_hilo_a_jvm y las dos funciones de conexion se
parchean, que es lo unico de este codigo que necesita Java.

Ejecucion:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/unit/test_bt2sql.py -v

    o en local:
        pip install "apache-airflow==2.10.5" pytest pandas pyarrow
        AIRFLOW_HOME=/tmp/af pytest airflow/tests/unit/test_bt2sql.py -v
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
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

    def execute(self, sql, parametros=None):
        self.conexion.sql.append((" ".join(sql.split()), parametros))
        respuesta = self.conexion.respuestas.pop(0) if self.conexion.respuestas else None
        self._filas = respuesta if respuesta is not None else []
        self.description = [("col",)] if self._filas else None
        return self

    def executemany(self, sql, filas):
        self.conexion.sql.append((" ".join(sql.split()), f"<{len(filas)} filas>"))
        self.conexion.insertadas.extend(filas)
        return self

    def fetchall(self):
        return list(self._filas)

    def fetchone(self):
        return self._filas[0] if self._filas else None

    def close(self):
        pass


class ConexionFalsa:
    def __init__(self, respuestas=None):
        self.sql: list[tuple[str, object]] = []
        self.insertadas: list = []
        self.respuestas = list(respuestas or [])
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


# ============================================================================
# FIXTURES
# ============================================================================
@pytest.fixture(scope="session", autouse=True)
def variables_publicadas():
    """Publica las dos Variables desde los JSON versionados del repo.

    Se leen del archivo y no de un diccionario escrito aqui a proposito: si
    alguien cambia una clave en el JSON y se olvida del codigo, estos tests
    fallan. Un diccionario copiado en el test no detectaria nada.
    """
    from airflow.models import Variable

    for nombre in ("BT2SQL_EXTRACCION", "BT2SQL_CARGA"):
        archivo = DIR_JSON / f"{nombre}.json"
        Variable.set(nombre, json.loads(archivo.read_text(encoding="utf-8")),
                     serialize_json=True)
    yield


@pytest.fixture
def cfg_ext(variables_publicadas):
    from etl_bt2sql.bt2sql_comun import cargar_config

    cargar_config.cache_clear()
    return cargar_config("BT2SQL_EXTRACCION")


@pytest.fixture
def cfg_carga(variables_publicadas):
    from etl_bt2sql.bt2sql_comun import cargar_config

    cargar_config.cache_clear()
    return cargar_config("BT2SQL_CARGA")


def fila_catalogo(**extra):
    base = {
        "ESQUEMA": "GPPPBTDB", "TABLA": "FSH005",
        "COLUMNAS": "PGCOD, FSH005FEC, FSH005TCV",
        "FILTRO": "", "ACTIVO": "S",
        "NOMBRE_PARQUET": "STG_FSH005", "TIPOS": None,
    }
    base.update(extra)
    return base


# ============================================================================
# LAS VARIABLES TIENEN QUE TENER LAS MISMAS CLAVES QUE LAS QUE YA FUNCIONAN
# ============================================================================
# El usuario pidio expresamente no inventar variables nuevas. Estos dos tests
# son los que lo hacen cumplir: si alguien agrega una clave a BT2SQL que no
# existe en la variable original, o se olvida de una, saltan.
# ============================================================================
def test_bt2sql_extraccion_tiene_las_claves_de_extraccion_bt_stg():
    original = json.loads((DIR_JSON / "EXTRACCION_BT_STG.json").read_text(encoding="utf-8"))
    nueva = json.loads((DIR_JSON / "BT2SQL_EXTRACCION.json").read_text(encoding="utf-8"))

    faltan = set(original) - set(nueva)
    assert not faltan, (
        f"BT2SQL_EXTRACCION no tiene estas claves de EXTRACCION_BT_STG: "
        f"{sorted(faltan)}. El codigo las lee por nombre."
    )

    # Lo unico que se permite de mas son los dos conn_id, que en el pipeline
    # de SingleStore estaban escritos en el codigo.
    sobran = set(nueva) - set(original)
    assert sobran <= {"conn_id_origen", "conn_id_destino"}, (
        f"BT2SQL_EXTRACCION tiene claves que no existen en la variable "
        f"original: {sorted(sobran - {'conn_id_origen', 'conn_id_destino'})}"
    )


def test_bt2sql_carga_tiene_las_claves_de_cargar_parquet_config():
    original = json.loads((DIR_JSON / "CARGAR_PARQUET_CONFIG.json").read_text(encoding="utf-8"))
    nueva = json.loads((DIR_JSON / "BT2SQL_CARGA.json").read_text(encoding="utf-8"))

    faltan = set(original) - set(nueva)
    assert not faltan, f"BT2SQL_CARGA no tiene: {sorted(faltan)}"

    sobran = set(nueva) - set(original)
    assert sobran <= {"conn_id_destino"}, f"claves de mas: {sorted(sobran)}"


def test_los_procesos_de_la_variable_cruzan_con_el_catalogo_de_ejemplo():
    """La doble llave (Variable + catalogo) es el error de configuracion mas
    comun del pipeline: si NOMBRE_PARQUET y ESQUEMA no coinciden exactamente
    con nombre_proceso y nombre_esquema, la tabla se extrae cero veces."""
    nueva = json.loads((DIR_JSON / "BT2SQL_EXTRACCION.json").read_text(encoding="utf-8"))
    sql = (RAIZ / "sql" / "bt2sql" / "21_bt2sql_catalogo_ejemplo.sql").read_text(encoding="utf-8")

    for proceso in nueva["procesos"]:
        nombre = proceso["nombre_proceso"]
        esquema = proceso["nombre_esquema"]
        assert f"'{nombre}'" in sql, (
            f"{nombre} esta en la Variable pero no en el catalogo de ejemplo: "
            f"nunca se extraeria."
        )
        assert f"'{esquema}'" in sql, f"el esquema {esquema} no esta en el catalogo"


# ============================================================================
# SEGURIDAD: el catalogo es una tabla que alguien edita
# ============================================================================
@pytest.mark.parametrize("veneno", [
    "FSH005; DROP TABLE STG_FSH005 --",
    "FSH005 WITH (NOLOCK)",
    "GPPPBTDB.FSH005",
    "FSH005'",
    "1_TABLA",
    "",
    None,
])
def test_un_nombre_raro_del_catalogo_no_llega_al_sql(veneno):
    """Los nombres de objeto no admiten parametros enlazados en ningun motor:
    van concatenados. Como salen de CTL_PARAMETROS_PARQUET, sin validarlos
    quien pueda editar esa tabla ejecuta lo que quiera con los permisos del
    pipeline, contra el core bancario."""
    from airflow.exceptions import AirflowException
    from etl_bt2sql.bt2sql_comun import validar_identificador

    with pytest.raises(AirflowException):
        validar_identificador(veneno, "prueba")


def test_un_nombre_normal_si_pasa():
    from etl_bt2sql.bt2sql_comun import validar_identificador

    for bueno in ("FSH005", "MSFD008", "GPPPBTDB", "STG_FSH005", "_tmp"):
        assert validar_identificador(bueno, "prueba") == bueno


def test_el_calificado_del_select_cuadra_con_la_propiedad_naming():
    """Las dos formas no son intercambiables y tienen que cuadrar:

        naming=sql     ->  GPPPBTDB.FSH005
        naming=system  ->  GPPPBTDB/FSH005

    Si no cuadran, el driver responde un -204 "objeto no encontrado" que parece
    un problema de permisos y manda a buscar donde no es. Este test es el que
    impide que alguien cambie una de las dos cosas sin la otra."""
    from etl_bt2sql.bt2sql_comun import PROPIEDADES_BANTOTAL
    from etl_bt2sql.bt2sql_extraccion import construir_select

    sql = construir_select(fila_catalogo())

    if "naming=sql" in PROPIEDADES_BANTOTAL:
        assert "GPPPBTDB.FSH005" in sql
        assert "GPPPBTDB/FSH005" not in sql
    else:
        assert "naming=system" in PROPIEDADES_BANTOTAL
        assert "GPPPBTDB/FSH005" in sql
        assert "GPPPBTDB.FSH005" not in sql


def test_el_filtro_del_catalogo_se_agrega_como_where():
    from etl_bt2sql.bt2sql_extraccion import construir_select

    sql = construir_select(fila_catalogo(FILTRO="FSH005FEC >= 20260101"))
    assert sql.rstrip().endswith("WHERE FSH005FEC >= 20260101")


def test_sin_columnas_trae_todas():
    from etl_bt2sql.bt2sql_extraccion import construir_select

    assert construir_select(fila_catalogo(COLUMNAS=None)).startswith("SELECT *")


# ============================================================================
# LAS CARPETAS: por dia y por tabla
# ============================================================================
# El usuario lo pidio con estas palabras: "debe de dejar las carpetas de
# extraccion por dia, por nombre _tabla". Estos tests son los que lo fijan.
# ============================================================================
def test_la_carpeta_es_por_dia_y_dentro_por_tabla(cfg_ext):
    from etl_bt2sql.bt2sql_comun import carpeta_de_tabla, carpeta_del_dia, ruta_parquet

    dia = date(2026, 9, 26)

    assert carpeta_del_dia(cfg_ext, dia).endswith(os.path.join("bt2sql", "20260926"))
    assert carpeta_de_tabla(cfg_ext, dia, "FSH005").endswith(
        os.path.join("20260926", "FSH005"))
    assert ruta_parquet(cfg_ext, dia, "FSH005").endswith(
        os.path.join("20260926", "FSH005", "FSH005.parquet"))


def test_la_ruta_queda_dentro_del_output_dir_configurado(cfg_ext):
    """La ruta que se guarda en la bitacora es la de DENTRO del contenedor. Si
    alguien cambiara output_dir a una ruta del host de Windows, las dos mitades
    del pipeline dejarian de encontrarse."""
    from etl_bt2sql.bt2sql_comun import ruta_parquet

    ruta = ruta_parquet(cfg_ext, date(2026, 9, 26), "FSH005")
    assert ruta.startswith(cfg_ext["output_dir"])
    assert not ruta.startswith("C:")
    assert "\\" not in cfg_ext["output_dir"]


def test_una_tabla_con_nombre_raro_no_escapa_de_la_carpeta(cfg_ext):
    """Sin validar, una TABLA con '../..' escribiria el parquet fuera del
    directorio de datos. Sale del catalogo, asi que es posible."""
    from airflow.exceptions import AirflowException
    from etl_bt2sql.bt2sql_comun import ruta_parquet

    with pytest.raises(AirflowException):
        ruta_parquet(cfg_ext, date(2026, 9, 26), "../../etc/passwd")


# ============================================================================
# LA FECHA DE PROCESO SALE DEL CORE
# ============================================================================
def test_la_fecha_acepta_el_numero_yyyymmdd_de_bantotal(cfg_ext):
    """FST017.PGFCIE es un DECIMAL(8,0): llega como 20260926 o como
    20260926.0, nunca como un date. Si no se acepta esa forma, la extraccion
    muere antes de leer una sola fila."""
    from etl_bt2sql.bt2sql_extraccion import leer_fecha_proceso

    for valor in (20260926, "20260926", 20260926.0, "20260926.0"):
        conn = ConexionFalsa(respuestas=[[(valor,)]])
        assert leer_fecha_proceso(conn.cursor(), cfg_ext) == date(2026, 9, 26)


def test_la_fecha_acepta_tambien_date_y_iso(cfg_ext):
    from etl_bt2sql.bt2sql_extraccion import leer_fecha_proceso

    for valor in (date(2026, 9, 26), datetime(2026, 9, 26, 3, 15), "2026-09-26"):
        conn = ConexionFalsa(respuestas=[[(valor,)]])
        assert leer_fecha_proceso(conn.cursor(), cfg_ext) == date(2026, 9, 26)


def test_sin_fecha_falla_con_un_mensaje_que_dice_que_consulta_fue(cfg_ext):
    from airflow.exceptions import AirflowException
    from etl_bt2sql.bt2sql_extraccion import leer_fecha_proceso

    conn = ConexionFalsa(respuestas=[[]])
    with pytest.raises(AirflowException) as err:
        leer_fecha_proceso(conn.cursor(), cfg_ext)
    assert "FST017" in str(err.value)


def test_una_fecha_ilegible_no_pasa_por_buena(cfg_ext):
    from airflow.exceptions import AirflowException
    from etl_bt2sql.bt2sql_extraccion import leer_fecha_proceso

    conn = ConexionFalsa(respuestas=[[("ayer",)]])
    with pytest.raises(AirflowException):
        leer_fecha_proceso(conn.cursor(), cfg_ext)


def test_la_consulta_de_fecha_va_contra_bantotal_no_contra_sql_server():
    """Es la unica diferencia de fondo con el pipeline de SingleStore, y el
    motivo esta explicado en leer_fecha_proceso: STG_FST017 en SQL Server la
    llena ESTE pipeline, asi que el primer dia estaria vacia."""
    cfg = json.loads((DIR_JSON / "BT2SQL_EXTRACCION.json").read_text(encoding="utf-8"))
    sql = cfg["sql_fecha"].upper()

    assert "FETCH FIRST" in sql, "sintaxis de DB2 for i, no de SQL Server"
    assert "SELECT TOP" not in sql, "TOP es T-SQL: esta consulta va contra el core"
    assert "STG_" not in sql, "no puede leer una tabla STG: es la que carga este pipeline"


# ============================================================================
# EL CRUCE VARIABLE <-> CATALOGO
# ============================================================================
def test_solo_entran_las_tablas_activas_en_los_dos_sitios(cfg_ext):
    from etl_bt2sql.bt2sql_extraccion import filtrar_procesos

    catalogo = [
        fila_catalogo(),                                        # si: en los dos
        fila_catalogo(TABLA="XXXXX", NOMBRE_PARQUET="STG_XXXXX"),  # no: no esta en la Variable
        fila_catalogo(TABLA="FSH031", NOMBRE_PARQUET="STG_FSH031"),  # si
    ]
    elegidas = {f["TABLA"] for f in filtrar_procesos(catalogo, cfg_ext, "diario")}
    assert elegidas == {"FSH005", "FSH031"}


def test_una_fila_inactiva_del_catalogo_no_entra(cfg_ext):
    from airflow.exceptions import AirflowException
    from etl_bt2sql.bt2sql_extraccion import filtrar_procesos

    with pytest.raises(AirflowException):
        filtrar_procesos([fila_catalogo(ACTIVO="N")], cfg_ext, "diario")


def test_el_calendario_semanal_no_arrastra_las_diarias(cfg_ext):
    """Las cuatro tablas de ejemplo tienen estado_semanal = 0. Una corrida
    semanal no debe traerlas 'por si acaso'."""
    from airflow.exceptions import AirflowException
    from etl_bt2sql.bt2sql_extraccion import filtrar_procesos

    with pytest.raises(AirflowException):
        filtrar_procesos([fila_catalogo()], cfg_ext, "semanal")


def test_un_tipo_de_ejecucion_inventado_falla_claro(cfg_ext):
    from airflow.exceptions import AirflowException
    from etl_bt2sql.bt2sql_extraccion import filtrar_procesos

    with pytest.raises(AirflowException) as err:
        filtrar_procesos([fila_catalogo()], cfg_ext, "trimestral")
    assert "diario" in str(err.value)


def test_el_orden_lo_manda_la_prioridad(cfg_ext):
    from etl_bt2sql.bt2sql_extraccion import filtrar_procesos

    catalogo = [
        fila_catalogo(TABLA="FSH031", NOMBRE_PARQUET="STG_FSH031"),   # prioridad 2
        fila_catalogo(TABLA="FSH005", NOMBRE_PARQUET="STG_FSH005"),   # prioridad 1
    ]
    orden = [f["TABLA"] for f in filtrar_procesos(catalogo, cfg_ext, "diario")]
    assert orden == ["FSH005", "FSH031"]


# ============================================================================
# LA BITACORA
# ============================================================================
def test_la_extraccion_abre_una_fila_por_tabla_con_el_batch_y_la_fecha(cfg_ext):
    from etl_bt2sql.bt2sql_extraccion import _abrir_log

    conn = ConexionFalsa(respuestas=[[(77,)]])
    id_log = _abrir_log(conn, cfg_ext, fila_catalogo(), "20260926041500", date(2026, 9, 26))

    assert id_log == 77
    sql, parametros = conn.sql[0]
    assert "INSERT INTO ctl_proceso_parquet" in sql
    assert "OUTPUT INSERTED.id_log" in sql, (
        "sin OUTPUT no hay forma de saber que fila se acaba de crear, y "
        "SCOPE_IDENTITY() no es fiable con varios hilos"
    )
    assert "20260926041500" in parametros
    assert date(2026, 9, 26) in parametros
    assert conn.commits == 1, "la fila de apertura se confirma ya: si no, un "\
                              "fallo duro no dejaria rastro de que se intento"


def test_la_ruta_completa_queda_guardada_al_cerrar(cfg_ext):
    """Es el punto de encuentro de las dos mitades: la carga lee de aqui, no
    de una ruta escrita en el codigo."""
    from etl_bt2sql.bt2sql_extraccion import _cerrar_log

    ruta = "/data/bt2sql/20260926/FSH005/FSH005.parquet"
    conn = ConexionFalsa()
    _cerrar_log(conn, cfg_ext, 77, "TERMINADO", filas=120, archivo=ruta, segundos=3.5)

    sql, parametros = conn.sql[0]
    assert "UPDATE ctl_proceso_parquet" in sql
    assert "COALESCE" in sql, (
        "sin COALESCE, cerrar con archivo=None borraria la ruta que ya estaba"
    )
    assert ruta in parametros


def test_un_fallo_al_escribir_la_bitacora_no_tapa_el_error_real(cfg_ext):
    """Si _cerrar_log lanzara dentro del except de la extraccion, el log de la
    tarea mostraria un problema de escritura en vez de la causa verdadera."""
    from etl_bt2sql.bt2sql_extraccion import _cerrar_log

    class ConexionRota(ConexionFalsa):
        def cursor(self):
            raise RuntimeError("la base se cayo")

    _cerrar_log(ConexionRota(), cfg_ext, 77, "ERROR", error="lo que fuera")


# ============================================================================
# LA CARGA
# ============================================================================
def test_con_batch_el_id_va_enlazado_y_no_concatenado(cfg_carga):
    from etl_bt2sql.bt2sql_carga import obtener_jobs

    conn = ConexionFalsa(respuestas=[[("/data/bt2sql/20260926/FSH005/FSH005.parquet",
                                       "STG_FSH005", 5000, 10)]])
    obtener_jobs(conn.cursor(), cfg_carga, "20260926041500")

    sql, parametros = conn.sql[0]
    assert "{BATCH_ID}" not in sql, "el token quedo sin sustituir"
    assert "20260926041500" not in sql, "el batch no puede ir dentro del texto del SQL"
    assert parametros == ("20260926041500",)


def test_sin_batch_cae_a_la_ultima_extraccion_terminada(cfg_carga):
    """Es el caso de relanzar solo la carga tras un clear de esa tarea. Tiene
    que funcionar, pero avisando: puede subir datos de un dia anterior."""
    from etl_bt2sql.bt2sql_carga import obtener_jobs

    conn = ConexionFalsa(respuestas=[[("/x/y.parquet", "STG_FSH005", 5000, 10)]])
    obtener_jobs(conn.cursor(), cfg_carga, None)

    sql, parametros = conn.sql[0]
    assert parametros is None or parametros == ()
    assert "MAX(p2.batch_id)" in sql
    assert "'TERMINADO'" in sql


def test_el_batch_id_string_None_no_pasa_por_bueno(cfg_carga):
    """Jinja convierte un XCom vacio en la CADENA 'None' salvo que el DAG lleve
    render_template_as_native_obj=True. Ese bug ya costo un fallo silencioso en
    el pipeline de SingleStore: la consulta buscaba batch_id = 'None', no
    encontraba nada, y el respaldo nunca se ejecutaba."""
    from etl_bt2sql.bt2sql_carga import obtener_jobs

    conn = ConexionFalsa(respuestas=[[("/x/y.parquet", "STG_FSH005", 5000, 10)]])
    obtener_jobs(conn.cursor(), cfg_carga, "None")
    _, parametros = conn.sql[0]
    assert parametros != ("None",), (
        "la cadena 'None' se estaria usando como batch_id de verdad"
    )


def test_el_dag_lleva_render_template_as_native_obj():
    """La defensa de verdad contra el bug de arriba esta en el DAG, no en el
    modulo. Este test lee el archivo porque importar el DAG requiere
    conexiones."""
    fuente = (DIR_PRODUCCION / "dag_bt2sql_stg.py").read_text(encoding="utf-8")
    assert "render_template_as_native_obj=True" in fuente


def test_sql_jobs_tiene_que_devolver_exactamente_cuatro_columnas(cfg_carga):
    """El codigo las desempaqueta por posicion. Si alguien agrega una columna
    al SELECT de la Variable, sin esta comprobacion los datos irian a la
    variable equivocada y nadie se enteraria hasta ver la tabla mal cargada."""
    from airflow.exceptions import AirflowException
    from etl_bt2sql.bt2sql_carga import obtener_jobs

    conn = ConexionFalsa(respuestas=[[("/x/y.parquet", "STG_FSH005", 5000)]])
    with pytest.raises(AirflowException) as err:
        obtener_jobs(conn.cursor(), cfg_carga, "20260926041500")
    assert "4" in str(err.value)


def test_la_staging_se_llama_como_la_destino_mas_el_sufijo(cfg_carga):
    from etl_bt2sql.bt2sql_carga import nombre_staging

    assert nombre_staging(cfg_carga, "STG_FSH005") == "[dbo].[STG_FSH005_STG]"
    assert nombre_staging(cfg_carga, "dbo.STG_FSH005") == "[dbo].[STG_FSH005_STG]"


def test_la_staging_copia_los_tipos_de_la_destino(cfg_carga):
    """SELECT TOP 0 INTO y no un CREATE TABLE escrito a mano: asi los tipos,
    longitudes y precisiones los copia el motor. Declarandolos aqui, un
    VARCHAR(50) en destino contra un VARCHAR(MAX) en staging pasa
    desapercibido hasta que una fila larga revienta el INSERT final, ya dentro
    de la transaccion."""
    from etl_bt2sql.bt2sql_carga import recrear_staging

    conn = ConexionFalsa()
    recrear_staging(conn, cfg_carga, "STG_FSH005", ["FECHA_PROCESO", "PGCOD", "BATCH_ID"])

    texto = conn.texto()
    assert "SELECT TOP 0" in texto
    assert "INTO [dbo].[STG_FSH005_STG]" in texto
    assert "FROM [dbo].[STG_FSH005]" in texto
    assert "DROP TABLE IF EXISTS" in texto


def test_el_truncate_va_en_la_misma_transaccion_que_el_insert(cfg_carga):
    """El orden importa mas de lo que parece. Si se truncara la destino ANTES
    de leer el parquet y el archivo estuviera corrupto, la tabla quedaria vacia
    hasta la corrida siguiente. Haciendolo al final, solo se vacia cuando los
    datos nuevos ya estan dentro de la base.

    Detalle de SQL Server que no se cumple en MySQL ni SingleStore: aqui
    TRUNCATE SI es transaccional y un ROLLBACK lo deshace."""
    from etl_bt2sql.bt2sql_carga import aplicar_a_destino

    conn = ConexionFalsa()
    aplicar_a_destino(conn, cfg_carga, "STG_FSH005", "[dbo].[STG_FSH005_STG]",
                      ["FECHA_PROCESO", "PGCOD", "BATCH_ID"])

    texto = conn.texto()
    pos_truncate = texto.find("TRUNCATE TABLE")
    pos_insert = texto.find("INSERT INTO [dbo].[STG_FSH005]")
    assert pos_truncate != -1 and pos_insert != -1
    assert pos_truncate < pos_insert
    assert conn.commits == 1


def test_si_el_insert_falla_se_hace_rollback(cfg_carga):
    """Es lo que salva la tabla destino: el TRUNCATE ya se ejecuto, y sin el
    rollback quedaria vacia."""
    from etl_bt2sql.bt2sql_carga import aplicar_a_destino

    class ConexionQueFalla(ConexionFalsa):
        def cursor(self):
            cur = super().cursor()
            ejecutar = cur.execute

            def execute(sql, parametros=None):
                if "INSERT INTO" in sql:
                    raise RuntimeError("se acabo el espacio")
                return ejecutar(sql, parametros)

            cur.execute = execute
            return cur

    conn = ConexionQueFalla()
    with pytest.raises(RuntimeError):
        aplicar_a_destino(conn, cfg_carga, "STG_FSH005", "[dbo].[STG_FSH005_STG]",
                          ["PGCOD"])
    assert conn.rollbacks == 1
    assert conn.commits == 0


def test_no_se_abre_una_transaccion_explicita(cfg_carga):
    """jaydebeapi con autocommit=False YA tiene una transaccion abierta. Un
    BEGIN TRANSACTION encima deja @@TRANCOUNT en 2, y entonces el primer
    COMMIT no confirma nada: solo baja el contador a 1. Es un fallo que no da
    ningun error y deja los datos sin guardar."""
    from etl_bt2sql.bt2sql_carga import aplicar_a_destino

    conn = ConexionFalsa()
    aplicar_a_destino(conn, cfg_carga, "STG_FSH005", "[dbo].[STG_FSH005_STG]",
                      ["PGCOD"])
    assert "BEGIN TRANSACTION" not in conn.texto().upper()


# ============================================================================
# EL PARQUET, DE VERDAD
# ============================================================================
def test_fecha_proceso_va_primera_y_batch_id_ultima(tmp_path):
    """La carga arma el INSERT leyendo el esquema del propio parquet, asi que
    el orden y los nombres tienen que ser exactos o SQL Server responde
    'Invalid column name'."""
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    bloque = pd.DataFrame({"PGCOD": [1, 2], "FSH005TCV": [3.75, 3.80]})
    bloque.insert(0, "FECHA_PROCESO", date(2026, 9, 26))
    bloque["BATCH_ID"] = "20260926041500"

    ruta = tmp_path / "FSH005.parquet"
    tabla = pa.Table.from_pandas(bloque, preserve_index=False)
    pq.write_table(tabla, ruta)

    columnas = list(pq.read_table(ruta).to_pandas().columns)
    assert columnas[0] == "FECHA_PROCESO"
    assert columnas[-1] == "BATCH_ID"


def test_un_segundo_bloque_se_fuerza_al_esquema_del_primero(tmp_path):
    """Sin fijar el esquema, pandas infiere int64 en un bloque y float64 en el
    siguiente por un solo NULL, y ParquetWriter aborta a mitad del archivo
    dejando un parquet corrupto que la carga no sabe distinguir de uno bueno."""
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    ruta = tmp_path / "x.parquet"
    primero = pd.DataFrame({"A": [1, 2]})
    tabla = pa.Table.from_pandas(primero, preserve_index=False)
    escritor = pq.ParquetWriter(ruta, tabla.schema)
    escritor.write_table(tabla)

    segundo = pd.DataFrame({"A": [3.0, None]})          # float por el NULL
    escritor.write_table(pa.Table.from_pandas(
        segundo, schema=tabla.schema, preserve_index=False, safe=False))
    escritor.close()

    assert len(pq.read_table(ruta)) == 4


def test_los_tipos_de_numpy_se_convierten_a_nativos():
    """JPype no sabe convertir un numpy.int64 a un tipo de Java: falla con un
    'No matching overloads' que no menciona numpy por ninguna parte. Y NaN
    tiene que llegar como NULL, no como el texto 'nan'."""
    pytest.importorskip("numpy")
    import numpy as np

    from etl_bt2sql.bt2sql_comun import filas_nativas

    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"A": np.array([1, 2], dtype="int64"),
                       "B": np.array([1.5, np.nan], dtype="float64")})

    filas = filas_nativas(df)
    assert filas[0] == (1, 1.5)
    assert filas[1][1] is None
    for fila in filas:
        for valor in fila:
            assert not isinstance(valor, np.generic), f"{valor!r} sigue siendo de numpy"


# ============================================================================
# LA COMPUERTA
# ============================================================================
def test_sin_archivos_la_carga_se_salta_en_vez_de_fallar(monkeypatch, cfg_ext):
    """Una corrida en la que no habia nada que extraer no es un fallo. Dejarla
    en rojo entrena al equipo a ignorar los rojos, que es peor que no tener
    alertas."""
    from etl_bt2sql import bt2sql_carga

    monkeypatch.setattr(bt2sql_carga, "conexion_sqlserver",
                        lambda *a, **k: ConexionFalsa(respuestas=[[]]))
    assert bt2sql_carga.verificar_parquet("20260926041500") is False


def test_si_la_bitacora_dice_que_hay_archivo_y_no_esta_en_disco_se_cancela(monkeypatch):
    """Cargar solo una parte dejaria el dia incompleto sin que nada lo
    advirtiera. Y este es el sintoma tipico de que la carpeta externa no esta
    montada en el worker que tomo la tarea."""
    from airflow.exceptions import AirflowException
    from etl_bt2sql import bt2sql_carga

    monkeypatch.setattr(
        bt2sql_carga, "conexion_sqlserver",
        lambda *a, **k: ConexionFalsa(respuestas=[[("/no/existe/FSH005.parquet",)]]))

    with pytest.raises(AirflowException):
        bt2sql_carga.verificar_parquet("20260926041500")


# ============================================================================
# LA LIMPIEZA
# ============================================================================
def test_solo_borra_carpetas_con_nombre_de_fecha(monkeypatch, tmp_path, cfg_ext):
    """La carpeta esta FUERA del contenedor y puede tener vecinos que no son de
    este pipeline. Borrar por antiguedad sin mirar el nombre seria destruir
    datos ajenos."""
    from etl_bt2sql import bt2sql_extraccion

    for nombre in ("20200101", "20200102", "no_tocar", "backup_2020"):
        (tmp_path / nombre).mkdir()
    (tmp_path / "suelto.txt").write_text("x")

    cfg = dict(cfg_ext, output_dir=str(tmp_path), retencion_dias=7)
    monkeypatch.setattr(bt2sql_extraccion, "config_extraccion", lambda: cfg)

    bt2sql_extraccion.limpiar_parquet()

    quedan = {p.name for p in tmp_path.iterdir()}
    assert quedan == {"no_tocar", "backup_2020", "suelto.txt"}


def test_con_retencion_cero_no_borra_nada(monkeypatch, tmp_path, cfg_ext):
    from etl_bt2sql import bt2sql_extraccion

    (tmp_path / "20200101").mkdir()
    cfg = dict(cfg_ext, output_dir=str(tmp_path), retencion_dias=0)
    monkeypatch.setattr(bt2sql_extraccion, "config_extraccion", lambda: cfg)

    bt2sql_extraccion.limpiar_parquet()
    assert (tmp_path / "20200101").exists()


def test_la_carpeta_de_hoy_nunca_se_borra(monkeypatch, tmp_path, cfg_ext):
    from etl_bt2sql import bt2sql_extraccion

    hoy = date.today().strftime("%Y%m%d")
    (tmp_path / hoy).mkdir()
    cfg = dict(cfg_ext, output_dir=str(tmp_path), retencion_dias=1)
    monkeypatch.setattr(bt2sql_extraccion, "config_extraccion", lambda: cfg)

    bt2sql_extraccion.limpiar_parquet()
    assert (tmp_path / hoy).exists()


# ============================================================================
# LA JVM
# ============================================================================
def test_los_hilos_se_atan_a_la_jvm_antes_de_tocar_java():
    """Sin esto la JVM no lanza una excepcion: ABORTA EL PROCESO del worker.
    La tarea queda zombie y en el log no hay traza, solo un corte a media
    frase. Es de los fallos mas caros de diagnosticar, y como la extraccion usa
    ThreadPoolExecutor, es un fallo que ocurriria siempre, no a veces."""
    fuente = (DIR_PRODUCCION / "etl_bt2sql" / "bt2sql_extraccion.py").read_text(encoding="utf-8")

    pos_atar = fuente.find("atar_hilo_a_jvm()")
    pos_conexion = fuente.find("conexion_bantotal(config)", fuente.find("def extraer_tabla"))
    assert pos_atar != -1, "extraer_tabla corre en un hilo y no ata el hilo a la JVM"
    assert pos_atar < pos_conexion, "se ata DESPUES de abrir la conexion: demasiado tarde"


def test_la_jvm_arranca_con_los_dos_jars_de_una_vez():
    """JPype levanta UNA sola JVM por proceso y no se le puede ampliar el
    classpath despues. Arrancarla solo con el jar de Bantotal dejaria la carga
    a SQL Server sin driver y sin forma de arreglarlo en caliente."""
    fuente = (DIR_PRODUCCION / "etl_bt2sql" / "bt2sql_comun.py").read_text(encoding="utf-8")
    inicio = fuente.find("def _arrancar_jvm")
    fin = fuente.find("\ndef ", inicio + 10)
    cuerpo = fuente[inicio:fin]

    assert "JAR_BANTOTAL" in cuerpo and "JAR_SQLSERVER" in cuerpo


def test_los_jars_se_buscan_donde_los_deja_el_dockerfile():
    """Es la misma carpeta donde caen los descargados y los de airflow/jars/.
    Si divergieran, el driver estaria en la imagen y el codigo no lo veria."""
    from etl_bt2sql.bt2sql_comun import JAR_BANTOTAL, JAR_SQLSERVER

    for jar in (JAR_BANTOTAL, JAR_SQLSERVER):
        assert jar.startswith("/opt/airflow/jars/"), jar


# ============================================================================
# EL DAG
# ============================================================================
def test_el_dag_no_importa_nada_pesado_al_parsearse():
    """El scheduler parsea este archivo cada 30 segundos. Importar jaydebeapi
    arriba levantaria una JVM en cada pasada, y un driver que falte en un
    worker tumbaria el archivo entero con un Broken DAG en vez de fallar solo
    esa tarea con su traza."""
    fuente = (DIR_PRODUCCION / "dag_bt2sql_stg.py").read_text(encoding="utf-8")
    cabecera = fuente[:fuente.find("# CALLABLES")]

    for pesado in ("jaydebeapi", "jpype", "pyarrow", "import pandas"):
        assert pesado not in cabecera, f"{pesado} se importa al parsear el DAG"


def test_el_dag_tiene_dagrun_timeout():
    """Con max_active_runs=1, una corrida atascada bloquea TODAS las
    siguientes. execution_timeout protege cada tarea pero no el tiempo en
    queued ni en up_for_retry."""
    fuente = (DIR_PRODUCCION / "dag_bt2sql_stg.py").read_text(encoding="utf-8")
    assert "dagrun_timeout=" in fuente


def test_todas_las_tareas_tienen_execution_timeout():
    """Sin el, una consulta trabada contra el core deja la tarea en running
    indefinidamente y, con max_active_runs=1, el DAG entero parado."""
    fuente = (DIR_PRODUCCION / "dag_bt2sql_stg.py").read_text(encoding="utf-8")
    assert fuente.count("execution_timeout=") == fuente.count("task_id=")


def test_la_limpieza_corre_aunque_falle_la_carga():
    """Si no, las carpetas viejas se acumulan justo los dias con problemas, que
    es cuando menos falta hace quedarse sin disco."""
    fuente = (DIR_PRODUCCION / "dag_bt2sql_stg.py").read_text(encoding="utf-8")
    assert 'trigger_rule="all_done"' in fuente


def test_el_cortocircuito_no_se_lleva_por_delante_la_limpieza():
    """Sin ignore_downstream_trigger_rules=False, el ShortCircuit salta TODO lo
    que viene despues, incluida la limpieza, que tiene all_done justamente para
    correr siempre."""
    fuente = (DIR_PRODUCCION / "dag_bt2sql_stg.py").read_text(encoding="utf-8")
    assert "ignore_downstream_trigger_rules=False" in fuente


def test_la_carpeta_de_bt2sql_esta_montada_en_todos_los_servicios():
    """El bind mount tiene que existir en los cinco servicios de los dos
    compose. Si falta en uno, ese worker no ve los archivos y la carga falla
    SOLO A VECES, segun que worker tome la tarea, que es la forma mas cara de
    que falle algo."""
    for nombre in ("docker-compose.windows.yml", "docker-compose.rhel.yml"):
        archivo = RAIZ / nombre
        if not archivo.exists():
            continue
        texto = archivo.read_text(encoding="utf-8")
        montajes = [l for l in texto.splitlines() if "BT2SQL_PARQUET_HOST_DIR" in l]

        assert len(montajes) >= 5, (
            f"{nombre} solo monta BT2SQL_PARQUET_HOST_DIR en {len(montajes)} "
            f"sitios; hacen falta al menos 5 (webserver, scheduler, worker, "
            f"spark-master, spark-worker)."
        )
        assert ":-./data/bt2sql}" in texto, (
            "sin valor por defecto, un .env sin esa linea da el "
            "'invalid spec: :/data/bt2sql: empty section between colons'"
        )
        if nombre.endswith("rhel.yml"):
            for linea in montajes:
                assert linea.rstrip().endswith(":z"), (
                    f"falta la etiqueta SELinux ':z':\n  {linea.strip()}"
                )


def test_la_ruta_de_la_variable_cuadra_con_la_del_contenedor():
    """output_dir es la ruta de DENTRO del contenedor. Si no coincide con el
    lado derecho del bind mount, la extraccion escribe en el sistema de
    archivos del contenedor y los archivos se pierden al reiniciarlo."""
    cfg = json.loads((DIR_JSON / "BT2SQL_EXTRACCION.json").read_text(encoding="utf-8"))
    compose = (RAIZ / "docker-compose.windows.yml").read_text(encoding="utf-8")

    assert cfg["output_dir"] == "/data/bt2sql"
    assert f":-{cfg['output_dir']}}}" in compose


def test_el_modulo_esta_ignorado_por_el_scheduler():
    """etl_bt2sql/ son modulos, no DAGs. Sin el .airflowignore el scheduler
    intenta parsearlos como DAGs cada 30 segundos."""
    ignorar = (DIR_DAGS / ".airflowignore").read_text(encoding="utf-8")
    assert "etl_bt2sql" in ignorar


def test_las_credenciales_no_estan_en_ningun_archivo_del_pipeline():
    """Las de Bantotal y SQL Server salen de Connections cifradas con el Fernet
    key. Un archivo versionado con la clave del core en texto plano es una
    fuga permanente: queda en el historial de git aunque se borre despues."""
    archivos = [DIR_PRODUCCION / "dag_bt2sql_stg.py"] + \
               list((DIR_PRODUCCION / "etl_bt2sql").glob("*.py")) + \
               [DIR_JSON / "BT2SQL_EXTRACCION.json", DIR_JSON / "BT2SQL_CARGA.json"]

    sospechosas = ("password=", "pwd=", "166.73.3.", "PASSWORD'")
    for archivo in archivos:
        texto = archivo.read_text(encoding="utf-8")
        for patron in sospechosas:
            assert patron not in texto, f"{archivo.name} contiene {patron!r}"
