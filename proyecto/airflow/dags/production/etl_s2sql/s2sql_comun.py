"""
s2sql_comun - Piezas compartidas del pipeline SingleStore -> SQL Server 2022.

Todo lo de este pipeline lleva el prefijo s2sql para que se distinga de un
vistazo del pipeline BT (etl/) y del de DataHub (etl-datahub/):

    DAG            S2SQL_EXPORT          airflow/dags/production/dag_s2sql_export.py
    Modulos        etl_s2sql/s2sql_*.py
    Variable       S2SQL_EXPORT_CONFIG   airflow/config/json/
    Tablas         CTL.CTL_S2SQL_*       en SQL Server (el destino)
    Parquet        /data/s2sql/<yyyyMMdd>/<TABLA>.parquet
    Pool           AIRFLOW_POOL_SQLSERVER

DONDE VIVE CADA COSA
--------------------
El catalogo de que se exporta y el log de que paso viven en SQL SERVER, no en
SingleStore. La razon es practica: quien consume el destino puede auditar que
llego y cuando sin pedir acceso al origen.

La Variable de Airflow NO lista tablas. Solo tiene infraestructura: nombres de
Connection, rutas, tamanos de lote. Asi el DAG no necesita consultar SQL Server
para dibujar su grafo, que es lo que lo dejaria roto cada vez que el destino
este caido. Las tareas por tabla se crean en tiempo de EJECUCION, con el
catalogo ya leido (ver dag_s2sql_export.py).

CONEXIONES
----------
    Origen   SingleStore  por singlestoredb (protocolo MySQL)
    Destino  SQL Server   por JDBC (jaydebeapi sobre JPype), con el driver
                          oficial de Microsoft en /opt/airflow/jars/

El destino NO va por ODBC. Eso tiene consecuencias en todo el pipeline, y
estan explicadas en el bloque "SQL SERVER POR JDBC" mas abajo: no hay
fast_executemany, rowcount no sirve, y hay que convertir los tipos de numpy
antes de enviarlos.

IMPORTANTE SOBRE LOS IMPORTS
---------------------------
jaydebeapi, pyarrow, pandas y singlestoredb se importan DENTRO de las
funciones, nunca arriba. El proceso que parsea los DAGs importa este modulo
cada 30 segundos y no tiene por que cargar cuatro librerias pesadas para nada.

Con jaydebeapi hay una razon de mas: importarlo arrastra JPype, y JPype
levanta una JVM. Una JVM por cada proceso que parsea DAGs, cada 30 segundos,
seria un desperdicio de memoria notable en un scheduler.
"""

from __future__ import annotations

import logging
import os
import re
import socket
from datetime import date, datetime
from functools import lru_cache
from typing import Any

from airflow.exceptions import AirflowException


logger = logging.getLogger(__name__)

VARIABLE_CONFIG = "S2SQL_EXPORT_CONFIG"

# Estados que se escriben en CTL_S2SQL_LOTE y CTL_S2SQL_LOG_CARGA.
ESTADO_EJECUTANDO = "EJECUTANDO"
ESTADO_TERMINADO = "TERMINADO"
ESTADO_ERROR = "ERROR"
ESTADO_SIN_DATOS = "SIN_DATOS"

MODO_REEMPLAZO = "REEMPLAZO"
MODO_INCREMENTAL = "INCREMENTAL"
MODO_MERGE = "MERGE"
MODOS_VALIDOS = (MODO_REEMPLAZO, MODO_INCREMENTAL, MODO_MERGE)

CONFIG_DEFECTO: dict[str, Any] = {
    "conexiones": {"singlestore": "CONEXION_SINGLESTORE", "sqlserver": "CONEXION_SQLSERVER"},
    "parquet": {"directorio": "/data/s2sql", "retencion_dias": 7, "compresion": "snappy"},
    "lectura": {"chunk_filas": 50000},
    "escritura": {
        "batch_filas": 5000,
        "esquema_staging": "STG",
        "prefijo_staging": "S2SQL_",
    },
    "jdbc": {
        "clase": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jar": "",  # vacio = se elige solo segun la version de Java
        "jar_jre11": "/opt/airflow/jars/mssql-jdbc.jar",
        "jar_jre8": "/opt/airflow/jars/mssql-jdbc-jre8.jar",
        "propiedades": {
            "encrypt": "true",
            "trustServerCertificate": "true",
            "loginTimeout": "30",
        },
    },
    "control": {
        "esquema": "CTL",
        "catalogo": "CTL_S2SQL_CATALOGO",
        "lote": "CTL_S2SQL_LOTE",
        "log": "CTL_S2SQL_LOG_CARGA",
    },
    "limites": {"mensaje_error": 4000, "tablas_por_corrida": 200},
}


# ============================================================================
# CONFIGURACION
# ============================================================================
@lru_cache(maxsize=1)
def cargar_config() -> dict[str, Any]:
    """Lee la Variable S2SQL_EXPORT_CONFIG y la fusiona con los defectos.

    Cacheada por proceso: una tarea la pide varias veces y no tiene sentido
    consultar la metadata database en cada una.

    La fusion es por seccion, no profunda: basta con que una clave nueva del
    codigo no obligue a reescribir la Variable entera.
    """
    from airflow.models import Variable

    try:
        crudo = Variable.get(VARIABLE_CONFIG, deserialize_json=True)
    except Exception as exc:
        raise AirflowException(
            f"Falta la Variable {VARIABLE_CONFIG} o no es JSON valido ({exc}). "
            f"Cargala con  python scripts/sync_variables.py --solo {VARIABLE_CONFIG}"
        ) from exc

    if not isinstance(crudo, dict):
        raise AirflowException(f"La Variable {VARIABLE_CONFIG} debe ser un objeto JSON.")

    config: dict[str, Any] = {}
    for seccion, defecto in CONFIG_DEFECTO.items():
        valor = crudo.get(seccion)
        config[seccion] = {**defecto, **valor} if isinstance(valor, dict) else dict(defecto)
    return config


def nombre_control(config: dict[str, Any], clave: str) -> str:
    """-> '[CTL].[CTL_S2SQL_LOG_CARGA]', listo para interpolar en el SQL."""
    ctl = config["control"]
    return f"[{ctl['esquema']}].[{ctl[clave]}]"


# ============================================================================
# IDENTIFICADORES: lo unico que no se puede parametrizar
# ============================================================================
# Los nombres de tabla y de columna NO admiten parametros enlazados en ningun
# motor: van concatenados en el texto del SQL. Como salen del catalogo, que es
# una tabla que alguien edita, hay que validarlos antes de concatenarlos o el
# catalogo se convierte en un vector de inyeccion SQL con permisos de carga.
#
# La regla es deliberadamente estrecha: letras, digitos y guion bajo, sin
# empezar por digito. Cubre cualquier nombre razonable y rechaza corchetes,
# comillas, puntos y punto y coma.
IDENTIFICADOR_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validar_identificador(valor: Any, donde: str) -> str:
    texto = str(valor or "").strip()
    if not IDENTIFICADOR_RE.match(texto):
        raise AirflowException(
            f"{donde}: {valor!r} no es un identificador valido. Se admiten letras, "
            f"digitos y guion bajo, sin empezar por digito. Revisa la fila "
            f"correspondiente de CTL_S2SQL_CATALOGO."
        )
    return texto


def corchetes(*partes: str) -> str:
    """('CTL', 'MI_TABLA') -> '[CTL].[MI_TABLA]'"""
    return ".".join(f"[{p}]" for p in partes)


def lista_columnas(columnas: list[str], donde: str) -> str:
    """['A','B'] -> '[A], [B]', validando cada nombre."""
    return ", ".join(f"[{validar_identificador(c, donde)}]" for c in columnas)


def separar_lista(valor: Any) -> list[str]:
    """'A, B , C' -> ['A','B','C'].  None o '' -> []."""
    if valor is None:
        return []
    if isinstance(valor, (list, tuple)):
        return [str(v).strip() for v in valor if str(v).strip()]
    return [p.strip() for p in str(valor).split(",") if p.strip()]


# ============================================================================
# CONEXIONES
# ============================================================================
def conexion_singlestore(config: dict[str, Any]):
    """Conexion al ORIGEN, a partir de una Connection de Airflow.

    La Connection guarda la clave cifrada con el Fernet key; aqui no hay
    credenciales en texto ni en el codigo ni en la Variable.
    """
    import singlestoredb as s2
    from airflow.hooks.base import BaseHook

    conn_id = config["conexiones"]["singlestore"]
    conn = BaseHook.get_connection(conn_id)
    if not conn.host:
        raise AirflowException(f"La Connection {conn_id!r} no tiene host configurado.")

    return s2.connect(
        host=conn.host,
        port=int(conn.port or 3306),
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        # Si el servidor se cae a mitad de un chunk, es mejor enterarse que
        # quedarse colgado hasta que lo mate execution_timeout.
        connect_timeout=int((conn.extra_dejson or {}).get("connect_timeout", 30)),
    )


# ----------------------------------------------------------------------------
# SQL SERVER POR JDBC
# ----------------------------------------------------------------------------
# La conexion al destino va por JDBC (jaydebeapi sobre JPype), no por ODBC.
# Eso trae tres diferencias de comportamiento que el resto del pipeline tiene
# en cuenta y conviene tener presentes al leer el codigo:
#
#   1. NO existe fast_executemany. Ese acelerador es de pyodbc. En JDBC el
#      equivalente es el batch del PreparedStatement, que jaydebeapi usa por
#      debajo en executemany. El tamano del lote es lo que manda, no un flag.
#
#   2. cursor.rowcount NO es fiable: jaydebeapi devuelve -1 casi siempre. Por
#      eso ninguna funcion de carga cuenta filas con rowcount; las cuenta con
#      consultas explicitas.
#
#   3. JPype NO convierte tipos de numpy. Un numpy.int64 que en pyodbc pasaba
#      sin problema, aqui revienta con un error de conversion. Por eso los
#      lotes se normalizan a tipos nativos de Python antes de enviarlos
#      (ver filas_nativas mas abajo).
#
# Y una restriccion de JPype que condiciona el diseno: se levanta UNA sola JVM
# por proceso de Python y no se puede reiniciar. De ahi que el jar se elija
# segun la version de Java con la que arranco el proceso.
# ----------------------------------------------------------------------------
@lru_cache(maxsize=1)
def java_mayor() -> int:
    """Version mayor de la JVM que JPype va a levantar (8, 11, 17...).

    Se lee del archivo 'release' que toda distribucion de OpenJDK deja en la
    raiz de JAVA_HOME. Es mas barato y mas fiable que lanzar 'java -version',
    que ademas escribe en stderr y cambia de formato entre versiones.

    Java 8 se identifica como 1.8.0_xxx; de la 9 en adelante, como 17.0.2.
    """
    java_home = os.environ.get("JAVA_HOME", "")
    archivo = os.path.join(java_home, "release")
    try:
        with open(archivo, encoding="utf-8") as fh:
            for linea in fh:
                if linea.startswith("JAVA_VERSION="):
                    version = linea.split("=", 1)[1].strip().strip('"')
                    partes = version.split(".")
                    return int(partes[1]) if partes[0] == "1" else int(partes[0])
    except (OSError, ValueError, IndexError):
        pass

    # Sin JAVA_HOME legible no se puede saber. Se asume 11+ porque es lo que
    # trae la imagen por defecto, y se avisa: si de verdad fuera Java 8, el
    # error posterior seria un UnsupportedClassVersionError.
    logger.warning(
        "No se pudo leer la version de Java en %s. Se asume 11+. Si el driver "
        "falla con UnsupportedClassVersionError, fija jdbc.jar en la Variable "
        "%s apuntando al jar .jre8.", archivo or "(JAVA_HOME vacio)", VARIABLE_CONFIG,
    )
    return 11


def jar_sqlserver(config: dict[str, Any]) -> str:
    """Elige el jar del driver segun la JVM en uso.

    El sufijo jreNN del driver de Microsoft dice para que bytecode se compilo
    el jar. Cargar el equivocado da UnsupportedClassVersionError, cuyo mensaje
    habla de 'class file version 55.0' y no menciona Java por ninguna parte.
    Elegirlo aqui evita ese rato perdido.

    jdbc.jar en la Variable fuerza uno concreto y salta esta deteccion.
    """
    jdbc = config["jdbc"]
    if jdbc.get("jar"):
        return jdbc["jar"]
    return jdbc["jar_jre8"] if java_mayor() <= 8 else jdbc["jar_jre11"]


def url_jdbc(conn, config: dict[str, Any]) -> str:
    """Arma la URL JDBC a partir de la Connection de Airflow.

    Si el campo Extra trae 'jdbc_url', se usa tal cual: hay despliegues con
    instancias con nombre, failover partner o Always On donde la URL la da el
    area de base de datos y no hay que reconstruirla.
    """
    extra = conn.extra_dejson or {}
    if extra.get("jdbc_url"):
        return extra["jdbc_url"]

    propiedades = {**config["jdbc"]["propiedades"], **(extra.get("propiedades") or {})}
    sufijo = "".join(f";{clave}={valor}" for clave, valor in propiedades.items())
    return (
        f"jdbc:sqlserver://{conn.host}:{int(conn.port or 1433)}"
        f";databaseName={conn.schema}{sufijo}"
    )


def conexion_sqlserver(config: dict[str, Any], autocommit: bool = False):
    """Conexion al DESTINO (SQL Server 2022) por JDBC.

    Las credenciales salen de una Connection de Airflow, cifradas con el Fernet
    key: ni en el codigo ni en la Variable hay contrasenas.

    autocommit=False es lo normal: cada tabla se aplica dentro de una
    transaccion. Se pone True solo donde el DDL no debe quedar pendiente.

    Sobre el cifrado: encrypt=true es el defecto desde el driver 10. Contra un
    servidor con certificado autofirmado hace falta ademas
    trustServerCertificate=true, o la conexion falla con un error de cadena de
    certificacion que no dice claramente que el problema es el certificado.
    """
    import jaydebeapi
    from airflow.hooks.base import BaseHook

    conn_id = config["conexiones"]["sqlserver"]
    conn = BaseHook.get_connection(conn_id)
    if not conn.host and not (conn.extra_dejson or {}).get("jdbc_url"):
        raise AirflowException(
            f"La Connection {conn_id!r} no tiene host ni jdbc_url en el Extra."
        )

    url = url_jdbc(conn, config)
    clase = config["jdbc"]["clase"]
    jar = jar_sqlserver(config)

    if not os.path.isfile(jar):
        raise AirflowException(
            f"No esta el driver JDBC de SQL Server en {jar}.\n"
            f"La imagen lo descarga al construirse. Comprueba que el build llegara "
            f"a ese paso:\n"
            f"  docker compose exec airflow-worker ls -la /opt/airflow/jars/"
        )

    try:
        conexion = jaydebeapi.connect(clase, url, [conn.login, conn.password], jar)
    except Exception as exc:
        raise AirflowException(
            f"No se pudo conectar a SQL Server por JDBC ({conn_id}).\n"
            f"  URL   : {url}\n"
            f"  Driver: {clase}\n"
            f"  Jar   : {jar}  (Java {java_mayor()})\n"
            f"  Error : {exc}\n"
            f"Si el error menciona 'class file version', el jar no corresponde a "
            f"esta JVM: fija jdbc.jar en la Variable {VARIABLE_CONFIG}."
        ) from exc

    # jaydebeapi expone la conexion Java real en .jconn. Ahi se controla la
    # transaccion: sin esto, cada sentencia haria commit por su cuenta y el
    # TRUNCATE del modo REEMPLAZO quedaria confirmado aunque el INSERT
    # siguiente fallara, que es exactamente lo que no debe pasar.
    try:
        conexion.jconn.setAutoCommit(bool(autocommit))
    except Exception as exc:
        conexion.close()
        raise AirflowException(
            f"No se pudo fijar autocommit={autocommit} en la conexion JDBC: {exc}"
        ) from exc

    return conexion


def filas_nativas(df) -> list[tuple]:
    """Convierte un DataFrame en filas de tipos NATIVOS de Python.

    Es obligatorio con JDBC. JPype no sabe convertir numpy.int64, numpy.float64
    ni pandas.Timestamp, y falla con un error de conversion que solo dice el
    nombre del tipo Java que esperaba. Con pyodbc esto no hacia falta, asi que
    es la trampa numero uno al pasar de ODBC a JDBC.

    Se hace columna por columna porque Series.tolist() ya devuelve tipos
    nativos, mientras que DataFrame.values mezcla dtypes y los deja en object
    sin convertir nada.

    Los nulos se detectan ANTES de convertir: despues, un NaN es un float
    corriente y se insertaria como tal en una columna numerica, o como el
    texto 'nan' en una de texto.
    """
    import pandas as pd

    columnas = []
    for nombre in df.columns:
        serie = df[nombre]
        nulos = serie.isna().tolist()
        valores = serie.tolist()
        convertidos = []
        for es_nulo, valor in zip(nulos, valores):
            if es_nulo:
                convertidos.append(None)
            elif isinstance(valor, pd.Timestamp):
                convertidos.append(valor.to_pydatetime())
            else:
                convertidos.append(valor)
        columnas.append(convertidos)
    return list(zip(*columnas)) if columnas else []


# ============================================================================
# LOTE Y RUTAS
# ============================================================================
def nuevo_batch_id(momento: datetime | None = None) -> str:
    """yyyyMMddHHmmss. Identifica una corrida completa del pipeline."""
    return (momento or datetime.now()).strftime("%Y%m%d%H%M%S")


def carpeta_del_lote(config: dict[str, Any], fecha_proceso: date) -> str:
    """/data/s2sql/20260924

    Se usa la FECHA DE PROCESO, no la del reloj. Si una corrida arranca a las
    23:58 y la extraccion cruza la medianoche, todos los archivos siguen
    cayendo en la carpeta del dia al que pertenecen los datos.
    """
    return os.path.join(config["parquet"]["directorio"], fecha_proceso.strftime("%Y%m%d"))


def ruta_parquet(config: dict[str, Any], fecha_proceso: date, tabla_origen: str) -> str:
    return os.path.join(carpeta_del_lote(config, fecha_proceso), f"{tabla_origen}.parquet")


def nombre_staging(config: dict[str, Any], tabla_destino: str) -> tuple[str, str]:
    """-> ('STG', 'S2SQL_MI_TABLA')

    La tabla de staging es por tabla destino, no por corrida: se recrea en
    cada carga. Tener una fija evita llenar la base de tablas huerfanas cuando
    una corrida muere a mitad.
    """
    esc = config["escritura"]
    return esc["esquema_staging"], f"{esc['prefijo_staging']}{tabla_destino}"


# ============================================================================
# UTILIDADES
# ============================================================================
def host() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "desconocido"


def recortar(texto: Any, limite: int) -> str:
    """Recorta un mensaje de error al ancho de la columna que lo va a guardar.

    Sin esto, una traza larga hace fallar el propio INSERT del log con
    'String or binary data would be truncated', y se pierde el error original,
    que es el que importaba.
    """
    texto = str(texto or "")
    return texto if len(texto) <= limite else texto[: limite - 3] + "..."


def duracion(inicio: datetime, fin: datetime | None = None) -> float:
    return round(((fin or datetime.now()) - inicio).total_seconds(), 2)
