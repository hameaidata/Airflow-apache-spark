"""
bt2sql_comun - Piezas compartidas del pipeline Bantotal -> SQL Server.

Todo lo de este pipeline lleva el prefijo bt2sql, para distinguirlo de un
vistazo de los otros dos que ya existen:

    etl/          Bantotal/SingleStore -> parquet -> STG SingleStore
    etl_s2sql/    SingleStore          -> parquet -> SQL Server
    etl_bt2sql/   Bantotal (IBM i)     -> parquet -> STG SQL Server   <- este

    DAG        STG_BT2SQL_CARGA        airflow/dags/production/dag_stg_bt2sql_carga.py
    Variables  BT2SQL_EXTRACCION y BT2SQL_CARGA
    Tablas     CTL_PARAMETROS_PARQUET, ctl_proceso_parquet, ctl_carga_stg
               en SQL Server (GNBPE_DATAHUB)
    Parquet    /data/bt2sql/<yyyyMMdd>/<TABLA>/<TABLA>.parquet

LAS VARIABLES TIENEN LAS MISMAS CLAVES QUE LAS DEL PIPELINE DE SINGLESTORE
--------------------------------------------------------------------------
BT2SQL_EXTRACCION replica exactamente las claves de EXTRACCION_BT_STG, y
BT2SQL_CARGA las de CARGAR_PARQUET_CONFIG. No se agrego ninguna clave nueva.
Lo unico que cambia es el CONTENIDO: el SQL va en T-SQL en vez de en dialecto
MySQL, porque el catalogo y la bitacora viven en SQL Server.

    EXTRACCION_BT_STG   "SELECT ... LIMIT 1"          MySQL/SingleStore
    BT2SQL_EXTRACCION   "SELECT TOP 1 ..."            T-SQL

Por eso son Variables separadas y no una sola: compartirlas obligaria a que el
mismo SQL corriera en los dos motores, y no hay forma de que eso ocurra.

ORIGEN: BANTOTAL SOBRE IBM i
----------------------------
La conexion usa exactamente las mismas propiedades que el script
test_bt_preproduccion.py, que ya esta probado contra preproduccion. La unica
diferencia es que aqui el servidor y las credenciales salen de una Connection
de Airflow, cifradas con el Fernet key, en vez de estar escritas en el archivo.

IMPORTANTE SOBRE LOS IMPORTS
---------------------------
jaydebeapi, pyarrow y pandas se importan DENTRO de las funciones. El proceso
que parsea los DAGs importa este modulo cada 30 segundos y no tiene por que
cargar tres librerias pesadas para nada. Con jaydebeapi hay una razon de mas:
arrastra JPype, que levanta una JVM.
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

VARIABLE_EXTRACCION = "BT2SQL_EXTRACCION"
VARIABLE_CARGA = "BT2SQL_CARGA"

# Estados. Los valores por defecto coinciden con los que ya usa el pipeline de
# SingleStore, para que las consultas de operacion sirvan en los dos.
ESTADO_INICIADO = "INICIADO"
ESTADO_EJECUTANDO = "EJECUTANDO"
ESTADO_TERMINADO = "TERMINADO"
ESTADO_SIN_DATOS = "SIN_DATOS"
ESTADO_ERROR = "ERROR"

# Clase del driver JTOpen. NO es com.ibm.db2.jcc.DB2Driver: ese es el de Db2
# para LUW y z/OS y no sirve contra IBM i, aunque el nombre lo sugiera.
DRIVER_BANTOTAL = "com.ibm.as400.access.AS400JDBCDriver"
DRIVER_SQLSERVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

JAR_BANTOTAL = "/opt/airflow/jars/jt400.jar"
JAR_SQLSERVER = "/opt/airflow/jars/mssql-jdbc.jar"

# Propiedades de la URL de Bantotal. Son las del script que ya funciona, mas
# tres que protegen el core y que ese script no traia:
#
#   prompt=false              el driver intenta abrir un DIALOGO GRAFICO si algo
#                             falta en la autenticacion. En un contenedor sin
#                             escritorio la tarea se cuelga sin un solo mensaje
#                             util, hasta que la mata execution_timeout.
#   transaction isolation     sin bloqueos de lectura sobre las tablas del core
#   access=read only          el driver rechaza cualquier escritura: red de
#                             seguridad barata contra un error de codigo
#   errors=full               mensajes legibles en vez de codigos cripticos
#   date format=iso           sin esto el formato depende del job del sistema y
#                             cambia entre entornos
#
#   naming=sql                MUY IMPORTANTE, y la fuente del error mas comun
#                             al conectarse a un AS/400 desde codigo nuevo.
#                             Esta propiedad decide como se escriben los
#                             nombres DENTRO del SQL, y las dos formas no son
#                             intercambiables:
#
#                                 naming=sql     ->  GPPPBTDB.FSH005
#                                 naming=system  ->  GPPPBTDB/FSH005
#
#                             Si no coinciden, el driver responde un -204
#                             "objeto no encontrado" que parece un problema de
#                             permisos o de nombre mal escrito, y se pierden
#                             horas buscando donde no es.
#
#                             Se usa sql porque el catalogo da SIEMPRE el
#                             nombre completo (ESQUEMA + TABLA): la ventaja de
#                             system es resolver nombres sin calificar contra
#                             la lista de bibliotecas del job, y aqui no hay
#                             ningun nombre sin calificar que resolver.
PROPIEDADES_BANTOTAL = (
    "naming=sql",
    "errors=full",
    "prompt=false",
    "transaction isolation=none",
    "access=read only",
    "date format=iso",
    "time format=iso",
    "blocking enabled=true",
    "block size=512",
)


# ============================================================================
# CONFIGURACION
# ============================================================================
@lru_cache(maxsize=2)
def cargar_config(variable: str) -> dict[str, Any]:
    """Lee una de las dos Variables del pipeline.

    Cacheada por proceso: una tarea la pide varias veces y no tiene sentido
    consultar la metadata database en cada una.
    """
    from airflow.models import Variable

    try:
        config = Variable.get(variable, deserialize_json=True)
    except Exception as exc:
        raise AirflowException(
            f"Falta la Variable {variable} o no es JSON valido ({exc}). "
            f"Cargala con  python scripts/sync_variables.py --solo {variable}"
        ) from exc

    if not isinstance(config, dict):
        raise AirflowException(f"La Variable {variable} debe ser un objeto JSON.")
    return config


def config_extraccion() -> dict[str, Any]:
    return cargar_config(VARIABLE_EXTRACCION)


def config_carga() -> dict[str, Any]:
    return cargar_config(VARIABLE_CARGA)


# ============================================================================
# IDENTIFICADORES
# ============================================================================
# Los nombres de tabla y de columna NO admiten parametros enlazados en ningun
# motor: van concatenados en el texto del SQL. Como salen del catalogo, que es
# una tabla que alguien edita, hay que validarlos antes de concatenarlos o el
# catalogo se convierte en un vector de inyeccion SQL con permisos de carga.
IDENTIFICADOR_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")


def validar_identificador(valor: Any, donde: str) -> str:
    texto = str(valor or "").strip()
    if not IDENTIFICADOR_RE.match(texto):
        raise AirflowException(
            f"{donde}: {valor!r} no es un identificador valido. Se admiten letras, "
            f"digitos y _ $ #, sin empezar por digito. Revisa la fila "
            f"correspondiente de CTL_PARAMETROS_PARQUET."
        )
    return texto


def corchetes(*partes: str) -> str:
    """('dbo', 'STG_FSR011') -> '[dbo].[STG_FSR011]'  (SQL Server)"""
    return ".".join(f"[{p}]" for p in partes)


def calificar(esquema: str, tabla: str) -> str:
    """('GPPPBTDB', 'FSH005') -> 'GPPPBTDB.FSH005'  (DB2 for i con naming=sql)

    El punto va de la mano con "naming=sql" en PROPIEDADES_BANTOTAL. Con
    "naming=system" habria que escribir GPPPBTDB/FSH005 y el punto daria un
    -204. Las dos cosas se cambian juntas o ninguna.
    """
    return f"{esquema}.{tabla}"


def lista_columnas(columnas: list[str], donde: str, motor: str = "sqlserver") -> str:
    if not columnas:
        return "*"
    if motor == "sqlserver":
        return ", ".join(f"[{validar_identificador(c, donde)}]" for c in columnas)
    return ", ".join(validar_identificador(c, donde) for c in columnas)


def separar_lista(valor: Any) -> list[str]:
    """'A, B , C' -> ['A','B','C'].  None o '' -> []."""
    if valor is None:
        return []
    if isinstance(valor, (list, tuple)):
        return [str(v).strip() for v in valor if str(v).strip()]
    return [p.strip() for p in str(valor).split(",") if p.strip()]


# ============================================================================
# JVM
# ============================================================================
def _arrancar_jvm(jar: str) -> None:
    """Levanta la JVM con headless activado, ANTES de que jaydebeapi lo haga.

    -Djava.awt.headless=true es lo mismo que sus scripts de Spark ya ponen. Sin
    eso, cualquier intento del driver de abrir una ventana termina en una
    HeadlessException poco clara, o peor, en un proceso colgado.

    JPype levanta UNA sola JVM por proceso de Python y NO se puede reiniciar ni
    ampliar su classpath despues. Por eso se arranca con los dos jars de una
    vez: si se arrancara con el de Bantotal y luego hiciera falta el de SQL
    Server, ya no habria forma de agregarlo.
    """
    import jpype

    if jpype.isJVMStarted():
        return

    classpath = [j for j in (JAR_BANTOTAL, JAR_SQLSERVER) if os.path.isfile(j)]
    if jar not in classpath and os.path.isfile(jar):
        classpath.append(jar)
    if not classpath:
        raise AirflowException(
            f"No hay ningun driver JDBC en /opt/airflow/jars/.\n"
            f"La imagen los descarga al construirse. Comprueba con:\n"
            f"  docker compose exec airflow-worker ls -la /opt/airflow/jars/"
        )

    jpype.startJVM(
        "-Djava.awt.headless=true",
        "-Dfile.encoding=UTF-8",
        classpath=classpath,
    )
    logger.info("JVM arrancada (headless) con classpath: %s", classpath)


def atar_hilo_a_jvm() -> None:
    """Registra el hilo actual en la JVM. OBLIGATORIO fuera del hilo principal.

    Este es el detalle que rompe el uso de jaydebeapi con ThreadPoolExecutor y
    que no sale en ningun ejemplo de la documentacion.

    JPype solo conoce los hilos que se le presentan. El hilo principal se ata
    solo al arrancar la JVM; los que crea ThreadPoolExecutor, no. Si uno de
    esos hilos llama a Java sin estar atado, no salta una excepcion de Python
    que se pueda capturar y registrar: la JVM aborta el PROCESO entero. El
    worker de Airflow se cae de golpe, la tarea queda en estado zombie y en el
    log no hay traza, solo un corte a media frase. Es de los fallos mas caros
    de diagnosticar porque parece un problema de memoria o del contenedor.

    Atar un hilo ya atado no cuesta nada y es seguro, asi que se llama sin
    condiciones al entrar en cada hilo. Si la JVM aun no arranco no hay nada
    que atar todavia: la levantara la primera conexion, ya dentro de este mismo
    hilo.
    """
    import jpype

    if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
        jpype.attachThreadToJVM()


def _conectar(driver: str, url: str, usuario: str, clave: str, jar: str, donde: str):
    import jaydebeapi

    if not os.path.isfile(jar):
        raise AirflowException(
            f"No esta el driver JDBC en {jar}.\n"
            f"La imagen lo descarga al construirse, o se deja en airflow/jars/ "
            f"del repositorio. Comprueba con:\n"
            f"  docker compose exec airflow-worker ls -la /opt/airflow/jars/"
        )
    _arrancar_jvm(jar)
    try:
        return jaydebeapi.connect(driver, url, [usuario, clave], jar)
    except Exception as exc:
        raise AirflowException(
            f"No se pudo conectar a {donde}.\n"
            f"  URL    : {url}\n"
            f"  Driver : {driver}\n"
            f"  Jar    : {jar}\n"
            f"  Error  : {exc}"
        ) from exc


# ============================================================================
# CONEXIONES
# ============================================================================
def conexion_bantotal(config: dict[str, Any]):
    """Conexion de SOLO LECTURA al core, por JDBC con JTOpen.

    El servidor y las credenciales salen de una Connection de Airflow. La
    biblioteca por defecto sale del campo schema de esa Connection, y la lista
    completa de bibliotecas del Extra, como 'libraries'.

    Equivale exactamente a lo que hace test_bt_preproduccion.py, con las
    propiedades de proteccion del core anadidas.
    """
    from airflow.hooks.base import BaseHook

    conn_id = config.get("conn_id_origen", "CONEXION_BANTOTAL")
    conn = BaseHook.get_connection(conn_id)
    if not conn.host:
        raise AirflowException(f"La Connection {conn_id!r} no tiene host configurado.")

    extra = conn.extra_dejson or {}
    propiedades = list(PROPIEDADES_BANTOTAL)
    if extra.get("libraries"):
        propiedades.append(f"libraries={extra['libraries']}")

    url = f"jdbc:as400://{conn.host}:{int(conn.port or 8471)};" + ";".join(propiedades)

    conexion = _conectar(DRIVER_BANTOTAL, url, conn.login, conn.password, JAR_BANTOTAL,
                         f"Bantotal ({conn_id}, {conn.host})")
    try:
        conexion.jconn.setReadOnly(True)
    except Exception:
        # No todos los drivers lo soportan; access=read only en la URL ya cubre
        # el caso. No vale la pena fallar por esto.
        pass
    return conexion


def conexion_sqlserver(config: dict[str, Any], autocommit: bool = False):
    """Conexion al DESTINO (SQL Server), por JDBC.

    autocommit=False es lo normal: cada tabla se aplica dentro de una
    transaccion. Con JDBC eso se controla en la conexion Java real, que
    jaydebeapi expone en .jconn.
    """
    from airflow.hooks.base import BaseHook

    conn_id = config.get("conn_id_destino", "CONEXION_SQLSERVER")
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson or {}

    if extra.get("jdbc_url"):
        # Instancia con nombre, failover partner o Always On: la URL la da el
        # area de base de datos y no hay que reconstruirla.
        url = extra["jdbc_url"]
    else:
        if not conn.host:
            raise AirflowException(
                f"La Connection {conn_id!r} no tiene host ni jdbc_url en el Extra."
            )
        partes = [f"jdbc:sqlserver://{conn.host}"]
        if conn.port:
            partes.append(f":{int(conn.port)}")
        partes.append(";")
        if extra.get("instanceName"):
            partes.append(f"instanceName={extra['instanceName']};")
        partes.append(f"databaseName={conn.schema};")
        partes.append(f"encrypt={extra.get('encrypt', 'true')};")
        partes.append(f"trustServerCertificate={extra.get('trustServerCertificate', 'true')};")
        url = "".join(partes)

    conexion = _conectar(DRIVER_SQLSERVER, url, conn.login, conn.password, JAR_SQLSERVER,
                         f"SQL Server ({conn_id})")
    try:
        conexion.jconn.setAutoCommit(bool(autocommit))
    except Exception as exc:
        conexion.close()
        raise AirflowException(
            f"No se pudo fijar autocommit={autocommit} en la conexion JDBC: {exc}"
        ) from exc
    return conexion


# ============================================================================
# RUTAS: CARPETA POR DIA Y POR TABLA
# ============================================================================
# La estructura es
#
#     <output_dir>/<yyyyMMdd>/<TABLA>/<TABLA>.parquet
#
# Una carpeta por dia y, dentro, una por tabla. No un archivo suelto por tabla.
#
# Por que asi:
#   - La carpeta del dia se borra entera cuando vence la retencion, sin tener
#     que mirar archivo por archivo.
#   - La carpeta por tabla deja sitio para partir un parquet en varios archivos
#     el dia que una tabla no quepa comoda en uno solo, sin cambiar rutas ni
#     tocar lo que ya esta guardado en la bitacora.
#   - Un listado del dia dice de un vistazo que tablas se extrajeron.
# ============================================================================
def carpeta_del_dia(config: dict[str, Any], fecha_proceso: date) -> str:
    """/data/bt2sql/20260926

    Se usa la FECHA DE PROCESO, no la del reloj. Si una corrida arranca a las
    23:58 y la extraccion cruza la medianoche, todos los archivos siguen
    cayendo en la carpeta del dia al que pertenecen los datos.
    """
    return os.path.join(config["output_dir"], fecha_proceso.strftime("%Y%m%d"))


def carpeta_de_tabla(config: dict[str, Any], fecha_proceso: date, tabla: str) -> str:
    """/data/bt2sql/20260926/FSR011

    El nombre se valida ANTES de construir la ruta, y no por manía. TABLA sale
    de CTL_PARAMETROS_PARQUET, que es una tabla que alguien edita. Un valor
    como '../../opt/airflow' convertiria os.path.join en una escritura fuera
    del directorio de datos, con los permisos del worker. Es la misma razon por
    la que se valida antes de meterlo en el SQL, aplicada al sistema de
    archivos.
    """
    seguro = validar_identificador(tabla, "CTL_PARAMETROS_PARQUET.TABLA")
    return os.path.join(carpeta_del_dia(config, fecha_proceso), seguro)


def ruta_parquet(config: dict[str, Any], fecha_proceso: date, tabla: str) -> str:
    """/data/bt2sql/20260926/FSR011/FSR011.parquet

    Es la RUTA COMPLETA que se guarda en ctl_proceso_parquet.archivo_parquet y
    la que la carga abre despues. Guardar solo el nombre no serviria: la
    carpeta cambia cada dia.
    """
    return os.path.join(carpeta_de_tabla(config, fecha_proceso, tabla), f"{tabla}.parquet")


# ============================================================================
# UTILIDADES
# ============================================================================
def normalizar_batch_id(valor: Any) -> str | None:
    """Convierte en None todo lo que no sea un batch_id de verdad.

    El DAG lleva render_template_as_native_obj=True, que es la defensa
    principal: sin el, "{{ ti.xcom_pull(...) }}" entrega la CADENA 'None'
    cuando no hay XCom, y un 'if batch_id:' la da por buena. La consulta
    buscaria entonces batch_id = 'None', no encontraria nada, y el respaldo de
    "toma la ultima extraccion TERMINADA" nunca se ejecutaria: la carga saldria
    verde sin haber subido ni una fila. Ese fallo ya ocurrio una vez en el
    pipeline de SingleStore y no dio ningun error.

    Esta funcion es el cinturon por si alguien quita esa opcion del DAG, o por
    si el batch llega de otro sitio: de un disparo manual con configuracion
    JSON, de una llamada desde otro DAG. Cuesta una comparacion y cierra un
    fallo que es silencioso, que es el peor tipo.

    Un batch_id valido son 14 digitos: yyyyMMddHHmmss.
    """
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto or texto.lower() in ("none", "null", "nan", "{{ batch_id }}"):
        return None
    if not (len(texto) == 14 and texto.isdigit()):
        logger.warning(
            "El batch_id recibido (%r) no tiene la forma yyyyMMddHHmmss. Se usa "
            "tal cual, pero si no encuentra nada, mira de donde viene.", texto,
        )
    return texto


def nuevo_batch_id(momento: datetime | None = None) -> str:
    """yyyyMMddHHmmss. Identifica una corrida completa del pipeline."""
    return (momento or datetime.now()).strftime("%Y%m%d%H%M%S")


def host() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "desconocido"


def recortar(texto: Any, limite: int) -> str:
    """Recorta un mensaje al ancho de la columna que lo va a guardar.

    Sin esto, una traza larga hace fallar el propio INSERT del log con
    'String or binary data would be truncated', y se pierde el error original,
    que es el que importaba.
    """
    texto = str(texto or "")
    return texto if len(texto) <= limite else texto[: limite - 3] + "..."


def duracion(inicio: datetime, fin: datetime | None = None) -> float:
    return round(((fin or datetime.now()) - inicio).total_seconds(), 2)


def filas_nativas(df) -> list[tuple]:
    """Convierte un DataFrame en filas de tipos NATIVOS de Python.

    Es obligatorio con JDBC. JPype no sabe convertir numpy.int64, numpy.float64
    ni pandas.Timestamp, y falla con un error que solo nombra el tipo Java que
    esperaba. Es la trampa numero uno al pasar de ODBC a JDBC.

    Los nulos se detectan ANTES de convertir: despues, un NaN es un float
    corriente y se insertaria como tal en una columna numerica, o como el texto
    'nan' en una de texto.
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
