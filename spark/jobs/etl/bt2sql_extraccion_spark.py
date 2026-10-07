"""
bt2sql_extraccion_spark - Bantotal (IBM i) -> Parquet, con Spark.

GEMELO DEL PIPELINE DE PANDAS, NO SU REEMPLAZO
----------------------------------------------
Hace lo mismo que etl_bt2sql/bt2sql_extraccion.py: lee el catalogo, consulta
Bantotal tabla por tabla y escribe un parquet por tabla, dejando el rastro en
ctl_proceso_parquet. Lo que cambia es QUIEN lee: aqui el origen se lee con
varios hilos en paralelo, uno por rango de la columna de particion.

El pipeline de pandas NO se toca. Los dos pueden convivir y producir el mismo
dia para compararse, porque escriben en carpetas distintas y se registran en la
bitacora con nom_proceso distinto.

DONDE GANA SPARK, Y DONDE NO
----------------------------
Gana en la LECTURA del core: spark.read.jdbc con partitionColumn abre N
conexiones y cada una trae un rango de filas. Contra una tabla historica de
decenas de millones, eso es la diferencia entre horas y minutos.

NO gana en nada mas. El catalogo y la bitacora se leen y escriben desde el
DRIVER con JDBC crudo, en un solo hilo, porque son decenas de filas. Y si la
tabla no declara particion, la lectura cae a UNA sola conexion: Spark hace
entonces el mismo trabajo que pandas pero con la latencia de un cluster. Por
eso el job AVISA cuando eso ocurre, en vez de dejarlo pasar en silencio.

LOS TIPOS SALEN DEL CATALOGO
----------------------------
La columna TIPOS de CTL_PARAMETROS_PARQUET manda sobre el esquema del parquet,
igual que en el pipeline de pandas. Sin eso, Spark infiere del JDBC y una
columna DECIMAL sin escala acaba como texto, o peor: el primer bloque la ve
toda NULL y el esquema queda inservible para los siguientes.

INVOCACION  -- la hace el DAG, no se ejecuta a mano
    spark-submit /opt/spark-apps/etl/bt2sql_extraccion_spark.py \
        --jdbc-url <url de Bantotal>  --driver-class com.ibm.as400.access.AS400JDBCDriver \
        --config /opt/spark-data/runtime/bt2sql_spark.json \
        --batch-id 20261005120000 --tipo-ejecucion diario
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bt2sql_extraccion_spark")

IDENTIFICADOR = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ============================================================================
# CONEXIONES CRUDAS DESDE EL DRIVER
# ----------------------------------------------------------------------------
# El catalogo y la bitacora viven en SQL Server y son decenas de filas. Leerlas
# con spark.read.jdbc costaria mas en coordinacion que lo que tardan. Se abren
# desde la JVM del driver, igual que hace ejecutar_sp_spark.py.
# ============================================================================
def abrir_jdbc(spark, url: str, usuario: str, clave: str, clase: str):
    jvm = spark._jvm
    jvm.Class.forName(clase)
    props = jvm.java.util.Properties()
    props.setProperty("user", usuario or "")
    props.setProperty("password", clave or "")
    return jvm.java.sql.DriverManager.getConnection(url, props)


def filas_de(conexion, sql: str) -> list[dict]:
    """Ejecuta un SELECT y devuelve lista de dicts. Para consultas pequenas."""
    st = conexion.createStatement()
    try:
        rs = st.executeQuery(sql)
        meta = rs.getMetaData()
        n = meta.getColumnCount()
        nombres = [meta.getColumnLabel(i + 1) for i in range(n)]
        salida = []
        while rs.next():
            salida.append({nombres[i]: rs.getObject(i + 1) for i in range(n)})
        rs.close()
        return salida
    finally:
        st.close()


def ejecutar(conexion, sql: str, parametros: tuple = ()) -> None:
    ps = conexion.prepareStatement(sql)
    try:
        for i, v in enumerate(parametros, start=1):
            ps.setObject(i, v)
        ps.execute()
    finally:
        ps.close()


# ============================================================================
# TIPOS DEL CATALOGO  (mismo contrato que el pipeline de pandas)
# ============================================================================
def parsear_tipos(tipos_str, tabla: str = "") -> dict[str, dict]:
    """COLUMNA:TIPO|COLUMNA:TIPO  ->  {columna: {type, precision, scale}}."""
    mapa: dict[str, dict] = {}
    if not tipos_str or not str(tipos_str).strip():
        return mapa
    for item in str(tipos_str).split("|"):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise SystemExit(
                f"[{tabla}] TIPOS mal formado: {item!r}. "
                f"El formato es COLUMNA:TIPO separado por |")
        col, tipo = item.split(":", 1)
        col, tipo = col.strip(), tipo.strip().lower()
        if tipo.startswith(("char", "varchar", "nchar", "nvarchar")):
            mapa[col] = {"type": "string"}
        elif tipo.startswith(("decimal", "numeric")):
            m = re.search(r"\((\d+)\s*,\s*(\d+)\)", tipo)
            if not m:
                raise SystemExit(f"[{tabla}] DECIMAL sin precision: {item!r}")
            mapa[col] = {"type": "decimal",
                         "precision": int(m.group(1)), "scale": int(m.group(2))}
        elif tipo in ("int", "integer", "smallint"):
            mapa[col] = {"type": "int"}
        elif tipo == "bigint":
            mapa[col] = {"type": "bigint"}
        elif tipo in ("float", "double", "real"):
            mapa[col] = {"type": "float"}
        elif tipo == "date":
            mapa[col] = {"type": "date"}
        elif tipo.startswith(("datetime", "timestamp", "smalldatetime")):
            mapa[col] = {"type": "timestamp"}
        elif tipo == "bit":
            # Se trata como entero y no como booleano: en el core un indicador
            # viene como 0/1 numerico, y un BooleanType en el parquet obliga a
            # convertir otra vez al escribir en un BIT de SQL Server.
            mapa[col] = {"type": "int"}
        else:
            logger.warning("[%s] tipo no reconocido %r en %r: se trata como texto",
                           tabla, tipo, col)
            mapa[col] = {"type": "string"}
    return mapa


# SQL Server -> el mismo diccionario que devuelve parsear_tipos
SQLSERVER_A_TIPO = {
    "decimal": "decimal", "numeric": "decimal", "money": "decimal",
    "smallmoney": "decimal",
    "int": "int", "integer": "int", "smallint": "int", "tinyint": "int",
    "bit": "int",
    "bigint": "bigint",
    "float": "float", "real": "float",
    "char": "string", "varchar": "string", "nchar": "string",
    "nvarchar": "string", "text": "string", "ntext": "string",
    "uniqueidentifier": "string",
    "date": "date",
    "datetime": "timestamp", "datetime2": "timestamp",
    "smalldatetime": "timestamp", "datetimeoffset": "timestamp",
}

# Las inyecta la extraccion, no vienen del core: no se leen del destino.
COLUMNAS_INYECTADAS = {"FECHA_PROCESO", "BATCH_ID"}


def tipos_de_destino(conn_ctl, tabla_destino: str, tabla: str) -> dict[str, dict]:
    """Deduce los tipos leyendo la TABLA DESTINO de SQL Server.

    POR QUE ESTA ES LA MEJOR FUENTE DE TIPOS
    ----------------------------------------
    El objetivo del cast no es estetico: es que el parquet tenga exactamente
    los tipos que la tabla destino espera, porque si no coinciden el INSERT por
    JDBC falla -o, peor, no falla y trunca-. Y quien sabe con certeza esos
    tipos no es la Variable ni el catalogo: es la propia tabla destino.

    Asi que el orden de preferencia es:

        1. tablas[].tipos de la Variable, si esta declarado. Es el override
           explicito y gana siempre: sirve para los casos en que el destino
           tiene un tipo mas ancho a proposito.
        2. INFORMATION_SCHEMA.COLUMNS de la tabla destino. No hay que
           mantenerlo a mano y no se desincroniza nunca, porque ES el destino.
        3. Lo que el driver JDBC infiera del core. Ultimo recurso.

    Si la tabla destino no existe, se avisa y se cae al paso 3 en vez de
    fallar: el error de "tabla destino inexistente" tiene que salir de la
    carga, que es quien lo puede explicar bien, no de aqui.
    """
    partes = str(tabla_destino).replace("[", "").replace("]", "").split(".")
    esquema, nombre = ("dbo", partes[0]) if len(partes) == 1 else (partes[0], partes[1])

    filas = filas_de(conn_ctl,
        "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{validar(esquema, 'esquema destino')}' "
        f"  AND TABLE_NAME = '{validar(nombre, 'tabla destino')}'")
    if not filas:
        logger.warning(
            "[%s] la tabla destino %s no existe o no tiene columnas visibles: "
            "no se pueden deducir tipos de ahi. Se usara lo que infiera el "
            "driver JDBC, que es justo lo que suele romper la escritura.",
            tabla, tabla_destino)
        return {}

    mapa: dict[str, dict] = {}
    desconocidos = []
    for f in filas:
        col = str(f["COLUMN_NAME"])
        if col.upper() in COLUMNAS_INYECTADAS:
            continue
        sql_tipo = str(f["DATA_TYPE"]).strip().lower()
        tipo = SQLSERVER_A_TIPO.get(sql_tipo)
        if tipo is None:
            desconocidos.append(f"{col}:{sql_tipo}")
            continue
        if tipo == "decimal":
            mapa[col] = {"type": "decimal",
                         "precision": int(f["NUMERIC_PRECISION"] or 38),
                         "scale": int(f["NUMERIC_SCALE"] or 0)}
        else:
            mapa[col] = {"type": tipo}

    if desconocidos:
        logger.warning("[%s] tipos de %s que no se saben traducir, se dejan "
                       "como vengan: %s", tabla, tabla_destino,
                       ", ".join(desconocidos))
    logger.info("[%s] tipos deducidos de la tabla destino %s: %s columna(s)",
                tabla, tabla_destino, len(mapa))
    return mapa


def aplicar_tipos(df, mapa: dict[str, dict], tabla: str):
    """Castea las columnas declaradas. La cadena vacia pasa a NULL ANTES del
    cast: '' casteado a decimal da NULL en Spark, pero a int da 0 en algunas
    versiones, y un cero inventado en un importe es peor que un nulo."""
    equivalencias = {
        "int": T.IntegerType(),
        "bigint": T.LongType(),
        "float": T.DoubleType(),
        "string": T.StringType(),
        "date": T.DateType(),
        "timestamp": T.TimestampType(),
    }
    presentes = set(df.columns)
    for col, cfg in mapa.items():
        if col not in presentes:
            logger.warning("[%s] TIPOS declara %r y no viene en el SELECT: se ignora",
                           tabla, col)
            continue
        if cfg["type"] == "decimal":
            destino = T.DecimalType(cfg["precision"], cfg["scale"])
        else:
            destino = equivalencias[cfg["type"]]

        if cfg["type"] in ("int", "bigint", "float", "decimal", "date", "timestamp"):
            df = df.withColumn(col, F.when(F.trim(F.col(col).cast("string")) == "", None)
                                     .otherwise(F.col(col)))
        df = df.withColumn(col, F.col(col).cast(destino))
    return df


# ============================================================================
# LECTURA DEL ORIGEN
# ============================================================================
def validar(valor, campo: str) -> str:
    v = str(valor or "").strip()
    if not IDENTIFICADOR.match(v):
        raise SystemExit(f"{campo} no es un identificador valido: {valor!r}")
    return v


def construir_select(fila: dict) -> str:
    esquema = validar(fila["ESQUEMA"], "ESQUEMA")
    tabla = validar(fila["TABLA"], "TABLA")
    columnas = (str(fila.get("COLUMNAS") or "*")).strip() or "*"
    sql = f"SELECT {columnas} FROM {esquema}.{tabla}"
    filtro = (str(fila.get("FILTRO") or "")).strip()
    if filtro:
        sql += f" WHERE {filtro}"
    return sql


def leer_origen(spark, url: str, clase: str, usuario: str, clave: str,
                fila: dict, particion: dict | None, fetchsize: int):
    """Lee la tabla del core. Con particion, en paralelo; sin ella, en un hilo.

    El aviso cuando falta la particion no es decorativo: sin ella este job hace
    exactamente el trabajo del pipeline de pandas pero arrastrando un cluster,
    y conviene que eso se vea en el log en vez de descubrirlo comparando
    tiempos semana tras semana.
    """
    tabla = fila["TABLA"]
    consulta = f"({construir_select(fila)}) AS q"

    lector = (spark.read.format("jdbc")
              .option("url", url)
              .option("driver", clase)
              .option("user", usuario)
              .option("password", clave)
              .option("dbtable", consulta)
              .option("fetchsize", int(fetchsize)))

    if particion:
        col = validar(particion["columna"], "particion.columna")
        lector = (lector
                  .option("partitionColumn", col)
                  .option("lowerBound", str(particion["desde"]))
                  .option("upperBound", str(particion["hasta"]))
                  .option("numPartitions", str(particion["particiones"])))
        logger.info("[%s] lectura en %s particiones por %s [%s..%s]",
                    tabla, particion["particiones"], col,
                    particion["desde"], particion["hasta"])
    else:
        logger.warning(
            "[%s] SIN particion declarada: se lee con UNA sola conexion. "
            "Spark no aporta paralelismo aqui. Declare la tabla en "
            "'particiones' de la Variable para que lea en varios hilos.", tabla)

    return lector.load()


# ============================================================================
# BITACORA  (ctl_proceso_parquet, en SQL Server)
# ============================================================================
def filtrar_por_procesos(filas: list[dict], cfg: dict, tipo_ejecucion: str) -> list[dict]:
    """Aplica la lista 'procesos' y ORDENA por prioridad.

    ESTO FALTABA, Y ERA UN FALLO DE VERDAD
    --------------------------------------
    Hasta el 2026-10-07 este job extraia todas las tablas con activo='S' y en el
    orden en que estuvieran escritas en la Variable: ignoraba 'procesos' por
    completo. El resultado era que una tabla con estado=0, o con el flag del
    calendario del dia en 0, se extraia igual. El DAG la reportaba como
    'inactiva' en el inventario y Spark la extraia a continuacion.

    Eso no es solo un dia de datos de mas: rompe la unica validacion que dice
    que la traduccion a Spark salio bien, que es correr los dos pipelines sobre
    el mismo dia y comparar fila a fila. El de pandas si filtra.

    La semantica se copia de filtrar_procesos() de etl_bt2sql/bt2sql_extraccion.py
    para que las dos mitades decidan igual:

      - el cruce es (nombre_proceso, nombre_esquema) contra
        (NOMBRE_PARQUET o TABLA, ESQUEMA), todo en mayusculas
      - el proceso tiene que traer estado=1 Y el flag del dia en 1
      - prioridad ordena; sin prioridad declarada, 99, o sea al final
    """
    flag = {"diario": "estado_diario",
            "semanal": "estado_semanal",
            "mensual": "estado_mensual"}.get(tipo_ejecucion)
    if flag is None:
        raise SystemExit(
            f"tipo_ejecucion={tipo_ejecucion!r} no soportado. "
            f"Use diario, semanal o mensual.")

    declarados = cfg.get("procesos") or []
    if not declarados:
        raise SystemExit(
            "La configuracion no trae 'extraccion.procesos', asi que no hay "
            "calendario que aplicar. Declarelo en la Variable: una tabla entra "
            "en una corrida solo si esta en 'tablas' con activo='S' Y en "
            "'procesos' con estado=1 y el flag del dia en 1.")

    activos, inactivos = {}, []
    for p in declarados:
        clave = (str(p.get("nombre_proceso", "")).strip().upper(),
                 str(p.get("nombre_esquema", "")).strip().upper())
        if int(p.get("estado", 0)) == 1 and int(p.get(flag, 0)) == 1:
            activos[clave] = p
        else:
            motivo = ("estado=0" if int(p.get("estado", 0)) != 1
                      else f"{flag}=0")
            inactivos.append(f"{clave[0]} ({motivo})")

    seleccion, fuera = [], []
    for fila in filas:
        nombre = str(fila.get("NOMBRE_PARQUET") or fila["TABLA"]).strip().upper()
        clave = (nombre, str(fila["ESQUEMA"]).strip().upper())
        proc = activos.get(clave)
        if proc is None:
            fuera.append(f"{clave[1]}.{fila['TABLA']} (proceso {clave[0]})")
            continue
        seleccion.append({**fila, "_PRIORIDAD": int(proc.get("prioridad", 99))})

    logger.info("CALENDARIO tipo_ejecucion=%s (flag %s): %s de %s proceso(s) activos",
                tipo_ejecucion, flag, len(activos), len(declarados))
    if inactivos:
        logger.info("  procesos que no entran hoy: %s", ", ".join(inactivos))
    if fuera:
        logger.info("  tablas que no entran hoy   : %s", ", ".join(fuera))

    if not seleccion:
        raise SystemExit(
            f"Ninguna tabla declarada coincide con los procesos activos para "
            f"tipo_ejecucion={tipo_ejecucion!r}.\n"
            f"El cruce es tablas[].nombre_proceso + tablas[].esquema contra "
            f"procesos[].nombre_proceso + procesos[].nombre_esquema, y el "
            f"proceso necesita estado=1 y {flag}=1.")

    seleccion.sort(key=lambda f: (f["_PRIORIDAD"], str(f["TABLA"])))
    logger.info("ORDEN DE EXTRACCION por prioridad:")
    for f in seleccion:
        logger.info("  prioridad %-3s %s.%s", f["_PRIORIDAD"], f["ESQUEMA"], f["TABLA"])
    return seleccion


def catalogo_de_tablas(cfg: dict, conn_ctl, tipo_ejecucion: str = "diario") -> list[dict]:
    """Devuelve las tablas por extraer, de la Variable o del catalogo.

    Con origen_tablas='variable' (lo normal) la lista sale de la Variable de
    Airflow y NO se consulta CTL_PARAMETROS_PARQUET. La ventaja no es ahorrar
    una consulta: es que agregar una tabla pasa a ser editar un JSON versionado
    en el repositorio, revisable en un diff, en vez de un UPDATE a mano contra
    una tabla de produccion que nadie ve pasar.

    Se normalizan las claves A MAYUSCULAS en los dos casos para que el resto
    del job no tenga que saber de donde vino la lista.
    """
    origen = cfg.get("origen_tablas", "variable")

    if origen == "catalogo":
        filas = filas_de(conn_ctl, cfg["sql_parametros_parquet"])
        if not filas:
            raise SystemExit(
                "origen_tablas='catalogo' y CTL_PARAMETROS_PARQUET no devolvio "
                "ninguna tabla con ACTIVO='S'.")
        for fila in filas:
            fila["_PARTICION"] = (cfg.get("particiones") or {}).get(str(fila["TABLA"]))
        logger.info("%s tabla(s) activas, leidas del CATALOGO en la base", len(filas))
        return filtrar_por_procesos(filas, cfg, tipo_ejecucion)

    if origen != "variable":
        raise SystemExit(
            f"origen_tablas='{origen}' no es valido. Use 'variable' o 'catalogo'.")

    declaradas = cfg.get("tablas") or []
    if not declaradas:
        raise SystemExit(
            "origen_tablas='variable' pero la Variable no trae ninguna tabla en "
            "'tablas'. Agreguelas en airflow/config/json/BT2SQL_SPARK.json y "
            "sincronice con  python scripts/sync_variables.py --solo BT2SQL_SPARK")

    filas, inactivas = [], []
    for entrada in declaradas:
        if str(entrada.get("activo", "S")).strip().upper() != "S":
            inactivas.append(str(entrada.get("tabla", "?")))
            continue
        filas.append({
            "ESQUEMA": entrada["esquema"],
            "TABLA": entrada["tabla"],
            "NOMBRE_PARQUET": entrada.get("nombre_proceso") or entrada["tabla"],
            "COLUMNAS": entrada.get("columnas") or "",
            "FILTRO": entrada.get("filtro") or "",
            "TIPOS": entrada.get("tipos") or "",
            "ACTIVO": "S",
            "TABLA_DESTINO": entrada.get("tabla_destino") or "",
            "_PARTICION": entrada.get("particion"),
        })
    if not filas:
        raise SystemExit(
            f"Las {len(declaradas)} tabla(s) de la Variable estan todas con "
            f"activo distinto de 'S'. No hay nada que extraer.")

    logger.info("%s tabla(s) activas, leidas de la VARIABLE de Airflow", len(filas))
    if inactivas:
        logger.info("  omitidas por activo<>'S': %s", ", ".join(inactivas))
    sin_particion = [f["TABLA"] for f in filas if not f.get("_PARTICION")]
    if sin_particion:
        logger.warning(
            "sin columna de particion, se leen con UNA conexion: %s. "
            "Spark no aporta paralelismo de lectura en esas tablas.",
            ", ".join(sin_particion))
    return filtrar_por_procesos(filas, cfg, tipo_ejecucion)


def abrir_log(conn_ctl, cfg: dict, fila: dict, batch_id: str, fecha_proceso) -> int:
    ejecutar(conn_ctl,
             f"INSERT INTO {cfg['tb_proceso_parquet']} "
             f"(nom_proceso, batch_id, esquema, tabla_origen, fecha_proceso, "
             f" archivo_parquet, fec_inicio, estado, host_name) "
             f"VALUES (?, ?, ?, ?, ?, ?, SYSDATETIME(), ?, ?)",
             (cfg["nom_proceso"], batch_id, str(fila["ESQUEMA"]), str(fila["TABLA"]),
              str(fecha_proceso), None, cfg["estado_ejecutando"],
              os.environ.get("HOSTNAME", "spark")))
    filas = filas_de(conn_ctl,
                     f"SELECT MAX(id_log) AS ID FROM {cfg['tb_proceso_parquet']} "
                     f"WHERE batch_id = '{batch_id}' "
                     f"  AND tabla_origen = '{fila['TABLA']}'")
    return int(filas[0]["ID"])


def cerrar_log(conn_ctl, cfg: dict, id_log: int, estado: str,
               archivo: str | None = None, filas: int | None = None,
               segundos: float | None = None, error: str | None = None) -> None:
    try:
        ejecutar(conn_ctl,
                 f"UPDATE {cfg['tb_proceso_parquet']} SET "
                 f"  estado = ?, fec_termino = SYSDATETIME(), "
                 f"  archivo_parquet = COALESCE(?, archivo_parquet), "
                 f"  filas_extraidas = COALESCE(?, filas_extraidas), "
                 f"  duracion_segundos = ?, msg_error = ? "
                 f"WHERE id_log = ?",
                 (estado, archivo, filas, segundos,
                  (error or "")[:int(cfg["error_size_limit"])] or None, id_log))
    except Exception as exc:                                   # noqa: BLE001
        logger.error("no se pudo cerrar la fila %s de la bitacora: %s", id_log, exc)


# ============================================================================
# MAIN
# ============================================================================
def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--jdbc-url", required=True, help="URL de Bantotal (origen)")
    p.add_argument("--driver-class", required=True)
    p.add_argument("--config", required=True, help="JSON materializado por el DAG")
    p.add_argument("--batch-id", required=True)
    p.add_argument("--tipo-ejecucion", default="diario")
    p.add_argument(
        "--proceso", default=None,
        help=("nombre_proceso de UNA sola tabla. Cuando el DAG dibuja una tarea "
              "por proceso, cada tarea envia su propio job con este argumento. "
              "Sin el, se extrae todo el calendario del dia en una sola "
              "aplicacion, que es como corria antes y sigue sirviendo para "
              "lanzar el job a mano."))
    args = p.parse_args()

    with open(args.config, encoding="utf-8") as fh:
        documento = json.load(fh)
    cfg = documento["extraccion"]
    # 'tablas' y 'origen_tablas' son del pipeline entero, no de una mitad: la
    # misma entrada define QUE se extrae y A DONDE se sube, para no tener que
    # agregar una tabla en dos sitios y acordarse de los dos.
    cfg["tablas"] = documento.get("tablas", [])
    cfg["origen_tablas"] = str(documento.get("origen_tablas", "variable")).strip().lower()

    # Credenciales por entorno, nunca por --conf ni por argumento: lo que va en
    # spark.* aparece en la pestana Environment de la interfaz, en ps dentro del
    # contenedor, y -con eventLog activo- queda escrito en disco.
    bt_usuario = os.environ.get("ORIGEN_USUARIO", "")
    bt_clave = os.environ.get("ORIGEN_CLAVE", "")
    ctl_usuario = os.environ.get("DESTINO_USUARIO", "")
    ctl_clave = os.environ.get("DESTINO_CLAVE", "")
    ctl_url = os.environ.get("DESTINO_JDBC_URL", "")
    ctl_clase = os.environ.get("DESTINO_DRIVER_CLASS",
                               "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    if not ctl_url:
        raise SystemExit(
            "Falta DESTINO_JDBC_URL en el entorno. El catalogo y la bitacora "
            "viven en SQL Server, no en Bantotal.")

    spark = (SparkSession.builder
             .appName(f"bt2sql_extraccion_spark {args.batch_id}")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
             .getOrCreate())

    conn_ctl = abrir_jdbc(spark, ctl_url, ctl_usuario, ctl_clave, ctl_clase)
    conn_bt = abrir_jdbc(spark, args.jdbc_url, bt_usuario, bt_clave, args.driver_class)

    resultados, con_error = [], []
    try:
        # --- fecha de proceso: del CORE, no del destino ---------------------
        # La toma de Bantotal por la misma razon que el pipeline de pandas: la
        # tabla STG del destino la llena este mismo proceso, asi que leerla de
        # ahi seria preguntarle a la corrida anterior.
        fp = filas_de(conn_bt, cfg["sql_fecha"])
        if not fp:
            raise SystemExit(f"sql_fecha no devolvio ninguna fila: {cfg['sql_fecha']}")
        crudo = str(list(fp[0].values())[0]).strip().split(".")[0]
        fecha_proceso = (datetime.strptime(crudo, "%Y%m%d").date()
                         if crudo.isdigit() and len(crudo) == 8
                         else datetime.fromisoformat(crudo[:10]).date())
        logger.info("fecha de proceso del core: %s", fecha_proceso)

        catalogo = catalogo_de_tablas(cfg, conn_ctl, args.tipo_ejecucion)

        # --proceso recorta DESPUES del calendario, no antes: asi una tarea de
        # un proceso que hoy no entra no lo extrae por la puerta de atras. El
        # DAG ya marca esa tarea como saltada, pero el job no puede confiar en
        # eso -se le puede invocar a mano- y el calendario tiene que valer
        # igual por los dos caminos.
        if args.proceso:
            pedido = args.proceso.strip().upper()
            catalogo = [f for f in catalogo
                        if str(f.get("NOMBRE_PARQUET") or f["TABLA"]).strip().upper() == pedido]
            if not catalogo:
                raise SystemExit(
                    f"El proceso {args.proceso!r} no esta entre los que entran "
                    f"en una corrida {args.tipo_ejecucion!r}.\n"
                    f"O no esta declarado en 'tablas', o su entrada en "
                    f"'procesos' tiene estado=0 o el flag del dia en 0.")
            logger.info("Corrida de UN SOLO proceso: %s", args.proceso)
        carpeta_dia = f"{cfg['output_dir']}/{fecha_proceso:%Y%m%d}"

        for fila in catalogo:
            tabla = str(fila["TABLA"])
            inicio = datetime.now()
            id_log = abrir_log(conn_ctl, cfg, fila, args.batch_id, fecha_proceso)
            destino = f"{carpeta_dia}/{tabla}"
            try:
                df = leer_origen(spark, args.jdbc_url, args.driver_class,
                                 bt_usuario, bt_clave, fila,
                                 fila.get("_PARTICION"), cfg.get("fetchsize", 10000))

                # Orden de preferencia de los tipos: Variable > tabla destino
                # > lo que infiera el driver. Ver tipos_de_destino().
                mapa_tipos = parsear_tipos(fila.get("TIPOS"), tabla)
                if mapa_tipos:
                    logger.info("[%s] tipos declarados en la Variable: %s columna(s)",
                                tabla, len(mapa_tipos))
                elif str(cfg.get("tipos_desde_destino", True)).lower() not in ("false", "0", "no"):
                    mapa_tipos = tipos_de_destino(conn_ctl, fila.get("TABLA_DESTINO", ""), tabla)
                if not mapa_tipos:
                    logger.warning(
                        "[%s] sin tipos declarados ni deducibles: el parquet "
                        "queda con lo que infiera el driver JDBC. Si la carga "
                        "falla por tipos, declare 'tipos' para esta tabla en la "
                        "Variable.", tabla)
                df = aplicar_tipos(df, mapa_tipos, tabla)

                # Las dos columnas propias, con el MISMO criterio que el
                # pipeline de pandas: FECHA_PROCESO primera, BATCH_ID ultima.
                # Sin esto los dos parquet no son comparables columna a columna.
                df = (df.withColumn("FECHA_PROCESO", F.lit(str(fecha_proceso)).cast(T.DateType()))
                        .withColumn("BATCH_ID", F.lit(args.batch_id)))
                orden = ["FECHA_PROCESO"] + [c for c in df.columns
                                             if c not in ("FECHA_PROCESO", "BATCH_ID")] + ["BATCH_ID"]
                df = df.select(*orden)

                n = df.count()
                if n == 0:
                    cerrar_log(conn_ctl, cfg, id_log, cfg["estado_sin_datos"],
                               filas=0, segundos=(datetime.now() - inicio).total_seconds())
                    logger.info("[%s] sin filas: no se escribe parquet", tabla)
                    resultados.append({"tabla": tabla, "filas": 0, "estado": "SIN_DATOS"})
                    continue

                (df.coalesce(int(cfg.get("archivos_por_tabla", 1)))
                   .write.mode("overwrite")
                   .parquet(destino))

                segundos = (datetime.now() - inicio).total_seconds()
                cerrar_log(conn_ctl, cfg, id_log, cfg["estado_terminado"],
                           archivo=destino, filas=n, segundos=segundos)
                logger.info("[%s] %s filas -> %s  (%.1f s)", tabla, f"{n:,}", destino, segundos)
                resultados.append({"tabla": tabla, "filas": n, "estado": "TERMINADO"})

            except Exception as exc:                            # noqa: BLE001
                segundos = (datetime.now() - inicio).total_seconds()
                cerrar_log(conn_ctl, cfg, id_log, cfg["estado_error"],
                           segundos=segundos, error=str(exc))
                logger.error("[%s] ERROR: %s", tabla, exc)
                con_error.append(tabla)
                resultados.append({"tabla": tabla, "filas": 0, "estado": "ERROR"})
    finally:
        try:
            conn_bt.close()
            conn_ctl.close()
        except Exception:                                       # noqa: BLE001
            pass
        spark.stop()

    total = sum(r["filas"] for r in resultados)
    logger.info("batch %s: %s tabla(s), %s filas, %s con error",
                args.batch_id, len(resultados), f"{total:,}", len(con_error))

    if con_error:
        logger.error("tablas con error: %s", ", ".join(con_error))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
