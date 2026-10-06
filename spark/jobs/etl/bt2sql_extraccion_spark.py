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
        else:
            logger.warning("[%s] tipo no reconocido %r en %r: se trata como texto",
                           tabla, tipo, col)
            mapa[col] = {"type": "string"}
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

        if cfg["type"] in ("int", "bigint", "float", "decimal"):
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
    args = p.parse_args()

    with open(args.config, encoding="utf-8") as fh:
        cfg = json.load(fh)["extraccion"]

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

        catalogo = filas_de(conn_ctl, cfg["sql_parametros_parquet"])
        if not catalogo:
            raise SystemExit("El catalogo no devolvio ninguna tabla activa.")
        logger.info("%s tabla(s) activas en el catalogo", len(catalogo))

        particiones = cfg.get("particiones", {})
        carpeta_dia = f"{cfg['output_dir']}/{fecha_proceso:%Y%m%d}"

        for fila in catalogo:
            tabla = str(fila["TABLA"])
            inicio = datetime.now()
            id_log = abrir_log(conn_ctl, cfg, fila, args.batch_id, fecha_proceso)
            destino = f"{carpeta_dia}/{tabla}"
            try:
                df = leer_origen(spark, args.jdbc_url, args.driver_class,
                                 bt_usuario, bt_clave, fila,
                                 particiones.get(tabla), cfg.get("fetchsize", 10000))

                df = aplicar_tipos(df, parsear_tipos(fila.get("TIPOS"), tabla), tabla)

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
