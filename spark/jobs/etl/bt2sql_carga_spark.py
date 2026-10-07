"""
bt2sql_carga_spark - Parquet -> STG en SQL Server, con Spark.

GEMELO DE etl_bt2sql/bt2sql_carga.py
------------------------------------
Mismo contrato: lee ctl_proceso_parquet para saber que archivos subir y a que
tabla, pasa por una tabla de staging y aplica con UNA sentencia dentro de una
transaccion. Lo que cambia es que la ESCRITURA va en paralelo.

EL REPARTO ENTRE DRIVER Y EXECUTORS, Y POR QUE
-----------------------------------------------
    [driver]     leer ctl_proceso_parquet        decenas de filas, 1 hilo
    [driver]     DROP + SELECT TOP 0 ... INTO    DDL, 1 sentencia
    [executors]  escribir el parquet en staging  N conexiones en paralelo
    [driver]     TRUNCATE + INSERT ... SELECT    1 transaccion

El paso del medio es el unico que se beneficia de Spark, y es el unico que
tarda. Los otros tres son sentencias sueltas: mandarlas por el cluster solo
agregaria coordinacion.

El TRUNCATE + INSERT final NO se puede hacer con spark.write.jdbc. Su modo
"overwrite" borra y recrea la tabla con los tipos que Spark infiera, lo que
destruiria el DDL de la tabla STG -longitudes, precisiones, indices-. Por eso
ese paso va por JDBC crudo desde el driver, dentro de una transaccion que
tambien cubre el TRUNCATE: en SQL Server TRUNCATE es transaccional y se puede
deshacer con ROLLBACK.

INVOCACION  -- la hace el DAG
    spark-submit /opt/spark-apps/etl/bt2sql_carga_spark.py \
        --jdbc-url <url de SQL Server> \
        --driver-class com.microsoft.sqlserver.jdbc.SQLServerDriver \
        --config /opt/spark-data/runtime/bt2sql_spark.json \
        --batch-id 20261005120000
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bt2sql_carga_spark")

IDENTIFICADOR = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def abrir_jdbc(spark, url: str, usuario: str, clave: str, clase: str):
    jvm = spark._jvm
    jvm.Class.forName(clase)
    props = jvm.java.util.Properties()
    props.setProperty("user", usuario or "")
    props.setProperty("password", clave or "")
    return jvm.java.sql.DriverManager.getConnection(url, props)


def filas_de(conexion, sql: str) -> list[dict]:
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


def validar(valor, campo: str) -> str:
    v = str(valor or "").strip()
    if not IDENTIFICADOR.match(v):
        raise SystemExit(f"{campo} no es un identificador valido: {valor!r}")
    return v


def partir_destino(tabla_destino: str) -> tuple[str, str]:
    """'dbo.STG_FSH005' -> ('dbo','STG_FSH005'). Sin esquema -> dbo."""
    partes = str(tabla_destino).replace("[", "").replace("]", "").split(".")
    if len(partes) == 1:
        return "dbo", validar(partes[0], "tabla_destino")
    return validar(partes[0], "tabla_destino.esquema"), validar(partes[1], "tabla_destino.tabla")


def corchetes(*partes: str) -> str:
    return ".".join(f"[{p}]" for p in partes)


# ============================================================================
# BITACORA DE CARGA  (ctl_carga_stg)
# ============================================================================
def lotes_por_cargar(cfg: dict, conn, batch_id: str) -> list[dict]:
    """Cruza lo que dejo la extraccion con el destino declarado en la Variable.

    LA SEPARACION QUE HACE ESTA FUNCION
    -----------------------------------
    Hay dos cosas distintas y conviene no mezclarlas:

      - QUE produjo la extraccion de ESTE batch: archivo_parquet, tabla_origen.
        Es estado de la corrida. Vive en ctl_proceso_parquet y solo la base lo
        sabe.
      - A DONDE va cada tabla y con que tamanos: tabla_destino, batch_size,
        commit_every. Es configuracion. Vive en la Variable.

    Antes las dos salian de la misma consulta, con un INNER JOIN contra
    CTL_PARAMETROS_PARQUET. Ese join tenia una consecuencia que no se ve
    leyendolo: una tabla extraida sin fila en el catalogo desaparecia del
    resultado y nadie se enteraba -el parquet quedaba escrito en disco y la
    tabla destino con los datos del dia anterior-. Tanto es asi que el script
    del catalogo trae una consulta especifica para detectar esa perdida.

    Aqui una tabla extraida que no este declarada es un ERROR con nombre, no
    una desaparicion.
    """
    sql = cfg["sql_lotes"].replace("{BATCH_ID}", f"'{batch_id}'")
    if "{BATCH_ID}" in cfg["sql_lotes"] and batch_id not in sql:
        raise SystemExit("No se pudo sustituir {BATCH_ID} en sql_lotes.")
    producidos = filas_de(conn, sql)
    if not producidos:
        raise SystemExit(
            f"No hay nada que cargar para el batch {batch_id}. "
            f"Revise que la extraccion haya dejado filas en TERMINADO.")

    if cfg.get("origen_tablas", "variable") != "variable":
        # Con origen_tablas='catalogo' el destino lo pone la base: se espera
        # que la consulta ya devuelva tabla_destino.
        for fila in producidos:
            if not fila.get("tabla_destino"):
                raise SystemExit(
                    f"origen_tablas='catalogo' pero la consulta no devolvio "
                    f"tabla_destino para {fila.get('tabla_origen')}.")
        return producidos

    # Indice por (esquema, tabla), ambos normalizados: el core devuelve los
    # nombres en mayusculas y una Variable escrita a mano casi nunca.
    declaradas = {
        (str(e["esquema"]).strip().upper(), str(e["tabla"]).strip().upper()): e
        for e in (cfg.get("tablas") or [])
    }

    lotes, huerfanas = [], []
    for fila in producidos:
        clave = (str(fila["esquema"]).strip().upper(),
                 str(fila["tabla_origen"]).strip().upper())
        entrada = declaradas.get(clave)
        if entrada is None:
            huerfanas.append(f"{clave[0]}.{clave[1]}")
            continue
        destino = str(entrada.get("tabla_destino") or "").strip()
        if not destino:
            raise SystemExit(
                f"La tabla {clave[0]}.{clave[1]} esta en 'tablas' pero sin "
                f"'tabla_destino'. No se sabe donde cargarla.")
        lotes.append({
            "archivo_parquet": fila["archivo_parquet"],
            "tabla_destino": destino,
            "batch_size": int(entrada.get("batch_size") or cfg["batch_default"]),
            "commit_every": int(entrada.get("commit_every") or cfg["commit_default"]),
        })

    if huerfanas:
        raise SystemExit(
            f"La extraccion dejo parquet de tabla(s) que no estan declaradas en "
            f"'tablas' de la Variable: {', '.join(sorted(set(huerfanas)))}.\n"
            f"Se corta aqui a proposito: cargar el resto y callar esto dejaria "
            f"esas tablas destino con los datos del dia anterior y el parquet "
            f"nuevo en disco sin que nadie lo note.\n"
            f"Declarelas en airflow/config/json/BT2SQL_SPARK.json, o desactivelas "
            f"en 'extraccion.procesos' para que no se extraigan.")

    logger.info("%s archivo(s) por cargar, destino tomado de la VARIABLE", len(lotes))
    return lotes


def abrir_log(conn, cfg: dict, batch_id: str, ruta: str, destino: str) -> int:
    ejecutar(conn,
             f"INSERT INTO {cfg['tabla_control']} "
             f"(nom_proceso, batch_id, archivo_parquet, tabla_destino, "
             f" fec_inicio, estado, host_name) "
             f"VALUES (?, ?, ?, ?, SYSDATETIME(), ?, ?)",
             (cfg["nom_proceso"], batch_id, ruta, str(destino),
              cfg["estado_iniciado"], os.environ.get("HOSTNAME", "spark")))
    f = filas_de(conn, f"SELECT MAX(id_log) AS ID FROM {cfg['tabla_control']} "
                       f"WHERE batch_id = '{batch_id}' AND archivo_parquet = '{ruta}'")
    return int(f[0]["ID"])


def cerrar_log(conn, cfg: dict, id_log: int, estado: str,
               filas: int | None = None, segundos: float | None = None,
               error: str | None = None) -> None:
    try:
        ejecutar(conn,
                 f"UPDATE {cfg['tabla_control']} SET "
                 f"  estado = ?, fec_termino = SYSDATETIME(), duracion_segundos = ?, "
                 f"  filas_cargadas = COALESCE(?, filas_cargadas), msg_error = ? "
                 f"WHERE id_log = ?",
                 (estado, segundos, filas,
                  (error or "")[:int(cfg["error_size_limit"])] or None, id_log))
    except Exception as exc:                                    # noqa: BLE001
        logger.error("no se pudo cerrar la fila %s: %s", id_log, exc)


# ============================================================================
# MAIN
# ============================================================================
def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--jdbc-url", required=True, help="URL de SQL Server (destino)")
    p.add_argument("--driver-class", required=True)
    p.add_argument("--config", required=True)
    p.add_argument("--batch-id", required=True)
    args = p.parse_args()

    with open(args.config, encoding="utf-8") as fh:
        documento = json.load(fh)
    cfg = documento["carga"]
    cfg["tablas"] = documento.get("tablas", [])
    cfg["origen_tablas"] = str(documento.get("origen_tablas", "variable")).strip().lower()

    usuario = os.environ.get("ORIGEN_USUARIO", "")
    clave = os.environ.get("ORIGEN_CLAVE", "")

    spark = (SparkSession.builder
             .appName(f"bt2sql_carga_spark {args.batch_id}")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
             .getOrCreate())

    conn = abrir_jdbc(spark, args.jdbc_url, usuario, clave, args.driver_class)
    conn.setAutoCommit(True)

    propiedades = spark._jvm.java.util.Properties()
    propiedades.setProperty("user", usuario)
    propiedades.setProperty("password", clave)
    propiedades.setProperty("driver", args.driver_class)

    resultados, con_error = [], []
    try:
        jobs = lotes_por_cargar(cfg, conn, args.batch_id)

        for job in jobs:
            ruta = str(job["archivo_parquet"])
            destino_nombre = str(job["tabla_destino"])
            inicio = datetime.now()
            id_log = abrir_log(conn, cfg, args.batch_id, ruta, destino_nombre)
            esquema, tabla = partir_destino(destino_nombre)
            destino = corchetes(esquema, tabla)
            staging = corchetes(esquema, f"{tabla}{cfg['sufijo_staging']}")
            try:
                df = spark.read.parquet(ruta)
                columnas = df.columns
                if not columnas:
                    raise SystemExit(f"El parquet {ruta} no declara columnas.")
                cols = ", ".join(f"[{c}]" for c in columnas)

                # 1) staging con los tipos de la DESTINO, no los que Spark
                #    infiera del parquet. Si se declararan aqui, un VARCHAR(50)
                #    contra un VARCHAR(MAX) pasaria desapercibido hasta que una
                #    fila larga reventara el INSERT final.
                ejecutar(conn, f"DROP TABLE IF EXISTS {staging}")
                ejecutar(conn, f"SELECT TOP 0 {cols} INTO {staging} FROM {destino}")

                # 2) la parte que SI paraleliza. mode("append") y no
                #    "overwrite": overwrite borraria la staging recien creada y
                #    la recrearia con los tipos inferidos por Spark, que es
                #    justo lo que el paso 1 evita.
                (df.write
                   .mode("append")
                   .option("batchsize", int(job.get("batch_size") or cfg["batch_default"]))
                   .option("isolationLevel", "READ_COMMITTED")
                   .jdbc(args.jdbc_url, f"{esquema}.{tabla}{cfg['sufijo_staging']}",
                         properties=propiedades))

                escritas = int(filas_de(conn, f"SELECT COUNT(*) AS N FROM {staging}")[0]["N"])

                # 3) el intercambio, en UNA transaccion
                conn.setAutoCommit(False)
                try:
                    ejecutar(conn, f"TRUNCATE TABLE {destino}")
                    ejecutar(conn, f"INSERT INTO {destino} ({cols}) SELECT {cols} FROM {staging}")
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    conn.setAutoCommit(True)

                segundos = (datetime.now() - inicio).total_seconds()
                cerrar_log(conn, cfg, id_log, cfg["estado_finalizado"],
                           filas=escritas, segundos=segundos)
                logger.info("%-28s -> %-28s  %s filas  %.1f s",
                            os.path.basename(ruta.rstrip("/")), destino_nombre,
                            f"{escritas:,}", segundos)
                resultados.append({"destino": destino_nombre, "filas": escritas})

            except Exception as exc:                            # noqa: BLE001
                segundos = (datetime.now() - inicio).total_seconds()
                cerrar_log(conn, cfg, id_log, cfg["estado_error"],
                           segundos=segundos, error=str(exc))
                logger.error("%-28s ERROR: %s", destino_nombre, exc)
                con_error.append(destino_nombre)
    finally:
        try:
            conn.close()
        except Exception:                                       # noqa: BLE001
            pass
        spark.stop()

    total = sum(r["filas"] for r in resultados)
    logger.info("batch %s: %s tabla(s) ok, %s con error, %s filas",
                args.batch_id, len(resultados), len(con_error), f"{total:,}")
    if con_error:
        logger.error("tablas con error: %s", ", ".join(con_error))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
