from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import PurePosixPath

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def validar_identificador(valor: str, campo: str) -> str:
    if not valor or not IDENTIFIER_RE.match(str(valor)):
        raise ValueError(f"{campo} invalido: {valor!r}")
    return str(valor)


def cargar_config(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def construir_sesion(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def jdbc_options(args, usuario: str, clave: str) -> dict[str, str]:
    return {
        "url": args.jdbc_url,
        "driver": args.driver_class,
        "user": usuario,
        "password": clave,
        "fetchsize": str(args.fetchsize),
    }


def consulta_escalar(spark: SparkSession, args, usuario: str, clave: str, sql: str):
    df = (
        spark.read.format("jdbc")
        .options(**jdbc_options(args, usuario, clave))
        .option("dbtable", f"({sql}) AS q")
        .load()
    )
    fila = df.first()
    if fila is None:
        raise ValueError(f"La consulta no devolvio filas: {sql}")
    return fila[0]


def leer_consulta(spark: SparkSession, args, usuario: str, clave: str, sql: str):
    return (
        spark.read.format("jdbc")
        .options(**jdbc_options(args, usuario, clave))
        .option("dbtable", f"({sql}) AS q")
        .load()
    )


def parse_dtype_map(tipos_str: str | None) -> dict[str, dict]:
    dtype_map: dict[str, dict] = {}
    if not tipos_str:
        return dtype_map

    for item in str(tipos_str).split("|"):
        if not item.strip():
            continue
        col, typ = item.split(":", 1)
        col, typ = col.strip(), typ.strip().lower()

        if typ.startswith(("char", "varchar")):
            dtype_map[col] = {"type": "string"}
        elif typ.startswith(("decimal", "numeric")):
            match = re.search(r"\((\d+),(\d+)\)", typ)
            if not match:
                raise ValueError(f"Decimal sin precision: {typ}")
            dtype_map[col] = {
                "type": "decimal",
                "precision": int(match.group(1)),
                "scale": int(match.group(2)),
            }
        elif typ in ("int", "integer"):
            dtype_map[col] = {"type": "int"}
        elif typ == "bigint":
            dtype_map[col] = {"type": "bigint"}
        elif typ in ("float", "double"):
            dtype_map[col] = {"type": "double"}
        else:
            dtype_map[col] = {"type": "string"}
    return dtype_map


def aplicar_tipos(df, dtype_map: dict[str, dict]):
    for col, config in dtype_map.items():
        if col not in df.columns:
            continue
        tipo = config["type"]
        if tipo == "string":
            df = df.withColumn(col, F.col(col).cast(T.StringType()))
        elif tipo == "int":
            df = df.withColumn(col, F.col(col).cast(T.IntegerType()))
        elif tipo == "bigint":
            df = df.withColumn(col, F.col(col).cast(T.LongType()))
        elif tipo == "double":
            df = df.withColumn(col, F.col(col).cast(T.DoubleType()))
        elif tipo == "decimal":
            df = df.withColumn(
                col,
                F.col(col).cast(T.DecimalType(config["precision"], config["scale"])),
            )
    return df


def procesos_por_prioridad(parametros, procesos_config: list[dict], tipo_ejecucion: str):
    procesos_dict = {p["nombre_proceso"]: p for p in procesos_config}
    prioridad_1 = []
    prioridad_2 = []

    for row in parametros.collect():
        datos = row.asDict()
        if str(datos.get("ACTIVO")) != "S":
            continue

        nombre_proceso = str(datos["TABLA"]).replace(".parquet", "")
        proc = procesos_dict.get(nombre_proceso)
        if not proc:
            print(f"[spark] AVISO: {nombre_proceso} no existe en configuracion JSON")
            continue

        ejecutar = False
        if tipo_ejecucion == "diario":
            ejecutar = int(proc.get("estado_diario", 0)) == 1
        elif tipo_ejecucion == "semanal":
            ejecutar = int(proc.get("estado_semanal", 0)) == 1
        elif tipo_ejecucion == "mensual":
            ejecutar = int(proc.get("estado_mensual", 0)) == 1

        if not ejecutar:
            continue

        prioridad = int(proc.get("prioridad", 2))
        (prioridad_1 if prioridad == 1 else prioridad_2).append(datos)

    return prioridad_1, prioridad_2


def ejecutar_update_jdbc(spark: SparkSession, args, usuario: str, clave: str, sql: str, params: list | tuple = ()):
    jvm = spark.sparkContext._jvm
    jvm.java.lang.Class.forName(args.driver_class)
    conn = jvm.java.sql.DriverManager.getConnection(args.jdbc_url, usuario, clave)
    try:
        st = conn.prepareStatement(sql)
        for idx, value in enumerate(params, start=1):
            st.setString(idx, "" if value is None else str(value))
        st.executeUpdate()
        st.close()
        if not conn.getAutoCommit():
            conn.commit()
    finally:
        conn.close()


def ejecutar_insert_id_jdbc(
    spark: SparkSession,
    args,
    usuario: str,
    clave: str,
    sql: str,
    params: list | tuple = (),
):
    jvm = spark.sparkContext._jvm
    jvm.java.lang.Class.forName(args.driver_class)
    conn = jvm.java.sql.DriverManager.getConnection(args.jdbc_url, usuario, clave)
    try:
        st = conn.prepareStatement(sql)
        for idx, value in enumerate(params, start=1):
            st.setString(idx, "" if value is None else str(value))
        st.executeUpdate()
        st.close()

        last_id_stmt = conn.createStatement()
        rs = last_id_stmt.executeQuery("SELECT LAST_INSERT_ID()")
        if not rs.next():
            raise RuntimeError("No se pudo recuperar LAST_INSERT_ID()")
        last_id = rs.getLong(1)
        rs.close()
        last_id_stmt.close()

        if not conn.getAutoCommit():
            conn.commit()
        return last_id
    finally:
        conn.close()


def registrar_inicio(spark, args, usuario, clave, tabla_log, nom_proceso, tabla, archivo, host_name):
    sql = f"""
        INSERT INTO {tabla_log}
            (nom_proceso, tabla_origen, archivo_parquet, fec_inicio, estado, host_name)
        VALUES (?, ?, ?, NOW(6), ?, ?)
    """
    return ejecutar_insert_id_jdbc(
        spark,
        args,
        usuario,
        clave,
        sql,
        [nom_proceso, tabla, archivo, "INICIADO", host_name],
    )


def registrar_estado(spark, args, usuario, clave, tabla_log, id_log, estado):
    ejecutar_update_jdbc(
        spark,
        args,
        usuario,
        clave,
        f"UPDATE {tabla_log} SET estado=? WHERE id_log=?",
        [estado, id_log],
    )


def registrar_fin(spark, args, usuario, clave, tabla_log, id_log, total_rows, total_time):
    ejecutar_update_jdbc(
        spark,
        args,
        usuario,
        clave,
        f"""
        UPDATE {tabla_log}
        SET fec_termino=NOW(6), estado=?, filas_procesadas=?, duracion_segundos=?
        WHERE id_log=?
        """,
        ["TERMINADO", total_rows, round(float(total_time), 2), id_log],
    )


def registrar_error(spark, args, usuario, clave, tabla_log, id_log, error):
    ejecutar_update_jdbc(
        spark,
        args,
        usuario,
        clave,
        f"""
        UPDATE {tabla_log}
        SET fec_termino=NOW(6), estado=?, msg_error=?
        WHERE id_log=?
        """,
        ["ERROR", str(error)[:4000], id_log],
    )


def construir_query(row: dict) -> str:
    columnas = row["COLUMNAS"]
    esquema = validar_identificador(row["ESQUEMA"], "ESQUEMA")
    tabla = validar_identificador(row["TABLA"], "TABLA")
    query = f"SELECT {columnas} FROM {esquema}.{tabla}"
    filtro = row.get("FILTRO")
    if filtro:
        query += f" WHERE {filtro}"
    return query


def procesar_tabla(spark, args, usuario, clave, config: dict, row: dict, fecha_proceso, batch_id: str):
    tabla = row["TABLA"]
    archivo = row["NOMBRE_PARQUET"]
    output_dir = config["output_dir"]
    output_path = str(PurePosixPath(output_dir) / archivo)
    tabla_log = config["tb_proceso_parquet"]
    nom_proceso = config.get("nom_proceso", "GENERACION_PARQUET")
    id_log = None
    inicio = time.time()

    try:
        id_log = registrar_inicio(
            spark,
            args,
            usuario,
            clave,
            tabla_log,
            nom_proceso,
            tabla,
            archivo,
            args.host_name,
        )
        registrar_estado(spark, args, usuario, clave, tabla_log, id_log, "EJECUTANDO")

        df = (
            spark.read.format("jdbc")
            .options(**jdbc_options(args, usuario, clave))
            .option("dbtable", f"({construir_query(row)}) AS src")
            .load()
        )
        df = aplicar_tipos(df, parse_dtype_map(row.get("TIPOS")))
        df = df.withColumn("FECHA_PROCESO", F.lit(str(fecha_proceso)).cast("date"))
        df = df.withColumn("BATCH_ID", F.lit(batch_id))

        total = df.count()
        (
            df.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(output_path)
        )

        registrar_fin(spark, args, usuario, clave, tabla_log, id_log, total, time.time() - inicio)
        print(f"[spark] OK {tabla}: {total} filas -> {output_path}")
        return {"tabla": tabla, "filas": total, "estado": "OK"}
    except Exception as exc:
        if id_log is not None:
            registrar_error(spark, args, usuario, clave, tabla_log, id_log, exc)
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--driver-class", required=True)
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--tipo-ejecucion", default="diario")
    parser.add_argument("--fetchsize", type=int, default=10000)
    parser.add_argument("--host-name", default=os.environ.get("HOSTNAME", "spark"))
    args = parser.parse_args()

    usuario = os.environ.get("ORIGEN_USUARIO")
    clave = os.environ.get("ORIGEN_CLAVE")
    if not usuario or not clave:
        print("[ERROR] Faltan ORIGEN_USUARIO / ORIGEN_CLAVE")
        return 1

    config = cargar_config(args.config_path)["extraccion"]
    config["tb_proceso_parquet"] = validar_identificador(
        config["tb_proceso_parquet"],
        "tb_proceso_parquet",
    )
    spark = construir_sesion("bt_extraccion_parquet_spark")
    spark.sparkContext.setLogLevel("WARN")

    try:
        fecha_proceso = consulta_escalar(spark, args, usuario, clave, config["sql_fecha"])
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        parametros = leer_consulta(spark, args, usuario, clave, config["sql_parametros_parquet"])
        prioridad_1, prioridad_2 = procesos_por_prioridad(
            parametros,
            config["procesos"],
            args.tipo_ejecucion,
        )

        print(f"[spark] Prioridad 1: {len(prioridad_1)}")
        print(f"[spark] Prioridad 2: {len(prioridad_2)}")

        errores: list[str] = []
        for nombre_lote, filas in (("prioridad_1", prioridad_1), ("prioridad_2", prioridad_2)):
            for row in filas:
                try:
                    procesar_tabla(spark, args, usuario, clave, config, row, fecha_proceso, batch_id)
                except Exception as exc:
                    errores.append(f"{nombre_lote}:{row.get('TABLA', '?')}: {exc}")

        if errores:
            raise RuntimeError("Fallaron procesos Spark de extraccion: " + " | ".join(errores))

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
