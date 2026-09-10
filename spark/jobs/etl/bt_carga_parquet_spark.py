from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time

from pyspark.sql import SparkSession


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
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def jdbc_options(args, usuario: str, clave: str) -> dict[str, str]:
    return {
        "url": args.jdbc_url,
        "driver": args.driver_class,
        "user": usuario,
        "password": clave,
        "batchsize": str(args.batchsize),
    }


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


def consulta(spark: SparkSession, args, usuario: str, clave: str, sql: str):
    return (
        spark.read.format("jdbc")
        .option("url", args.jdbc_url)
        .option("driver", args.driver_class)
        .option("user", usuario)
        .option("password", clave)
        .option("dbtable", f"({sql}) AS q")
        .load()
    )


def registrar_inicio(spark, args, usuario, clave, config, tabla_destino, ruta):
    tabla_control = config["tabla_control"]
    sql = f"""
        INSERT INTO {tabla_control}
            (nom_proceso, tabla_destino, archivo_parquet, fec_inicio, estado, host_name)
        VALUES (?, ?, ?, NOW(6), ?, ?)
    """
    return ejecutar_insert_id_jdbc(
        spark,
        args,
        usuario,
        clave,
        sql,
        [config["nom_proceso"], tabla_destino, ruta, config["estado_iniciado"], args.host_name],
    )


def registrar_estado(spark, args, usuario, clave, config, id_log, estado):
    ejecutar_update_jdbc(
        spark,
        args,
        usuario,
        clave,
        f"UPDATE {config['tabla_control']} SET estado=? WHERE id_log=?",
        [estado, id_log],
    )


def registrar_fin(spark, args, usuario, clave, config, id_log, total_rows, total_time):
    ejecutar_update_jdbc(
        spark,
        args,
        usuario,
        clave,
        f"""
        UPDATE {config['tabla_control']}
        SET fec_termino=NOW(6), estado=?, filas_cargadas=?, duracion_segundos=?
        WHERE id_log=?
        """,
        [config["estado_finalizado"], total_rows, round(float(total_time), 2), id_log],
    )


def registrar_error(spark, args, usuario, clave, config, id_log, error):
    ejecutar_update_jdbc(
        spark,
        args,
        usuario,
        clave,
        f"""
        UPDATE {config['tabla_control']}
        SET fec_termino=NOW(6), estado=?, msg_error=?
        WHERE id_log=?
        """,
        [config["estado_error"], str(error)[: int(config["error_size_limit"])], id_log],
    )


def procesar_job(spark, args, usuario, clave, config: dict, job: dict):
    ruta = job["ruta"]
    tabla_destino = validar_identificador(job["tabla_destino"], "tabla_destino")
    batch_size = int(job.get("batch_size") or config.get("batch_default") or args.batchsize)
    id_log = None
    inicio = time.time()

    try:
        id_log = registrar_inicio(spark, args, usuario, clave, config, tabla_destino, ruta)
        registrar_estado(spark, args, usuario, clave, config, id_log, config["estado_ejecutado"])
        ejecutar_update_jdbc(spark, args, usuario, clave, f"TRUNCATE TABLE {tabla_destino}")

        df = spark.read.parquet(ruta)
        total = df.count()
        (
            df.write.format("jdbc")
            .options(**jdbc_options(args, usuario, clave))
            .option("dbtable", tabla_destino)
            .option("batchsize", str(batch_size))
            .mode("append")
            .save()
        )

        registrar_fin(spark, args, usuario, clave, config, id_log, total, time.time() - inicio)
        print(f"[spark] OK carga {tabla_destino}: {total} filas desde {ruta}")
        return {"tabla_destino": tabla_destino, "filas": total, "estado": "OK"}
    except Exception as exc:
        if id_log is not None:
            registrar_error(spark, args, usuario, clave, config, id_log, exc)
        raise


def jobs_desde_sql(spark, args, usuario, clave, sql_jobs: str) -> list[dict]:
    rows = consulta(spark, args, usuario, clave, sql_jobs).collect()
    jobs = []
    for row in rows:
        datos = row.asDict()
        valores = list(datos.values())

        def por_posicion(posicion: int, default=None):
            return valores[posicion] if len(valores) > posicion else default

        jobs.append(
            {
                "ruta": datos.get("ruta") or datos.get("RUTA") or por_posicion(0),
                "tabla_destino": datos.get("tabla_destino") or datos.get("TABLA_DESTINO") or por_posicion(1),
                "batch_size": datos.get("batch_size") or datos.get("BATCH_SIZE") or por_posicion(2),
                "commit_every": datos.get("commit_every") or datos.get("COMMIT_EVERY") or por_posicion(3),
            }
        )
    return jobs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--driver-class", required=True)
    parser.add_argument("--config-path", required=True)
    parser.add_argument("--batchsize", type=int, default=10000)
    parser.add_argument("--host-name", default=os.environ.get("HOSTNAME", "spark"))
    args = parser.parse_args()

    usuario = os.environ.get("ORIGEN_USUARIO")
    clave = os.environ.get("ORIGEN_CLAVE")
    if not usuario or not clave:
        print("[ERROR] Faltan ORIGEN_USUARIO / ORIGEN_CLAVE")
        return 1

    config = cargar_config(args.config_path)["carga"]
    config["tabla_control"] = validar_identificador(config["tabla_control"], "tabla_control")
    spark = construir_sesion("bt_carga_parquet_spark")
    spark.sparkContext.setLogLevel("WARN")

    try:
        jobs = jobs_desde_sql(spark, args, usuario, clave, config["sql_jobs"])
        print(f"[spark] Jobs de carga: {len(jobs)}")
        errores: list[str] = []

        for job in jobs:
            try:
                procesar_job(spark, args, usuario, clave, config, job)
            except Exception as exc:
                errores.append(f"{job.get('tabla_destino', '?')}: {exc}")

        if errores:
            raise RuntimeError("Fallaron procesos Spark de carga: " + " | ".join(errores))

        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
