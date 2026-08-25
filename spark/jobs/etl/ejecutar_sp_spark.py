"""
Ejecuta un procedimiento almacenado DESDE Spark, y despues procesa el resultado.

===========================================================================
LO PRIMERO, PORQUE ES LA CONFUSION MAS COMUN

El lector JDBC de Spark (spark.read.jdbc) NO puede ejecutar un procedimiento.
Toma la opcion dbtable y la mete en  SELECT * FROM <dbtable>. Un CALL ahi
falla, y ningun truco lo arregla de forma fiable.

Lo que SI se puede: abrir una conexion JDBC cruda desde la JVM de Spark y
ejecutar el procedimiento por ahi. Es lo que hace este job:

    jvm.java.sql.DriverManager.getConnection(url, usuario, clave)
    conexion.prepareCall("{call esquema.sp(?)}")

Eso corre en el DRIVER de Spark, en un solo hilo. El procedimiento se ejecuta
dentro del motor de base de datos, igual que si lo llamara Airflow.

QUE APORTA SPARK ENTONCES: lo que viene despues. Una vez que el procedimiento
dejo sus resultados en una tabla, Spark la lee EN PARALELO —varias conexiones,
cada una con un rango de filas— y la procesa. Ahi si hay paralelismo real.

  Airflow  ->  Spark  ->  [driver] CALL sp()        1 hilo, en la BD
                      ->  [executors] leer tabla    N hilos en paralelo
                      ->  [executors] escribir      N hilos en paralelo

===========================================================================
BITACORA

Este job escribe su propio registro en la tabla de bitacora de la base
destino, con los mismos campos que el operador de Airflow. Asi la bitacora
queda completa: los pasos que corrieron directo y los que pasaron por Spark
aparecen en la misma tabla, comparables entre si.

===========================================================================
INVOCACION — la hace el DAG, no se ejecuta a mano

    spark-submit --master spark://spark-master:7077 \
      --jars /opt/airflow/jars/postgresql-jdbc.jar \
      /opt/spark-apps/etl/ejecutar_sp_spark.py \
      --jdbc-url jdbc:postgresql://postgres:5432/negocio_pruebas \
      --driver-class org.postgresql.Driver \
      --procedimiento public.sp_demo_resumen \
      --parametros 2026-08-22 \
      --tabla-resultado public.ventas \
      --columna-particion id \
      --particiones 4 \
      --salida-parquet /opt/spark-data/parquet/ventas \
      --tabla-bitacora airflow_bitacora_procesos \
      --dag-id orquestador_json --task-id consolidar \
      --run-id manual__2026-08-22 --intento 1 --fecha 2026-08-22

Las credenciales NO van por argumento: llegan por variables de entorno
ORIGEN_USUARIO y ORIGEN_CLAVE. Los argumentos son visibles con `ps` y en la
interfaz de Spark.
===========================================================================
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ===========================================================================
# JDBC crudo desde la JVM de Spark
# ===========================================================================

def abrir_conexion(spark, url: str, usuario: str, clave: str, driver: str):
    """Devuelve una java.sql.Connection viva, obtenida desde la JVM de Spark."""
    jvm = spark.sparkContext._jvm
    # Cargar la clase del driver. Sin esto, DriverManager no la encuentra
    # aunque el jar este en el classpath.
    jvm.java.lang.Class.forName(driver)
    return jvm.java.sql.DriverManager.getConnection(url, usuario, clave)


def ejecutar_procedimiento(conexion, procedimiento: str, parametros: list[str]) -> None:
    """Ejecuta el procedimiento con la sintaxis de escape de JDBC.

    La forma {call sp(?)} es estandar de JDBC y cada driver la traduce a lo que
    su motor entienda: EXEC en SQL Server, CALL en PostgreSQL y DB2, bloque
    anonimo en Oracle. Asi este job no necesita saber de que motor se trata.
    """
    marcadores = ", ".join(["?"] * len(parametros))
    sentencia = f"{{call {procedimiento}({marcadores})}}" if parametros else f"{{call {procedimiento}}}"
    print(f"[spark] Ejecutando en la base de datos: {sentencia}")

    llamada = conexion.prepareCall(sentencia)
    for i, valor in enumerate(parametros, start=1):
        llamada.setString(i, str(valor))
    llamada.execute()
    llamada.close()
    if not conexion.getAutoCommit():
        conexion.commit()
    print("[spark] Procedimiento completado")


def tipos_fecha(driver: str) -> tuple[str, str]:
    """Devuelve como convertir texto a fecha y a marca de tiempo, segun el motor.

    No es cosmetico: en SQL Server, TIMESTAMP es un tipo rowversion, NO una
    fecha. Un CAST(? AS TIMESTAMP) ahi falla. Cada motor nombra sus tipos a su
    manera y hay que respetarlo.
    """
    d = driver.lower()
    if "sqlserver" in d or "mssql" in d:
        return "CAST(? AS DATE)", "CAST(? AS DATETIME2)"
    if "oracle" in d:
        return ("TO_DATE(?, 'YYYY-MM-DD')",
                "TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS')")
    # PostgreSQL, DB2 y el resto usan los nombres estandar
    return "CAST(? AS DATE)", "CAST(? AS TIMESTAMP)"


def registrar_bitacora(conexion, tabla: str, datos: dict, driver: str = "") -> None:
    """Inserta una fila en la bitacora de la base destino.

    Mismos campos que el operador de Airflow, para que las dos vias —la que
    corre directo y la que pasa por Spark— sean comparables en la misma tabla.

    Los textos van como marcadores (?) porque pueden venir de fuera. Los
    numeros se insertan ya formateados: los calculamos nosotros, no proceden
    de ninguna entrada externa, y la API tipada de JDBC desde py4j es
    innecesariamente incomoda para decimales.
    """
    duracion = round(float(datos.get("duracion_seg") or 0), 3)
    filas = datos.get("filas_afectadas")
    filas_sql = str(int(filas)) if filas is not None else "NULL"
    intento = int(datos["intento"])
    cast_fecha, cast_ts = tipos_fecha(driver)

    sql = f"""
        INSERT INTO {tabla}
            (dag_id, task_id, run_id, intento, fecha_proceso,
             procedimiento, parametros, estado, iniciado_en, finalizado_en,
             duracion_seg, filas_afectadas, mensaje_error, ejecutado_por)
        VALUES (?, ?, ?, {intento}, {cast_fecha}, ?, ?, ?,
                {cast_ts}, {cast_ts},
                {duracion}, {filas_sql}, ?, ?)
    """

    st = conexion.prepareStatement(sql)
    st.setString(1, datos["dag_id"])
    st.setString(2, datos["task_id"])
    st.setString(3, datos["run_id"])
    st.setString(4, datos["fecha"])
    st.setString(5, datos["procedimiento"])
    st.setString(6, json.dumps(datos.get("parametros", []), ensure_ascii=False))
    st.setString(7, datos["estado"])
    st.setString(8, datos["iniciado_en"])
    st.setString(9, datos["finalizado_en"])
    st.setString(10, (datos.get("mensaje_error") or "")[:2000])
    st.setString(11, "spark")
    st.executeUpdate()
    st.close()

    if not conexion.getAutoCommit():
        conexion.commit()
    print(f"[spark] Bitacora registrada en {tabla}: estado={datos['estado']}")


# ===========================================================================

def main() -> int:
    p = argparse.ArgumentParser()
    # Conexion
    p.add_argument("--jdbc-url", required=True)
    p.add_argument("--driver-class", required=True)
    # Procedimiento
    p.add_argument("--procedimiento", required=True)
    p.add_argument("--parametros", nargs="*", default=[])
    # Procesamiento posterior (opcional)
    p.add_argument("--tabla-resultado", default=None)
    p.add_argument("--columna-particion", default=None)
    p.add_argument("--particiones", type=int, default=4)
    p.add_argument("--salida-parquet", default=None)
    # Bitacora y trazabilidad
    p.add_argument("--tabla-bitacora", default="airflow_bitacora_procesos")
    p.add_argument("--dag-id", default="?")
    p.add_argument("--task-id", default="?")
    p.add_argument("--run-id", default="?")
    p.add_argument("--intento", type=int, default=1)
    p.add_argument("--fecha", required=True)
    args = p.parse_args()

    usuario = os.environ.get("ORIGEN_USUARIO")
    clave = os.environ.get("ORIGEN_CLAVE")
    if not usuario or not clave:
        print("[ERROR] Faltan ORIGEN_USUARIO / ORIGEN_CLAVE en el entorno")
        return 1

    spark = (
        SparkSession.builder
        .appName(f"sp_{args.procedimiento}_{args.fecha}")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    iniciado = time.strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.monotonic()
    filas_escritas = None
    estado = "OK"
    error = None

    try:
        # --- 1. EJECUTAR EL PROCEDIMIENTO (driver, 1 hilo, dentro del motor)
        conexion = abrir_conexion(spark, args.jdbc_url, usuario, clave, args.driver_class)
        try:
            ejecutar_procedimiento(conexion, args.procedimiento, args.parametros)
        finally:
            conexion.close()

        # --- 2. PROCESAR EL RESULTADO (executors, en paralelo) -----------
        if args.tabla_resultado:
            print(f"[spark] Leyendo {args.tabla_resultado} en paralelo")
            lector = (
                spark.read.format("jdbc")
                .option("url", args.jdbc_url)
                .option("user", usuario)
                .option("password", clave)
                .option("driver", args.driver_class)
                .option("dbtable", args.tabla_resultado)
                .option("fetchsize", "10000")
            )

            if args.columna_particion:
                # Los limites se calculan de la tabla real. Inventarlos deja
                # particiones vacias y el paralelismo se pierde en silencio.
                limites = (
                    spark.read.format("jdbc")
                    .option("url", args.jdbc_url)
                    .option("user", usuario).option("password", clave)
                    .option("driver", args.driver_class)
                    .option("dbtable",
                            f"(SELECT MIN({args.columna_particion}) AS lo, "
                            f"MAX({args.columna_particion}) AS hi "
                            f"FROM {args.tabla_resultado}) AS lim")
                    .load().collect()[0]
                )
                lo, hi = limites["lo"], limites["hi"]
                if lo is not None and hi is not None:
                    lector = (lector
                              .option("partitionColumn", args.columna_particion)
                              .option("lowerBound", str(lo))
                              .option("upperBound", str(int(hi) + 1))
                              .option("numPartitions", str(args.particiones)))
                    print(f"[spark] {args.particiones} particiones sobre "
                          f"{args.columna_particion} [{lo}, {hi}]")
                else:
                    print("[spark] AVISO: la tabla esta vacia, lectura de un solo hilo")

            df = lector.load()
            df = df.withColumn("fecha_proceso", F.lit(args.fecha).cast("date"))
            filas_escritas = df.count()

            if args.salida_parquet:
                spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                (df.write.mode("overwrite")
                   .option("compression", "snappy")
                   .parquet(args.salida_parquet))
                print(f"[spark] {filas_escritas} filas escritas en {args.salida_parquet}")

        print(f"METRICA filas={filas_escritas}")

    except Exception as exc:
        estado = "ERROR"
        error = str(exc)
        print(f"[ERROR] {exc}")

    finally:
        duracion = time.monotonic() - t0
        # --- 3. BITACORA en la base destino ------------------------------
        try:
            conexion = abrir_conexion(spark, args.jdbc_url, usuario, clave, args.driver_class)
            try:
                registrar_bitacora(conexion, args.tabla_bitacora, {
                    "dag_id": args.dag_id, "task_id": args.task_id,
                    "run_id": args.run_id, "intento": args.intento,
                    "fecha": args.fecha, "procedimiento": args.procedimiento,
                    "parametros": args.parametros, "estado": estado,
                    "iniciado_en": iniciado,
                    "finalizado_en": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "duracion_seg": duracion, "filas_afectadas": filas_escritas,
                    "mensaje_error": error,
                }, driver=args.driver_class)
            finally:
                conexion.close()
        except Exception as exc:
            # Que falle la bitacora no debe cambiar el resultado del proceso.
            print(f"[AVISO] No se pudo registrar la bitacora: {exc}")

        spark.stop()

    return 0 if estado == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
