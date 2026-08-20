"""
Job de Spark: extraccion JDBC en paralelo -> Parquet particionado

Se ejecuta con spark-submit desde Airflow (ver DAG ej23_spark_jdbc_parquet).

    spark-submit \
      --master spark://spark-master:7077 \
      --jars /opt/airflow/jars/mssql-jdbc.jar \
      /opt/spark-apps/etl/jdbc_a_parquet.py \
      --fecha 2026-08-20 --tabla dbo.movimientos --particiones 8

Las credenciales NO se pasan por argumentos: los argumentos de spark-submit son
visibles en `ps`, en la UI de Spark y en los logs del driver. Se pasan por
variables de entorno, que el operador de Airflow inyecta desde la Connection.
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def construir_sesion(nombre_app: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(nombre_app)
        # Adaptive Query Execution: reajusta particiones en tiempo de ejecucion.
        # Evita tener que calibrar shuffle.partitions a mano para cada job.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Corrige el desfase de calendario juliano/gregoriano al escribir fechas
        # anteriores a 1582 en Parquet. Sin esto, Spark lanza excepcion.
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def leer_jdbc_particionado(spark, args, usuario, clave):
    """Lectura JDBC repartida entre executors.

    ESTO ES LO QUE DECIDE EL RENDIMIENTO.

    Sin partitionColumn, Spark abre UNA sola conexion y trae toda la tabla por
    un unico hilo. Da igual cuantos workers tengas: el cuello de botella es esa
    conexion. Es el error mas frecuente al empezar con Spark + JDBC.

    Con partitionColumn, Spark abre `numPartitions` conexiones y cada una trae
    un rango:  WHERE col >= X AND col < Y

    Requisitos de partitionColumn: numerica, fecha o timestamp, y con buena
    distribucion. Si el 90% de las filas cae en un rango, ese executor hace todo
    el trabajo y los demas esperan (skew).
    """
    lector = (
        spark.read.format("jdbc")
        .option("url", args.jdbc_url)
        .option("user", usuario)
        .option("password", clave)
        .option("driver", args.driver_class)
        # fetchsize: filas por viaje de red. El default de muchos drivers es 10,
        # lo que hace la lectura lentisima. 10.000 es un punto de partida sano.
        .option("fetchsize", "10000")
    )

    if args.consulta:
        # Subconsulta: DEBE ir entre parentesis y con alias.
        # Spark la envuelve en  SELECT * FROM <esto>
        lector = lector.option("dbtable", f"({args.consulta}) AS sub")
    else:
        lector = lector.option("dbtable", args.tabla)

    if args.columna_particion:
        lector = (
            lector
            .option("partitionColumn", args.columna_particion)
            .option("lowerBound", str(args.limite_inferior))
            .option("upperBound", str(args.limite_superior))
            .option("numPartitions", str(args.particiones))
        )
        print(
            f"[info] Lectura en {args.particiones} particiones sobre "
            f"{args.columna_particion} [{args.limite_inferior}, {args.limite_superior}]"
        )
    else:
        print("[AVISO] Sin columna de particion: la lectura sera de un solo hilo.")

    return lector.load()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--jdbc-url", required=True)
    p.add_argument("--driver-class", required=True)
    p.add_argument("--tabla", default=None)
    p.add_argument("--consulta", default=None, help="SQL alternativo a --tabla")
    p.add_argument("--ruta-salida", required=True)
    p.add_argument("--fecha", required=True)
    p.add_argument("--columna-particion", default=None)
    p.add_argument("--limite-inferior", type=int, default=0)
    p.add_argument("--limite-superior", type=int, default=1000000)
    p.add_argument("--particiones", type=int, default=8)
    args = p.parse_args()

    # Credenciales por entorno, nunca por argumento (ver cabecera).
    usuario = os.environ.get("ORIGEN_USUARIO")
    clave = os.environ.get("ORIGEN_CLAVE")
    if not usuario or not clave:
        print("[ERROR] Faltan ORIGEN_USUARIO / ORIGEN_CLAVE en el entorno")
        return 1

    spark = construir_sesion(f"jdbc_a_parquet_{args.fecha}")
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = leer_jdbc_particionado(spark, args, usuario, clave)

        print(f"[info] Esquema de origen:")
        df.printSchema()

        # --- Transformaciones -------------------------------------------------
        df = (
            df
            .withColumn("fecha_carga", F.current_timestamp())
            .withColumn("fecha_proceso", F.lit(args.fecha).cast("date"))
            .withColumn("anio", F.year("fecha_proceso"))
            .withColumn("mes", F.month("fecha_proceso"))
        )

        # --- Escritura a Parquet ---------------------------------------------
        #
        # partitionBy crea la estructura de carpetas:
        #     ruta/anio=2026/mes=8/parte-0000.snappy.parquet
        #
        # Al leer con un filtro sobre anio/mes, Spark salta las carpetas que no
        # aplican (partition pruning). Es la diferencia entre leer 2 GB y 2 TB.
        #
        # Cuidado con la granularidad: particionar por dia genera miles de
        # carpetas con archivos diminutos, y eso degrada mas de lo que ayuda.
        # Como regla, apunta a archivos de 128 MB a 1 GB.
        #
        # mode:
        #   overwrite -> reemplaza. Con partitionOverwriteMode=dynamic solo
        #                reescribe las particiones presentes en el DataFrame.
        #   append    -> anade. Cuidado con reejecuciones: duplica.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        (
            df.write
            .mode("overwrite")
            .partitionBy("anio", "mes")
            .option("compression", "snappy")
            .parquet(args.ruta_salida)
        )

        total = df.count()
        print(f"[ok] Escritas {total} filas en {args.ruta_salida}")

        # Metricas para que Airflow las recoja del log
        print(f"METRICA filas_escritas={total}")
        print(f"METRICA particiones={df.rdd.getNumPartitions()}")

        return 0

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
