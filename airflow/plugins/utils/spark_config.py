"""
Configuracion comun para enviar trabajos Spark desde Airflow.

Este modulo mantiene en un solo lugar las rutas de scripts, jars, drivers JDBC
y valores conservadores de spark-submit. Los DAGs deberian declarar el proceso;
esta capa se encarga de los detalles repetitivos del envio.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from posixpath import join as posix_join
from typing import Any

from airflow.exceptions import AirflowException


SPARK_CONN_ID = "spark_default"
SPARK_APPS_ROOT = "/opt/spark-apps"
JDBC_JARS_ROOT = "/opt/airflow/jars"


@dataclass(frozen=True)
class JdbcDriver:
    conn_types: tuple[str, ...]
    driver_class: str
    jar: str
    # False = el Dockerfile NO lo descarga. Se declara igual, para documentar
    # como habilitarlo y para que el error sea accionable en vez de un
    # FileNotFoundException sobre una ruta que nadie reconoce.
    descargado: bool = True


# ============================================================================
# CATALOGO DE DRIVERS JDBC
# ----------------------------------------------------------------------------
# Cada entrada tiene que corresponder con un jar que los Dockerfile descarguen
# de verdad: el principal (para Airflow) y spark/Dockerfile (para el cluster).
# Si solo esta en uno, el job falla al llegar al executor.
# ============================================================================
JDBC_DRIVERS: dict[str, JdbcDriver] = {
    # IBM i / AS-400: es el driver de Bantotal (JTOpen).
    # OJO: db2-jcc, mas abajo, es el de Db2 para LUW y z/OS. NO sirve contra
    # IBM i, aunque el nombre lo sugiera.
    "as400": JdbcDriver(
        conn_types=("as400", "ibmi", "db2i","generic"),
        driver_class="com.ibm.as400.access.AS400JDBCDriver",
        jar=f"{JDBC_JARS_ROOT}/jt400.jar",
    ),
    "postgres": JdbcDriver(
        conn_types=("postgres", "postgresql"),
        driver_class="org.postgresql.Driver",
        jar=f"{JDBC_JARS_ROOT}/postgresql-jdbc.jar",
    ),
    "mssql": JdbcDriver(
        conn_types=("mssql", "odbc"),
        driver_class="com.microsoft.sqlserver.jdbc.SQLServerDriver",
        jar=f"{JDBC_JARS_ROOT}/mssql-jdbc.jar",
    ),
    # Db2 para LUW y z/OS. Para IBM i use "as400".
    "jdbc": JdbcDriver(
        conn_types=("jdbc", "db2"),
        driver_class="com.ibm.db2.jcc.DB2Driver",
        jar=f"{JDBC_JARS_ROOT}/db2-jcc.jar",
    ),
    # NO SE DESCARGA. Para habilitarlo, anada esta coordenada a JDBC_DRIVERS de
    # los DOS Dockerfile:
    #     com.oracle.database.jdbc:ojdbc11:23.5.0.24.07:ojdbc.jar
    "oracle": JdbcDriver(
        conn_types=("oracle",),
        driver_class="oracle.jdbc.OracleDriver",
        jar=f"{JDBC_JARS_ROOT}/ojdbc.jar",
        descargado=False,
    ),
    "singlestore": JdbcDriver(
        conn_types=("singlestore",),
        driver_class="com.singlestore.jdbc.Driver",
        jar=f"{JDBC_JARS_ROOT}/singlestore-jdbc.jar",
    ),
    "mysql": JdbcDriver(
        conn_types=("mysql",),
        driver_class="com.mysql.cj.jdbc.Driver",
        jar=f"{JDBC_JARS_ROOT}/mysql-jdbc.jar",
    ),
}


SPARK_CONF_BASE: dict[str, str] = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
}


def spark_app_path(application: str) -> str:
    """Devuelve una ruta absoluta dentro de /opt/spark-apps."""
    if application.startswith("/"):
        return application
    return posix_join(SPARK_APPS_ROOT, application.lstrip("/"))


def driver_for_conn_type(conn_type: str) -> JdbcDriver:
    """Busca el driver JDBC asociado al conn_type de Airflow.

    Si el driver existe en el catalogo pero su jar no esta en la imagen, se
    avisa AQUI, al armar el envio, y no dentro del executor. La diferencia
    importa: un FileNotFoundException dentro de un executor sale enterrado en
    el log de Spark y no dice que hacer.
    """
    normalizado = (conn_type or "").lower()
    for nombre, driver in JDBC_DRIVERS.items():
        if normalizado in driver.conn_types:
            if not driver.descargado:
                raise AirflowException(
                    f"El driver {nombre!r} esta declarado pero su jar NO se descarga "
                    f"en la imagen ({driver.jar}).\n"
                    f"Anada la coordenada a JDBC_DRIVERS en el Dockerfile principal Y "
                    f"en spark/Dockerfile (ver el comentario junto a {nombre!r} en "
                    f"utils/spark_config.py) y reconstruya las dos imagenes."
                )
            return driver
    soportados = sorted({ct for driver in JDBC_DRIVERS.values() for ct in driver.conn_types})
    raise AirflowException(
        f"El tipo de conexion '{conn_type}' no tiene driver JDBC configurado. "
        f"Soportados: {', '.join(soportados)}."
    )


def jdbc_url(conn) -> str:
    """Construye la URL JDBC usando una Connection de Airflow."""
    conn_type = (conn.conn_type or "").lower()
    if conn_type in ("as400", "ibmi", "db2i","generic"):
        # Las propiedades NO son opcionales contra un core bancario:
        #   prompt=false              sin esto el driver intenta abrir un
        #                             dialogo grafico y la tarea se cuelga sin
        #                             un solo mensaje util en el log
        #   transaction isolation     sin bloqueos sobre las tablas del core
        #   access=read only          el driver rechaza cualquier escritura
        #   errors=full               mensajes legibles en vez de codigos
        # El Extra de la Connection puede traer 'libraries' y 'naming'.
        extra = conn.extra_dejson or {}
        propiedades = [
            "prompt=false",
            "transaction isolation=none",
            "access=read only",
            "errors=full",
            f"naming={extra.get('naming', 'system')}",
            "date format=iso",
            "time format=iso",
            "blocking enabled=true",
            "block size=512",
        ]
        if extra.get("libraries"):
            propiedades.append(f"libraries={extra['libraries']}")
        return f"jdbc:as400://{conn.host}/{conn.schema or ''};" + ";".join(propiedades)
    if conn_type in ("postgres", "postgresql"):
        return f"jdbc:postgresql://{conn.host}:{conn.port or 5432}/{conn.schema}"
    if conn_type in ("mssql", "odbc"):
        return (
            f"jdbc:sqlserver://{conn.host}:{conn.port or 1433};"
            f"databaseName={conn.schema};encrypt=true;trustServerCertificate=true"
        )
    if conn_type == "jdbc":
        return conn.host
    if conn_type == "oracle":
        return f"jdbc:oracle:thin:@{conn.host}:{conn.port or 1521}/{conn.schema}"
    if conn_type == "singlestore":
        return f"jdbc:singlestore://{conn.host}:{conn.port or 3306}/{conn.schema}"
    if conn_type == "mysql":
        return f"jdbc:mysql://{conn.host}:{conn.port or 3306}/{conn.schema}"
    raise AirflowException(f"No se como armar URL JDBC para conn_type='{conn.conn_type}'")


def merge_conf(*configs: Mapping[str, Any] | None, cores_max: int | str | None = None) -> dict[str, str]:
    """Combina configuraciones Spark convirtiendo todo a texto."""
    merged: dict[str, str] = dict(SPARK_CONF_BASE)
    if cores_max is not None:
        merged["spark.cores.max"] = str(cores_max)
    for config in configs:
        if config:
            merged.update({str(k): str(v) for k, v in config.items()})
    return merged


def merge_csv_values(*values: str | Iterable[str] | None) -> str | None:
    """Une listas o cadenas CSV eliminando vacios y duplicados."""
    items: list[str] = []
    for value in values:
        if not value:
            continue
        if isinstance(value, str):
            candidatos = value.split(",")
        else:
            candidatos = list(value)
        for candidato in candidatos:
            item = str(candidato).strip()
            if item and item not in items:
                items.append(item)
    return ",".join(items) if items else None
