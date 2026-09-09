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


JDBC_DRIVERS: dict[str, JdbcDriver] = {
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
    "jdbc": JdbcDriver(
        conn_types=("jdbc",),
        driver_class="com.ibm.db2.jcc.DB2Driver",
        jar=f"{JDBC_JARS_ROOT}/db2-jcc.jar",
    ),
    "oracle": JdbcDriver(
        conn_types=("oracle",),
        driver_class="oracle.jdbc.OracleDriver",
        jar=f"{JDBC_JARS_ROOT}/ojdbc.jar",
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
    """Busca el driver JDBC asociado al conn_type de Airflow."""
    normalizado = (conn_type or "").lower()
    for driver in JDBC_DRIVERS.values():
        if normalizado in driver.conn_types:
            return driver
    soportados = sorted({ct for driver in JDBC_DRIVERS.values() for ct in driver.conn_types})
    raise AirflowException(
        f"El tipo de conexion '{conn_type}' no tiene driver JDBC configurado. "
        f"Soportados: {', '.join(soportados)}."
    )


def jdbc_url(conn) -> str:
    """Construye la URL JDBC usando una Connection de Airflow."""
    conn_type = (conn.conn_type or "").lower()
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
