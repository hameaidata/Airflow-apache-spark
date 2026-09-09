"""
Operadores Spark propios del proyecto.

El objetivo es que los DAGs no repitan detalles de infraestructura:
spark_default, rutas bajo /opt/spark-apps, jars JDBC, credenciales por entorno
y configuraciones conservadoras de recursos.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context

from utils.spark_config import (
    SPARK_CONN_ID,
    driver_for_conn_type,
    jdbc_url,
    merge_conf,
    merge_csv_values,
    spark_app_path,
)

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    SPARK_PROVIDER_AVAILABLE = True
except ImportError:
    SPARK_PROVIDER_AVAILABLE = False

    class SparkSubmitOperator(BaseOperator):
        """Sustituto minimo para que los DAGs importen sin el provider Spark."""

        template_fields = ("application", "application_args", "conf", "env_vars", "jars")

        def __init__(
            self,
            *,
            application: str | None = None,
            application_args: Sequence[Any] | None = None,
            conf: dict[str, Any] | None = None,
            conn_id: str | None = None,
            jars: str | None = None,
            env_vars: dict[str, Any] | None = None,
            executor_memory: str | None = None,
            executor_cores: int | None = None,
            num_executors: int | None = None,
            driver_memory: str | None = None,
            verbose: bool | None = None,
            name: str | None = None,
            **kwargs: Any,
        ) -> None:
            super().__init__(**kwargs)
            self.application = application
            self.application_args = list(application_args or [])
            self.conf = dict(conf or {})
            self.conn_id = conn_id
            self.jars = jars
            self.env_vars = dict(env_vars or {})
            self.executor_memory = executor_memory
            self.executor_cores = executor_cores
            self.num_executors = num_executors
            self.driver_memory = driver_memory
            self.verbose = verbose
            self.name = name


def _spark_submit_kwargs(
    *,
    application: str,
    conn_id: str,
    application_args: Sequence[Any] | None,
    jars: str | Sequence[str] | None,
    conf: dict[str, Any] | None,
    cores_max: int | str | None,
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    submit_kwargs = dict(kwargs)
    submit_kwargs.setdefault("conn_id", conn_id)
    submit_kwargs["application"] = spark_app_path(application)
    submit_kwargs["application_args"] = list(application_args or [])
    submit_kwargs["jars"] = merge_csv_values(jars)
    submit_kwargs["conf"] = merge_conf(conf, cores_max=cores_max)
    return submit_kwargs


class BsgSparkSubmitOperator(SparkSubmitOperator):
    """SparkSubmitOperator con defaults del stack BSG.

    Use este operador cuando el job no necesita una Connection JDBC de Airflow,
    o cuando el propio script resuelve sus credenciales.
    """

    ui_color = "#4E6E58"
    ui_fgcolor = "#FFFFFF"

    def __init__(
        self,
        *,
        application: str,
        application_args: Sequence[Any] | None = None,
        conn_id: str = SPARK_CONN_ID,
        jars: str | Sequence[str] | None = None,
        conf: dict[str, Any] | None = None,
        cores_max: int | str | None = 2,
        executor_memory: str = "1g",
        executor_cores: int = 1,
        num_executors: int | None = 1,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("executor_memory", executor_memory)
        kwargs.setdefault("executor_cores", executor_cores)
        if num_executors is not None:
            kwargs.setdefault("num_executors", num_executors)

        super().__init__(
            **_spark_submit_kwargs(
                application=application,
                conn_id=conn_id,
                application_args=application_args,
                jars=jars,
                conf=conf,
                cores_max=cores_max,
                kwargs=kwargs,
            )
        )

    def execute(self, context: Context) -> Any:
        if not SPARK_PROVIDER_AVAILABLE:
            raise AirflowException(
                "Este operador requiere apache-airflow-providers-apache-spark. "
                "Construya la imagen personalizada del proyecto antes de ejecutar jobs Spark."
            )
        return super().execute(context)


class BsgSparkJdbcOperator(BsgSparkSubmitOperator):
    """Envia un job Spark inyectando JDBC desde una Connection de Airflow.

    El operador agrega automaticamente:

      --jdbc-url <url>
      --driver-class <clase>

    y envia usuario/clave por variables de entorno, no como argumentos visibles.
    """

    template_fields = BsgSparkSubmitOperator.template_fields + (
        "jdbc_conn_id",
        "jdbc_url_arg",
        "driver_class_arg",
    ) if SPARK_PROVIDER_AVAILABLE else ("jdbc_conn_id",)

    def __init__(
        self,
        *,
        jdbc_conn_id: str,
        application: str,
        application_args: Sequence[Any] | None = None,
        include_jdbc_args: bool = True,
        jdbc_url_arg: str = "--jdbc-url",
        driver_class_arg: str = "--driver-class",
        user_env: str = "ORIGEN_USUARIO",
        password_env: str = "ORIGEN_CLAVE",
        env_vars: dict[str, Any] | None = None,
        jars: str | Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        self.jdbc_conn_id = jdbc_conn_id
        self.include_jdbc_args = include_jdbc_args
        self.jdbc_url_arg = jdbc_url_arg
        self.driver_class_arg = driver_class_arg
        self.user_env = user_env
        self.password_env = password_env

        super().__init__(
            application=application,
            application_args=application_args,
            env_vars=dict(env_vars or {}),
            jars=jars,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        if not SPARK_PROVIDER_AVAILABLE:
            return super().execute(context)

        conn = BaseHook.get_connection(self.jdbc_conn_id)
        driver = driver_for_conn_type(conn.conn_type)

        app_args = list(self.application_args or [])
        ya_tiene_jdbc = (
            len(app_args) >= 4
            and app_args[0] == self.jdbc_url_arg
            and app_args[2] == self.driver_class_arg
        )
        if self.include_jdbc_args and not ya_tiene_jdbc:
            app_args = [
                self.jdbc_url_arg,
                jdbc_url(conn),
                self.driver_class_arg,
                driver.driver_class,
                *app_args,
            ]

        self.application_args = app_args
        self.jars = merge_csv_values(self.jars, driver.jar)
        self.env_vars = {
            **dict(self.env_vars or {}),
            self.user_env: conn.login,
            self.password_env: conn.password,
        }

        self.log.info(
            "Spark JDBC preparado | conn_id=%s motor=%s host=%s app=%s",
            self.jdbc_conn_id,
            conn.conn_type,
            conn.host,
            self.application,
        )
        return super().execute(context)
