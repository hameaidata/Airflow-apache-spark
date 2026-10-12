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
    driver_for_conn,
    jdbc_url,
    merge_conf,
    merge_csv_values,
    motor_de_conexion,
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

        # TODO ARGUMENTO A CADENA, Y NO ES PARANOIA.
        #
        # El provider de Spark agrega application_args al comando TAL CUAL y
        # despues hace " ".join(...) para registrarlo. Un argumento que no sea
        # str revienta ahi con un mensaje que no menciona ni el argumento ni la
        # tarea:
        #     TypeError: sequence item 31: expected str instance, int found
        #
        # Y llegar a un int es facil. Los DAGs de este proyecto usan
        # render_template_as_native_obj=True -hace falta: sin el,
        # "{{ ti.xcom_pull(...) }}" entrega la CADENA 'None' cuando no hay XCom
        # y la carga sale verde sin subir nada-. Con esa opcion Jinja aplica
        # literal_eval al texto renderizado, asi que un batch_id como
        # "20261011144216" se convierte en el ENTERO 20261011144216.
        #
        # El filtro | string no lo arregla: literal_eval corre sobre el texto
        # final de todas formas. Y quitar el render nativo reintroduce el fallo
        # silencioso. El sitio correcto es aqui, una vez renderizado: una linea
        # de comandos solo transporta cadenas, asi que convertir es lo fiel.
        #
        # None se descarta en vez de convertirse: un "None" literal en la linea
        # de comandos llegaria al job como la cadena 'None' y es el mismo fallo
        # silencioso por la puerta de atras.
        if self.application_args:
            self.application_args = [
                str(a) for a in self.application_args if a is not None
            ]

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
        driver = driver_for_conn(conn)

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

        # EL DRIVER EN CLIENT MODE NECESITA EL JAR EN SU CLASSPATH, Y --jars NO
        # SE LO DA.
        #
        # Los jars JDBC se instalan dentro de las imagenes a proposito, para
        # estar en el classpath "sin --jars y sin extraClassPath". Eso funciona
        # para los EXECUTORS, que corren la imagen de Spark. El driver no: en
        # client mode corre dentro de airflow-worker, con la imagen de Airflow,
        # donde los jars viven en /opt/airflow/jars y NO estan en el classpath
        # de la JVM.
        #
        # Y --jars no lo cubre. Distribuye a los executors y los agrega al
        # classloader interno de Spark -por eso spark.read.jdbc() si funciona-,
        # pero la JVM del driver ya esta arrancada cuando se procesan, asi que
        # un jvm.Class.forName() va por el classloader del sistema y falla:
        #     java.lang.ClassNotFoundException: com.microsoft.sqlserver.jdbc.SQLServerDriver
        # con el log diciendo "Added JAR ... mssql-jdbc.jar" tres lineas antes.
        #
        # Los jobs de este proyecto abren conexiones de control con
        # jvm.Class.forName (abrir_jdbc), no con spark.read.jdbc, asi que les
        # pasa de lleno.
        #
        # Se APPENDEA a lo que ya hubiera en conf: si alguien declaro su propio
        # extraClassPath en CONFIGURACION_SPARK, sobreescribirlo seria peor que
        # el problema. El separador es ':' porque los contenedores son Linux.
        if self.jars:
            previo = (self.conf or {}).get("spark.driver.extraClassPath", "")
            rutas = [r for r in previo.split(":") if r]
            for jar in self.jars.split(","):
                jar = jar.strip()
                if jar and jar not in rutas:
                    rutas.append(jar)
            self.conf = {**(self.conf or {}),
                         "spark.driver.extraClassPath": ":".join(rutas)}
        self.env_vars = {
            **dict(self.env_vars or {}),
            self.user_env: conn.login,
            self.password_env: conn.password,
        }

        # Se registra el motor RESUELTO y no conn.conn_type. Con las
        # Connections de tipo Generic de este proyecto, conn_type es literal
        # "generic", asi que el log decia "motor=generic" mientras el driver
        # que de verdad cargaba era el de SQL Server. Un log que contradice lo
        # que hace el codigo cuesta mas que no tenerlo.
        try:
            motor = motor_de_conexion(conn)
        except Exception:                                        # noqa: BLE001
            motor = f"{conn.conn_type} (sin resolver)"

        self.log.info(
            "Spark JDBC preparado | conn_id=%s motor=%s driver=%s host=%s app=%s",
            self.jdbc_conn_id,
            motor,
            driver.driver_class.rsplit(".", 1)[-1],
            conn.host,
            self.application,
        )
        return super().execute(context)
