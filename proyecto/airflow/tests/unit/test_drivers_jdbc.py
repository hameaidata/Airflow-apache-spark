"""
Tests de los drivers JDBC: que existan, que esten en las DOS imagenes y que el
catalogo del codigo no prometa jars que nadie descarga.

POR QUE ESTE ARCHIVO EXISTE
---------------------------
Los jars viven en dos sitios y tienen que ser los mismos:

    Dockerfile          -> /opt/airflow/jars      (donde corre el driver Spark)
    spark/Dockerfile    -> $SPARK_HOME/jars       (donde corren los executors)

Si solo estan en el primero, el job arranca bien y falla al llegar al executor
con "No suitable driver", que manda a cualquiera a buscar el problema donde no
esta. Duplicar la lista esta bien; que diverja en silencio, no.

Ejecucion:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/unit/test_drivers_jdbc.py -v
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest


RAIZ = Path(__file__).resolve().parents[3]
DOCKERFILE_AIRFLOW = RAIZ / "Dockerfile"
DOCKERFILE_SPARK = RAIZ / "spark" / "Dockerfile"
DIR_PLUGINS = RAIZ / "airflow" / "plugins"

if str(DIR_PLUGINS) not in sys.path:
    sys.path.append(str(DIR_PLUGINS))


def _drivers_declarados(dockerfile: Path) -> dict[str, str]:
    """-> {'jt400.jar': 'net.sf.jt400:jt400:21.0.7', ...}"""
    texto = dockerfile.read_text(encoding="utf-8")
    bloque = re.search(r'ARG JDBC_DRIVERS="(.*?)"', texto, re.S)
    assert bloque, f"{dockerfile.name} no declara ARG JDBC_DRIVERS"

    drivers = {}
    for linea in bloque.group(1).splitlines():
        linea = linea.strip().rstrip("\\").strip()
        if not linea or linea.startswith("#"):
            continue
        partes = linea.split(":")
        assert len(partes) == 4, f"{dockerfile.name}: coordenada mal formada -> {linea!r}"
        grupo, artefacto, version, destino = partes
        drivers[destino] = f"{grupo}:{artefacto}:{version}"
    return drivers


@pytest.fixture(scope="module")
def drivers_airflow():
    return _drivers_declarados(DOCKERFILE_AIRFLOW)


@pytest.fixture(scope="module")
def drivers_spark():
    if not DOCKERFILE_SPARK.exists():
        pytest.skip("no hay spark/Dockerfile")
    return _drivers_declarados(DOCKERFILE_SPARK)


# ============================================================================
def test_las_dos_imagenes_traen_los_mismos_drivers(drivers_airflow, drivers_spark):
    """El fallo que evita: un job que arranca bien y muere en el executor con
    'No suitable driver', porque el jar solo estaba en la imagen de Airflow."""
    solo_airflow = set(drivers_airflow) - set(drivers_spark)
    solo_spark = set(drivers_spark) - set(drivers_airflow)

    assert not solo_airflow, (
        f"Estos jars estan en Dockerfile pero NO en spark/Dockerfile: "
        f"{sorted(solo_airflow)}.\n"
        f"Los executors de Spark no los van a tener y el job fallara con "
        f"'No suitable driver'."
    )
    assert not solo_spark, (
        f"Estos jars estan en spark/Dockerfile pero NO en Dockerfile: "
        f"{sorted(solo_spark)}."
    )


def test_las_versiones_coinciden_entre_las_dos_imagenes(drivers_airflow, drivers_spark):
    """Mismo jar con version distinta en driver y executor produce errores de
    serializacion o de protocolo que no mencionan la version por ningun lado."""
    distintas = {
        jar: (drivers_airflow[jar], drivers_spark[jar])
        for jar in set(drivers_airflow) & set(drivers_spark)
        if drivers_airflow[jar] != drivers_spark[jar]
    }
    assert not distintas, f"Versiones distintas entre las dos imagenes: {distintas}"


def test_el_driver_de_bantotal_esta_declarado(drivers_airflow):
    """jt400 es el driver de IBM i. db2-jcc NO sirve contra AS/400, aunque el
    nombre lo sugiera: ese es el de Db2 para LUW y z/OS."""
    assert "jt400.jar" in drivers_airflow, (
        "Falta jt400 (JTOpen), que es el driver de Bantotal sobre IBM i. "
        "Tener db2-jcc no lo cubre: ese es el de Db2 para LUW y z/OS."
    )


def test_el_catalogo_del_codigo_no_promete_jars_inexistentes(drivers_airflow):
    """Cada driver marcado como descargado=True tiene que corresponder con un
    jar que el Dockerfile trae de verdad. Si no, el error sale recien al
    ejecutar el job, como FileNotFoundException sobre una ruta desconocida."""
    from utils.spark_config import JDBC_DRIVERS

    faltantes = []
    for nombre, driver in JDBC_DRIVERS.items():
        if not driver.descargado:
            continue
        archivo = driver.jar.rsplit("/", 1)[-1]
        if archivo not in drivers_airflow:
            faltantes.append(f"{nombre} -> {archivo}")

    assert not faltantes, (
        "El catalogo declara estos drivers como disponibles, pero el Dockerfile "
        f"no descarga su jar: {faltantes}.\n"
        "O se agrega la coordenada a los dos Dockerfile, o se marca "
        "descargado=False en utils/spark_config.py."
    )


def test_un_driver_no_descargado_falla_con_un_mensaje_util():
    """Un driver declarado pero sin jar debe explicar que hacer, no reventar
    dentro del executor."""
    from airflow.exceptions import AirflowException
    from utils.spark_config import driver_for_conn_type

    with pytest.raises(AirflowException, match="NO se descarga"):
        driver_for_conn_type("oracle")


def test_la_url_de_as400_lleva_las_propiedades_que_protegen_el_core():
    """prompt=false evita que la tarea se cuelgue esperando un dialogo grafico
    que en un contenedor nunca va a aparecer. Las otras dos evitan tomar
    bloqueos sobre las tablas del core."""
    from utils.spark_config import jdbc_url

    class ConnFalsa:
        conn_type = "as400"
        host = "as400.banco.local"
        schema = "BTDATA"
        extra_dejson = {"libraries": "BTDATA,BTPROD", "naming": "system"}

    url = jdbc_url(ConnFalsa())

    assert url.startswith("jdbc:as400://as400.banco.local/BTDATA")
    assert "prompt=false" in url
    assert "transaction isolation=none" in url
    assert "access=read only" in url
    assert "errors=full" in url
    assert "libraries=BTDATA,BTPROD" in url


def test_as400_y_db2_son_drivers_distintos():
    """Confundirlos es el error mas comun con IBM i."""
    from utils.spark_config import driver_for_conn_type

    assert driver_for_conn_type("as400").driver_class == "com.ibm.as400.access.AS400JDBCDriver"
    assert driver_for_conn_type("db2").driver_class == "com.ibm.db2.jcc.DB2Driver"
    assert driver_for_conn_type("as400").jar != driver_for_conn_type("db2").jar


# ============================================================================
# Coherencia del cluster
# ============================================================================
def _leer_conf(ruta: Path) -> dict[str, str]:
    conf = {}
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#"):
            partes = linea.split(None, 1)
            if len(partes) == 2:
                conf[partes[0]] = partes[1].strip()
    return conf


def _leer_env() -> dict[str, str]:
    env, ruta = {}, RAIZ / ".env"
    if ruta.exists():
        for linea in ruta.read_text(encoding="utf-8", errors="ignore").splitlines():
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                clave, _, valor = linea.partition("=")
                env[clave.strip()] = valor.strip()
    return env


def test_spark_cores_max_cabe_en_el_worker():
    """Pedir mas cores de los que hay NO falla: el job se queda en WAITING para
    siempre con 'Initial job has not accepted any resources', que no menciona
    la capacidad por ningun lado. Es de los diagnosticos mas largos que hay."""
    conf_path = RAIZ / "spark" / "config" / "spark-defaults.conf"
    if not conf_path.exists():
        pytest.skip("no hay spark-defaults.conf")

    conf = _leer_conf(conf_path)
    env = _leer_env()
    if "spark.cores.max" not in conf or "SPARK_WORKER_CORES" not in env:
        pytest.skip("faltan valores para comparar")

    cores_worker = int(env["SPARK_WORKER_CORES"])
    replicas = int(env.get("SPARK_WORKER_REPLICAS", 1))
    capacidad = cores_worker * replicas
    pedidos = int(conf["spark.cores.max"])

    assert pedidos <= capacidad, (
        f"spark.cores.max={pedidos} contra una capacidad de {capacidad} cores "
        f"({cores_worker} x {replicas} replica(s)). El job se quedaria en WAITING."
    )


def test_spark_defaults_esta_montado_en_los_compose():
    """El archivo existia en el repo sin estar montado en ningun servicio, asi
    que no tenia ningun efecto y el cluster corria con los valores por defecto."""
    for nombre in ("docker-compose.windows.yml", "docker-compose.rhel.yml"):
        archivo = RAIZ / nombre
        if not archivo.exists():
            continue
        texto = archivo.read_text(encoding="utf-8")
        assert "spark/config/spark-defaults.conf" in texto, (
            f"{nombre} no monta spark-defaults.conf: el cluster ignoraria esa "
            f"configuracion por completo."
        )


def test_si_se_registran_eventos_hay_quien_los_lea():
    """spark.eventLog.enabled sin History Server llena el volumen spark_events
    para siempre y nadie puede mirar esos eventos: es lo peor de los dos mundos.
    O hay servicio que los lea, o no se registran."""
    conf_path = RAIZ / "spark" / "config" / "spark-defaults.conf"
    if not conf_path.exists():
        pytest.skip("no hay spark-defaults.conf")

    if _leer_conf(conf_path).get("spark.eventLog.enabled", "false").lower() != "true":
        pytest.skip("eventLog desactivado")

    for nombre in ("docker-compose.windows.yml", "docker-compose.rhel.yml"):
        archivo = RAIZ / nombre
        if not archivo.exists():
            continue
        assert "spark-history" in archivo.read_text(encoding="utf-8"), (
            f"{nombre}: spark-defaults.conf activa eventLog pero no hay servicio "
            f"spark-history que los lea. El volumen spark_events creceria sin "
            f"limite y nadie podria abrir la interfaz de un job terminado."
        )


def test_el_history_server_limpia_los_eventos_viejos():
    """Sin cleaner, el volumen crece sin limite hasta llenar el disco."""
    for nombre in ("docker-compose.windows.yml", "docker-compose.rhel.yml"):
        archivo = RAIZ / nombre
        if not archivo.exists():
            continue
        texto = archivo.read_text(encoding="utf-8")
        if "spark-history" not in texto:
            continue
        assert "spark.history.fs.cleaner.enabled=true" in texto, (
            f"{nombre}: el History Server no tiene el cleaner activado."
        )
