"""
Tests de configuracion: que el .env que generan los setup cubra lo que los
compose piden, y que los dos setup no diverjan.

POR QUE EXISTE ESTE ARCHIVO

Durante meses el setup escribio 30 variables y los compose usaban 46. Las 16
que faltaban no daban ningun error al ejecutar setup.sh: el fallo aparecia
despues, al levantar el stack, y con mensajes que no mencionaban la variable
ausente. El peor caso fue SPARK_IMAGE, cuya ausencia no se nota al arrancar
sino dentro de un job, como "No suitable driver".

El problema de fondo es que agregar un servicio al compose y acordarse de
tocar los DOS setup son dos actos separados, y el segundo se olvida. Estos
tests convierten ese olvido en un fallo de CI.

Ejecucion:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/unit/test_configuracion.py -v
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


RAIZ = Path(__file__).resolve().parents[3]

COMPOSES = ["docker-compose.windows.yml", "docker-compose.rhel.yml",
            "docker-compose.ubuntu.yml", "docker-compose.tls.yml"]

# Docker la inyecta en cada contenedor; no va en el .env.
NO_VAN_EN_ENV = {"HOSTNAME"}


def variables_de_los_compose() -> set[str]:
    """Toda ${VARIABLE} que aparezca en cualquiera de los compose."""
    encontradas: set[str] = set()
    for nombre in COMPOSES:
        archivo = RAIZ / nombre
        if archivo.exists():
            encontradas |= set(re.findall(r"\$\{([A-Z_0-9]+)",
                                          archivo.read_text(encoding="utf-8")))
    return encontradas - NO_VAN_EN_ENV


def variables_de(script: str) -> set[str]:
    """Variables que el script escribe en el .env, comentadas incluidas.

    Las comentadas cuentan a proposito: una linea comentada con su explicacion
    al lado es documentacion util y el usuario solo tiene que descomentarla.
    Lo que no vale es que la variable no aparezca por ningun sitio.
    """
    texto = (RAIZ / script).read_text(encoding="utf-8")
    if script.endswith(".sh"):
        m = re.search(r"cat > \.env <<EOF\n(.*?)\nEOF", texto, re.S)
    else:
        m = re.search(r'\$envContent = @"\n(.*?)\n"@', texto, re.S)
    assert m, f"no encontre el bloque que genera el .env en {script}"
    return set(re.findall(r"^#?\s?([A-Z_0-9]+)=", m.group(1), re.M))


@pytest.mark.parametrize("script", ["setup.sh", "setup.ps1"])
def test_el_setup_cubre_todo_lo_que_piden_los_compose(script):
    """La prueba principal. Una variable que el compose usa y el setup no
    escribe deja un hueco que solo se nota al levantar el stack."""
    faltan = variables_de_los_compose() - variables_de(script)
    assert not faltan, (
        f"{script} no escribe estas variables que los compose SI usan: "
        f"{sorted(faltan)}.\n"
        f"Agreguelas al bloque que genera el .env, aunque sea comentadas."
    )


def test_los_dos_setup_generan_lo_mismo():
    """Windows y Linux tienen que producir el mismo juego de variables.

    Si divergen, el stack funciona en un sistema operativo y no en el otro, y
    la diferencia se descubre el dia del despliegue.
    """
    sh, ps = variables_de("setup.sh"), variables_de("setup.ps1")
    assert sh == ps, (
        f"solo en setup.sh: {sorted(sh - ps)}\n"
        f"solo en setup.ps1: {sorted(ps - sh)}"
    )


@pytest.mark.parametrize("variable", ["AIRFLOW_IMAGE", "SPARK_IMAGE"])
def test_las_imagenes_propias_no_quedan_comentadas(variable):
    """Las dos imagenes propias son obligatorias, no opcionales.

    SPARK_IMAGE es la que mas duele si se olvida: con la imagen oficial de
    Apache el stack arranca igual, y el fallo aparece mucho despues, dentro de
    un job, como "No suitable driver" -porque los jars estan en la imagen
    propia, no en la base- o como "Cannot run program python3" en el executor.
    """
    for script in ("setup.sh", "setup.ps1"):
        texto = (RAIZ / script).read_text(encoding="utf-8")
        assert re.search(rf"^{variable}=", texto, re.M), (
            f"{script} no define {variable} sin comentar. Con la imagen oficial "
            f"los drivers JDBC no estan y los DAGs fallan en tiempo de ejecucion."
        )


def test_el_compose_pasa_los_pools_a_los_contenedores():
    """Cuatro DAGs leen AIRFLOW_POOL_* con os.environ.

    Si el compose no las declara en su bloque environment, esas variables NO
    existen dentro del contenedor y el codigo cae siempre a su valor por
    defecto. El sintoma es que cambiar el .env no hace nada, sin ningun error
    que lo explique.
    """
    leidas = set()
    for py in (RAIZ / "airflow" / "dags").rglob("*.py"):
        leidas |= set(re.findall(r'os\.environ\.get\("(AIRFLOW_POOL_[A-Z_]+)"',
                                 py.read_text(encoding="utf-8", errors="replace")))
    assert leidas, "ningun DAG lee AIRFLOW_POOL_*: revise si este test sigue teniendo sentido"

    for nombre in ("docker-compose.windows.yml", "docker-compose.rhel.yml"):
        archivo = RAIZ / nombre
        if not archivo.exists():
            continue
        texto = archivo.read_text(encoding="utf-8")
        for variable in sorted(leidas):
            assert f"{variable}:" in texto, (
                f"{nombre} no pasa {variable} a los contenedores, pero el codigo "
                f"la lee. Agreguela al bloque environment de x-airflow-common."
            )


def test_las_rutas_de_parquet_cuadran_con_las_variables_de_airflow():
    """output_dir de cada Variable tiene que coincidir con el lado derecho del
    bind mount. Si no, la extraccion escribe en una ruta que no esta montada:
    Docker la crea DENTRO del contenedor, la corrida sale verde, y los archivos
    desaparecen al reiniciar."""
    import json

    dir_json = RAIZ / "airflow" / "config" / "json"
    compose = (RAIZ / "docker-compose.windows.yml").read_text(encoding="utf-8")

    esperado = {
        "EXTRACCION_BT_STG.json": ("output_dir", "/data/parquet"),
        "BT2SQL_EXTRACCION.json": ("output_dir", "/data/bt2sql"),
    }
    for archivo, (clave, ruta) in esperado.items():
        ruta_json = dir_json / archivo
        if not ruta_json.exists():
            continue
        cfg = json.loads(ruta_json.read_text(encoding="utf-8"))
        assert cfg[clave] == ruta, f"{archivo}: {clave} es {cfg[clave]!r}, se esperaba {ruta!r}"
        assert f":-{ruta}}}" in compose or f":{ruta}" in compose, (
            f"{ruta} no aparece como destino de ningun bind mount en el compose"
        )


# ============================================================================
# EL BUNDLE OFFLINE
# ----------------------------------------------------------------------------
# Estos tests existen porque el fallo que atrapan es irreversible: si el
# paquete viaja al servidor aislado sin spark-bsg, alla no hay forma de
# construirla -el build necesita bajar drivers de Maven- y el traslado hay que
# repetirlo entero.
# ============================================================================
@pytest.mark.parametrize("script", ["scripts/preparar-bundle-offline.ps1",
                                    "scripts/preparar-bundle-offline.sh"])
def test_el_bundle_exporta_las_dos_imagenes_propias(script):
    archivo = RAIZ / script
    if not archivo.exists():
        pytest.skip(f"{script} no existe")
    texto = archivo.read_text(encoding="utf-8")

    for imagen in ("airflow-bsg:2.11.2", "spark-bsg:3.5.3"):
        assert imagen in texto, (
            f"{script} no exporta {imagen}. El servidor aislado no puede "
            f"construirla: el build baja drivers de Maven."
        )


@pytest.mark.parametrize("script", ["scripts/preparar-bundle-offline.ps1",
                                    "scripts/preparar-bundle-offline.sh"])
def test_el_bundle_no_exporta_la_imagen_de_spark_equivocada(script):
    """apache/spark es la BASE. La que el .env nombra es spark-bsg.

    Exportar la base produce un paquete que parece completo y deja el destino
    sin poder levantar Spark: los tres servicios fallan con "image not found",
    y forzando la base, los jobs mueren con "No suitable driver".
    """
    archivo = RAIZ / script
    if not archivo.exists():
        pytest.skip(f"{script} no existe")

    # Se permite nombrarla en comentarios explicativos; lo que no vale es que
    # este en la lista de imagenes a exportar.
    for linea in archivo.read_text(encoding="utf-8").splitlines():
        limpia = linea.split("#", 1)[0]
        assert "apache/spark" not in limpia, (
            f"{script} exporta apache/spark en vez de spark-bsg:\n  {linea.strip()}"
        )


@pytest.mark.parametrize("script", ["scripts/construir_imagen.ps1",
                                    "scripts/construir_imagen.sh"])
def test_el_constructor_hace_las_dos_imagenes(script):
    """Construir solo airflow-bsg y descubrirlo en el destino es como se llega
    a un servidor aislado sin poder levantar Spark."""
    archivo = RAIZ / script
    if not archivo.exists():
        pytest.skip(f"{script} no existe")
    texto = archivo.read_text(encoding="utf-8")

    assert "spark-bsg" in texto, f"{script} no construye spark-bsg"
    assert "spark/Dockerfile" in texto or "spark\\Dockerfile" in texto, (
        f"{script} no referencia el Dockerfile de Spark"
    )


def test_el_bundle_copia_el_ddl():
    """Sin sql/ no viaja ningun DDL: ni las tablas de control ni los stored
    procedures. El destino quedaria sin donde escribir."""
    for script in ("scripts/preparar-bundle-offline.ps1",
                   "scripts/preparar-bundle-offline.sh"):
        archivo = RAIZ / script
        if not archivo.exists():
            continue
        texto = archivo.read_text(encoding="utf-8")
        assert re.search(r"['\"\s]sql['\"\s]", texto), (
            f"{script} no copia la carpeta sql/ al paquete"
        )


BUNDLE = ["scripts/preparar-bundle-offline.ps1", "scripts/preparar-bundle-offline.sh"]


@pytest.mark.parametrize("script", BUNDLE)
def test_el_bundle_limpia_el_destino_antes_de_copiar(script):
    """Copiar encima sobrescribe, pero no borra lo que sobra.

    Sin una limpieza previa, un archivo renombrado o eliminado en el
    repositorio sobrevive en el paquete indefinidamente. El caso que lo
    demostro: al renombrar dag_bt2sql_stg.py a dag_stg_bt2sql_carga.py, un
    paquete reutilizado llevaria LOS DOS, y el servidor aislado registraria
    BT2SQL_STG y STG_BT2SQL_CARGA a la vez -dos DAGs escribiendo en las mismas
    tablas STG-. Alla no hay Internet para descubrirlo comodamente.
    """
    archivo = RAIZ / script
    if not archivo.exists():
        pytest.skip(f"{script} no existe")
    texto = archivo.read_text(encoding="utf-8")

    if script.endswith(".sh"):
        limpia = re.search(r'rm -rf "\$\{DESTINO\}/proyecto"', texto)
    else:
        limpia = re.search(r"Remove-Item \$dirProyecto -Recurse -Force", texto)
    assert limpia, (
        f"{script} no borra la carpeta proyecto/ antes de copiar. "
        f"Un paquete reutilizado arrastra archivos que ya no existen."
    )


@pytest.mark.parametrize("script", BUNDLE)
def test_el_bundle_comprueba_que_lo_critico_aterrizo(script):
    """.airflowignore empieza por punto, y de el depende que el servidor
    aislado NO registre los cuatro DAGs de examples/ y templates/ -uno de los
    cuales ni siquiera compila-. Que viaje no puede quedar a la suerte de como
    trate cada herramienta los archivos ocultos: hay que comprobarlo."""
    archivo = RAIZ / script
    if not archivo.exists():
        pytest.skip(f"{script} no existe")
    texto = archivo.read_text(encoding="utf-8")
    assert ".airflowignore" in texto, (
        f"{script} no comprueba que .airflowignore llego al paquete"
    )


@pytest.mark.parametrize("script", BUNDLE)
def test_el_bundle_lleva_el_registro_privado(script):
    """registry:2 son 25 MB y es la unica forma de tener un registro dentro de
    la red aislada: ninguno de la nube se alcanza desde alla. Omitirlo no se
    puede corregir despues del corte."""
    archivo = RAIZ / script
    if not archivo.exists():
        pytest.skip(f"{script} no existe")
    assert "registry:2" in archivo.read_text(encoding="utf-8"), (
        f"{script} no exporta registry:2"
    )


@pytest.mark.parametrize("script", BUNDLE)
def test_el_bundle_no_lleva_ningun_env(script):
    """.env.bak lleva los MISMOS secretos que .env. Borrar solo .env dejaba el
    respaldo viajando al servidor destino."""
    archivo = RAIZ / script
    if not archivo.exists():
        pytest.skip(f"{script} no existe")
    texto = archivo.read_text(encoding="utf-8")
    assert ".env." in texto, (
        f"{script} borra .env del paquete pero no sus respaldos (.env.bak)"
    )
