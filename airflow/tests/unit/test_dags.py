"""
Tests de integridad de los DAGs.

Cargan el DagBag real usando los JSON versionados en airflow/config/json/, que
es exactamente lo que el scheduler hace cada 30 segundos. Atrapan en CI los
errores que hoy solo se descubren en la UI:

  - un DAG que no importa (SyntaxError, marcador de conflicto de git, typo)
  - ID_PROCESO duplicado entre ODS y BDS
  - dependencias hacia procesos inexistentes o inactivos
  - ciclos
  - nombres de grupo que generan el mismo group_id
  - group_id con caracteres que Airflow rechaza

Ejecucion:
    docker compose exec airflow-scheduler pytest /opt/airflow/tests/unit -v

    o en local:
        pip install "apache-airflow==2.10.5" pytest
        AIRFLOW_HOME=/tmp/af pytest airflow/tests/unit -v
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from airflow.models import DagBag, Variable


RAIZ = Path(__file__).resolve().parents[3]
DIR_DAGS = RAIZ / "airflow" / "dags"
DIR_JSON = RAIZ / "airflow" / "config" / "json"

# Reglas reales de airflow/utils/helpers.py
RE_TASK_ID = re.compile(r"^[\w.-]+$")
RE_GROUP_ID = re.compile(r"^[\w-]+$")

DAGS_ESPERADOS = {
    "BT_DATAHUB",
    "etl_bt_parquet_singlestore",
    "etl_bt_parquet_singlestore_spark",
}


@pytest.fixture(scope="session", autouse=True)
def cargar_variables():
    """Publica los JSON del repo como Variables antes de parsear los DAGs."""
    for archivo in sorted(DIR_JSON.glob("*.json")):
        Variable.set(archivo.stem, json.loads(archivo.read_text(encoding="utf-8")),
                     serialize_json=True)


@pytest.fixture(scope="session")
def dagbag(cargar_variables):
    return DagBag(str(DIR_DAGS), include_examples=False)


# ---------------------------------------------------------------- estructura
def test_los_json_son_validos():
    for archivo in sorted(DIR_JSON.glob("*.json")):
        json.loads(archivo.read_text(encoding="utf-8"))


def test_ningun_dag_roto(dagbag):
    assert not dagbag.import_errors, (
        "Hay DAGs que no importan:\n"
        + "\n".join(f"  {k}: {v.strip().splitlines()[-1]}" for k, v in dagbag.import_errors.items())
    )


def test_dags_esperados_presentes(dagbag):
    faltantes = DAGS_ESPERADOS - set(dagbag.dag_ids)
    assert not faltantes, f"No se registraron: {sorted(faltantes)}"


def test_no_hay_dags_duplicados_de_etl_datahub(dagbag):
    """table_ods.py y table_bds.py no deben registrar DAGs propios.

    Si lo hicieran, los mismos stored procedures quedarian en dos DAGs y se
    podrian disparar en paralelo sobre las mismas tablas.
    """
    duplicados = {"ODS_PROCESOS_DATAHUB", "BDS_PROCESOS_DATAHUB"} & set(dagbag.dag_ids)
    assert not duplicados, (
        f"{sorted(duplicados)} se registraron. Revisa airflow/dags/.airflowignore "
        f"y que el bloque 'with DAG(...)' de esos modulos siga comentado."
    )


def test_ids_de_airflow_validos(dagbag):
    for dag_id in dagbag.dag_ids:
        for tarea in dagbag.get_dag(dag_id).tasks:
            partes = tarea.task_id.split(".")
            for grupo in partes[:-1]:
                assert RE_GROUP_ID.match(grupo), (
                    f"{dag_id}: group_id {grupo!r} invalido. Airflow no admite "
                    f"puntos ni acentos en un group_id."
                )
            assert RE_TASK_ID.match(partes[-1]), f"{dag_id}: task_id {partes[-1]!r} invalido"


def test_sin_ciclos(dagbag):
    from airflow.utils.dag_cycle_tester import check_cycle

    for dag_id in dagbag.dag_ids:
        check_cycle(dagbag.get_dag(dag_id))


def test_toda_tarea_tiene_timeout(dagbag):
    """Una tarea sin timeout puede quedarse colgada indefinidamente."""
    sin_timeout = [
        f"{dag_id}.{t.task_id}"
        for dag_id in dagbag.dag_ids
        for t in dagbag.get_dag(dag_id).tasks
        if t.execution_timeout is None and t.task_id not in ("inicio", "fin",
                                                             "ods_completo", "bds_completo")
    ]
    assert not sin_timeout, f"Tareas sin execution_timeout: {sin_timeout}"


# ------------------------------------------------- contrato de configuracion
def _procesos(config: dict):
    for grupo in config.get("GRUPOS", []) or []:
        for proceso in grupo.get("PROCESOS", []) or []:
            yield grupo, proceso


def _cargar(nombre: str) -> dict:
    return json.loads((DIR_JSON / f"{nombre}.json").read_text(encoding="utf-8"))


def test_id_proceso_unico_entre_ods_y_bds():
    """ID_PROCESO es la clave de CTL_CFG_PROCESOS: un duplicado es un
    conflicto de catalogo aunque los procesos esten desactivados."""
    vistos: dict[int, str] = {}
    duplicados = []
    for variable in ("DAG_ODS_TABLAS", "DAG_BDS_TABLAS"):
        for grupo, proceso in _procesos(_cargar(variable)):
            pid = int(proceso["ID_PROCESO"])
            origen = f"{variable}.{grupo['NOMBRE']}"
            if pid in vistos and vistos[pid] != origen:
                duplicados.append(f"ID_PROCESO={pid} en {vistos[pid]} y en {origen}")
            vistos[pid] = origen
    assert not duplicados, "IDs repetidos:\n  " + "\n  ".join(duplicados)


def test_dependencias_bds_apuntan_a_procesos_activos():
    bds = _cargar("DAG_BDS_TABLAS")
    activos = {
        int(p["ID_PROCESO"]) for _, p in _procesos(bds)
        if str(p.get("ACTIVO", "")).upper() == "S"
    }
    declarados = {int(p["ID_PROCESO"]): p for _, p in _procesos(bds)}

    problemas = []
    for grupo, proceso in _procesos(bds):
        if str(proceso.get("ACTIVO", "")).upper() != "S":
            continue
        valor = proceso.get("DEPENDE_DE") or []
        deps = [valor] if isinstance(valor, int) else list(valor)
        for dep in deps:
            if dep not in declarados:
                problemas.append(f"{proceso['ID_PROCESO']} depende de {dep}, que no existe")
            elif dep not in activos:
                problemas.append(f"{proceso['ID_PROCESO']} depende de {dep}, que esta ACTIVO='N'")
    assert not problemas, "Dependencias rotas:\n  " + "\n  ".join(problemas)


def test_procesos_declaran_activo_y_stored_procedure():
    faltantes = []
    for variable in ("DAG_ODS_TABLAS", "DAG_BDS_TABLAS"):
        for grupo, proceso in _procesos(_cargar(variable)):
            donde = f"{variable}.{grupo['NOMBRE']}.{proceso.get('ID_PROCESO')}"
            if not {"ACTIVO", "HABILITADO", "ESTADO"} & set(proceso):
                faltantes.append(f"{donde}: sin ACTIVO")
            if str(proceso.get("TIPO_PROCESO", "SP")).upper() == "SP" \
                    and not proceso.get("STORED_PROCEDURE"):
                faltantes.append(f"{donde}: TIPO_PROCESO=SP sin STORED_PROCEDURE")
    assert not faltantes, "\n  " + "\n  ".join(faltantes)


def _leer_env() -> dict[str, str]:
    env = {}
    ruta = RAIZ / ".env"
    if ruta.exists():
        for linea in ruta.read_text(encoding="utf-8").splitlines():
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                clave, _, valor = linea.partition("=")
                env[clave.strip()] = valor.strip()
    return env


def test_output_dir_coincide_con_la_carpeta_montada():
    """La ruta de los parquet tiene que decir lo mismo en los tres sitios.

    output_dir (Variable) es la ruta que la extraccion guarda en
    CTL_PROCESO_PARQUET y la que la carga abre despues. Si no coincide con la
    carpeta que el docker-compose monta, la carga no encuentra nada y el
    error no dice por que.
    """
    env = _leer_env()
    destino = _cargar("EXTRACCION_BT_STG")["output_dir"]
    esperado = env.get("PARQUET_CONTAINER_DIR", "/data/parquet")

    assert destino == esperado, (
        f"output_dir={destino!r} pero PARQUET_CONTAINER_DIR={esperado!r} en .env. "
        f"Tienen que ser iguales."
    )


def test_una_sola_ruta_de_host_activa():
    """En .env solo puede haber una PARQUET_HOST_DIR sin comentar.

    Las dos rutas (Windows y Red Hat) conviven en el archivo y se conmutan
    comentando una. Si quedaran las dos activas, la ultima ganaria en
    silencio y se escribiria en la carpeta equivocada.
    """
    ruta = RAIZ / ".env"
    if not ruta.exists():
        pytest.skip("no hay .env en el repo")

    activas = [
        linea.strip()
        for linea in ruta.read_text(encoding="utf-8").splitlines()
        if linea.strip().startswith("PARQUET_HOST_DIR=")
    ]
    assert len(activas) == 1, f"Debe haber exactamente una PARQUET_HOST_DIR activa: {activas}"


def test_la_carpeta_de_parquet_esta_montada_en_los_compose():
    """El bind mount tiene que existir en Airflow Y en Spark, en los dos compose.

    Si falta en un servicio, ese worker no ve los archivos y la carga falla
    solo a veces, segun que worker tome la tarea.
    """
    servicios = ("airflow-webserver", "airflow-scheduler", "airflow-worker",
                 "spark-master", "spark-worker")

    for nombre in ("docker-compose.windows.yml", "docker-compose.rhel.yml"):
        archivo = RAIZ / nombre
        if not archivo.exists():
            continue
        texto = archivo.read_text(encoding="utf-8")
        assert "${PARQUET_HOST_DIR}" in texto, (
            f"{nombre} no monta PARQUET_HOST_DIR: los parquet quedarian dentro "
            f"del contenedor."
        )
        if nombre.endswith("rhel.yml"):
            assert "${PARQUET_HOST_DIR}:${PARQUET_CONTAINER_DIR:-/data/parquet}:z" in texto, (
                "En Red Hat el montaje necesita la etiqueta SELinux ':z' o el "
                "contenedor no podra escribir."
            )
