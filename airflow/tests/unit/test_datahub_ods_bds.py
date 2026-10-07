"""
Tests de BDS_DATAHUB_PROCESOS: STG -> ODS -> BDS.

LAS DOS REGLAS QUE ESTE ARCHIVO VIGILA
--------------------------------------
Las dos nacieron de un requisito explicito y las dos son faciles de romper sin
darse cuenta, porque dependen de detalles que no se ven leyendo el grafo:

  1. LA CAPA ODS TIENE QUE TERMINAR. Una tabla ODS que falla no puede detener a
     las demas, ni al nivel de ORDEN siguiente, ni a la capa BDS. Hasta el
     2026-10-07 no habia un solo trigger_rule en el DAG: todo era all_success y
     un fallo en una tabla se llevaba por delante la carga del dia.

  2. BDS CONTINUA SOLO CON LO QUE PASO. Una tabla BDS corre si y solo si las
     tablas ODS de las que depende terminaron en exito. Esa dependencia se
     declara con DEPENDE_DE_ODS y la comprueba ejecutar_bds en ejecucion, no el
     grafo: una tarea tiene UNA sola trigger_rule para todos sus padres, y los
     padres de una tarea BDS son de dos naturalezas distintas.

Y una tercera, que es la contrapartida de la primera: que un fallo no trunque
el proceso no quiere decir que se oculte. La corrida tiene que acabar en rojo.

Ejecucion:
    docker compose -f docker-compose.windows.yml exec airflow-scheduler \
        pytest /opt/airflow/tests/unit/test_datahub_ods_bds.py -v
"""

from __future__ import annotations

import copy
import json
import logging
import sys
from pathlib import Path

import pytest


RAIZ = Path(__file__).resolve().parents[3]
DIR_DAGS = RAIZ / "airflow" / "dags"
DIR_PRODUCCION = DIR_DAGS / "production"
DIR_JSON = RAIZ / "airflow" / "config" / "json"

DAG_ID = "BDS_DATAHUB_PROCESOS"
ARCHIVO_DAG = DIR_PRODUCCION / "dag_bds_datahub_procesos.py"
AUXILIARES = {"inicio", "ods_completo", "bds_completo", "fin"}

# La carpeta airflow/ del repositorio no tiene __init__.py: si pytest se lanza
# desde la raiz, Python la importa como namespace package y tapa el paquete
# airflow instalado, con un ImportError que no la menciona.
for _ruta in list(sys.path):
    try:
        if Path(_ruta or ".").resolve() == RAIZ:
            sys.path.remove(_ruta)
    except OSError:                                              # pragma: no cover
        pass


# ============================================================================
# DOBLES
# ============================================================================
class TareaFalsa:
    def __init__(self, task_id, state):
        self.task_id, self.state = task_id, state


class CorridaFalsa:
    """DagRun, con los estados que se le programen."""

    def __init__(self, estados: dict[str, str]):
        self.estados = estados

    def get_task_instance(self, task_id):             # noqa: D102
        if task_id not in self.estados:
            return None
        return TareaFalsa(task_id, self.estados[task_id])

    def get_task_instances(self):                     # noqa: D102
        return [TareaFalsa(t, e) for t, e in self.estados.items()]


def _modulo_dag():
    """Importa el archivo del DAG como modulo, sin construir el DAG.

    Se hace con un guard de __name__ no, porque el archivo construye el DAG al
    importarse. En su lugar se compila el modulo entero en un espacio propio y
    se deja que falle al llegar al 'with DAG(...)' si falta la metadata: lo que
    interesa esta definido antes.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("_dag_datahub_bajo_test", ARCHIVO_DAG)
    modulo = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = modulo
    spec.loader.exec_module(modulo)
    return modulo


@pytest.fixture(scope="module")
def dag_mod():
    return _modulo_dag()


@pytest.fixture(scope="module")
def variables_publicadas():
    from airflow.models import Variable

    for archivo in sorted(DIR_JSON.glob("*.json")):
        Variable.set(archivo.stem, json.loads(archivo.read_text(encoding="utf-8")),
                     serialize_json=True)


@pytest.fixture(scope="module")
def dag_datahub(variables_publicadas):
    from airflow.models import DagBag

    bolsa = DagBag(str(DIR_DAGS), include_examples=False)
    mios = {k: v for k, v in bolsa.import_errors.items()
            if Path(k).name == ARCHIVO_DAG.name}
    assert not mios, mios
    dag = bolsa.get_dag(DAG_ID)
    assert dag is not None, (
        f"{DAG_ID} no se registro. Otros errores de la carpeta: "
        f"{bolsa.import_errors}")
    return dag


@pytest.fixture
def config_ods():
    return json.loads((DIR_JSON / "DAG_ODS_TABLAS.json").read_text(encoding="utf-8"))


@pytest.fixture
def config_bds():
    return json.loads((DIR_JSON / "DAG_BDS_TABLAS.json").read_text(encoding="utf-8"))


def tareas_de(dag, capa: str):
    return [t for t in dag.tasks
            if t.task_id not in AUXILIARES and t.task_id.startswith(f"{capa}_")]


# ============================================================================
# 1. LA CAPA ODS TIENE QUE TERMINAR
# ============================================================================
def test_toda_tarea_de_ods_deja_pasar_a_la_siguiente(dag_datahub):
    """ODS no declara dependencias de datos -DEPENDE_DE esta prohibido ahi-, asi
    que el encadenamiento por ORDEN y por PARALELO=false es orden de ejecucion
    y no de datos: existe para no abrir veinte conexiones al core a la vez."""
    malas = [f"{t.task_id} ({t.trigger_rule})" for t in tareas_de(dag_datahub, "ods")
             if str(t.trigger_rule) != "all_done"]
    assert not malas, (
        f"tarea(s) de ODS con una regla que detiene la capa: {malas}. "
        f"Con all_success, una tabla caida bloquea el resto de su cadena, el "
        f"nivel de ORDEN siguiente, ods_completo y con eso toda la capa BDS.")


def test_ods_completo_se_alcanza_aunque_falle_una_tabla(dag_datahub):
    assert str(dag_datahub.get_task("ods_completo").trigger_rule) == "all_done", (
        "si ods_completo exige exito, un fallo en una sola tabla ODS deja sin "
        "ejecutar toda la capa BDS")


def test_el_cierre_no_se_queda_colgado(dag_datahub):
    for task_id in ("bds_completo", "fin"):
        assert str(dag_datahub.get_task(task_id).trigger_rule) == "all_done", (
            f"{task_id} tiene que ejecutarse pase lo que pase, o la corrida "
            f"nunca llega a reportar como acabo")


def test_ninguna_tarea_de_ods_cuelga_de_otra_capa(dag_datahub):
    """ODS solo puede depender de 'inicio' o de otra tarea de ODS. Si colgara de
    algo de BDS habria un ciclo logico entre capas."""
    for tarea in tareas_de(dag_datahub, "ods"):
        for arriba in tarea.upstream_list:
            assert arriba.task_id == "inicio" or arriba.task_id.startswith("ods_"), (
                f"{tarea.task_id} depende de {arriba.task_id}")


# ============================================================================
# 2. BDS CONTINUA SOLO CON LO QUE PASO
# ============================================================================
def test_la_dependencia_con_ods_viaja_a_la_tarea(dag_datahub, config_bds):
    """DEPENDE_DE_ODS del JSON tiene que acabar en op_kwargs['tareas_ods']."""
    declarados = {
        int(p["ID_PROCESO"]): p["DEPENDE_DE_ODS"]
        for g in config_bds["GRUPOS"] for p in g["PROCESOS"]
        if p.get("DEPENDE_DE_ODS") and str(p.get("ACTIVO", "N")).upper() == "S"
    }
    if not declarados:
        pytest.skip("ningun proceso BDS activo declara DEPENDE_DE_ODS")

    encontrados = {}
    for tarea in dag_datahub.tasks:
        kwargs = getattr(tarea, "op_kwargs", None) or {}
        if kwargs.get("tareas_ods"):
            encontrados[int(kwargs["id_proceso"])] = kwargs["tareas_ods"]

    assert set(encontrados) == set(declarados), (
        f"declarados en el JSON: {sorted(declarados)}, "
        f"cableados en el DAG: {sorted(encontrados)}")
    for pid, tareas in encontrados.items():
        assert tareas, f"ID_PROCESO={pid} quedo con la lista vacia"
        for task_id in tareas:
            assert task_id.startswith("ods_"), (
                f"ID_PROCESO={pid} requiere {task_id!r}, que no es de ODS")
            assert dag_datahub.has_task(task_id), (
                f"ID_PROCESO={pid} requiere {task_id!r}, que no existe en el grafo")


def test_la_regla_depende_de_si_hay_dependencias_de_datos(dag_datahub, config_bds):
    """Una dependencia de ODS y una de BDS son la MISMA clase de cosa -datos- y
    piden la misma regla. Solo quien no declara ninguna cuelga de 'ods_completo',
    que es orden, y ahi exigir exito volveria a truncar la capa."""
    declarado = {
        int(p["ID_PROCESO"]): bool(p.get("DEPENDE_DE") or p.get("DEPENDE_DE_ODS"))
        for g in config_bds["GRUPOS"] for p in g["PROCESOS"]
        if str(p.get("ACTIVO", "N")).upper() == "S"
    }
    vistos = 0
    for tarea in dag_datahub.tasks:
        kwargs = getattr(tarea, "op_kwargs", None) or {}
        pid = int(kwargs.get("id_proceso", -1))
        if pid not in declarado:
            continue
        vistos += 1
        esperada = "all_success" if declarado[pid] else "all_done"
        assert str(tarea.trigger_rule) == esperada, (
            f"{tarea.task_id}: declara datos={declarado[pid]} y su regla es "
            f"{tarea.trigger_rule}, deberia ser {esperada}")
    assert vistos, "ningun proceso BDS activo que comprobar"


# ----------------------------------------------------------------------------
# EL ESCENARIO COMPLETO, CON NOMBRES REALES
# ----------------------------------------------------------------------------
ODS_EJEMPLO = {"GRUPOS": [{"NOMBRE": "ODS_CARGA", "ORDEN": 1, "PARALELO": True, "PROCESOS": [
    {"ID_PROCESO": 10, "ACTIVO": "S", "STORED_PROCEDURE": "SP_ODS_MODULOS",
     "TABLA_DESTINO": "ODS_MODULOS"},
    {"ID_PROCESO": 11, "ACTIVO": "S", "STORED_PROCEDURE": "SP_ODS_PROCESOS",
     "TABLA_DESTINO": "ODS_PROCESOS"},
    {"ID_PROCESO": 12, "ACTIVO": "S", "STORED_PROCEDURE": "SP_ODS_OPERACIONES",
     "TABLA_DESTINO": "ODS_OPERACIONES"},
]}]}

BDS_EJEMPLO = {"GRUPOS": [{"NOMBRE": "BDS_CARGA", "ORDEN": 1, "PARALELO": True, "PROCESOS": [
    {"ID_PROCESO": 20, "ACTIVO": "S", "STORED_PROCEDURE": "SP_BDS_MODULOS",
     "TABLA_DESTINO": "BDS_MODULOS", "DEPENDE_DE": "", "DEPENDE_DE_ODS": [10]},
    {"ID_PROCESO": 21, "ACTIVO": "S", "STORED_PROCEDURE": "SP_BDS_PROCESOS",
     "TABLA_DESTINO": "BDS_PROCESOS", "DEPENDE_DE": "", "DEPENDE_DE_ODS": [11]},
    # bds_operaciones necesita su ODS Y que bds_procesos haya terminado.
    {"ID_PROCESO": 22, "ACTIVO": "S", "STORED_PROCEDURE": "SP_BDS_OPERACIONES",
     "TABLA_DESTINO": "BDS_OPERACIONES", "DEPENDE_DE": [21], "DEPENDE_DE_ODS": [12]},
]}]}


@pytest.fixture
def plan_ejemplo(dag_mod):
    return dag_mod.construir_plan(copy.deepcopy(ODS_EJEMPLO), copy.deepcopy(BDS_EJEMPLO))


def test_cada_bds_cuelga_de_su_propia_tabla_de_ods(plan_ejemplo):
    """ods_modulos -> bds_modulos, y no de 'ods_completo' en bloque. Es lo que
    hace que un fallo en ods_modulos no toque a bds_procesos."""
    aristas = set(plan_ejemplo.aristas_bds)
    assert (10, 20) in aristas, "bds_modulos no cuelga de ods_modulos"
    assert (11, 21) in aristas, "bds_procesos no cuelga de ods_procesos"
    assert (12, 22) in aristas, "bds_operaciones no cuelga de ods_operaciones"
    assert not [a for a in aristas if a[0] == "ods_completo"], (
        "alguna tabla BDS sigue colgando de ods_completo en bloque: un fallo en "
        "cualquier tabla ODS volveria a afectarla")


def test_la_cadena_entre_tablas_bds_se_respeta(plan_ejemplo):
    """bds_operaciones no arranca hasta que bds_procesos termine."""
    assert (21, 22) in set(plan_ejemplo.aristas_bds)


def test_una_tabla_bds_puede_depender_de_las_dos_cosas(plan_ejemplo):
    """bds_operaciones depende de ods_operaciones Y de bds_procesos. Las dos
    aristas tienen que existir: con una sola, o corre sin su insumo de ODS, o
    corre antes que bds_procesos."""
    de_22 = {o for o, d in plan_ejemplo.aristas_bds if d == 22}
    assert de_22 == {12, 21}, f"bds_operaciones depende de {de_22}"


def test_ninguna_tabla_bds_depende_de_una_ods_que_no_necesita(plan_ejemplo):
    """Si bds_procesos colgara tambien de ods_modulos, un fallo ahi lo
    arrastraria sin motivo. Es el error que todo esto quiere evitar."""
    de_21 = {o for o, d in plan_ejemplo.aristas_bds if d == 21}
    assert de_21 == {11}, f"bds_procesos depende de mas de la cuenta: {de_21}"
    de_20 = {o for o, d in plan_ejemplo.aristas_bds if d == 20}
    assert de_20 == {10}, f"bds_modulos depende de mas de la cuenta: {de_20}"


def test_un_ciclo_entre_tablas_bds_se_detecta(dag_mod):
    bds = copy.deepcopy(BDS_EJEMPLO)
    bds["GRUPOS"][0]["PROCESOS"][1]["DEPENDE_DE"] = [22]   # 21 -> 22 -> 21
    with pytest.raises(Exception) as exc:
        dag_mod.construir_plan(copy.deepcopy(ODS_EJEMPLO), bds)
    assert "Ciclo" in str(exc.value)


@pytest.mark.parametrize("estado", ["failed", "upstream_failed", "skipped", None])
def test_un_bds_se_salta_si_su_insumo_de_ods_no_salio_bien(dag_mod, estado):
    from airflow.exceptions import AirflowSkipException

    estados = {} if estado is None else {"ods_x.p104_tipo_cambio": estado}
    with pytest.raises(AirflowSkipException) as exc:
        dag_mod.ejecutar_bds(103, "G", tareas_ods=["ods_x.p104_tipo_cambio"],
                             dag_run=CorridaFalsa(estados))
    assert "p104" in str(exc.value)


def test_se_salta_y_no_falla(dag_mod):
    """Que una tabla BDS no corra porque su insumo no se cargo no es un fallo de
    esa tabla: es la consecuencia de un fallo que ya esta en rojo en otro sitio.
    Marcarla en rojo tambien multiplicaria el mismo incidente."""
    from airflow.exceptions import AirflowSkipException

    with pytest.raises(AirflowSkipException):
        dag_mod.ejecutar_bds(103, "G", tareas_ods=["ods_x.p1"],
                             dag_run=CorridaFalsa({"ods_x.p1": "failed"}))


def test_con_una_sola_dependencia_caida_ya_no_corre(dag_mod):
    from airflow.exceptions import AirflowSkipException

    corrida = CorridaFalsa({"ods_x.p1": "success", "ods_x.p2": "failed"})
    with pytest.raises(AirflowSkipException) as exc:
        dag_mod.ejecutar_bds(103, "G", tareas_ods=["ods_x.p1", "ods_x.p2"],
                             dag_run=corrida)
    assert "p2" in str(exc.value) and "p1" not in str(exc.value)


def test_si_todo_su_insumo_de_ods_salio_bien_si_corre(dag_mod, monkeypatch):
    llamadas = []

    class ModuloFalso:
        @staticmethod
        def ejecutar_proceso_desde_json(id_proceso, nombre_grupo):
            llamadas.append((id_proceso, nombre_grupo))
            return "cargado"

    monkeypatch.setattr(dag_mod, "importar_modulo", lambda *a, **k: ModuloFalso)
    salida = dag_mod.ejecutar_bds(103, "G", tareas_ods=["ods_x.p1"],
                                  dag_run=CorridaFalsa({"ods_x.p1": "success"}))
    assert salida == "cargado" and llamadas == [(103, "G")]


def test_un_bds_sin_dependencias_de_ods_corre_sin_mirar_nada(dag_mod, monkeypatch):
    monkeypatch.setattr(dag_mod, "importar_modulo",
                        lambda *a, **k: type("M", (), {
                            "ejecutar_proceso_desde_json": staticmethod(
                                lambda *x: "cargado")}))
    assert dag_mod.ejecutar_bds(2103, "G") == "cargado"


# ============================================================================
# 3. LA VALIDACION DE DEPENDE_DE_ODS
# ============================================================================
def _plan(dag_mod, config_ods, config_bds):
    return dag_mod.construir_plan(config_ods, config_bds)


def test_depende_de_ods_apuntando_a_un_proceso_bds_es_un_error(dag_mod, config_ods, config_bds):
    bds = copy.deepcopy(config_bds)
    bds["GRUPOS"][0]["PROCESOS"][0]["DEPENDE_DE_ODS"] = [
        bds["GRUPOS"][0]["PROCESOS"][0]["ID_PROCESO"]]
    with pytest.raises(Exception) as exc:
        _plan(dag_mod, config_ods, bds)
    assert "DEPENDE_DE_ODS" in str(exc.value)


def test_depende_de_ods_a_un_proceso_inexistente_es_un_error(dag_mod, config_ods, config_bds):
    bds = copy.deepcopy(config_bds)
    bds["GRUPOS"][0]["PROCESOS"][0]["DEPENDE_DE_ODS"] = [999999]
    with pytest.raises(Exception) as exc:
        _plan(dag_mod, config_ods, bds)
    assert "999999" in str(exc.value)


def test_depende_de_ods_a_un_proceso_apagado_es_un_error(dag_mod, config_ods, config_bds):
    """Si el insumo esta ACTIVO='N', esta tabla BDS se saltaria SIEMPRE. Es
    mejor decirlo al validar que descubrirlo en la corrida de manana."""
    ods = copy.deepcopy(config_ods)
    bds = copy.deepcopy(config_bds)
    apagado = next(p for g in ods["GRUPOS"] for p in g["PROCESOS"]
                   if str(p.get("ACTIVO", "")).upper() == "N")
    bds["GRUPOS"][0]["PROCESOS"][0]["DEPENDE_DE_ODS"] = [apagado["ID_PROCESO"]]
    with pytest.raises(Exception) as exc:
        _plan(dag_mod, config_ods, bds)
    assert str(apagado["ID_PROCESO"]) in str(exc.value)


def test_depende_de_ods_no_se_admite_dentro_de_ods(dag_mod, config_ods, config_bds):
    ods = copy.deepcopy(config_ods)
    activo = next(p for g in ods["GRUPOS"] for p in g["PROCESOS"]
                  if str(p.get("ACTIVO", "")).upper() == "S")
    activo["DEPENDE_DE_ODS"] = [104]
    with pytest.raises(Exception) as exc:
        _plan(dag_mod, ods, config_bds)
    assert "ODS" in str(exc.value)


def test_la_configuracion_del_repositorio_es_valida(dag_mod, config_ods, config_bds):
    plan = _plan(dag_mod, config_ods, config_bds)
    assert plan.nodos, "el plan quedo sin procesos activos"


# ============================================================================
# 4. QUE UN FALLO NO TRUNQUE NO QUIERE DECIR QUE SE OCULTE
# ============================================================================
def test_la_corrida_acaba_en_rojo_si_fallo_una_tabla(dag_mod):
    from airflow.exceptions import AirflowException

    corrida = CorridaFalsa({
        "inicio": "success", "ods_completo": "success",
        "ods_g.p1": "success", "ods_g.p2": "failed",
        "bds_g.p3": "skipped",
    })
    with pytest.raises(AirflowException) as exc:
        dag_mod.resumen_de_la_corrida(dag_run=corrida)
    mensaje = str(exc.value)
    assert "ods_g.p2" in mensaje, "el resumen no dice QUE tabla fallo"
    assert "ods_g.p1" not in mensaje


def test_el_resumen_cuenta_las_saltadas_aparte(dag_mod, caplog):
    corrida = CorridaFalsa({
        "ods_g.p1": "success", "bds_g.p2": "skipped", "bds_g.p3": "skipped",
    })
    with caplog.at_level(logging.INFO):
        resumen = dag_mod.resumen_de_la_corrida(dag_run=corrida)
    assert resumen == {"success": 1, "skipped": 2}
    assert "no se ejecutaron" in caplog.text


def test_una_corrida_limpia_termina_en_verde(dag_mod):
    corrida = CorridaFalsa({"ods_g.p1": "success", "bds_g.p2": "success"})
    assert dag_mod.resumen_de_la_corrida(dag_run=corrida) == {"success": 2}


def test_upstream_failed_cuenta_como_fallo(dag_mod):
    """Una tabla BDS que no corrio porque su DEPENDE_DE fallo tambien tiene que
    salir en el resumen: si no, un fallo en cadena pasa desapercibido."""
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException):
        dag_mod.resumen_de_la_corrida(
            dag_run=CorridaFalsa({"bds_g.p1": "upstream_failed"}))


def test_el_resumen_no_cuenta_las_tareas_auxiliares(dag_mod):
    """inicio, ods_completo, bds_completo y fin no son tablas."""
    corrida = CorridaFalsa({t: "success" for t in AUXILIARES} | {"ods_g.p1": "success"})
    assert dag_mod.resumen_de_la_corrida(dag_run=corrida) == {"success": 1}
