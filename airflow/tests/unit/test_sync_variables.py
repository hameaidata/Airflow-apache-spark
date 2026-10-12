"""
Tests de scripts/sync_variables.py, y en particular de --solo-nuevas.

POR QUE EXISTE ESTE ARCHIVO

airflow-init corre `sync_variables.py --solo-nuevas` en cada arranque. Toda la
razon de ser de esa bandera es que NO pise una Variable que ya existe: si lo
hiciera, cada `docker compose up` borraria en silencio la edicion que alguien
hubiera hecho por la interfaz -incluido el arreglo de urgencia de las 2 de la
madrugada-, y el sintoma seria que "el cambio se deshizo solo".

Ese es exactamente el tipo de fallo que no se nota en una prueba manual: en una
maquina de desarrollo las Variables casi nunca difieren del repo, asi que la
bandera parece funcionar aunque sobreescriba. De ahi estos tests, que lo
comprueban con un doble de Variable y cuentan las llamadas a set().

Ejecucion:
    docker compose exec airflow-scheduler \
        pytest /opt/airflow/tests/unit/test_sync_variables.py -v

DONDE EJECUTARLOS
-----------------
La suite tiene dos clases de test y no corren igual en los dos sitios:

    pytest airflow/tests/unit -q                 <- DESDE EL REPOSITORIO: todos
    docker compose exec -T airflow-scheduler \
        pytest /opt/airflow/tests/unit -q        <- en el contenedor: los que
                                                    necesitan Airflow instalado

Los que auditan archivos de la raiz (docker-compose*.yml, setup.sh, sql/) se
SALTAN en el contenedor con un motivo explicito, porque esa carpeta no esta
montada ahi. Ver airflow/tests/unit/raiz_repo.py.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml

from raiz_repo import DIR_SCRIPTS, RAIZ, requiere_repo  # noqa: E402
SCRIPT = DIR_SCRIPTS / "sync_variables.py"
COMPOSES_CON_INIT = ["docker-compose.windows.yml", "docker-compose.rhel.yml",
                     "docker-compose.ubuntu.yml"]


# ---------------------------------------------------------------------------
# El doble de airflow.models.Variable
# ---------------------------------------------------------------------------
class VariableFalsa:
    """Imita lo poco que el script usa de Variable, y cuenta las escrituras.

    Se cuentan las llamadas y no solo el estado final porque los dos fallos que
    importan son distintos: 'quedo mal' y 'se escribio cuando no debia'. Una
    sobreescritura con el mismo valor deja el estado correcto y es, igualmente,
    el bug que --solo-nuevas existe para evitar.
    """

    def __init__(self, inicial: dict | None = None):
        self.datos = dict(inicial or {})
        self.escrituras: list[str] = []

    def get(self, nombre, deserialize_json=False):
        if nombre not in self.datos:
            raise KeyError(nombre)
        return self.datos[nombre]

    def set(self, nombre, valor, serialize_json=False):
        self.escrituras.append(nombre)
        self.datos[nombre] = valor


@pytest.fixture
def sv(monkeypatch, tmp_path):
    """Carga el script con un airflow.models falso y un directorio de JSON propio."""
    import types

    falsa = VariableFalsa()
    modulo_airflow = types.ModuleType("airflow")
    modulo_models = types.ModuleType("airflow.models")
    modulo_models.Variable = falsa
    modulo_airflow.models = modulo_models

    # setitem y no un import a pelo: el script hace el import DENTRO de la
    # funcion, asi que basta con que sys.modules lo tenga en ese momento. Y
    # monkeypatch.setitem restaura el valor anterior al terminar el test, que
    # es lo que evita que esto se filtre a los demas archivos de la suite.
    monkeypatch.setitem(sys.modules, "airflow", modulo_airflow)
    monkeypatch.setitem(sys.modules, "airflow.models", modulo_models)

    spec = importlib.util.spec_from_file_location("sync_variables_bajo_prueba", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    monkeypatch.setenv("DATAHUB_JSON_DIR", str(tmp_path))
    spec.loader.exec_module(modulo)
    modulo.DIR_JSON = tmp_path
    modulo._falsa = falsa
    modulo._dir = tmp_path
    return modulo


def escribir(directorio: Path, nombre: str, valor: dict) -> None:
    (directorio / f"{nombre}.json").write_text(
        json.dumps(valor, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# --solo-nuevas
# ---------------------------------------------------------------------------
def test_solo_nuevas_crea_la_que_falta(sv):
    escribir(sv._dir, "NUEVA", {"a": 1})

    assert sv.sincronizar(dry_run=False, solo=None, solo_nuevas=True) == 0
    assert sv._falsa.datos["NUEVA"] == {"a": 1}
    assert sv._falsa.escrituras == ["NUEVA"]


def test_solo_nuevas_NO_pisa_una_que_ya_existe(sv):
    """El test que justifica la bandera entera."""
    sv._falsa.datos["YA_ESTA"] = {"editado": "por la interfaz"}
    escribir(sv._dir, "YA_ESTA", {"del": "repo"})

    assert sv.sincronizar(dry_run=False, solo=None, solo_nuevas=True) == 0
    # El valor editado a mano sobrevive...
    assert sv._falsa.datos["YA_ESTA"] == {"editado": "por la interfaz"}
    # ...y no hubo ni una escritura, no solo un resultado equivalente.
    assert sv._falsa.escrituras == []


def test_solo_nuevas_mezcla_crea_una_y_respeta_la_otra(sv):
    sv._falsa.datos["VIEJA"] = {"v": "en airflow"}
    escribir(sv._dir, "VIEJA", {"v": "en el repo"})
    escribir(sv._dir, "NUEVA", {"v": "solo en el repo"})

    assert sv.sincronizar(dry_run=False, solo=None, solo_nuevas=True) == 0
    assert sv._falsa.escrituras == ["NUEVA"]
    assert sv._falsa.datos["VIEJA"] == {"v": "en airflow"}
    assert sv._falsa.datos["NUEVA"] == {"v": "solo en el repo"}


def test_sin_la_bandera_si_sobreescribe(sv):
    """El comportamiento por defecto no cambia: el repo sigue mandando."""
    sv._falsa.datos["LA"] = {"v": "vieja"}
    escribir(sv._dir, "LA", {"v": "nueva"})

    assert sv.sincronizar(dry_run=False, solo=None, solo_nuevas=False) == 0
    assert sv._falsa.datos["LA"] == {"v": "nueva"}
    assert sv._falsa.escrituras == ["LA"]


def test_una_igual_no_se_reescribe_con_ninguna_bandera(sv):
    """Ni con --solo-nuevas ni sin ella: si ya dice lo mismo, no se toca.

    Importa porque cada set() es una escritura en la metadata database y un
    `up` que reescribiera todo generaria ruido en la auditoria sin cambiar nada.
    """
    for solo_nuevas in (False, True):
        sv._falsa.datos = {"IGUAL": {"v": 1}}
        sv._falsa.escrituras = []
        escribir(sv._dir, "IGUAL", {"v": 1})

        assert sv.sincronizar(dry_run=False, solo=None, solo_nuevas=solo_nuevas) == 0
        assert sv._falsa.escrituras == []


def test_dry_run_no_escribe_aunque_falte(sv):
    escribir(sv._dir, "NUEVA", {"a": 1})

    assert sv.sincronizar(dry_run=True, solo=None, solo_nuevas=True) == 0
    assert sv._falsa.escrituras == []
    assert "NUEVA" not in sv._falsa.datos


def test_solo_nuevas_se_combina_con_solo(sv):
    sv._falsa.datos["OTRA"] = {"v": "intacta"}
    escribir(sv._dir, "ESTA", {"v": 1})
    escribir(sv._dir, "OTRA", {"v": "del repo"})

    assert sv.sincronizar(dry_run=False, solo="ESTA", solo_nuevas=True) == 0
    assert sv._falsa.escrituras == ["ESTA"]
    assert sv._falsa.datos["OTRA"] == {"v": "intacta"}


def test_un_json_invalido_no_impide_las_demas(sv):
    (sv._dir / "ROTA.json").write_text("{esto no es json", encoding="utf-8")
    escribir(sv._dir, "BUENA", {"v": 1})

    # Devuelve 1 para que CI lo note, pero la buena si se publica.
    assert sv.sincronizar(dry_run=False, solo=None, solo_nuevas=True) == 1
    assert sv._falsa.datos["BUENA"] == {"v": 1}


def test_export_y_solo_nuevas_juntos_se_rechazan(sv, monkeypatch, capsys):
    """Van en direcciones contrarias; aceptarlo en silencio seria peor."""
    monkeypatch.setattr(sys, "argv",
                        ["sync_variables.py", "--export", "--solo-nuevas"])
    assert sv.main() == 1
    assert "direcciones" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# El cableado en los compose
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("compose", COMPOSES_CON_INIT)
@requiere_repo
def test_airflow_init_publica_las_variables(compose):
    texto = (RAIZ / compose).read_text(encoding="utf-8")
    assert "sync_variables.py --solo-nuevas" in texto, (
        f"{compose}: airflow-init no publica las Variables. Un stack nuevo "
        f"arrancaria sin ellas y el DAG fallaria en su primera tarea.")


@pytest.mark.parametrize("compose", COMPOSES_CON_INIT)
@requiere_repo
def test_airflow_init_no_sincroniza_a_lo_bruto(compose):
    """Sin --solo-nuevas, cada `up` borraria las ediciones hechas por la UI."""
    texto = (RAIZ / compose).read_text(encoding="utf-8")
    for linea in texto.splitlines():
        # Los comentarios del compose mencionan el script por su nombre; lo que
        # se audita aqui son las lineas que lo EJECUTAN.
        pelada = linea.strip()
        if pelada.startswith("#") or "sync_variables.py" not in pelada:
            continue
        if "--dry-run" not in pelada:
            assert "--solo-nuevas" in pelada, (
                f"{compose}: esta linea sincroniza sin --solo-nuevas y "
                f"sobreescribiria Variables en cada arranque:\n    {pelada}")


@pytest.mark.parametrize("compose", COMPOSES_CON_INIT)
@requiere_repo
def test_airflow_init_ve_la_carpeta_de_scripts(compose):
    """Sin el montaje, la linea del init falla con 'No such file or directory'.

    Y como va con `|| echo`, fallaria SIN cortar el arranque: el stack
    levantaria sin Variables y la unica senal seria una linea de advertencia
    en los logs del init, que nadie lee. De ahi que esto se compruebe aqui.
    """
    d = yaml.safe_load((RAIZ / compose).read_text(encoding="utf-8"))
    vols = d["services"]["airflow-init"].get("volumes") or []
    assert any("scripts:/opt/airflow/scripts" in str(v) for v in vols), (
        f"{compose}: airflow-init no monta ./scripts, asi que no puede "
        f"ejecutar sync_variables.py")


@pytest.mark.parametrize("compose", COMPOSES_CON_INIT)
@requiere_repo
def test_el_fallo_al_publicar_no_corta_el_arranque(compose):
    """Un Airflow que no levanta por esto es peor de diagnosticar que una
    Variable ausente, que el propio DAG reporta en su primera tarea."""
    texto = (RAIZ / compose).read_text(encoding="utf-8")
    bloque = [l for l in texto.splitlines() if "sync_variables.py --solo-nuevas" in l]
    assert bloque, f"{compose}: no se encontro la linea"
    i = texto.splitlines().index(bloque[0])
    siguientes = "\n".join(texto.splitlines()[i:i + 3])
    assert "||" in siguientes, (
        f"{compose}: la publicacion de Variables no tiene salida de escape. "
        f"Con `set -e` activo, un fallo ahi impide arrancar todo el stack.")
