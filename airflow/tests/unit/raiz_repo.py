"""Localiza la raiz del repositorio, y dice honestamente cuando no esta.

EL PROBLEMA
-----------
Los tests de este directorio son de dos clases distintas, y hasta ahora las dos
calculaban la raiz igual:

    RAIZ = Path(__file__).resolve().parents[3]

Eso funciona EJECUTANDO DESDE EL REPOSITORIO, donde el archivo esta en
<repo>/airflow/tests/unit/test_x.py y parents[3] es <repo>.

Dentro del contenedor no. Ahi los tests se montan en /opt/airflow/tests/unit/,
asi que parents[3] es `/opt` -que existe, pero no es la raiz de nada- y todo
test que lea docker-compose.windows.yml, setup.sh o sql/ muere con

    FileNotFoundError: '/opt/docker-compose.windows.yml'

Y el contenedor es justo donde los docstrings dicen que hay que ejecutarlos.
Veinte tests en rojo por una ruta, mezclados con los fallos de verdad, que es
lo que hace el volcado ilegible: lo que importa se pierde entre lo que no.

La raiz del repositorio NO esta montada en los contenedores, y montarla
entera -con .env y .git dentro- para que estos tests pasen seria pagar
demasiado. Asi que:

  - Los tests que solo necesitan `airflow/` (dags, plugins, config) corren
    igual en los dos sitios: ese arbol SI esta montado.
  - Los que auditan archivos de la raiz se SALTAN con un motivo que dice
    donde ejecutarlos, en vez de fallar.

Un salto es honesto; un rojo que no significa nada entrena a la gente a
ignorar los rojos.

USO
---
    from raiz_repo import RAIZ, requiere_repo

    @requiere_repo
    def test_que_lee_el_compose():
        (RAIZ / "docker-compose.windows.yml").read_text()

La variable de entorno DATAHUB_REPO tiene prioridad, para quien monte la raiz
en el contenedor y quiera que estos tests corran ahi tambien.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# Archivos que solo existen en la raiz del repositorio. Se piden VARIOS porque
# un solo nombre podria coincidir por casualidad en un directorio cualquiera.
MARCADORES = ("docker-compose.windows.yml", "setup.sh", "setup.ps1")


def _es_raiz(ruta: Path) -> bool:
    return all((ruta / m).is_file() for m in MARCADORES)


def localizar() -> tuple[Path, bool]:
    """Devuelve (raiz, encontrada). Si no la encuentra, devuelve la conjetura
    de siempre para que los tests que no la usan sigan funcionando igual."""
    env = os.environ.get("DATAHUB_REPO")
    if env and _es_raiz(Path(env)):
        return Path(env).resolve(), True

    aqui = Path(__file__).resolve()
    for candidata in aqui.parents:
        if _es_raiz(candidata):
            return candidata, True

    return aqui.parents[3], False


RAIZ, HAY_REPO = localizar()

#: Arbol de Airflow. Esto SI esta montado en los contenedores, asi que los
#: tests que solo tocan dags/plugins/config no necesitan `requiere_repo`.
#
#: Se deriva de la ubicacion de ESTE archivo y no de una ruta fija: este
#: archivo vive en <algo>/airflow/tests/unit/, asi que parents[2] es ese
#: `airflow/` tanto en el repositorio como montado en /opt/airflow. Poner
#: "/opt/airflow" a mano fallaba en cualquier otro punto de montaje.
DIR_AIRFLOW = Path(__file__).resolve().parents[2]


def localizar_scripts() -> Path:
    """Carpeta scripts/, que esta en DOS sitios distintos segun donde se corra.

    En el repositorio es <repo>/scripts. En los contenedores se monta en
    /opt/airflow/scripts -ahora tambien en el de Ubuntu-, que es la ruta que
    usa airflow-init. Un test que la busque solo en la raiz se salta sin
    necesidad dentro del contenedor, donde el archivo si esta a mano.
    """
    for candidata in (RAIZ / "scripts", DIR_AIRFLOW / "scripts",
                      Path("/opt/airflow/scripts")):
        if (candidata / "sync_variables.py").is_file():
            return candidata
    return RAIZ / "scripts"


DIR_SCRIPTS = localizar_scripts()

requiere_repo = pytest.mark.skipif(
    not HAY_REPO,
    reason=(
        "Este test audita archivos de la raiz del repositorio "
        "(docker-compose*.yml, setup.sh, sql/...) y esa carpeta no esta "
        "montada aqui. Ejecutelo desde el repositorio:\n"
        "    pytest airflow/tests/unit -q\n"
        "o monte la raiz en el contenedor y exporte DATAHUB_REPO."
    ),
)
