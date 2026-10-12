#!/usr/bin/env python3
"""Anade o quita las tres tablas de prueba de carga en la Variable BT2SQL_SPARK.

POR QUE UN SCRIPT Y NO UN BLOQUE JSON PARA PEGAR
------------------------------------------------
BT2SQL_SPARK tiene mas de 200 lineas y es la unica fuente de verdad del
pipeline. Editarla a mano para una prueba, y volver a editarla despues para
deshacerla, es la forma mas comoda de dejarse una coma de mas o una tabla
olvidada en produccion. Esto es idempotente -se puede correr dos veces- y
reversible con --quitar.

Las entradas que anade van marcadas con "_prueba_carga": true, que es como
--quitar las encuentra. No borra nada que no lleve esa marca.

USO
---
    python scripts/tablas_prueba_carga.py              # anade
    python scripts/tablas_prueba_carga.py --quitar     # deshace
    python scripts/tablas_prueba_carga.py --activar PAYTX
    python scripts/tablas_prueba_carga.py --listar

Despues de cualquier cambio hay que PUBLICAR, o el DAG seguira viendo lo de
antes:

    docker compose -f docker-compose.windows.yml exec -T airflow-scheduler \
        python /opt/airflow/scripts/sync_variables.py --solo BT2SQL_SPARK

QUE TABLA QUEDA ACTIVA
----------------------
Solo CARDTX, y las otras dos en "N". Para medir hay que mover UNA cosa cada
vez: con las tres activas, el tiempo total no dice cual tardo en que, y la
cola del pool de Spark mezcla los tiempos de espera con los de trabajo.
Use --activar para ir cambiando.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
JSON_VARIABLE = RAIZ / "airflow" / "config" / "json" / "BT2SQL_SPARK.json"

MARCA = "_prueba_carga"

# ---------------------------------------------------------------------------
# Las tres tablas.
#
# La columna de particion es lo que decide si subir NUM_EXECUTORS sirve para
# algo: sin ella Spark lee JDBC en UNA conexion, y da igual cuantos cores
# tenga el cluster. "particiones" se deja en 8 para las dos de Kaggle -que
# tienen volumen de sobra para repartir- y en 4 para BTGEN, que se usa a
# varios tamanos y con pocas filas 8 particiones dejarian casi todas vacias.
#
# "tipos" va vacio a proposito: la Variable trae tipos_desde_destino = true,
# asi que el job los deduce de INFORMATION_SCHEMA.COLUMNS de la tabla STG. Es
# el camino que conviene ejercitar, y evita mantener la lista de tipos en dos
# sitios (aqui y en la DDL) donde puede divergir en silencio.
# ---------------------------------------------------------------------------
TABLAS = [
    {
        "nombre_proceso": "STG_CARDTX",
        "esquema": "GPPPBTDB",
        "tabla": "CARDTX",
        "descripcion": "Prueba de carga: 284.807 filas x 31 columnas numericas",
        "activo": "S",
        "columnas": "",
        "filtro": "",
        "tipos": "",
        "tabla_destino": "STG_CARDTX",
        "batch_size": 50000,
        "commit_every": 20,
        "particion": {
            "columna": "TIME_SEG",
            "desde": 0,
            "hasta": 172792,
            "particiones": 8,
        },
        MARCA: True,
    },
    {
        "nombre_proceso": "STG_PAYTX",
        "esquema": "GPPPBTDB",
        "tabla": "PAYTX",
        "descripcion": "Prueba de carga: ~6,3M filas x 11 columnas mixtas",
        "activo": "N",
        "columnas": "",
        "filtro": "",
        "tipos": "",
        "tabla_destino": "STG_PAYTX",
        "batch_size": 50000,
        "commit_every": 20,
        "particion": {
            "columna": "STEP",
            "desde": 1,
            "hasta": 743,
            "particiones": 8,
        },
        MARCA: True,
    },
    {
        "nombre_proceso": "STG_BTGEN",
        "esquema": "GPPPBTDB",
        "tabla": "BTGEN",
        "descripcion": "Prueba de carga: filas generadas, para la curva de escalado",
        "activo": "N",
        "columnas": "",
        "filtro": "",
        "_particion_nota": (
            "'hasta' tiene que coincidir con el numero de filas que genero el "
            "bloque 3b de sql/bt2sql/92_bt2sql_tablas_carga_kaggle.sql. Si "
            "genera 10 millones y deja 1000000 aqui, Spark reparte solo el "
            "primer millon entre las particiones y el resto cae todo en la "
            "ultima: el job no falla, pero el tiempo medido no significa nada."
        ),
        "tipos": "",
        "tabla_destino": "STG_BTGEN",
        "batch_size": 50000,
        "commit_every": 20,
        "particion": {
            "columna": "ID",
            "desde": 1,
            "hasta": 1000000,
            "particiones": 4,
        },
        MARCA: True,
    },
]

# Prioridad 1, igual que FSH005: no hay dependencia entre ellas, y encadenarlas
# en niveles distintos solo anadiria espera sin medir nada.
PROCESOS = [
    {
        "nombre_proceso": t["nombre_proceso"],
        "nombre_esquema": "GPPPBTDB",
        "estado": 1,
        "prioridad": 1,
        "estado_diario": 1,
        "estado_semanal": 0,
        "estado_mensual": 0,
        MARCA: True,
    }
    for t in TABLAS
]


def leer() -> dict:
    if not JSON_VARIABLE.is_file():
        sys.exit(f"No existe {JSON_VARIABLE}")
    return json.loads(JSON_VARIABLE.read_text(encoding="utf-8"))


def escribir(doc: dict) -> None:
    JSON_VARIABLE.write_text(
        json.dumps(doc, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def clave(e: dict) -> str:
    return f'{e.get("esquema") or e.get("nombre_esquema")}.{e.get("nombre_proceso")}'


def anadir(doc: dict) -> int:
    cambios = 0
    for lista, nuevos in (("tablas", TABLAS),
                          ("procesos", PROCESOS)):
        destino = doc if lista == "tablas" else doc["extraccion"]
        actuales = destino.setdefault(lista, [])
        indice = {clave(e): i for i, e in enumerate(actuales)}
        for nuevo in nuevos:
            k = clave(nuevo)
            if k in indice:
                # Se reemplaza en su sitio y no se duplica: dos entradas con el
                # mismo nombre_proceso harian que el grafo dibujara dos tareas
                # con el mismo task_id y el DAG no se parsea.
                actuales[indice[k]] = dict(nuevo)
                print(f"  actualiza  {lista:9} {k}")
            else:
                actuales.append(dict(nuevo))
                print(f"  anade      {lista:9} {k}")
            cambios += 1
    return cambios


def quitar(doc: dict) -> int:
    cambios = 0
    for lista in ("tablas", "procesos"):
        destino = doc if lista == "tablas" else doc["extraccion"]
        actuales = destino.get(lista) or []
        quedan = [e for e in actuales if not e.get(MARCA)]
        for e in actuales:
            if e.get(MARCA):
                print(f"  quita      {lista:9} {clave(e)}")
                cambios += 1
        destino[lista] = quedan
    return cambios


def activar(doc: dict, tabla: str) -> int:
    """Deja activa SOLO la tabla indicada, entre las de prueba."""
    tabla = tabla.upper()
    de_prueba = [e for e in (doc.get("tablas") or []) if e.get(MARCA)]
    if not de_prueba:
        sys.exit("No hay tablas de prueba en la Variable. Corra sin --quitar primero.")
    nombres = [e["tabla"] for e in de_prueba]
    if tabla not in nombres:
        sys.exit(f"{tabla} no es una tabla de prueba. Hay: {', '.join(nombres)}")
    for e in de_prueba:
        antes = e["activo"]
        e["activo"] = "S" if e["tabla"] == tabla else "N"
        if antes != e["activo"]:
            print(f"  {e['tabla']:8} {antes} -> {e['activo']}")
    return 1


def listar(doc: dict) -> None:
    print(f"{'tabla':10} {'activo':7} {'destino':14} {'particion':28} prueba")
    print("-" * 74)
    for e in doc.get("tablas") or []:
        p = e.get("particion") or {}
        desc = (f'{p.get("columna")} {p.get("desde")}..{p.get("hasta")} '
                f'/{p.get("particiones")}') if p else "(una sola conexion)"
        print(f'{e.get("tabla",""):10} {e.get("activo",""):7} '
              f'{e.get("tabla_destino",""):14} {desc:28} '
              f'{"si" if e.get(MARCA) else ""}')


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--quitar", action="store_true",
                   help="borra las entradas marcadas como de prueba")
    g.add_argument("--activar", metavar="TABLA",
                   help="deja activa solo esa tabla de prueba (CARDTX/PAYTX/BTGEN)")
    g.add_argument("--listar", action="store_true",
                   help="muestra el estado actual y no cambia nada")
    args = ap.parse_args()

    doc = leer()

    if args.listar:
        listar(doc)
        return

    if args.quitar:
        n = quitar(doc)
    elif args.activar:
        n = activar(doc, args.activar)
    else:
        n = anadir(doc)

    if not n:
        print("Nada que cambiar.")
        return

    escribir(doc)
    print(f"\n{JSON_VARIABLE.relative_to(RAIZ)} actualizado.")
    print("\nAHORA HAY QUE PUBLICAR, o el DAG seguira viendo lo de antes:")
    print("    docker compose -f docker-compose.windows.yml exec -T "
          "airflow-scheduler \\")
    print("        python /opt/airflow/scripts/sync_variables.py "
          "--solo BT2SQL_SPARK")


if __name__ == "__main__":
    main()
