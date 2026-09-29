#!/usr/bin/env python3
"""
Sincroniza airflow/config/json/*.json hacia las Variables de Airflow.

EL PROBLEMA QUE RESUELVE
------------------------
Los JSON estan versionados en git, pero lo que el scheduler realmente lee son
las Variables de la metadata database. Si se cargan a mano por la UI, en unas
semanas el repo y produccion dicen cosas distintas y nadie sabe cual manda.
Este script hace que el repo sea la fuente de verdad.

USO
---
    # ver que cambiaria, sin tocar nada
    docker compose exec airflow-scheduler \\
        python /opt/airflow/scripts/sync_variables.py --dry-run

    # aplicar
    docker compose exec airflow-scheduler \\
        python /opt/airflow/scripts/sync_variables.py

    # exportar lo que hay hoy en Airflow hacia los JSON (para el primer
    # arranque, si alguien ya edito por la UI y quiere versionarlo)
    docker compose exec airflow-scheduler \\
        python /opt/airflow/scripts/sync_variables.py --export

    # una sola
    ... sync_variables.py --solo DAG_BDS_TABLAS

El nombre de la Variable es el nombre del archivo sin .json.
Devuelve codigo 1 si algo fallo, para poder usarlo en CI.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _localizar_json() -> Path:
    """Encuentra airflow/config/json, corra donde corra.

    Dentro del contenedor la carpeta esta montada en /opt/airflow/config,
    no en la ruta relativa al repo, asi que se prueban varios candidatos:

      1. la variable de entorno DATAHUB_JSON_DIR, si esta definida
      2. /opt/airflow/config/json   (dentro de los contenedores de Airflow)
      3. <repo>/airflow/config/json (ejecutandolo desde Windows o Linux)
    """
    candidatos = []
    if os.environ.get("DATAHUB_JSON_DIR"):
        candidatos.append(Path(os.environ["DATAHUB_JSON_DIR"]))
    candidatos.append(Path("/opt/airflow/config/json"))
    try:
        candidatos.append(Path(__file__).resolve().parents[1] / "airflow" / "config" / "json")
    except NameError:
        # __file__ no existe si el script llega por stdin (python - < archivo)
        pass
    candidatos.append(Path.cwd() / "airflow" / "config" / "json")

    for ruta in candidatos:
        if ruta.is_dir():
            return ruta
    return candidatos[0]


DIR_JSON = _localizar_json()
RAIZ = DIR_JSON.parents[2]


def _resumen(valor) -> str:
    texto = json.dumps(valor, ensure_ascii=False, sort_keys=True)
    return texto if len(texto) <= 70 else f"{texto[:67]}..."


def sincronizar(dry_run: bool, solo: str | None) -> int:
    from airflow.models import Variable

    if not DIR_JSON.is_dir():
        print(f"ERROR: no existe {DIR_JSON}", file=sys.stderr)
        return 1

    archivos = sorted(DIR_JSON.glob("*.json"))
    if solo:
        archivos = [a for a in archivos if a.stem == solo]
        if not archivos:
            print(f"ERROR: no existe {DIR_JSON / (solo + '.json')}", file=sys.stderr)
            return 1

    iguales = nuevas = actualizadas = fallidas = 0

    for archivo in archivos:
        nombre = archivo.stem
        try:
            deseado = json.loads(archivo.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"  ERROR   {nombre}: JSON invalido ({exc})")
            fallidas += 1
            continue

        try:
            actual = Variable.get(nombre, deserialize_json=True)
            existe = True
        except Exception:
            actual, existe = None, False

        if existe and actual == deseado:
            print(f"  igual   {nombre}")
            iguales += 1
            continue

        etiqueta = "actualiza" if existe else "crea"
        print(f"  {etiqueta:<9} {nombre}   {_resumen(deseado)}")
        if not dry_run:
            try:
                Variable.set(nombre, deseado, serialize_json=True)
            except Exception as exc:
                print(f"  ERROR   {nombre}: {exc}")
                fallidas += 1
                continue
        actualizadas += existe
        nuevas += not existe

    print(
        f"\n{'(dry-run) ' if dry_run else ''}"
        f"sin cambios: {iguales} | creadas: {nuevas} | actualizadas: {actualizadas}"
        f" | errores: {fallidas}"
    )
    return 1 if fallidas else 0


def exportar(solo: str | None) -> int:
    """Escribe en los JSON lo que hay hoy en Airflow."""
    from airflow.models import Variable

    nombres = [solo] if solo else [a.stem for a in sorted(DIR_JSON.glob("*.json"))]
    fallidas = 0
    for nombre in nombres:
        try:
            valor = Variable.get(nombre, deserialize_json=True)
        except Exception as exc:
            print(f"  ERROR   {nombre}: no esta en Airflow ({exc})")
            fallidas += 1
            continue
        destino = DIR_JSON / f"{nombre}.json"
        destino.write_text(
            json.dumps(valor, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(f"  exporta {nombre} -> {destino}")
    return 1 if fallidas else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="muestra que cambiaria sin escribir")
    parser.add_argument("--export", action="store_true",
                        help="direccion inversa: Airflow -> archivos JSON")
    parser.add_argument("--solo", metavar="NOMBRE",
                        help="procesa una sola Variable")
    args = parser.parse_args()

    print(f"Directorio: {DIR_JSON}\n")
    return exportar(args.solo) if args.export else sincronizar(args.dry_run, args.solo)


if __name__ == "__main__":
    sys.exit(main())
