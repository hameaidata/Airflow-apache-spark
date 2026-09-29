#!/usr/bin/env python3
"""
Aplica la matriz de roles definida en matriz_roles.py

SE EJECUTA DENTRO DEL CONTENEDOR. Use los envoltorios:
    Windows:  .\\scripts\\crear_roles.ps1
    Linux:    ./scripts/crear_roles.sh

O directamente:
    docker compose -f docker-compose.windows.yml exec airflow-webserver \\
        python /opt/airflow/config/aplicar_roles.py --simular

Opciones:
    --simular       Muestra lo que haria, sin tocar nada. USELO PRIMERO.
    --aplicar       Ejecuta los cambios.
    --verificar     Compara lo que hay contra la matriz y reporta diferencias.

-----------------------------------------------------------------------------
COMO FUNCIONA

Invoca el CLI de Airflow (`airflow roles create` / `add-perms`) en lugar de
tocar la base de datos. Dos razones:

  - El CLI es interfaz publica y estable; el esquema interno no lo es
  - Cada cambio queda en la bitacora de auditoria de Airflow, atribuido

Ambos comandos son idempotentes: volver a ejecutarlos no duplica nada.

-----------------------------------------------------------------------------
NO TOCA LOS ROLES DE FABRICA

Admin, Op, User, Viewer y Public quedan intactos. Conviene conservar al menos
un usuario con el rol Admin de fabrica como acceso de emergencia: si la matriz
queda mal configurada, es la unica forma de volver a entrar.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys

try:
    from matriz_roles import PERMISOS_BASE, ROLES, resumen
except ImportError:
    print("ERROR: no encuentro matriz_roles.py")
    print("Debe estar junto a este archivo, en /opt/airflow/config/")
    sys.exit(1)


VERDE, AMARILLO, ROJO, CYAN, FIN = "\033[0;32m", "\033[1;33m", "\033[0;31m", "\033[0;36m", "\033[0m"


def ok(msg):    print(f"{VERDE}[ok]{FIN} {msg}")
def aviso(msg): print(f"{AMARILLO}[aviso]{FIN} {msg}")
def error(msg): print(f"{ROJO}[error]{FIN} {msg}")
def info(msg):  print(f"{CYAN}[..]{FIN} {msg}")


def correr(cmd: list[str], simular: bool) -> tuple[bool, str]:
    """Ejecuta un comando de Airflow. En modo simulacion solo lo imprime."""
    if simular:
        # shlex.quote para que la linea impresa sea copiable tal cual. Sin esto,
        # un recurso como "Audit Logs" se partiria en dos al pegarlo en la shell.
        print(f"    $ {' '.join(shlex.quote(c) for c in cmd)}")
        return True, ""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return r.returncode == 0, (r.stdout + r.stderr).strip()
    except subprocess.TimeoutExpired:
        return False, "tiempo de espera agotado"
    except Exception as exc:
        return False, str(exc)


def roles_existentes() -> set[str]:
    r = subprocess.run(["airflow", "roles", "list", "--output", "plain"],
                       capture_output=True, text=True, timeout=60)
    nombres = set()
    for linea in r.stdout.splitlines()[1:]:
        if linea.strip():
            nombres.add(linea.split()[0])
    return nombres


def permisos_actuales() -> dict[str, set[tuple[str, str]]]:
    """Devuelve {rol: {(recurso, accion), ...}} leyendo del CLI."""
    r = subprocess.run(["airflow", "roles", "list", "--permission", "--output", "plain"],
                       capture_output=True, text=True, timeout=90)
    actual: dict[str, set[tuple[str, str]]] = {}
    for linea in r.stdout.splitlines()[1:]:
        partes = linea.split()
        if len(partes) >= 3:
            rol, recurso, accion = partes[0], " ".join(partes[1:-1]), partes[-1]
            actual.setdefault(rol, set()).add((recurso, accion))
    return actual


# =============================================================================

def aplicar(simular: bool) -> int:
    print(f"{CYAN}{'=' * 70}{FIN}")
    print(f"{CYAN}  {'SIMULACION — no se cambia nada' if simular else 'APLICANDO CAMBIOS'}{FIN}")
    print(f"{CYAN}{'=' * 70}{FIN}\n")

    existentes = roles_existentes() if not simular else set()
    fallos: list[str] = []
    creados = actualizados = 0

    for nombre, cfg in ROLES.items():
        print(f"{CYAN}── {nombre}{FIN}")
        print(f"   {cfg['descripcion']}")

        # 1. Crear el rol. add_role de FAB no duplica si ya existe.
        if nombre in existentes:
            print(f"   (ya existe, se actualizan sus permisos)")
        else:
            bien, salida = correr(["airflow", "roles", "create", nombre], simular)
            if bien:
                creados += 1
            else:
                fallos.append(f"{nombre}: no se pudo crear — {salida}")
                error(f"   no se pudo crear: {salida}")
                continue

        # 2. Permisos base + los propios del rol
        todos = dict(PERMISOS_BASE)
        for recurso, acciones in cfg["permisos"].items():
            todos.setdefault(recurso, [])
            todos[recurso] = sorted(set(todos[recurso]) | set(acciones))

        # Se aplica recurso por recurso, no todo junto. Si un nombre de recurso
        # esta mal, falla solo ese y el resto del rol queda aplicado; ademas el
        # mensaje dice exactamente cual.
        aplicados = 0
        for recurso, acciones in sorted(todos.items()):
            cmd = ["airflow", "roles", "add-perms", nombre, "-r", recurso, "-a", *acciones]
            bien, salida = correr(cmd, simular)
            if bien:
                aplicados += 1
            else:
                fallos.append(f"{nombre} / {recurso}: {salida}")
                error(f"   recurso '{recurso}': {salida}")

        actualizados += 1
        print(f"   {aplicados}/{len(todos)} recursos aplicados\n")

    print(f"{CYAN}{'=' * 70}{FIN}")
    if simular:
        print("Simulacion terminada. Para aplicar de verdad:")
        print("    python /opt/airflow/config/aplicar_roles.py --aplicar")
    else:
        ok(f"{creados} roles creados, {actualizados} procesados")
        if fallos:
            print()
            error(f"{len(fallos)} problemas:")
            for f in fallos:
                print(f"    - {f}")
            print()
            aviso("Un 'Resource named X does not exist' suele significar que el")
            aviso("nombre del recurso cambio de version. Revise matriz_roles.py.")
            return 1
        print()
        print("Siguiente paso — asignar personas:")
        print("    airflow users add-role -u <usuario> -r BSG_IngenieroDatos")
        print()
        aviso("Conserve al menos un usuario con el rol Admin de fabrica como")
        aviso("acceso de emergencia, por si la matriz queda mal configurada.")
    return 0


def verificar() -> int:
    """Compara lo que hay en la instancia contra la matriz."""
    print(f"{CYAN}{'=' * 70}{FIN}")
    print(f"{CYAN}  VERIFICACION{FIN}")
    print(f"{CYAN}{'=' * 70}{FIN}\n")

    actual = permisos_actuales()
    desviaciones = 0

    for nombre, cfg in ROLES.items():
        if nombre not in actual:
            error(f"{nombre}: NO EXISTE en la instancia")
            desviaciones += 1
            continue

        esperado: set[tuple[str, str]] = set()
        for recurso, acciones in {**PERMISOS_BASE, **cfg["permisos"]}.items():
            base = PERMISOS_BASE.get(recurso, [])
            propio = cfg["permisos"].get(recurso, [])
            for accion in set(base) | set(propio):
                esperado.add((recurso, accion))

        faltan = esperado - actual[nombre]
        sobran = actual[nombre] - esperado

        if not faltan and not sobran:
            ok(f"{nombre}: correcto ({len(esperado)} permisos)")
        else:
            desviaciones += 1
            error(f"{nombre}: DESVIADO")
            for r, a in sorted(faltan):
                print(f"      falta:  {a:12} sobre {r}")
            for r, a in sorted(sobran):
                print(f"      SOBRA:  {a:12} sobre {r}   <-- permiso no autorizado")

    print()
    if desviaciones:
        error(f"{desviaciones} rol(es) no coinciden con la matriz")
        return 1
    ok("Todos los roles coinciden con lo definido")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Aplica la matriz de roles de Airflow")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--simular", action="store_true", help="Muestra lo que haria")
    g.add_argument("--aplicar", action="store_true", help="Ejecuta los cambios")
    g.add_argument("--verificar", action="store_true", help="Compara con la matriz")
    g.add_argument("--resumen", action="store_true", help="Solo muestra la matriz")
    args = p.parse_args()

    if args.resumen:
        print(resumen())
        return 0
    if args.verificar:
        return verificar()
    return aplicar(simular=args.simular)


if __name__ == "__main__":
    sys.exit(main())
