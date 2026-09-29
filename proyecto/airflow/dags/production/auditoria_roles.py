"""
Auditoría de roles y permisos

Verifica semanalmente que los roles de Airflow siguen coincidiendo con lo
definido en `config/matriz_roles.py`, y que nadie acumula combinaciones de
roles que rompen la segregación de funciones.

POR QUÉ EXISTE ESTE DAG

Los roles se crean una vez y se olvidan. Con el tiempo alguien concede un
permiso "temporal" para resolver una urgencia, y nadie lo retira. Seis meses
después el Analista puede disparar procesos y nadie recuerda por qué.

Este DAG convierte esa desviación en un fallo visible, con fecha. En la
práctica es la evidencia que se le entrega a un auditor: no "los roles están
bien configurados", sino "se verifican cada lunes y aquí está el historial".

Falla a propósito cuando detecta una desviación. Un DAG en verde que nadie
mira no es un control.
"""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# Airflow añade $AIRFLOW_HOME/config al sys.path al arrancar, así que la matriz
# se importa directamente. Es el mismo archivo que usa aplicar_roles.py: un
# solo origen de verdad, sin copias que se desincronicen.
try:
    from matriz_roles import (
        COMBINACIONES_PROHIBIDAS,
        LIMITES_POR_ROL,
        PERMISOS_BASE,
        ROLES,
    )
    MATRIZ_DISPONIBLE = True
except ImportError as exc:  # pragma: no cover
    MATRIZ_DISPONIBLE = False
    ERROR_IMPORT = str(exc)


def _correr(cmd: list[str]) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"Falló {' '.join(cmd)}: {r.stderr.strip()}")
    return r.stdout


# =============================================================================

def verificar_matriz(**context) -> dict:
    """Compara los permisos reales contra la matriz definida."""
    if not MATRIZ_DISPONIBLE:
        raise RuntimeError(
            f"No se pudo importar matriz_roles: {ERROR_IMPORT}. "
            "Verifique que existe en /opt/airflow/config/"
        )

    salida = _correr(["airflow", "roles", "list", "--permission", "--output", "json"])
    filas = json.loads(salida)

    # El CLI devuelve una fila por (rol, recurso, acción). Los nombres de las
    # claves han variado entre versiones, así que se toleran ambas formas.
    actual: dict[str, set[tuple[str, str]]] = {}
    for fila in filas:
        rol = fila.get("role") or fila.get("Role") or ""
        recurso = fila.get("resource") or fila.get("Resource") or ""
        accion = fila.get("action") or fila.get("Action") or ""
        if rol:
            actual.setdefault(rol, set())
            if recurso and accion:
                actual[rol].add((recurso, accion))

    desviaciones: list[dict] = []

    for nombre, cfg in ROLES.items():
        if nombre not in actual:
            desviaciones.append({
                "rol": nombre,
                "tipo": "ROL_INEXISTENTE",
                "detalle": "El rol no existe en la instancia",
                "severidad": "ALTA",
            })
            log.error("%s: no existe", nombre)
            continue

        esperado: set[tuple[str, str]] = set()
        recursos = set(PERMISOS_BASE) | set(cfg["permisos"])
        for recurso in recursos:
            for accion in set(PERMISOS_BASE.get(recurso, [])) | set(cfg["permisos"].get(recurso, [])):
                esperado.add((recurso, accion))

        faltan = esperado - actual[nombre]
        sobran = actual[nombre] - esperado

        for recurso, accion in sorted(sobran):
            # Un permiso de más es siempre más grave que uno de menos: significa
            # que alguien tiene acceso que nadie autorizó.
            desviaciones.append({
                "rol": nombre,
                "tipo": "PERMISO_NO_AUTORIZADO",
                "detalle": f"{accion} sobre '{recurso}'",
                "severidad": "ALTA",
            })
            log.error("%s: permiso NO AUTORIZADO — %s sobre '%s'", nombre, accion, recurso)

        for recurso, accion in sorted(faltan):
            desviaciones.append({
                "rol": nombre,
                "tipo": "PERMISO_FALTANTE",
                "detalle": f"{accion} sobre '{recurso}'",
                "severidad": "MEDIA",
            })
            log.warning("%s: falta %s sobre '%s'", nombre, accion, recurso)

        if not faltan and not sobran:
            log.info("%s: correcto (%d permisos)", nombre, len(esperado))

    context["task_instance"].xcom_push(key="desviaciones_matriz", value=desviaciones)
    return {"roles_revisados": len(ROLES), "desviaciones": len(desviaciones)}


def verificar_segregacion(**context) -> dict:
    """Comprueba que nadie acumula roles incompatibles."""
    if not MATRIZ_DISPONIBLE:
        raise RuntimeError("No se pudo importar matriz_roles")

    salida = _correr(["airflow", "users", "list", "--output", "json"])
    usuarios = json.loads(salida)

    incumplimientos: list[dict] = []
    conteo_por_rol: dict[str, int] = {}

    for u in usuarios:
        login = u.get("username") or u.get("Username") or "?"
        crudo = u.get("roles") or u.get("Roles") or ""
        # El campo llega como texto tipo "[Admin, Op]" o como lista
        if isinstance(crudo, str):
            roles_usuario = {r.strip(" []'\"") for r in crudo.split(",") if r.strip(" []'\"")}
        else:
            roles_usuario = {str(r) for r in crudo}

        for rol in roles_usuario:
            conteo_por_rol[rol] = conteo_por_rol.get(rol, 0) + 1

        for rol_a, rol_b, motivo in COMBINACIONES_PROHIBIDAS:
            if rol_a in roles_usuario and rol_b in roles_usuario:
                incumplimientos.append({
                    "usuario": login,
                    "tipo": "SEGREGACION_DE_FUNCIONES",
                    "detalle": f"acumula {rol_a} y {rol_b} — {motivo}",
                    "severidad": "ALTA",
                })
                log.error("%s acumula %s y %s: %s", login, rol_a, rol_b, motivo)

    for rol, limite in LIMITES_POR_ROL.items():
        n = conteo_por_rol.get(rol, 0)
        if limite and n > limite:
            incumplimientos.append({
                "usuario": "—",
                "tipo": "EXCESO_DE_PERSONAS",
                "detalle": f"{rol} lo tienen {n} personas, el límite definido es {limite}",
                "severidad": "MEDIA",
            })
            log.warning("%s: %d personas, límite %d", rol, n, limite)

    log.info("Personas por rol: %s", json.dumps(conteo_por_rol, ensure_ascii=False))

    ti = context["task_instance"]
    ti.xcom_push(key="incumplimientos_sod", value=incumplimientos)
    ti.xcom_push(key="conteo_por_rol", value=conteo_por_rol)
    return {"usuarios_revisados": len(usuarios), "incumplimientos": len(incumplimientos)}


def emitir_informe(**context) -> dict:
    """Consolida, deja el informe en la bitácora y falla si hay hallazgos altos."""
    ti = context["task_instance"]
    desviaciones = ti.xcom_pull(task_ids="verificar_matriz", key="desviaciones_matriz") or []
    incumplimientos = ti.xcom_pull(task_ids="verificar_segregacion", key="incumplimientos_sod") or []
    conteo = ti.xcom_pull(task_ids="verificar_segregacion", key="conteo_por_rol") or {}

    hallazgos = desviaciones + incumplimientos
    altos = [h for h in hallazgos if h.get("severidad") == "ALTA"]
    medios = [h for h in hallazgos if h.get("severidad") == "MEDIA"]

    informe = {
        "fecha": context["ds"],
        "run_id": context["dag_run"].run_id,
        "roles_definidos": len(ROLES) if MATRIZ_DISPONIBLE else 0,
        "personas_por_rol": conteo,
        "hallazgos_altos": len(altos),
        "hallazgos_medios": len(medios),
        "detalle": hallazgos,
        "resultado": "CONFORME" if not hallazgos else "CON HALLAZGOS",
    }

    # Se escribe como JSON en una sola línea para que un recolector de bitácoras
    # pueda indexarlo. Es la evidencia que queda para la auditoría.
    log.warning("INFORME_AUDITORIA_ROLES %s", json.dumps(informe, ensure_ascii=False))

    print("=" * 66)
    print(f"  AUDITORÍA DE ROLES — {context['ds']}")
    print("=" * 66)
    print(f"  Roles definidos en la matriz : {informe['roles_definidos']}")
    print(f"  Hallazgos de severidad alta  : {len(altos)}")
    print(f"  Hallazgos de severidad media : {len(medios)}")
    print()
    if conteo:
        print("  Personas por rol:")
        for rol, n in sorted(conteo.items()):
            print(f"    {rol:30} {n}")
        print()
    if hallazgos:
        print("  Hallazgos:")
        for h in hallazgos:
            quien = h.get("rol") or h.get("usuario", "—")
            print(f"    [{h['severidad']:5}] {quien:28} {h['tipo']}")
            print(f"             {h['detalle']}")
    else:
        print("  Sin hallazgos. La configuración coincide con lo definido.")
    print("=" * 66)

    if altos:
        raise ValueError(
            f"{len(altos)} hallazgo(s) de severidad alta en la auditoría de roles. "
            "Revise la bitácora de esta tarea. Corrija con: "
            "python /opt/airflow/config/aplicar_roles.py --aplicar"
        )

    return informe


default_args = {
    "owner": "seguridad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="auditoria_roles",
    description="Verifica que los roles y la segregación de funciones no se desvían",
    default_args=default_args,
    schedule="0 7 * * 1",          # lunes 07:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["seguridad", "auditoria", "cumplimiento"],
) as dag:

    t1 = PythonOperator(
        task_id="verificar_matriz",
        python_callable=verificar_matriz,
    )

    t2 = PythonOperator(
        task_id="verificar_segregacion",
        python_callable=verificar_segregacion,
    )

    t3 = PythonOperator(
        task_id="emitir_informe",
        python_callable=emitir_informe,
        trigger_rule="all_done",   # informa aunque una verificación falle
    )

    [t1, t2] >> t3
