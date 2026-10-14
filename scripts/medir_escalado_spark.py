#!/usr/bin/env python3
"""Mide si el cluster de Spark escala de verdad al subir cores y workers.

    docker compose -f docker-compose.windows.yml exec -T airflow-worker \
        python /opt/airflow/scripts/medir_escalado_spark.py --estado

MODOS
-----
  --estado   Lo que el cluster tiene AHORA, leido del propio Spark, mas los
             topes de configuracion que limitan el paralelismo. Es el modo
             importante: dice si la curva va a salir plana ANTES de gastar
             una tarde midiendo.

  --marcar E Guarda la configuracion actual con la etiqueta E, para poder
             cruzarla despues con los tiempos. Se corre justo ANTES de lanzar
             el DAG.

  --curva    La tabla de resultados: cada corrida con sus filas por segundo y
             su eficiencia frente al escalado lineal.

POR QUE NO BASTA CON CRONOMETRAR
--------------------------------
Spark NO FALLA cuando no puede darle los recursos que pide. Si pide 8 cores y
el cluster tiene 2, el job corre con 2 y termina bien: la unica senal es que
tarda lo mismo que antes. Medir sin comprobar lo que el cluster concedio de
verdad lleva a la conclusion equivocada -- "no escala" -- cuando lo que pasa es
que nunca se le dieron los recursos.

Por eso todo lo que este script afirma sobre el cluster sale del endpoint JSON
del master, no de la configuracion. La configuracion es lo que se pidio; el
JSON es lo que hay.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

MASTER = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")
VARIABLE_SPARK = "CONFIGURACION_SPARK"
VARIABLE_PIPE = "BT2SQL_SPARK"
# Va en el volumen de parquet porque es el unico sitio montado en el host que
# sobrevive a un docker compose down. /tmp se pierde y nadie lo sabe hasta que
# busca el historico.
REGISTRO = Path(os.environ.get("BT2SQL_PARQUET_CONTAINER_DIR", "/data/bt2sql")) \
    / "escalado_spark.jsonl"


# ---------------------------------------------------------------------------
# EL CLUSTER REAL
# ---------------------------------------------------------------------------
def cluster() -> dict:
    """Lo que el master dice que tiene. No lo que el .env pidio."""
    try:
        with urllib.request.urlopen(f"{MASTER}/json/", timeout=10) as r:
            d = json.loads(r.read().decode())
    except Exception as exc:                                     # noqa: BLE001
        return {"error": f"{exc}  (master: {MASTER})"}

    vivos = [w for w in (d.get("workers") or [])
             if str(w.get("state", "")).upper() == "ALIVE"]
    return {
        "workers_vivos": len(vivos),
        "cores_totales": int(d.get("cores", 0)),
        "cores_en_uso": int(d.get("coresused", 0)),
        "memoria_mb": int(d.get("memory", 0)),
        "apps_activas": len(d.get("activeapps") or []),
        "detalle": [
            {"id": w.get("id"), "cores": w.get("cores"),
             "en_uso": w.get("coresused"), "mem_mb": w.get("memory")}
            for w in vivos
        ],
    }


def variable(nombre: str) -> dict:
    from airflow.models import Variable
    try:
        return Variable.get(nombre, deserialize_json=True) or {}
    except Exception:                                            # noqa: BLE001
        return {}


# ---------------------------------------------------------------------------
# LOS TOPES
# ---------------------------------------------------------------------------
def topes(c: dict) -> list[tuple[str, str, bool]]:
    """Las cinco cosas que limitan el paralelismo, en el orden en que muerden.

    Devuelve (nombre, explicacion, es_un_problema). Cada una puede dejar la
    curva plana por si sola, asi que de poco sirve arreglar una si queda otra.
    """
    rec = (variable(VARIABLE_SPARK) or {}).get("RECURSOS") or {}
    pipe = variable(VARIABLE_PIPE) or {}
    ext = pipe.get("extraccion") or {}
    tablas = pipe.get("tablas") or []

    cores_cluster = c.get("cores_totales", 0)
    cores_max = int(rec.get("CORES_MAX", 0) or 0)
    num_exec = int(rec.get("NUM_EXECUTORS", 0) or 0)
    exec_cores = int(rec.get("EXECUTOR_CORES", 0) or 0)
    archivos = int(ext.get("archivos_por_tabla", 1) or 1)

    activas = [t for t in tablas
               if str(t.get("activo", "S")).strip().upper() == "S"]
    sin_particion = [t.get("tabla") for t in activas if not t.get("particion")]

    out = []

    # 1 -- el cluster
    out.append((
        f"Cluster: {c.get('workers_vivos')} worker(s), {cores_cluster} core(s)",
        "Es el techo absoluto. Nada de lo de abajo puede superarlo.",
        cores_cluster <= 2))

    # 2 -- lo que el job pide
    pedidos = num_exec * exec_cores
    out.append((
        f"RECURSOS pide {num_exec} executor(s) x {exec_cores} core(s) = "
        f"{pedidos}, con CORES_MAX {cores_max}",
        "Si CORES_MAX es menor que los cores del cluster, sobran workers "
        "inactivos y el job no los toca. Si es mayor, Spark espera a que "
        "aparezcan y puede quedarse en WAITING sin fallar.",
        cores_max != cores_cluster or pedidos > cores_max))

    # 3 -- la lectura
    out.append((
        f"Particion de lectura: {len(activas) - len(sin_particion)} de "
        f"{len(activas)} tabla(s) activa(s) la declaran",
        "Sin columna de particion, Spark lee JDBC en UNA conexion. Subir "
        "executors no cambia NADA en la lectura: no hay como repartir. "
        + (f"Sin particion: {', '.join(str(t) for t in sin_particion)}"
           if sin_particion else ""),
        bool(sin_particion)))

    # 4 -- la escritura
    out.append((
        f"archivos_por_tabla = {archivos}",
        "Es un coalesce(N) antes de escribir. Con 1, TODA la escritura del "
        "parquet ocurre en una sola tarea, en un core, por muchos executors "
        "que haya. Debe acompanar al paralelismo, no quedarse en 1.",
        archivos == 1))

    # 5 -- el pool de Airflow
    pool = os.environ.get("AIRFLOW_POOL_SPARK", "(sin declarar)")
    out.append((
        f"Pool de Airflow: {pool}",
        "Limita cuantos spark-submit corren a la vez. Con un solo slot, dos "
        "tablas nunca se extraen en paralelo aunque el cluster tenga sitio.",
        pool == "(sin declarar)"))

    return out


# ---------------------------------------------------------------------------
# MODOS
# ---------------------------------------------------------------------------
def modo_estado() -> int:
    c = cluster()
    print("=" * 74)
    print("  EL CLUSTER, SEGUN EL PROPIO SPARK")
    print("=" * 74)
    if "error" in c:
        print(f"  No se pudo leer el master: {c['error']}")
        print("  Sin esto no se puede afirmar nada del cluster. Compruebe que")
        print("  spark-master esta arriba y que este contenedor lo alcanza.")
        return 2

    print(f"  workers vivos : {c['workers_vivos']}")
    print(f"  cores         : {c['cores_totales']}  "
          f"(en uso ahora: {c['cores_en_uso']})")
    print(f"  memoria       : {c['memoria_mb']} MB")
    print(f"  apps activas  : {c['apps_activas']}")
    for w in c["detalle"]:
        print(f"      {w['id']}  {w['cores']} core(s), "
              f"{w['mem_mb']} MB, {w['en_uso']} en uso")

    print()
    print("=" * 74)
    print("  LOS CINCO TOPES, EN EL ORDEN EN QUE MUERDEN")
    print("=" * 74)
    problemas = 0
    for titulo, detalle, malo in topes(c):
        marca = "  !!" if malo else "  ok"
        if malo:
            problemas += 1
        print(f"{marca}  {titulo}")
        for linea in detalle.split(". "):
            if linea.strip():
                print(f"        {linea.strip().rstrip('.')}.")
        print()

    print("=" * 74)
    if problemas == 0:
        print("  VEREDICTO: nada obvio limita el paralelismo. La curva deberia")
        print("  mover. Mida con --marcar y --curva.")
    else:
        print(f"  VEREDICTO: {problemas} tope(s) activos. Con cualquiera de")
        print("  ellos puesto, subir workers NO va a cambiar el tiempo, y la")
        print("  medicion diria 'no escala' cuando el problema es otro.")
        print()
        print("  Arreglelos TODOS antes de medir: de poco sirve quitar uno si")
        print("  queda otro, porque el que quede marca el tiempo igual.")
    print("=" * 74)
    return 0 if problemas == 0 else 1


def modo_marcar(etiqueta: str) -> int:
    c = cluster()
    if "error" in c:
        print(f"No se pudo leer el master: {c['error']}")
        return 2
    rec = (variable(VARIABLE_SPARK) or {}).get("RECURSOS") or {}
    ext = (variable(VARIABLE_PIPE) or {}).get("extraccion") or {}

    fila = {
        "etiqueta": etiqueta,
        "momento": datetime.now().isoformat(timespec="seconds"),
        "workers": c["workers_vivos"],
        "cores_cluster": c["cores_totales"],
        "num_executors": rec.get("NUM_EXECUTORS"),
        "executor_cores": rec.get("EXECUTOR_CORES"),
        "cores_max": rec.get("CORES_MAX"),
        "executor_memory": rec.get("EXECUTOR_MEMORY"),
        "archivos_por_tabla": ext.get("archivos_por_tabla"),
    }
    REGISTRO.parent.mkdir(parents=True, exist_ok=True)
    with open(REGISTRO, "a", encoding="utf-8") as f:
        f.write(json.dumps(fila, ensure_ascii=False) + "\n")
    print(f"Marcado '{etiqueta}' en {REGISTRO}")
    for k, v in fila.items():
        print(f"   {k:20} {v}")
    print()
    print("Ahora lance el DAG. Cuando termine, 'python medir_escalado_spark.py "
          "--curva' cruza esto con los tiempos.")
    return 0


def modo_curva() -> int:
    """Los tiempos de ctl_proceso_parquet, cruzados con las marcas por hora."""
    sys.path.insert(0, "/opt/airflow/dags/production")
    from etl_bt2sql.bt2sql_comun import conexion_sqlserver

    pipe = variable(VARIABLE_PIPE)
    cfg = (pipe.get("extraccion") or {})
    tabla = cfg.get("tb_proceso_parquet", "ctl_proceso_parquet")
    conn_id = cfg.get("conn_id_destino", "CONEXION_SQLSERVER")

    conn = conexion_sqlserver({"conn_id_destino": conn_id})
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT batch_id, tabla_origen, filas_procesadas, "
            f"       duracion_segundos, fec_inicio "
            f"FROM {tabla} WHERE estado = 'TERMINADO' "
            f"  AND filas_procesadas > 0 AND duracion_segundos > 0 "
            f"ORDER BY id_log")
        filas = [tuple(f) for f in cur.fetchall()]
    finally:
        conn.close()

    if not filas:
        print("No hay corridas TERMINADO con filas en la bitacora. "
              "Lance el DAG primero.")
        return 1

    marcas = []
    if REGISTRO.exists():
        for ln in REGISTRO.read_text(encoding="utf-8").splitlines():
            if ln.strip():
                try:
                    marcas.append(json.loads(ln))
                except Exception:                                # noqa: BLE001
                    pass

    def marca_de(inicio) -> dict:
        """La marca mas reciente ANTERIOR al inicio de la corrida."""
        try:
            t = str(inicio)[:19]
        except Exception:                                        # noqa: BLE001
            return {}
        previas = [m for m in marcas if m.get("momento", "") <= t]
        return previas[-1] if previas else {}

    print("=" * 102)
    print(f"  {'etiqueta':14} {'W':>2} {'cor':>3} {'arch':>4}  "
          f"{'tabla':14} {'filas':>10} {'seg':>8} {'filas/s':>10}  eficiencia")
    print("=" * 102)

    base = None
    for batch, tab, n, seg, ini in filas:
        m = marca_de(ini)
        n, seg = int(n), float(seg)
        vel = n / seg if seg else 0
        cores = m.get("cores_cluster") or 0

        # La eficiencia se mide contra la PRIMERA corrida medida, escalada por
        # los cores. Con 2x cores, el ideal es 2x velocidad; 100% es escalado
        # lineal perfecto, que en la practica no se alcanza nunca.
        if base is None and cores:
            base = (vel, cores)
            efic = "  (base)"
        elif base and cores:
            ideal = base[0] * (cores / base[1])
            efic = f"{100 * vel / ideal:6.0f}%" if ideal else "      -"
        else:
            efic = "      -"

        print(f"  {str(m.get('etiqueta', '?'))[:14]:14} "
              f"{m.get('workers', '?'):>2} {cores or '?':>3} "
              f"{m.get('archivos_por_tabla', '?'):>4}  "
              f"{str(tab)[:14]:14} {n:>10,} {seg:>8.1f} {vel:>10,.0f}  {efic}")

    print("=" * 102)
    print()
    print("  COMO SE LEE")
    print("  Eficiencia 100% = escalado lineal perfecto: el doble de cores, el")
    print("  doble de velocidad. En la practica nunca se alcanza.")
    print()
    print("    por encima del 70%   escala bien")
    print("    entre 40% y 70%      escala, con coste de coordinacion normal")
    print("    por debajo del 40%   hay un tope. Corra --estado: casi siempre")
    print("                         es la particion de lectura o el coalesce")
    print("    plana (misma filas/s con el doble de cores)")
    print("                         algo serializa el trabajo entero. Es el")
    print("                         sintoma de archivos_por_tabla = 1 o de")
    print("                         leer sin particion")
    print()
    print("  Y si la velocidad BAJA al subir cores, no es un error de medida:")
    print("  mas executors sobre los mismos datos anaden coordinacion y, si el")
    print("  trabajo por executor es pequeno, el reparto cuesta mas que lo que")
    print("  ahorra. Con tablas de pocas filas es lo esperado.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--estado", action="store_true",
                   help="cluster real + los cinco topes + veredicto")
    g.add_argument("--marcar", metavar="ETIQUETA",
                   help="registra la configuracion actual antes de una corrida")
    g.add_argument("--curva", action="store_true",
                   help="tabla de resultados con filas/s y eficiencia")
    a = ap.parse_args()

    if a.estado:
        return modo_estado()
    if a.marcar:
        return modo_marcar(a.marcar)
    return modo_curva()


if __name__ == "__main__":
    raise SystemExit(main())
