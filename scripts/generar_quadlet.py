#!/usr/bin/env python3
"""
Genera las unidades Quadlet de RHEL a partir de docker-compose.rhel.yml.

===========================================================================
POR QUE UN GENERADOR Y NO DIECISEIS ARCHIVOS ESCRITOS A MANO

Son diez contenedores, cinco volumenes y una red. Mantenerlos a mano en
paralelo con el compose garantiza que en tres meses digan cosas distintas, y
que nadie sepa cual de los dos es la verdad.

Generandolos, el compose sigue siendo la unica fuente. Si cambia un puerto o
una variable alli, se vuelve a ejecutar esto y listo.

===========================================================================
LA TRAMPA QUE HACE FALTA ESTE SCRIPT

Quadlet NO expande la sintaxis de shell. Un archivo con

    Image=docker.io/library/postgres:${POSTGRES_IMAGE_TAG:-16-alpine}

no resuelve nada: Podman intenta descargar una imagen que se llama
literalmente asi, y falla con un mensaje que no menciona la causa.

systemd solo expande variables en ExecStart y bajo condiciones estrictas.
Las directivas propias de Quadlet -Image, PublishPort, Volume- se toman tal
cual estan escritas.

Por eso este generador RESUELVE los valores al generar, leyendo el .env real.
Las unidades salen con valores concretos:

    Image=docker.io/library/postgres:16-alpine

CONSECUENCIA IMPORTANTE: si cambia el .env, hay que volver a generar. El
script lo recuerda al terminar.

===========================================================================
USO

    python3 scripts/generar_quadlet.py                 usa .env
    python3 scripts/generar_quadlet.py --env .env.rhel usa otra plantilla
    python3 scripts/generar_quadlet.py --salida /tmp/q

===========================================================================
"""

from __future__ import annotations

import argparse
import os
import re
import sys

try:
    import yaml
except ImportError:
    print("Falta PyYAML.  pip install pyyaml   (o dnf install python3-pyyaml)")
    sys.exit(1)


RE_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def leer_env(ruta: str) -> dict[str, str]:
    """Lee un archivo .env a un diccionario. Tolera comentarios y comillas."""
    valores: dict[str, str] = {}
    if not os.path.exists(ruta):
        return valores
    for linea in open(ruta, encoding="utf-8"):
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, _, valor = linea.partition("=")
        valores[clave.strip()] = valor.strip().strip('"').strip("'")
    return valores


CENTINELA = "\x00ESCAPADO\x00"


def resolver(texto, env: dict[str, str], faltantes: set[str]):
    """Sustituye ${VAR} y ${VAR:-defecto} por su valor real.

    RESPETA EL ESCAPE $$ DE COMPOSE. En un compose, $${HOSTNAME} significa
    "pasa un ${HOSTNAME} literal al contenedor, que lo expanda el shell de
    dentro". Es lo que hace el healthcheck del worker de Celery:

        celery ... inspect ping -d "celery@$${HOSTNAME}"

    Resolver eso aqui lo dejaria como  celery@  y la comprobacion de salud
    del worker fallaria siempre, sin decir por que. Se protege antes de
    sustituir y se restaura despues como un unico $.

    Una variable sin valor y sin defecto se anota en `faltantes` y se deja
    vacia. Es preferible una unidad visiblemente incompleta a una que parece
    correcta y falla al arrancar.
    """
    if not isinstance(texto, str):
        return texto

    texto = texto.replace("$$", CENTINELA)

    def _sub(m):
        nombre, defecto = m.group(1), m.group(2)
        if nombre in env and env[nombre] != "":
            return env[nombre]
        if defecto is not None:
            return defecto
        faltantes.add(nombre)
        return ""

    # En bucle, porque los defectos pueden anidar variables:
    #   ${AIRFLOW_IMAGE:-docker.io/apache/airflow:${AIRFLOW_IMAGE_TAG:-2.11.2}}
    # Una sola pasada resuelve la de fuera y deja la de dentro sin tocar, y la
    # unidad se queda con un nombre de imagen que no existe.
    for _ in range(10):
        nuevo = RE_VAR.sub(_sub, texto)
        if nuevo == texto:
            break
        texto = nuevo

    return texto.replace(CENTINELA, "$")


def comando_salud(test, env, faltantes) -> str:
    """Convierte el healthcheck de compose en el HealthCmd de Quadlet.

    Compose admite dos formas y NO son intercambiables:

        ["CMD", "curl", "--fail", "http://..."]   -> ejecutar sin shell
        ["CMD-SHELL", "pg_isready -U airflow"]    -> pasar a un shell

    Tomar solo el ultimo elemento -que es lo obvio y lo equivocado- convierte
    el primer caso en  http://...  a secas: una URL donde tendria que haber un
    comando. La comprobacion de salud fallaria siempre y el contenedor se
    reiniciaria en bucle sin explicar nada.
    """
    if isinstance(test, str):
        return resolver(test, env, faltantes)
    if not test:
        return ""
    tipo, resto = test[0], test[1:]
    if tipo == "CMD-SHELL":
        return resolver(" ".join(resto), env, faltantes)
    if tipo == "CMD":
        return resolver(" ".join(resto), env, faltantes)
    # NONE, o una lista sin prefijo: se toma tal cual
    return resolver(" ".join(test), env, faltantes)


CABECERA = """# GENERADO por scripts/generar_quadlet.py desde docker-compose.rhel.yml
# NO editar a mano: los cambios se pierden al regenerar.
# Si cambia el .env o el compose, vuelva a ejecutar el generador.
"""


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--compose", default="docker-compose.rhel.yml")
    p.add_argument("--env", default=".env")
    p.add_argument("--salida", default="rhel/quadlet")
    p.add_argument("--raiz-remota", default="%h/airflow-spark",
                   help="Ruta del proyecto en el servidor RHEL")
    args = p.parse_args()

    aqui = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    compose = os.path.join(aqui, args.compose)
    if not os.path.exists(compose):
        print(f"ERROR: no encuentro {compose}")
        return 1

    ruta_env = os.path.join(aqui, args.env)
    env = leer_env(ruta_env)
    if not env:
        alt = os.path.join(aqui, ".env.rhel")
        env = leer_env(alt)
        print(f"[aviso] {args.env} no existe o esta vacio; se usa .env.rhel")
        print("[aviso] Regenere en el servidor, con el .env real, antes de instalar.")
    print(f"[..] {len(env)} variables leidas")

    d = yaml.safe_load(open(compose, encoding="utf-8"))
    servicios, volumenes = d["services"], (d.get("volumes") or {})

    salida = os.path.join(aqui, args.salida)
    os.makedirs(salida, exist_ok=True)
    faltantes: set[str] = set()

    def escribir(nombre: str, texto: str):
        ruta = os.path.join(salida, nombre)
        with open(ruta, "w", encoding="utf-8") as f:
            f.write(texto)
        # LAS UNIDADES CONTIENEN SECRETOS RESUELTOS.
        # La cadena de conexion a PostgreSQL lleva la contrasena dentro, y no
        # hay forma de evitarlo: systemd no expande variables en Environment=.
        # Permisos 0600, y ver el aviso del final.
        os.chmod(ruta, 0o600)

    # --- red -----------------------------------------------------------------
    escribir("airflow.network", CABECERA + """
[Unit]
Description=Red interna del stack Airflow + Spark

[Network]
NetworkName=airflow-network

[Install]
WantedBy=default.target
""")

    # --- volumenes -----------------------------------------------------------
    for v in volumenes:
        escribir(f"{v}.volume", CABECERA + f"""
[Unit]
Description=Volumen {v}

[Volume]
VolumeName={v}

[Install]
WantedBy=default.target
""")

    # --- contenedores --------------------------------------------------------
    def traducir_volumen(v: str) -> str:
        partes = v.split(":")
        origen, destino = partes[0], partes[1]
        opciones = ":".join(partes[2:])
        if origen.startswith("./"):
            # Bind mount: ruta absoluta en el servidor, con etiqueta de SELinux
            opciones = opciones or "z"
            return f"Volume={args.raiz_remota}/{origen[2:]}:{destino}:{opciones}"
        # Volumen nombrado: se referencia la unidad .volume
        sufijo = f":{opciones}" if opciones else ""
        return f"Volume={origen}.volume:{destino}{sufijo}"

    generados = []
    for nombre, s in servicios.items():
        L = [CABECERA, "\n[Unit]", f"Description={nombre} - Airflow + Spark"]

        deps = s.get("depends_on") or {}
        if isinstance(deps, dict):
            deps = list(deps.keys())
        for dep in deps:
            # OJO: systemd espera a que la unidad ARRANQUE, no a que este SANA.
            # Podman 4.x (RHEL 9.4) no tiene ordenacion por salud en Quadlet.
            # Lo cubre Restart=always: si arranca antes de tiempo, reintenta.
            L.append(f"After={dep}.service")
            L.append(f"Wants={dep}.service")

        L.append("\n[Container]")
        L.append(f"ContainerName={nombre}")
        L.append("Image=" + resolver(s["image"], env, faltantes))
        L.append("Network=airflow.network")
        # El .env va igualmente: cubre las variables que los procesos de dentro
        # leen en ejecucion, no solo las que resuelve este generador.
        L.append(f"EnvironmentFile={args.raiz_remota}/.env")

        if s.get("user"):
            u = resolver(str(s["user"]), env, faltantes).split(":")
            L.append(f"User={u[0]}")
            if len(u) > 1:
                L.append(f"Group={u[1]}")
        if s.get("userns_mode"):
            L.append(f"UserNS={s['userns_mode']}")

        for k, v in (s.get("environment") or {}).items():
            L.append(f"Environment={k}=" + resolver(str(v), env, faltantes))

        for v in (s.get("volumes") or []):
            L.append(traducir_volumen(resolver(v, env, faltantes)))

        for p_ in (s.get("ports") or []):
            L.append("PublishPort=" + resolver(str(p_), env, faltantes))

        if s.get("entrypoint"):
            e = s["entrypoint"]
            L.append("Entrypoint=" + (e if isinstance(e, str) else e[0]))

        hc = s.get("healthcheck") or {}
        if hc.get("test"):
            L.append("HealthCmd=" + comando_salud(hc["test"], env, faltantes))
            if hc.get("interval"):
                L.append(f"HealthInterval={hc['interval']}")
            if hc.get("retries"):
                L.append(f"HealthRetries={hc['retries']}")

        L.append("\n[Service]")
        if nombre.endswith("-init"):
            # Los init corren una vez y terminan. Sin esto, systemd los da por
            # fallidos al salir con codigo 0.
            L.append("Type=oneshot")
            L.append("RemainAfterExit=yes")
            L.append("Restart=on-failure")
        else:
            L.append("Restart=always")
            L.append("RestartSec=15")
        # La primera arrancada migra la base de datos y puede tardar.
        L.append("TimeoutStartSec=900")

        L.append("\n[Install]")
        L.append("WantedBy=default.target\n")

        escribir(f"{nombre}.container", "\n".join(L))
        generados.append(nombre)

    # --- resumen -------------------------------------------------------------
    total = len(os.listdir(salida))
    print(f"[ok] {total} unidades en {salida}")
    print(f"     {len(generados)} contenedores, {len(volumenes)} volumenes, 1 red")

    if faltantes:
        print()
        print("[aviso] Variables sin valor ni defecto; quedaron vacias:")
        for f in sorted(faltantes):
            print(f"          {f}")
        print("        Complete el .env y vuelva a generar.")

    print()
    print("  " + "=" * 66)
    print("  ESTAS UNIDADES CONTIENEN SECRETOS EN CLARO.")
    print("  " + "=" * 66)
    print("  La cadena de conexion a PostgreSQL lleva la contrasena dentro.")
    print("  systemd no expande variables en Environment=, asi que no hay")
    print("  forma de dejarla fuera. Se escribieron con permisos 0600.")
    print()
    print("  En consecuencia:")
    print("    - NO las suba al repositorio (rhel/quadlet/ va en .gitignore)")
    print("    - NO las incluya en el paquete de traslado")
    print("    - GENERELAS EN EL SERVIDOR, con el .env real de ese servidor")
    print()
    print("  Instalar en el servidor RHEL:")
    print("      mkdir -p ~/.config/containers/systemd")
    print(f"      cp {args.salida}/* ~/.config/containers/systemd/")
    print("      systemctl --user daemon-reload")
    print("      systemctl --user start airflow-webserver")
    print()
    print("  Los valores quedaron RESUELTOS en las unidades. Si cambia el .env,")
    print("  vuelva a ejecutar este generador.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
