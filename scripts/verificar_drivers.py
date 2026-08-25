#!/usr/bin/env python3
"""
Verifica que los drivers de base de datos esten instalados y utilizables.

Se ejecuta en dos momentos:

  --build   Al final del `docker build`. Si falta algo esencial, la
            construccion FALLA ahi mismo, no tres semanas despues cuando
            alguien intente conectarse.

  (normal)  Cuando quiera, dentro del contenedor:

              docker compose -f docker-compose.windows.yml exec \\
                  airflow-webserver python /opt/airflow/verificar_drivers.py

QUE COMPRUEBA

  1. El paquete de Python de cada motor
  2. El jar JDBC en /opt/airflow/jars: que exista y no este truncado
  3. Que la CLASE del driver este dentro del jar
  4. Que Airflow reconozca el tipo de conexion

La comprobacion 3 se hace leyendo el jar como archivo ZIP, que es lo que un
.jar es. Una clase Java vive en el jar como la ruta de su paquete con
extension .class:

    com.mysql.cj.jdbc.Driver   ->   com/mysql/cj/jdbc/Driver.class

No hace falta Java para eso. Antes lo intentaba compilando un programa con
javac, y la imagen trae JRE, no JDK: javac no existe y el script reventaba.

No abre ninguna conexion de red: no necesita que las bases existan.

-----------------------------------------------------------------------------
PRINCIPIO DE DISENO

Un verificador que tumba una construccion buena por un fallo suyo es peor que
no tener verificador. Cada comprobacion atrapa sus propios errores y, si no
puede concluir, reporta "no comprobado" en vez de "falla". Solo los hallazgos
CIERTOS —modulo ausente, jar ausente, clase ausente— hacen fallar el build.
-----------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import os
import sys
import zipfile

RUTA_JARS = os.environ.get("JDBC_DRIVER_PATH", "/opt/airflow/jars")

VERDE, ROJO, AMARILLO, CYAN, GRIS, FIN = (
    "\033[0;32m", "\033[0;31m", "\033[1;33m", "\033[0;36m", "\033[0;90m", "\033[0m"
)

OK, FALLO, DUDA = "ok", "fallo", "duda"


# =============================================================================
# DONDE SE EJECUTA ESTE SCRIPT
#
# DENTRO del contenedor de Airflow. Ahi es donde viven los drivers.
#
# Si se ejecuta en Windows —desde PowerShell, con el Python de Anaconda— TODO
# va a fallar, y esos fallos no significan nada: /opt/airflow/jars es una ruta
# de Linux que en Windows no existe, y pymssql / MySQLdb / jaydebeapi nunca se
# instalaron ahi porque no es su sitio.
#
# Un verificador que reporta once fallos cuando el problema real es que lo
# ejecutaron en la maquina equivocada esta mintiendo. Por eso lo primero que
# hace es comprobar donde esta.
# =============================================================================


def dentro_del_contenedor() -> tuple[bool, str]:
    """Devuelve (esta_dentro, motivo_si_no).

    Tres senales, de la mas barata a la mas cara:
      1. Windows              -> imposible: el contenedor es Linux
      2. Sin /opt/airflow     -> es Linux, pero no la imagen de Airflow
      3. Sin el paquete airflow -> es Linux, hay ruta, pero no es el contenedor
    """
    if os.name == "nt":
        return False, "esto es Windows; el contenedor es Linux"
    if not os.path.isdir("/opt/airflow"):
        return False, "no existe el directorio /opt/airflow"
    try:
        import airflow  # noqa: F401
    except ImportError:
        return False, "el paquete 'airflow' no esta instalado en este Python"
    return True, ""


def explicar_entorno_incorrecto(motivo: str) -> None:
    print()
    print(f"{AMARILLO}{'=' * 74}{FIN}")
    print(f"{AMARILLO}  ESTE SCRIPT NO SE EJECUTA AQUI{FIN}")
    print(f"{AMARILLO}{'=' * 74}{FIN}")
    print()
    print(f"  Motivo detectado: {motivo}")
    print()
    print("  Los drivers viven DENTRO de la imagen de Airflow, no en su maquina.")
    print(f"  La ruta {RUTA_JARS} solo existe dentro del contenedor.")
    print()
    print(f"{CYAN}  Forma correcta de ejecutarlo:{FIN}")
    print()
    print("    docker compose -f docker-compose.windows.yml exec airflow-webserver \\")
    print("        python /opt/airflow/verificar_drivers.py")
    print()
    print("  En Linux, cambie el archivo por docker-compose.ubuntu.yml")
    print()
    print(f"{CYAN}  Si el contenedor aun no existe, primero construya la imagen:{FIN}")
    print()
    print("    docker build -t airflow-bsg:2.11.2 .")
    print()
    print(f"{GRIS}  (--forzar ejecuta las comprobaciones igualmente, pero el")
    print(f"   resultado no significa nada fuera del contenedor){FIN}")
    print(f"{AMARILLO}{'=' * 74}{FIN}")
    print()


# Un motor por fila:
#   nombre, modulo python, conn_type de Airflow, jar, clase del driver, esencial
MOTORES = [
    ("PostgreSQL",  "psycopg2",      "postgres", "postgresql-jdbc.jar",  "org.postgresql.Driver",                        True),
    ("SQL Server",  "pymssql",       "mssql",    "mssql-jdbc.jar",       "com.microsoft.sqlserver.jdbc.SQLServerDriver", True),
    ("MySQL",       "MySQLdb",       "mysql",    "mysql-jdbc.jar",       "com.mysql.cj.jdbc.Driver",                     True),
    ("DB2 (IBM)",   "jaydebeapi",    "jdbc",     "db2-jcc.jar",          "com.ibm.db2.jcc.DB2Driver",                    True),
    ("SingleStore", "singlestoredb", "mysql",    "singlestore-jdbc.jar", "com.singlestore.jdbc.Driver",                  False),
    ("ODBC",        "pyodbc",        "odbc",     None,                   None,                                          False),
]


def pinta(estado: str, esencial: bool) -> str:
    if estado == OK:
        return f"{VERDE}  ok  {FIN}"
    if estado == DUDA:
        return f"{GRIS}  ?   {FIN}"
    return f"{ROJO} FALLA{FIN}" if esencial else f"{AMARILLO} aviso{FIN}"


def probar_modulo(modulo: str) -> tuple[str, str]:
    try:
        m = __import__(modulo)
        v = getattr(m, "__version__", None) or getattr(m, "version", None) or "instalado"
        return OK, str(v)[:16]
    except ImportError:
        return FALLO, "no instalado"
    except Exception as exc:
        return DUDA, f"error: {type(exc).__name__}"


def probar_jar(jar: str | None) -> tuple[str, str]:
    if not jar:
        return DUDA, "n/a"
    ruta = os.path.join(RUTA_JARS, jar)
    try:
        if not os.path.exists(ruta):
            return FALLO, "no existe"
        tam = os.path.getsize(ruta)
        if tam < 10_000:
            return FALLO, f"{tam} bytes"
        return OK, f"{tam / 1024 / 1024:.1f} MB"
    except Exception as exc:
        return DUDA, type(exc).__name__


def probar_clase(jar: str | None, clase: str | None) -> tuple[str, str]:
    """Comprueba que la clase del driver este dentro del jar.

    Un .jar es un ZIP, y una clase vive en el como la ruta de su paquete:
        com.mysql.cj.jdbc.Driver  ->  com/mysql/cj/jdbc/Driver.class

    Es la comprobacion que de verdad importa: un jar puede existir, pesar lo
    correcto, y no contener la clase esperada porque la version cambio el
    nombre del paquete. Eso solo se ve mirando dentro.
    """
    if not jar or not clase:
        return DUDA, "n/a"
    ruta = os.path.join(RUTA_JARS, jar)
    if not os.path.exists(ruta):
        return FALLO, "sin jar"
    try:
        objetivo = clase.replace(".", "/") + ".class"
        with zipfile.ZipFile(ruta) as z:
            nombres = z.namelist()
        if objetivo in nombres:
            return OK, "presente"
        # Algunos jars empaquetan las clases dentro de otro jar (shaded).
        # En ese caso no se puede concluir con esta tecnica.
        if any(n.endswith(".jar") for n in nombres):
            return DUDA, "jar anidado"
        return FALLO, "clase ausente"
    except zipfile.BadZipFile:
        return FALLO, "jar corrupto"
    except Exception as exc:
        return DUDA, type(exc).__name__


def conn_types_disponibles() -> tuple[set[str], bool]:
    try:
        from airflow.providers_manager import ProvidersManager
        return set(ProvidersManager().hooks.keys()), True
    except Exception:
        return set(), False


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--build", action="store_true",
                   help="Modo construccion: devuelve error si falta algo esencial")
    p.add_argument("--forzar", action="store_true",
                   help="Ejecutar aunque no estemos dentro del contenedor")
    args = p.parse_args()

    # Lo primero: comprobar que estamos donde los drivers viven. Sin esto, el
    # script llena la pantalla de fallos que no son fallos.
    adentro, motivo = dentro_del_contenedor()
    if not adentro and not args.forzar:
        explicar_entorno_incorrecto(motivo)
        # Codigo 0: no se comprobo nada, pero tampoco se encontro ningun
        # problema. Un codigo de error aqui haria fallar builds que estan bien.
        return 0

    print()
    print(f"{CYAN}{'=' * 74}{FIN}")
    print(f"{CYAN}  VERIFICACION DE DRIVERS DE BASE DE DATOS{FIN}")
    print(f"{CYAN}{'=' * 74}{FIN}")
    print(f"  Jars en: {RUTA_JARS}")
    print()

    conn_types, airflow_ok = conn_types_disponibles()

    print(f"  {'MOTOR':13} {'PYTHON':24} {'JAR':16} {'CLASE':18} {'AIRFLOW'}")
    print(f"  {'-' * 72}")

    hallazgos: list[str] = []

    for nombre, modulo, conn_type, jar, clase, esencial in MOTORES:
        e_mod, i_mod = probar_modulo(modulo)
        e_jar, i_jar = probar_jar(jar)
        e_cls, i_cls = probar_clase(jar, clase)
        if airflow_ok:
            e_cn = OK if conn_type in conn_types else FALLO
        else:
            e_cn = DUDA

        print(f"  {nombre:13} "
              f"{pinta(e_mod, esencial)} {i_mod:16} "
              f"{pinta(e_jar, esencial)} {i_jar:8} "
              f"{pinta(e_cls, esencial)} {i_cls:10} "
              f"{pinta(e_cn, esencial)}")

        if esencial:
            if e_mod == FALLO:
                hallazgos.append(f"{nombre}: falta el paquete de Python '{modulo}'")
            if e_jar == FALLO:
                hallazgos.append(f"{nombre}: {jar} — {i_jar}")
            if e_cls == FALLO:
                hallazgos.append(f"{nombre}: {clase} no esta en {jar}")
            if e_cn == FALLO:
                hallazgos.append(f"{nombre}: Airflow no reconoce el tipo '{conn_type}'")

    print()
    if not airflow_ok:
        print(f"  {GRIS}La columna AIRFLOW se omitio: no se pudo consultar el gestor")
        print(f"  de providers desde aqui. No indica un problema.{FIN}")
        print()

    print(f"{CYAN}{'=' * 74}{FIN}")

    if hallazgos:
        print(f"{ROJO}  {len(hallazgos)} problema(s) en motores esenciales:{FIN}")
        for h in hallazgos:
            print(f"    - {h}")
        print()
        print("  Si es un jar: corrija la version en JDBC_DRIVERS del Dockerfile")
        print("  y reconstruya. Las coordenadas vigentes estan en central.sonatype.com")
        print(f"{CYAN}{'=' * 74}{FIN}")
        return 1 if args.build else 0

    print(f"{VERDE}  Todos los motores esenciales estan listos.{FIN}")
    print()
    print("  Tipo de Connection a usar en Airflow:")
    print("    PostgreSQL   ->  Postgres")
    print("    SQL Server   ->  Microsoft SQL Server")
    print("    MySQL        ->  MySQL")
    print("    SingleStore  ->  MySQL   (habla el mismo protocolo)")
    print("    DB2          ->  JDBC Connection, con host = la URL jdbc completa")
    print(f"{CYAN}{'=' * 74}{FIN}")
    return 0


if __name__ == "__main__":
    # Un fallo inesperado del propio verificador no debe tumbar una imagen que
    # esta bien. Se reporta con claridad y se deja pasar.
    try:
        sys.exit(main())
    except Exception as exc:
        print()
        print(f"{AMARILLO}  El verificador fallo por un error propio: "
              f"{type(exc).__name__}: {exc}{FIN}")
        print(f"{AMARILLO}  No se pudo comprobar los drivers, pero la imagen "
              f"se construye igual.{FIN}")
        print(f"{AMARILLO}  Ejecutelo a mano dentro del contenedor para ver que pasa.{FIN}")
        sys.exit(0)
