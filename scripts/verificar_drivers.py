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
  4. Con que version de Java se compilo ese jar, y si hay una JVM que lo admita
  5. Que Airflow reconozca el tipo de conexion
  6. Que drivers ODBC estan registrados en el sistema
  7. Que jars hay de mas: los que alguien dejo en airflow/jars/

Las comprobaciones 3 y 4 se hacen leyendo el jar como archivo ZIP, que es lo
que un .jar es. Una clase Java vive dentro como la ruta de su paquete con
extension .class:

    com.mysql.cj.jdbc.Driver   ->   com/mysql/cj/jdbc/Driver.class

y los bytes 7 y 8 de ese archivo dicen para que version de Java se compilo.
No hace falta Java para averiguarlo. Antes se intentaba compilando un programa
con javac, y la imagen trae JRE, no JDK: javac no existe y el script reventaba.

No abre ninguna conexion de red: no necesita que las bases existan.

-----------------------------------------------------------------------------
PRINCIPIO DE DISENO

Un verificador que tumba una construccion buena por un fallo suyo es peor que
no tener verificador. Cada comprobacion atrapa sus propios errores y, si no
puede concluir, reporta "no comprobado" en vez de "falla". Solo los hallazgos
CIERTOS —modulo ausente, jar ausente, clase ausente, bytecode que ninguna JVM
de la imagen puede leer— hacen fallar el build.
-----------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import struct
import subprocess
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
    print(f"{AMARILLO}{'=' * 78}{FIN}")
    print(f"{AMARILLO}  ESTE SCRIPT NO SE EJECUTA AQUI{FIN}")
    print(f"{AMARILLO}{'=' * 78}{FIN}")
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
    print(f"{AMARILLO}{'=' * 78}{FIN}")
    print()


# =============================================================================
# LOS MOTORES
# =============================================================================
# Un motor por fila:
#   nombre, modulo python, conn_type de Airflow, jar, clase del driver, esencial
#
# BANTOTAL / AS400 usa jt400, NO db2-jcc. Son dos productos distintos de IBM y
# confundirlos cuesta un dia entero:
#
#   db2-jcc.jar  ->  DB2 LUW (Linux/Unix/Windows), el DB2 "normal"
#   jt400.jar    ->  DB2 for i, el que corre DENTRO del AS/400 (IBM i)
#
# El de LUW se conecta al puerto 50000 y no entiende nada de bibliotecas ni del
# sistema de archivos del IBM i. Con el driver equivocado el sintoma es un
# timeout o un "No suitable driver", nunca un mensaje que diga "ese no es".
# =============================================================================
MOTORES = [
    # nombre           modulo          conn_type  jar                     clase                                            esencial
    ("PostgreSQL",     "psycopg2",     "postgres", "postgresql-jdbc.jar",  "org.postgresql.Driver",                         True),
    ("SQL Server",     "pymssql",      "mssql",    "mssql-jdbc.jar",       "com.microsoft.sqlserver.jdbc.SQLServerDriver",  True),
    ("SQL Server jre8", None,          None,       "mssql-jdbc-jre8.jar",  "com.microsoft.sqlserver.jdbc.SQLServerDriver",  False),
    ("MySQL",          "MySQLdb",      "mysql",    "mysql-jdbc.jar",       "com.mysql.cj.jdbc.Driver",                      True),
    ("Bantotal/IBM i", "jaydebeapi",   "jdbc",     "jt400.jar",            "com.ibm.as400.access.AS400JDBCDriver",          True),
    ("DB2 LUW",        "jaydebeapi",   "jdbc",     "db2-jcc.jar",          "com.ibm.db2.jcc.DB2Driver",                     False),
    ("SingleStore",    "singlestoredb", "mysql",   "singlestore-jdbc.jar", "com.singlestore.jdbc.Driver",                   False),
    ("ODBC",           "pyodbc",       "odbc",     None,                   None,                                           False),
    ("Puente JDBC",    "jpype",        None,       None,                   None,                                           True),
]


# =============================================================================
# BYTECODE DE JAVA
# =============================================================================
# El numero "major" de la cabecera de un .class dice para que version de Java
# se compilo. Una JVM puede ejecutar bytecode de su version o anterior, nunca
# posterior.
#
# Es EL detalle del que depende todo el asunto de Java 8. El error, cuando
# ocurre, es este:
#
#     java.lang.UnsupportedClassVersionError: com/ibm/as400/access/AS400JDBCDriver
#     has been compiled by a more recent version of the Java Runtime
#     (class file version 55.0), this version ... recognizes 52.0
#
# y aparece en tiempo de EJECUCION, la primera vez que alguien se conecta, no
# al construir la imagen. Comprobarlo aqui lo adelanta semanas.
# =============================================================================
def java_de_bytecode(major: int) -> tuple[int | None, str]:
    """major del .class -> (version numerica de Java, etiqueta para mostrar).

    Desde Java 5 la cuenta es exacta: version = major - 44.
        49 -> 5      52 -> 8      55 -> 11     61 -> 17     65 -> 21

    Por debajo de 49 son las versiones 1.1 a 1.4, que cualquier JVM actual lee
    sin problema. Se devuelve None como version numerica para que la
    comparacion las trate como "siempre compatible" en vez de intentar
    convertir "1.4" a entero, que es un ValueError esperando a ocurrir.
    """
    if major < 45:
        return None, "?"
    if major < 49:
        return None, f"1.{major - 44}"
    return major - 44, str(major - 44)


def version_java_del_jar(jar: str, clase: str) -> tuple[int | None, str] | None:
    """Lee la cabecera del .class dentro del jar.

    Devuelve (version_numerica_o_None, etiqueta), o None si no se pudo leer.

    Los primeros 8 bytes de un .class son:
        4  magico   0xCAFEBABE
        2  minor
        2  major     <- este

    No hace falta ejecutar nada ni tener un JDK.
    """
    ruta = os.path.join(RUTA_JARS, jar)
    objetivo = clase.replace(".", "/") + ".class"
    try:
        with zipfile.ZipFile(ruta) as z:
            with z.open(objetivo) as f:
                cabecera = f.read(8)
        if len(cabecera) < 8 or cabecera[:4] != b"\xca\xfe\xba\xbe":
            return None
        major = struct.unpack(">H", cabecera[6:8])[0]
        return java_de_bytecode(major)
    except Exception:
        return None


def jvms_instaladas() -> list[tuple[str, int, str]]:
    """Encuentra las JVM de la imagen y devuelve (ruta, version_mayor, origen).

    Busca en tres sitios, porque cada una llega de una forma distinta:
      JAVA_HOME     la que usa Spark y la que hereda JPype por defecto
      JAVA_HOME_8   la que agrega el Dockerfile para los drivers que la piden
      /usr/lib/jvm  cualquier otra que el sistema de paquetes haya dejado
    """
    candidatas: dict[str, str] = {}

    for variable in ("JAVA_HOME", "JAVA_HOME_8", "JAVA_HOME_17"):
        valor = os.environ.get(variable)
        if valor and os.path.isdir(valor):
            candidatas[os.path.realpath(valor)] = variable

    for ruta in sorted(glob.glob("/usr/lib/jvm/*")):
        if os.path.isdir(ruta):
            candidatas.setdefault(os.path.realpath(ruta), "/usr/lib/jvm")

    encontradas = []
    for ruta, origen in candidatas.items():
        version = _version_de_jvm(ruta)
        if version:
            encontradas.append((ruta, version, origen))
    return sorted(encontradas, key=lambda x: x[1])


def _version_de_jvm(java_home: str) -> int | None:
    """Version mayor de una JVM. Primero el archivo, luego el binario.

    El archivo 'release' es texto plano y no cuesta nada:
        JAVA_VERSION="1.8.0_432"   ->   8
        JAVA_VERSION="17.0.13"     ->   17

    Si no esta, se ejecuta 'java -version', que escribe en STDERR, no en
    stdout: leer solo stdout devuelve vacio y hace creer que no hay Java.
    """
    archivo = os.path.join(java_home, "release")
    if os.path.isfile(archivo):
        try:
            with open(archivo, encoding="utf-8", errors="replace") as f:
                texto = f.read()
            m = re.search(r'JAVA_VERSION="?(?:1\.)?(\d+)', texto)
            if m:
                return int(m.group(1))
        except Exception:
            pass

    binario = os.path.join(java_home, "bin", "java")
    if not os.path.isfile(binario):
        return None
    try:
        r = subprocess.run([binario, "-version"], capture_output=True,
                           text=True, timeout=15)
        salida = (r.stderr or "") + (r.stdout or "")
        m = re.search(r'version "?(?:1\.)?(\d+)', salida)
        return int(m.group(1)) if m else None
    except Exception:
        return None


# =============================================================================
# COMPROBACIONES POR MOTOR
# =============================================================================
def pinta(estado: str, esencial: bool) -> str:
    if estado == OK:
        return f"{VERDE}  ok  {FIN}"
    if estado == DUDA:
        return f"{GRIS}  -   {FIN}"
    return f"{ROJO} FALLA{FIN}" if esencial else f"{AMARILLO} aviso{FIN}"


def probar_modulo(modulo: str | None) -> tuple[str, str]:
    if not modulo:
        return DUDA, "n/a"
    try:
        m = __import__(modulo)
        v = getattr(m, "__version__", None) or getattr(m, "version", None) or "instalado"
        # psycopg2 devuelve "2.9.11 (dt dec pq3 ext lo64)": la version util son
        # los primeros caracteres, el resto son los flags de compilacion, que
        # en la tabla solo empujan las columnas.
        partes = str(v).split()
        return OK, (partes[0][:14] if partes else "instalado")
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
            # Una descarga cortada deja un archivo pequeno, a menudo con una
            # pagina de error HTML dentro. Existe, pero no sirve.
            return FALLO, f"{tam} bytes"
        return OK, f"{tam / 1024 / 1024:.1f} MB"
    except Exception as exc:
        return DUDA, type(exc).__name__


def probar_clase(jar: str | None, clase: str | None) -> tuple[str, str]:
    """Comprueba que la clase del driver este dentro del jar.

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


# =============================================================================
# SECCIONES DEL INFORME
# =============================================================================
def seccion_java(jvms: list[tuple[str, int, str]]) -> list[str]:
    """Que JVM hay en la imagen. Devuelve los hallazgos."""
    hallazgos: list[str] = []

    print(f"{CYAN}  JAVA{FIN}")
    print(f"  {'-' * 76}")

    if not jvms:
        print(f"  {ROJO}No se encontro ninguna JVM.{FIN}")
        print("  Sin Java no funciona NINGUNA conexion JDBC: ni Bantotal, ni")
        print("  SQL Server por JDBC, ni Spark.")
        print()
        return ["No hay ninguna JVM instalada en la imagen"]

    maxima = max(v for _, v, _ in jvms)
    for ruta, version, origen in jvms:
        marca = "  (por defecto)" if os.environ.get("JAVA_HOME") and \
            os.path.realpath(os.environ["JAVA_HOME"]) == ruta else ""
        print(f"  {VERDE}ok{FIN}   Java {str(version):<4} {ruta}{GRIS}{marca}{FIN}")

    if not any(v == 8 for _, v, _ in jvms):
        # No es un fallo: casi todos los drivers modernos corren en 17. Pero si
        # alguien necesita el jt400 antiguo o el mssql-jdbc jre8, es lo primero
        # que va a preguntar.
        print()
        print(f"  {GRIS}No hay Java 8. No es un problema mientras todos los jars de")
        print(f"  abajo se compilaran para Java {maxima} o menos.{FIN}")

    print()
    return hallazgos


def seccion_motores(jvms: list[tuple[str, int, str]]) -> list[str]:
    hallazgos: list[str] = []
    conn_types, airflow_ok = conn_types_disponibles()
    java_max = max((v for _, v, _ in jvms), default=None)

    print(f"{CYAN}  MOTORES{FIN}")
    print(f"  {'-' * 76}")
    # Los anchos cuadran con los de la fila de datos de abajo: cada pinta()
    # ocupa 6 caracteres visibles, mas el espacio y el texto que la acompana.
    print(f"  {'MOTOR':16} {'PYTHON':21} {'JAR':16} {'CLASE':19} {'JAVA':19} AIRFLOW")

    for nombre, modulo, conn_type, jar, clase, esencial in MOTORES:
        e_mod, i_mod = probar_modulo(modulo)
        e_jar, i_jar = probar_jar(jar)
        e_cls, i_cls = probar_clase(jar, clase)

        # Version de Java del jar, y si alguna JVM de la imagen puede leerlo
        e_java, i_java = DUDA, "n/a"
        if jar and clase and e_cls == OK:
            leido = version_java_del_jar(jar, clase)
            if leido is None:
                e_java, i_java = DUDA, "?"
            else:
                necesita, etiqueta = leido
                if necesita is None:
                    # Bytecode anterior a Java 5: lo lee cualquier JVM.
                    e_java, i_java = OK, f"Java {etiqueta}"
                elif java_max is None:
                    e_java, i_java = DUDA, f"pide Java {etiqueta}"
                elif necesita <= java_max:
                    e_java, i_java = OK, f"Java {etiqueta}"
                else:
                    e_java, i_java = FALLO, f"pide Java {etiqueta}"

        if conn_type and airflow_ok:
            e_cn = OK if conn_type in conn_types else FALLO
        else:
            e_cn = DUDA

        print(f"  {nombre:16} "
              f"{pinta(e_mod, esencial)} {i_mod:14} "
              f"{pinta(e_jar, esencial)} {i_jar:9} "
              f"{pinta(e_cls, esencial)} {i_cls:12} "
              f"{pinta(e_java, esencial)} {i_java:12} "
              f"{pinta(e_cn, esencial)}")

        if esencial:
            if e_mod == FALLO:
                hallazgos.append(f"{nombre}: falta el paquete de Python '{modulo}'")
            if e_jar == FALLO:
                hallazgos.append(f"{nombre}: {jar} - {i_jar}")
            if e_cls == FALLO:
                hallazgos.append(f"{nombre}: {clase} no esta en {jar}")
            if e_java == FALLO:
                hallazgos.append(
                    f"{nombre}: {jar} {i_java} y la JVM mas nueva de la imagen "
                    f"es Java {java_max}. Dara UnsupportedClassVersionError al "
                    f"conectarse."
                )
            if e_cn == FALLO:
                hallazgos.append(f"{nombre}: Airflow no reconoce el tipo '{conn_type}'")

    if not airflow_ok:
        print()
        print(f"  {GRIS}La columna AIRFLOW se omitio: no se pudo consultar el gestor")
        print(f"  de providers desde aqui. No indica un problema.{FIN}")

    print()
    return hallazgos


def seccion_odbc() -> None:
    """Que drivers ODBC estan registrados.

    pyodbc instalado NO significa que haya drivers: pyodbc es solo el puente a
    unixODBC, y sin un driver registrado toda conexion falla con

        Can't open lib '...' : file not found

    que suena a un problema de permisos y no lo es. Por eso se listan aqui los
    que el sistema conoce de verdad.
    """
    print(f"{CYAN}  DRIVERS ODBC REGISTRADOS{FIN}")
    print(f"  {'-' * 76}")
    try:
        import pyodbc
        drivers = sorted(pyodbc.drivers())
    except ImportError:
        print(f"  {GRIS}pyodbc no esta instalado: no se puede consultar.{FIN}")
        print()
        return
    except Exception as exc:
        print(f"  {GRIS}No se pudo consultar: {type(exc).__name__}{FIN}")
        print()
        return

    if not drivers:
        print(f"  {AMARILLO}Ninguno.{FIN} pyodbc esta, pero no hay driver registrado:")
        print("  toda conexion ODBC fallara con \"file not found\".")
    else:
        for d in drivers:
            pista = ""
            if "IBM i" in d or "iSeries" in d or "iAccess" in d:
                pista = "  <- Bantotal / AS400"
            elif "SQL Server" in d:
                pista = "  <- SQL Server"
            print(f"  {VERDE}ok{FIN}   {d}{GRIS}{pista}{FIN}")
    print()


def seccion_jars_extra() -> None:
    """Jars que estan en la carpeta pero no los declara ningun motor.

    Casi siempre son los que alguien dejo en airflow/jars/ del repositorio, que
    el Dockerfile copia aqui DESPUES de las descargas. Listarlos importa por
    dos motivos: uno con el mismo nombre que un descargado lo reemplaza sin
    avisar, y uno con nombre distinto pero de la misma familia deja DOS
    versiones del mismo driver en el classpath, que es como se consiguen
    errores de "NoSuchMethodError" imposibles de explicar.
    """
    print(f"{CYAN}  JARS EN LA CARPETA{FIN}")
    print(f"  {'-' * 76}")

    try:
        presentes = sorted(os.path.basename(p)
                           for p in glob.glob(os.path.join(RUTA_JARS, "*.jar")))
    except Exception as exc:
        print(f"  {GRIS}No se pudo listar {RUTA_JARS}: {type(exc).__name__}{FIN}")
        print()
        return

    declarados = {jar for _, _, _, jar, _, _ in MOTORES if jar}
    extra = [j for j in presentes if j not in declarados]

    print(f"  {len(presentes)} jar(s) en {RUTA_JARS}")

    if not extra:
        print(f"  {GRIS}Ninguno sin declarar. Todo lo que hay lo pone el Dockerfile.{FIN}")
        print()
        return

    print()
    print(f"  {AMARILLO}Sin declarar en este script ({len(extra)}):{FIN}")
    for jar in extra:
        try:
            mb = os.path.getsize(os.path.join(RUTA_JARS, jar)) / 1024 / 1024
            print(f"    {jar:<36} {mb:>6.1f} MB")
        except Exception:
            print(f"    {jar}")

    # Duplicados de familia: dos jars cuyo nombre empieza igual hasta el primer
    # numero. jt400.jar y jt400-11.2.jar son la misma biblioteca dos veces.
    def familia(nombre: str) -> str:
        return re.split(r"[-_]?\d", nombre, maxsplit=1)[0].lower()

    familias: dict[str, list[str]] = {}
    for jar in presentes:
        familias.setdefault(familia(jar), []).append(jar)

    repetidas = {f: js for f, js in familias.items() if len(js) > 1}
    if repetidas:
        print()
        print(f"  {AMARILLO}Posibles versiones duplicadas del mismo driver:{FIN}")
        for _, jars in sorted(repetidas.items()):
            print(f"    {', '.join(jars)}")
        print(f"  {GRIS}Dos versiones de la misma biblioteca en el classpath dan")
        print(f"  NoSuchMethodError en tiempo de ejecucion, y cual gana depende del")
        print(f"  orden en que la JVM las lea. Deje solo una.{FIN}")
    print()


def seccion_spark() -> None:
    """Recordatorio de que Spark tiene su propia copia de los jars.

    Este script corre en el contenedor de Airflow y no puede ver los de Spark.
    Es justo el fallo que mas despista: el job arranca bien en el driver y
    muere al llegar al executor con "No suitable driver", porque /opt/airflow
    no existe en los contenedores de Spark.
    """
    print(f"{CYAN}  SPARK{FIN}")
    print(f"  {'-' * 76}")
    print(f"  {GRIS}Los contenedores de Spark tienen su PROPIA copia de los jars, en")
    print(f"  $SPARK_HOME/jars. Este script no los ve desde aqui. Para comprobarlos:{FIN}")
    print()
    print("    docker compose -f docker-compose.windows.yml exec spark-master \\")
    print("        ls -la /opt/spark/jars/ | grep -E \"jt400|jdbc|jcc\"")
    print()


# =============================================================================
def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--build", action="store_true",
                   help="Modo construccion: devuelve error si falta algo esencial")
    p.add_argument("--forzar", action="store_true",
                   help="Ejecutar aunque no estemos dentro del contenedor")
    p.add_argument("--breve", action="store_true",
                   help="Solo la tabla de motores")
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
    print(f"{CYAN}{'=' * 78}{FIN}")
    print(f"{CYAN}  VERIFICACION DE DRIVERS DE BASE DE DATOS{FIN}")
    print(f"{CYAN}{'=' * 78}{FIN}")
    print(f"  Jars en: {RUTA_JARS}")
    print()

    jvms = jvms_instaladas()

    hallazgos: list[str] = []
    if not args.breve:
        hallazgos += seccion_java(jvms)
    hallazgos += seccion_motores(jvms)
    if not args.breve:
        seccion_odbc()
        seccion_jars_extra()
        seccion_spark()

    print(f"{CYAN}{'=' * 78}{FIN}")

    if hallazgos:
        print(f"{ROJO}  {len(hallazgos)} problema(s) en motores esenciales:{FIN}")
        for h in hallazgos:
            print(f"    - {h}")
        print()
        print("  Si es un jar: corrija la version en JDBC_DRIVERS del Dockerfile")
        print("  y reconstruya. Las coordenadas vigentes estan en central.sonatype.com")
        print("  Si es la version de Java: use el jar de la variante jre8, o agregue")
        print("  una JVM mas nueva a la imagen.")
        print(f"{CYAN}{'=' * 78}{FIN}")
        return 1 if args.build else 0

    print(f"{VERDE}  Todos los motores esenciales estan listos.{FIN}")
    print()
    print("  Tipo de Connection a usar en Airflow:")
    print("    PostgreSQL     ->  Postgres")
    print("    SQL Server     ->  Microsoft SQL Server   (o Generic, si va por JDBC)")
    print("    MySQL          ->  MySQL")
    print("    SingleStore    ->  MySQL   (habla el mismo protocolo)")
    print("    Bantotal/IBM i ->  Generic, con el host y el puerto 8471")
    print("    DB2 LUW        ->  JDBC Connection, con host = la URL jdbc completa")
    print(f"{CYAN}{'=' * 78}{FIN}")
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
