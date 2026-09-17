#!/usr/bin/env python3
# ============================================================================
# SCRIPT DE VERIFICACIÓN DE DRIVERS - COMPLETO
# ============================================================================
# Verifica que TODOS los drivers JDBC, ODBC y Python están instalados
# correctamente para conectarse a SQL Server y BT
#
# USO:
#   python verificar_drivers_completo.py          # Normal
#   python verificar_drivers_completo.py --build  # Durante docker build
#
# SALIDA:
#   - Tabla de estado de cada driver
#   - Avisos si algo falta
#   - Recomendaciones de acción
# ============================================================================

import os
import sys
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple

class DriverVerification:
    """Verifica instalación completa de drivers"""

    def __init__(self, is_build=False):
        self.is_build = is_build
        self.jdbc_path = Path("/opt/airflow/jars")
        self.results = {}

    def check_jdbc_drivers(self) -> Dict[str, bool]:
        """Verifica drivers JDBC"""
        print("\n" + "="*70)
        print("JDBC DRIVERS")
        print("="*70)

        expected_jars = {
            "mssql-jdbc.jar": "SQL Server",
            "db2-jcc.jar": "IBM DB2",
            "mysql-jdbc.jar": "MySQL",
            "postgresql-jdbc.jar": "PostgreSQL",
            "singlestore-jdbc.jar": "SingleStore",
        }

        results = {}
        for jar_name, description in expected_jars.items():
            jar_path = self.jdbc_path / jar_name
            exists = jar_path.exists()
            results[jar_name] = exists

            status = "✅ OK" if exists else "❌ FALTA"
            size = f"({jar_path.stat().st_size / 1024 / 1024:.1f} MB)" if exists else ""

            print(f"{status}  {description:20} ({jar_name:25}) {size}")

        return results

    def check_odbc_drivers(self) -> Dict[str, bool]:
        """Verifica drivers ODBC"""
        print("\n" + "="*70)
        print("ODBC DRIVERS")
        print("="*70)

        results = {}

        # Verificar ODBC Driver 18 para SQL Server
        try:
            result = subprocess.run(
                ["odbcinst", "-q"],
                capture_output=True,
                text=True,
                timeout=5
            )

            if "ODBC Driver 18 for SQL Server" in result.stdout:
                print("✅ OK  ODBC Driver 18 for SQL Server (Instalado)")
                results["ODBC Driver 18"] = True
            else:
                print("❌ FALTA  ODBC Driver 18 for SQL Server")
                results["ODBC Driver 18"] = False

        except Exception as e:
            print(f"⚠️  AVISO  No se pudo verificar ODBC: {str(e)}")
            results["ODBC Driver 18"] = False

        # Verificar Kerberos
        try:
            krb5_path = Path("/etc/krb5.conf")
            if krb5_path.exists():
                print("✅ OK  Kerberos (krb5.conf configurado)")
                results["Kerberos"] = True
            else:
                print("⚠️  AVISO  Kerberos no configurado (opcional si no usan AD)")
                results["Kerberos"] = False

        except Exception as e:
            print(f"⚠️  AVISO  Error verificando Kerberos: {str(e)}")
            results["Kerberos"] = False

        return results

    def check_python_packages(self) -> Dict[str, bool]:
        """Verifica librerías Python"""
        print("\n" + "="*70)
        print("LIBRERÍAS PYTHON")
        print("="*70)

        packages = {
            "jaydebeapi": "JDBC bridge",
            "pyodbc": "ODBC bridge",
            "pymssql": "SQL Server nativo",
            "psycopg2": "PostgreSQL",
            "mysql": "MySQL",
            "singlestoredb": "SingleStore",
            "ibm_db": "IBM DB2",
            "pyspark": "Apache Spark",
            "pandas": "Data manipulation",
            "pyarrow": "Columnar format",
            "sqlalchemy": "SQL toolkit",
            "tenacity": "Reintentos automáticos",
            "retry": "Reintentos",
        }

        results = {}
        for package, description in packages.items():
            try:
                __import__(package)
                print(f"✅ OK  {description:30} ({package})")
                results[package] = True
            except ImportError:
                # Algunos packages tienen nombre diferente
                if package == "mysql":
                    try:
                        import mysql.connector
                        print(f"✅ OK  {description:30} ({package})")
                        results[package] = True
                    except:
                        print(f"❌ FALTA  {description:30} ({package})")
                        results[package] = False
                elif package == "singlestoredb":
                    print(f"⚠️  AVISO  {description:30} (opcional)")
                    results[package] = False
                elif package == "ibm_db":
                    print(f"⚠️  AVISO  {description:30} (opcional si usas JDBC)")
                    results[package] = False
                else:
                    print(f"❌ FALTA  {description:30} ({package})")
                    results[package] = False

        return results

    def check_environment_variables(self) -> Dict[str, str]:
        """Verifica variables de entorno"""
        print("\n" + "="*70)
        print("VARIABLES DE ENTORNO")
        print("="*70)

        env_vars = {
            "JDBC_DRIVER_PATH": "Ruta de drivers JDBC",
            "JAVA_HOME": "Directorio de Java",
            "SPARK_HOME": "Directorio de Spark",
            "SQLSERVER_HOST": "Host SQL Server",
            "BT_HOST": "Host de BT",
        }

        results = {}
        for var, description in env_vars.items():
            value = os.getenv(var, "(no configurado)")

            if var in ["SQLSERVER_HOST", "BT_HOST"]:
                # Estos son opcionales en tiempo de build
                status = "⚠️  OPCIONAL" if value == "(no configurado)" else "✅ OK"
            else:
                status = "✅ OK" if value != "(no configurado)" else "❌ FALTA"

            print(f"{status}  {description:30} {var:25} = {value}")
            results[var] = value

        return results

    def verify_connectivity_test_files(self) -> Dict[str, bool]:
        """Verifica que los scripts de test estén listos"""
        print("\n" + "="*70)
        print("SCRIPTS DE TEST (Para conectividad)")
        print("="*70)

        test_files = {
            "/opt/airflow/dags/test_sql_server.py": "Test SQL Server",
            "/opt/airflow/dags/test_bt.py": "Test BT",
        }

        results = {}
        for file_path, description in test_files.items():
            path = Path(file_path)
            exists = path.exists()
            status = "✅ OK" if exists else "❌ NO ENCONTRADO (será creado después)"

            print(f"{status}  {description:30} ({file_path})")
            results[description] = exists

        return results

    def print_summary(self):
        """Imprime resumen final"""
        print("\n" + "="*70)
        print("RESUMEN")
        print("="*70)

        jdbc_ok = all([v for v in self.results.get("jdbc", {}).values()])
        odbc_ok = self.results.get("odbc", {}).get("ODBC Driver 18", False)
        python_ok = all([v for k, v in self.results.get("python", {}).items()
                        if k not in ["singlestoredb", "ibm_db"]])

        print(f"JDBC Drivers:     {'✅ COMPLETO' if jdbc_ok else '❌ INCOMPLETO'}")
        print(f"ODBC Drivers:     {'✅ PRESENTE' if odbc_ok else '⚠️  OPCIONAL'}")
        print(f"Librerías Python: {'✅ COMPLETO' if python_ok else '❌ INCOMPLETO'}")

        if not (jdbc_ok and python_ok):
            print("\n⚠️  ADVERTENCIA: Algunos componentes esenciales faltan.")
            print("   Los jobs de Spark/Airflow pueden no funcionar correctamente.")
        else:
            print("\n✅ TODOS LOS DRIVERS ESENCIALES ESTÁN INSTALADOS")
            print("   Listo para conectarse a SQL Server y BT")

    def print_recommendations(self):
        """Imprime recomendaciones"""
        print("\n" + "="*70)
        print("RECOMENDACIONES")
        print("="*70)

        recommendations = [
            "1. ANTES DE USAR:",
            "   - Configura variables de entorno en .env (SQLSERVER_HOST, BT_HOST, etc.)",
            "   - Verifica conectividad de red a SQL Server y BT",
            "   - Crea conexiones en Airflow > Admin > Connections",
            "",
            "2. PARA PROBAR CONECTIVIDAD:",
            "   - Ejecuta: docker exec airflow-webserver python /opt/airflow/dags/test_sql_server.py",
            "   - Ejecuta: docker exec airflow-webserver python /opt/airflow/dags/test_bt.py",
            "",
            "3. SI ALGO FALLA:",
            "   - Revisa logs: docker compose logs airflow-webserver",
            "   - Verifica drivers: docker exec airflow-webserver ls -lh /opt/airflow/jars/",
            "   - Prueba JDBC: docker exec airflow-webserver java -version",
            "",
            "4. PARA USAR CON SQL SERVER:",
            "   - Mínimo: SQLSERVER_HOST, SQLSERVER_USER, SQLSERVER_PASSWORD",
            "   - Opcional: Kerberos para Windows AD",
            "",
            "5. PARA USAR CON BT:",
            "   - Mínimo: BT_HOST, BT_USER, BT_PASSWORD",
            "   - Verifica: Licencia válida de BT",
            "   - Verifica: Driver JDBC específico de BT instalado",
        ]

        for rec in recommendations:
            print(rec)

    def run(self):
        """Ejecuta todas las verificaciones"""
        print("\n" + "="*80)
        print("VERIFICACIÓN COMPLETA DE DRIVERS - BT + SQL SERVER")
        print("="*80)

        self.results["jdbc"] = self.check_jdbc_drivers()
        self.results["odbc"] = self.check_odbc_drivers()
        self.results["python"] = self.check_python_packages()
        self.results["env"] = self.check_environment_variables()
        self.results["test_files"] = self.verify_connectivity_test_files()

        self.print_summary()

        if not self.is_build:
            self.print_recommendations()

        print("\n" + "="*80)

        # Retornar 0 si todo está OK, 1 si falta algo esencial
        jdbc_ok = all([v for v in self.results.get("jdbc", {}).values()])
        python_ok = all([v for k, v in self.results.get("python", {}).items()
                        if k not in ["singlestoredb", "ibm_db"]])

        return 0 if (jdbc_ok and python_ok) else 1


if __name__ == "__main__":
    is_build = "--build" in sys.argv

    verifier = DriverVerification(is_build=is_build)
    exit_code = verifier.run()

    sys.exit(exit_code)
