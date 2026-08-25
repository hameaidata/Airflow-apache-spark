"""
Registro del plugin en Airflow.

Sin este archivo los operadores funcionan igual (Airflow anade la carpeta
plugins/ al sys.path), pero NO aparecen en Admin -> Plugins.

Registrarlos sirve para dos cosas concretas:

  - Que un auditor pueda ver, desde la interfaz, que codigo propio corre en la
    plataforma. "Ninguno" y "no se sabe" no son la misma respuesta.
  - Que el equipo descubra los operadores disponibles sin leer el repositorio.

Tras cambiar este archivo hay que reiniciar el webserver y el scheduler: los
plugins se cargan al arrancar, no cada 30 segundos como los DAGs.

    docker compose -f docker-compose.windows.yml restart airflow-webserver airflow-scheduler
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from operators.sp_operator import ConsultarBitacoraOperator, EjecutarSPOperator


class BsgPlugin(AirflowPlugin):
    """Operadores propios del area de datos."""

    name = "bsg_datos"

    operators = [
        EjecutarSPOperator,
        ConsultarBitacoraOperator,
    ]

    # Otros puntos de extension disponibles, por si hacen falta mas adelante:
    #   hooks              conexiones a sistemas propios
    #   macros             funciones usables dentro de plantillas Jinja
    #   flask_blueprints   paginas propias dentro de la interfaz
    #   appbuilder_views   entradas de menu propias
    #   listeners          reaccionar a eventos del ciclo de vida
