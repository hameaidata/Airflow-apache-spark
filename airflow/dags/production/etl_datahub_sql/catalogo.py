"""Lectura y validacion de los catalogos DAG_ODS_TABLAS y DAG_BDS_TABLAS.

QUE ES UN CATALOGO
------------------
Una Variable de Airflow con esta forma:

    {
      "CAPA": "ODS",
      "AUDITORIA": {"TABLA_LOG": "CONTROL_EJECUCIONES_SP", ...},
      "GRUPOS": [
        {"NOMBRE": "...", "ORDEN": 1, "PARALELO": true,
         "PROCESOS": [
           {"ID_PROCESO": 101, "NOMBRE_PROCESO": "...",
            "STORED_PROCEDURE": "SP_ODS_CALENDARIOS", "TIPO_PROCESO": "SP",
            "SCHEMA_ORIGEN": "DATAHUB", "TABLA_ORIGEN": "STG_FST028",
            "SCHEMA_DESTINO": "DATAHUB", "TABLA_DESTINO": "ODS_CALENDARIOS",
            "ACTIVO": "S", "PARAMETROS": [], "TIMEOUT_MINUTOS": 20, ...}
         ]}
      ]
    }

El catalogo manda: el DAG no sabe que tablas existen, las lee de aqui. Anadir
un proceso es anadir una entrada y publicar la Variable, sin tocar codigo.

POR QUE SE LEE DEL ARCHIVO Y NO DE Variable.get()
-------------------------------------------------
Esto corre en CADA pasada del parser, cada 30 segundos y por cada DAG. Un
Variable.get() ahi es una consulta a la base de metadatos por DAG y por pasada.
Se intenta primero el JSON del repositorio, que es un read de disco, y solo se
cae a la Variable si no esta -- que es la ventana de arranque en frio, cuando
nadie ha publicado todavia.

El mismo criterio y el mismo bug evitado que en dag_stg_bt2sql_carga_spark.py.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# airflow/dags/production/etl_datahub_sql/catalogo.py -> airflow/config/json
DIR_JSON = Path(__file__).resolve().parents[3] / "config" / "json"


@dataclass
class Proceso:
    """Un stored procedure a ejecutar, ya normalizado."""

    id_proceso: int
    nombre: str
    sp: str                       # nombre del SP, sin esquema
    schema_destino: str
    activo: bool
    parametros: list = field(default_factory=list)
    timeout_min: int = 60
    reintentos: int = 1
    tabla_origen: str = ""
    schema_origen: str = ""
    tabla_destino: str = ""
    depende_de_ods: list = field(default_factory=list)
    grupo: str = ""
    orden: int = 1
    paralelo: bool = True

    @property
    def sp_calificado(self) -> str:
        """DATAHUB.SP_ODS_CALENDARIOS -- con esquema, como lo pide el CALL.

        Si STORED_PROCEDURE YA trae esquema no se le antepone otro. El catalogo
        mezcla las dos formas: 'SP_BDS_TIPO_CAMBIO' a secas y
        'BDS.SP_CARGAR_CLIENTE' con esquema. Concatenando a ciegas salia
        DATAHUB.BDS.SP_CARGAR_CLIENTE -- tres partes-, que SQL Server
        interpreta como base.esquema.objeto y da "Invalid object name" sin
        decir que sobraba un prefijo.
        """
        if "." in self.sp:
            return self.sp
        return f"{self.schema_destino}.{self.sp}" if self.schema_destino else self.sp

    @property
    def destino_calificado(self) -> str:
        return (f"{self.schema_destino}.{self.tabla_destino}"
                if self.schema_destino else self.tabla_destino)

    @property
    def task_id(self) -> str:
        """Minusculas y sin caracteres raros: es un task_id de Airflow."""
        limpio = "".join(c if (c.isalnum() or c in "_-.") else "_"
                         for c in self.nombre.strip().lower())
        return limpio or f"proceso_{self.id_proceso}"


def leer_catalogo(variable: str) -> dict:
    """El documento crudo. Archivo primero, Variable como respaldo."""
    ruta = DIR_JSON / f"{variable}.json"
    try:
        return json.loads(ruta.read_text(encoding="utf-8"))
    except FileNotFoundError:
        pass
    except Exception as exc:                                     # noqa: BLE001
        logger.warning("%s existe pero no se pudo leer (%s); se prueba la "
                       "Variable.", ruta, exc)

    from airflow.models import Variable

    try:
        return Variable.get(variable, deserialize_json=True) or {}
    except Exception as exc:                                     # noqa: BLE001
        logger.warning(
            "No se pudo leer el catalogo %s ni de %s ni de la Variable (%s). "
            "El DAG se dibuja con una sola tarea que explica esto.",
            variable, ruta, exc)
        return {}


def procesos_de(variable: str, solo_activos: bool = True) -> list[Proceso]:
    """Aplana GRUPOS -> PROCESOS, ordenado por ORDEN y luego por ID_PROCESO.

    Un proceso con TIPO_PROCESO distinto de 'SP' se descarta con un aviso: este
    DAG solo sabe ejecutar procedimientos. Descartarlo en silencio seria peor
    que no dibujarlo -- nadie buscaria una tabla que no se carga si nada lo
    dice.
    """
    doc = leer_catalogo(variable)
    salida: list[Proceso] = []

    for grupo in sorted(doc.get("GRUPOS") or [],
                        key=lambda g: int(g.get("ORDEN", 99))):
        nombre_grupo = str(grupo.get("NOMBRE") or "sin_nombre")
        orden = int(grupo.get("ORDEN", 99))
        paralelo = bool(grupo.get("PARALELO", True))

        for p in grupo.get("PROCESOS") or []:
            tipo = str(p.get("TIPO_PROCESO") or "SP").strip().upper()
            if tipo != "SP":
                logger.warning(
                    "%s: el proceso %s (%s) declara TIPO_PROCESO=%r y este DAG "
                    "solo ejecuta 'SP'. NO se dibuja.",
                    variable, p.get("ID_PROCESO"), p.get("NOMBRE_PROCESO"), tipo)
                continue

            activo = str(p.get("ACTIVO", "S")).strip().upper() == "S"
            if solo_activos and not activo:
                continue

            if not p.get("STORED_PROCEDURE"):
                logger.warning(
                    "%s: el proceso %s (%s) esta activo y no declara "
                    "STORED_PROCEDURE. NO se dibuja.",
                    variable, p.get("ID_PROCESO"), p.get("NOMBRE_PROCESO"))
                continue

            salida.append(Proceso(
                id_proceso=int(p.get("ID_PROCESO", 0)),
                nombre=str(p.get("NOMBRE_PROCESO") or p["STORED_PROCEDURE"]),
                sp=str(p["STORED_PROCEDURE"]).strip(),
                schema_destino=str(p.get("SCHEMA_DESTINO") or "").strip(),
                activo=activo,
                parametros=list(p.get("PARAMETROS") or []),
                timeout_min=int(p.get("TIMEOUT_MINUTOS", 60) or 60),
                reintentos=int(p.get("REINTENTOS", 1) or 1),
                tabla_origen=str(p.get("TABLA_ORIGEN") or "").strip(),
                schema_origen=str(p.get("SCHEMA_ORIGEN") or "").strip(),
                tabla_destino=str(p.get("TABLA_DESTINO") or "").strip(),
                depende_de_ods=[int(x) for x in (p.get("DEPENDE_DE_ODS") or [])],
                grupo=nombre_grupo,
                orden=orden,
                paralelo=paralelo,
            ))

    salida.sort(key=lambda x: (x.orden, x.id_proceso))
    return salida


def tabla_bitacora(variable: str) -> str:
    doc = leer_catalogo(variable)
    aud = doc.get("AUDITORIA") or {}
    return str(aud.get("TABLA_LOG") or "CONTROL_EJECUCIONES_SP")


def inventario(variable: str) -> str:
    """Texto para el log: TODOS los procesos, activos e inactivos.

    Los dos huecos de este modelo son silenciosos -- un proceso inactivo no se
    dibuja, y uno con TIPO_PROCESO raro tampoco-, y aqui los dos salen con
    nombre. Es el mismo criterio que inventario_tablas del pipeline de Spark.
    """
    doc = leer_catalogo(variable)
    lineas = [f"CATALOGO {variable}  (capa {doc.get('CAPA', '?')}, "
              f"ambiente {doc.get('AMBIENTE', '?')})", ""]
    lineas.append(f"  {'ID':>6}  {'ACT':3}  {'PROCESO':38}  "
                  f"{'ORIGEN':24}  {'DESTINO':24}  SP")
    lineas.append("  " + "-" * 128)

    total = activos = 0
    for grupo in sorted(doc.get("GRUPOS") or [],
                        key=lambda g: int(g.get("ORDEN", 99))):
        lineas.append(f"  grupo {grupo.get('ORDEN')}: {grupo.get('NOMBRE')}"
                      f"   (paralelo={grupo.get('PARALELO', True)})")
        for p in grupo.get("PROCESOS") or []:
            total += 1
            act = str(p.get("ACTIVO", "S")).strip().upper()
            if act == "S":
                activos += 1
            origen = ".".join(x for x in (p.get("SCHEMA_ORIGEN"),
                                          p.get("TABLA_ORIGEN")) if x)
            destino = ".".join(x for x in (p.get("SCHEMA_DESTINO"),
                                           p.get("TABLA_DESTINO")) if x)
            lineas.append(
                f"  {p.get('ID_PROCESO', ''):>6}  {act:3}  "
                f"{str(p.get('NOMBRE_PROCESO', ''))[:38]:38}  "
                f"{origen[:24]:24}  {destino[:24]:24}  "
                f"{p.get('STORED_PROCEDURE', '')}")
    lineas.append("")
    lineas.append(f"  {activos} activo(s) de {total} declarado(s)")
    return "\n".join(lineas)
