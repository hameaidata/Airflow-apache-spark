"""
24 — Logs y auditoria en base de datos

Hay que separar tres cosas que suelen confundirse:

  1. LOGS DE TAREA        -> la salida de tu codigo (print, logger.info).
                             Airflow los guarda como ARCHIVOS.

  2. AUDITORIA DE AIRFLOW -> quien disparo que DAG, quien edito una Connection.
                             Ya existe: tabla `log`, visible en Browse > Audit Logs.

  3. AUDITORIA DE NEGOCIO -> "el proceso X cargo N filas de la cuenta Y a las Z".
                             Esto NO existe. Lo construyes tu. Es lo que pide
                             un auditor de SOX o de la SBS.

--- Correccion importante sobre el punto 1 ----------------------------------
Airflow NO sabe escribir los logs de tarea en una base de datos. El remote
logging soporta S3, GCS, Azure Blob, Alibaba OSS y Elasticsearch. No hay
handler de base de datos, y escribir uno propio es mala idea: cada linea de log
seria un INSERT, y un DAG hablador tumbaria la base.

Si necesitas los logs centralizados y consultables, lo estandar es
Elasticsearch. Para tener trazabilidad en base de datos, lo que se hace es el
punto 3: una tabla de auditoria propia con eventos de negocio, no lineas de log.
=============================================================================
"""

from __future__ import annotations

import json
import logging
import socket
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# Base de datos de auditoria SEPARADA de la metastore de Airflow.
#
# Nunca escribas tus tablas dentro del esquema de Airflow:
#   - `airflow db migrate` puede tocar o bloquear objetos en una actualizacion
#   - compites por conexiones con el scheduler, que es sensible a eso
#   - un auditor querra ver una base cuyo retention controlas tu
CONN_AUDITORIA = "postgres_auditoria"

DDL_AUDITORIA = """
CREATE TABLE IF NOT EXISTS auditoria_procesos (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          VARCHAR(250)  NOT NULL,
    task_id         VARCHAR(250)  NOT NULL,
    run_id          VARCHAR(250)  NOT NULL,
    intento         INTEGER       NOT NULL,
    fecha_logica    DATE          NOT NULL,
    evento          VARCHAR(50)   NOT NULL,
    severidad       VARCHAR(20)   NOT NULL DEFAULT 'INFO',
    usuario         VARCHAR(100),
    host            VARCHAR(100),
    origen          VARCHAR(200),
    destino         VARCHAR(200),
    filas_leidas    BIGINT,
    filas_escritas  BIGINT,
    duracion_seg    NUMERIC(12,3),
    detalle         JSONB,
    registrado_en   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Indices pensados para las consultas que hara un auditor
CREATE INDEX IF NOT EXISTS ix_aud_dag_fecha
    ON auditoria_procesos (dag_id, fecha_logica DESC);
CREATE INDEX IF NOT EXISTS ix_aud_evento
    ON auditoria_procesos (evento, registrado_en DESC);
CREATE INDEX IF NOT EXISTS ix_aud_run
    ON auditoria_procesos (run_id);
"""


def registrar_evento(
    context,
    evento: str,
    severidad: str = "INFO",
    origen: str | None = None,
    destino: str | None = None,
    filas_leidas: int | None = None,
    filas_escritas: int | None = None,
    detalle: dict | None = None,
) -> None:
    """Escribe una fila de auditoria.

    Se envuelve en try/except a proposito: un fallo al auditar no debe tumbar
    el proceso de negocio. Se registra la incidencia en el log de la tarea y se
    continua. La decision contraria (fallar si no se puede auditar) tambien es
    defendible en banca — depende de tu politica. Si esa es la tuya, quita el
    try/except.
    """
    ti = context["task_instance"]
    try:
        hook = PostgresHook(postgres_conn_id=CONN_AUDITORIA)
        hook.run(
            """
            INSERT INTO auditoria_procesos
                (dag_id, task_id, run_id, intento, fecha_logica, evento,
                 severidad, usuario, host, origen, destino,
                 filas_leidas, filas_escritas, duracion_seg, detalle)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            parameters=(
                ti.dag_id,
                ti.task_id,
                ti.run_id,
                ti.try_number,
                context["ds"],
                evento,
                severidad,
                context.get("dag_run").run_type if context.get("dag_run") else None,
                socket.gethostname(),
                origen,
                destino,
                filas_leidas,
                filas_escritas,
                (ti.duration if ti.duration else None),
                json.dumps(detalle or {}),
            ),
            autocommit=True,
        )
        log.info("Auditoria registrada: %s", evento)
    except Exception as exc:
        log.error("No se pudo registrar la auditoria (%s): %s", evento, exc)


# =============================================================================
# Callbacks a nivel de DAG: se disparan solos en cada fallo o exito
# =============================================================================

def al_fallar(context):
    """Se ejecuta automaticamente cuando una tarea falla, tras el ultimo intento."""
    excepcion = context.get("exception")
    registrar_evento(
        context,
        evento="TAREA_FALLIDA",
        severidad="ERROR",
        detalle={
            "excepcion": str(excepcion)[:2000],
            "tipo": type(excepcion).__name__ if excepcion else None,
            "log_url": context["task_instance"].log_url,
        },
    )


def al_tener_exito(context):
    registrar_evento(context, evento="TAREA_OK", severidad="INFO")


# =============================================================================
# Tareas de ejemplo
# =============================================================================

def crear_tablas(**context):
    PostgresHook(postgres_conn_id=CONN_AUDITORIA).run(DDL_AUDITORIA, autocommit=True)
    log.info("Tabla de auditoria lista")


def proceso_con_auditoria(**context):
    """Un proceso de negocio que deja rastro de lo que hizo."""
    registrar_evento(
        context,
        evento="CARGA_INICIADA",
        origen="sqlserver://BD_NEGOCIO/dbo.movimientos",
        destino="parquet:///datos/movimientos",
    )

    # ... el trabajo real iria aqui ...
    leidas, escritas = 125_000, 124_850

    registrar_evento(
        context,
        evento="CARGA_COMPLETADA",
        origen="sqlserver://BD_NEGOCIO/dbo.movimientos",
        destino="parquet:///datos/movimientos",
        filas_leidas=leidas,
        filas_escritas=escritas,
        detalle={
            "descartadas": leidas - escritas,
            "motivo_descarte": "clave duplicada",
            "formato": "parquet/snappy",
        },
    )

    # Los descuadres se registran como advertencia, no se ocultan.
    # Un auditor pregunta precisamente por estos casos.
    if escritas < leidas:
        registrar_evento(
            context,
            evento="DESCUADRE_CONTEO",
            severidad="WARN",
            filas_leidas=leidas,
            filas_escritas=escritas,
            detalle={"diferencia": leidas - escritas},
        )

    return {"leidas": leidas, "escritas": escritas}


def consultar_auditoria(**context):
    """Como se consulta despues. Esto es lo que le entregas al auditor."""
    hook = PostgresHook(postgres_conn_id=CONN_AUDITORIA)

    filas = hook.get_records(
        """
        SELECT dag_id, evento, severidad,
               SUM(filas_escritas) AS total_filas,
               COUNT(*)            AS veces,
               MAX(registrado_en)  AS ultimo
        FROM auditoria_procesos
        WHERE fecha_logica >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY dag_id, evento, severidad
        ORDER BY ultimo DESC
        """
    )
    for f in filas:
        log.info("  %s", f)
    return {"resumen": len(filas)}


default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ej24_auditoria_bd",
    description="Auditoria de negocio en base de datos",
    default_args=default_args,
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ejemplo", "auditoria", "compliance"],
    on_failure_callback=al_fallar,      # a nivel de DAG: aplica a todas las tareas
    on_success_callback=al_tener_exito,
) as dag:

    ddl = PythonOperator(task_id="crear_tablas", python_callable=crear_tablas)
    proceso = PythonOperator(task_id="proceso", python_callable=proceso_con_auditoria)
    consulta = PythonOperator(task_id="consultar", python_callable=consultar_auditoria)

    ddl >> proceso >> consulta


# =============================================================================
# LOGS DE TAREA CENTRALIZADOS (el punto 1 de la cabecera)
# =============================================================================
#
# Airflow guarda los logs de tarea en archivos. Para centralizarlos:
#
#   AIRFLOW__LOGGING__REMOTE_LOGGING=True
#   AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://mi-bucket/airflow-logs
#   AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=s3_logs
#
# Destinos soportados: S3, GCS, Azure Blob, Alibaba OSS, Elasticsearch.
# NO hay opcion de base de datos.
#
# Por que importa en este stack: los workers de Celery son efimeros. Si escalas
# a la baja, el contenedor desaparece Y SUS LOGS CON EL. Sin remote logging, la
# UI te mostrara "log file not found" para tareas que si se ejecutaron. En un
# entorno regulado eso es un hallazgo de auditoria.
#
# Para logs en JSON (necesario si vas a Elasticsearch):
#   AIRFLOW__LOGGING__JSON_FORMAT=True
#   AIRFLOW__LOGGING__JSON_FIELDS=asctime,filename,lineno,levelname,message
#
# Y la retencion de la propia metastore, que crece sin parar:
#   airflow db clean --clean-before-timestamp '2026-01-01'
# Con --dry-run primero, siempre. Y ojo: en banca hay que conservar el rastro
# N anos, asi que archiva antes de limpiar.
