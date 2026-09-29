"""
Operador propio: ejecutar un procedimiento almacenado con bitacora en la
misma base de datos donde se ejecuta.

===========================================================================
QUE HACE

  1. Establece la conexion usando una Connection de Airflow
  2. Se asegura de que exista la tabla de bitacora EN ESA MISMA BASE
  3. Registra el inicio de la ejecucion
  4. Ejecuta el procedimiento almacenado que se le indique por nombre
  5. Registra el fin: duracion, filas afectadas, estado, error si lo hubo

El nombre del procedimiento llega como parametro. No esta fijo en el codigo:
se puede pasar al disparar el DAG, o venir de una Variable, o de una lista.

===========================================================================
POR QUE LA BITACORA VA EN LA BASE DE DESTINO Y NO EN AIRFLOW

Airflow ya registra si una tarea termino bien o mal. Lo que NO registra es lo
que le importa al DBA y al auditor: que procedimiento se ejecuto, sobre que
base, cuanto tardo, cuantas filas movio.

Poniendo la bitacora en la misma base:

  - El DBA la consulta con sus propias herramientas, sin entrar a Airflow
  - Queda dentro del mismo respaldo que los datos que describe
  - Si Airflow se reinstala, el historial no se pierde
  - Se puede unir por SQL con las tablas de negocio

===========================================================================
MOTORES SOPORTADOS

  postgres  probado
  mssql     sintaxis lista, sin probar (requiere la imagen propia)
  odbc      sintaxis lista, sin probar
  jdbc/db2  sintaxis lista, sin probar
  oracle    sintaxis lista, sin probar

Cada motor difiere en tres cosas: como se llama a un procedimiento, que
marcador de posicion usa para los parametros, y como se declara una columna
autoincremental. Todo eso esta en DIALECTOS, abajo.
===========================================================================
"""

from __future__ import annotations

import json
import time
from typing import Any

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.utils.context import Context


# ===========================================================================
# DIALECTOS — lo unico que cambia entre motores
# ===========================================================================

DIALECTOS: dict[str, dict[str, Any]] = {

    "postgres": {
        "marcador": "%s",
        "llamada": "CALL {sp}({args})",
        "llamada_sin_args": "CALL {sp}()",
        "ddl": """
            CREATE TABLE IF NOT EXISTS {tabla} (
                id              BIGSERIAL PRIMARY KEY,
                dag_id          VARCHAR(250)  NOT NULL,
                task_id         VARCHAR(250)  NOT NULL,
                run_id          VARCHAR(250)  NOT NULL,
                intento         INTEGER       NOT NULL,
                fecha_proceso   DATE,
                procedimiento   VARCHAR(400)  NOT NULL,
                parametros      TEXT,
                estado          VARCHAR(20)   NOT NULL,
                iniciado_en     TIMESTAMP     NOT NULL,
                finalizado_en   TIMESTAMP,
                duracion_seg    NUMERIC(12,3),
                filas_afectadas BIGINT,
                mensaje_error   TEXT,
                ejecutado_por   VARCHAR(100)
            )
        """,
        "indice": "CREATE INDEX IF NOT EXISTS ix_{corto}_dag ON {tabla} (dag_id, iniciado_en DESC)",
        "id_insertado": "RETURNING id",
    },

    "mssql": {
        "marcador": "%s",
        "llamada": "SET NOCOUNT ON; EXEC {sp} {args}",
        "llamada_sin_args": "SET NOCOUNT ON; EXEC {sp}",
        "ddl": """
            IF OBJECT_ID('{tabla}', 'U') IS NULL
            CREATE TABLE {tabla} (
                id              BIGINT IDENTITY(1,1) PRIMARY KEY,
                dag_id          VARCHAR(250)  NOT NULL,
                task_id         VARCHAR(250)  NOT NULL,
                run_id          VARCHAR(250)  NOT NULL,
                intento         INT           NOT NULL,
                fecha_proceso   DATE,
                procedimiento   VARCHAR(400)  NOT NULL,
                parametros      VARCHAR(MAX),
                estado          VARCHAR(20)   NOT NULL,
                iniciado_en     DATETIME2     NOT NULL,
                finalizado_en   DATETIME2,
                duracion_seg    DECIMAL(12,3),
                filas_afectadas BIGINT,
                mensaje_error   VARCHAR(MAX),
                ejecutado_por   VARCHAR(100)
            )
        """,
        "indice": None,
        "id_insertado": "; SELECT SCOPE_IDENTITY() AS id",
    },

    "odbc": {
        "marcador": "?",
        "llamada": "{{CALL {sp}({args})}}",
        "llamada_sin_args": "{{CALL {sp}}}",
        "ddl": None,     # se reutiliza el de mssql; ver _dialecto()
        "indice": None,
        "id_insertado": None,
    },

    "jdbc": {          # DB2 y otros por JDBC
        "marcador": "?",
        "llamada": "CALL {sp}({args})",
        "llamada_sin_args": "CALL {sp}()",
        "ddl": """
            CREATE TABLE {tabla} (
                id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                dag_id          VARCHAR(250)  NOT NULL,
                task_id         VARCHAR(250)  NOT NULL,
                run_id          VARCHAR(250)  NOT NULL,
                intento         INTEGER       NOT NULL,
                fecha_proceso   DATE,
                procedimiento   VARCHAR(400)  NOT NULL,
                parametros      CLOB,
                estado          VARCHAR(20)   NOT NULL,
                iniciado_en     TIMESTAMP     NOT NULL,
                finalizado_en   TIMESTAMP,
                duracion_seg    DECIMAL(12,3),
                filas_afectadas BIGINT,
                mensaje_error   CLOB,
                ejecutado_por   VARCHAR(100)
            )
        """,
        "indice": None,
        "id_insertado": None,
    },

    "oracle": {
        "marcador": ":1",
        "llamada": "BEGIN {sp}({args}); END;",
        "llamada_sin_args": "BEGIN {sp}; END;",
        "ddl": None,
        "indice": None,
        "id_insertado": None,
    },
}

# El conn_type de SQL Server por ODBC comparte la sintaxis de tabla con mssql
DIALECTOS["odbc"]["ddl"] = DIALECTOS["mssql"]["ddl"]


# ===========================================================================

class EjecutarSPOperator(BaseOperator):
    """Ejecuta un procedimiento almacenado y lo registra en la base de destino.

    :param conn_id:        Connection de Airflow. Define motor, servidor y credenciales
    :param nombre_sp:      Nombre del procedimiento, con esquema. Ej: 'dbo.sp_cargar'
    :param parametros:     Lista o tupla de parametros posicionales
    :param tabla_bitacora: Tabla donde registrar. Se crea sola si no existe
    :param crear_bitacora: False si el DBA prefiere crearla el mismo
    :param registrar:      False para ejecutar sin dejar rastro en la bitacora
    """

    # Estos campos aceptan plantillas Jinja: se puede pasar
    # nombre_sp="{{ params.procedimiento }}" y se resuelve al ejecutar.
    template_fields = ("nombre_sp", "parametros", "tabla_bitacora")

    ui_color = "#2E7D32"
    ui_fgcolor = "#FFFFFF"

    def __init__(
        self,
        *,
        conn_id: str,
        nombre_sp: str,
        parametros: list | tuple | None = None,
        tabla_bitacora: str = "airflow_bitacora_sp",
        crear_bitacora: bool = True,
        registrar: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.nombre_sp = nombre_sp
        self.parametros = parametros
        self.tabla_bitacora = tabla_bitacora
        self.crear_bitacora = crear_bitacora
        self.registrar = registrar

    # ---------------------------------------------------------------- #

    def _dialecto(self, conn_type: str) -> dict:
        d = DIALECTOS.get(conn_type)
        if d is None:
            raise AirflowException(
                f"El motor '{conn_type}' no esta soportado por este operador. "
                f"Soportados: {', '.join(sorted(DIALECTOS))}. "
                f"Para anadirlo, agregue una entrada en DIALECTOS con su sintaxis "
                f"de CALL, su marcador de parametros y su DDL."
            )
        return d

    def _armar_llamada(self, dial: dict) -> tuple[str, tuple]:
        """Construye la sentencia de llamada al procedimiento.

        Los parametros NUNCA se concatenan en el texto: van como marcadores.
        Concatenarlos permitiria inyeccion a traves del valor de un parametro.
        """
        params = tuple(self.parametros or ())
        if not params:
            return dial["llamada_sin_args"].format(sp=self.nombre_sp), ()

        if dial["marcador"] == ":1":          # Oracle numera los marcadores
            marcadores = ", ".join(f":{i + 1}" for i in range(len(params)))
        else:
            marcadores = ", ".join([dial["marcador"]] * len(params))

        return dial["llamada"].format(sp=self.nombre_sp, args=marcadores), params

    def _asegurar_bitacora(self, hook, dial: dict) -> None:
        if not self.crear_bitacora:
            return
        ddl = dial.get("ddl")
        if not ddl:
            self.log.warning(
                "No hay DDL de bitacora para este motor. Cree la tabla '%s' a mano "
                "o use crear_bitacora=False.", self.tabla_bitacora
            )
            return
        try:
            hook.run(ddl.format(tabla=self.tabla_bitacora), autocommit=True)
            if dial.get("indice"):
                corto = self.tabla_bitacora.replace(".", "_")[-25:]
                hook.run(dial["indice"].format(tabla=self.tabla_bitacora, corto=corto),
                         autocommit=True)
            self.log.info("Tabla de bitacora lista: %s", self.tabla_bitacora)
        except Exception as exc:
            # Que falle la bitacora no debe impedir el proceso de negocio.
            # Si su politica es la contraria, cambie esto por un raise.
            self.log.warning("No se pudo preparar la bitacora: %s", exc)

    def _registrar_inicio(self, hook, dial: dict, context: Context) -> None:
        if not self.registrar:
            return
        ti = context["task_instance"]
        m = dial["marcador"]
        marcadores = ", ".join([m] * 10) if m != ":1" else ", ".join(f":{i+1}" for i in range(10))
        sql = f"""
            INSERT INTO {self.tabla_bitacora}
                (dag_id, task_id, run_id, intento, fecha_proceso,
                 procedimiento, parametros, estado, iniciado_en, ejecutado_por)
            VALUES ({marcadores})
        """
        try:
            hook.run(sql, parameters=(
                ti.dag_id, ti.task_id, ti.run_id, ti.try_number,
                context["ds"], self.nombre_sp,
                json.dumps(list(self.parametros or []), ensure_ascii=False),
                "EN_PROCESO", context["ts"], f"airflow:{self.conn_id}",
            ), autocommit=True)
        except Exception as exc:
            self.log.warning("No se pudo registrar el inicio: %s", exc)

    def _registrar_fin(self, hook, dial: dict, context: Context,
                       estado: str, duracion: float,
                       filas: int | None, error: str | None) -> None:
        if not self.registrar:
            return
        ti = context["task_instance"]
        m = dial["marcador"]
        if m == ":1":
            s = "finalizado_en = :1, duracion_seg = :2, filas_afectadas = :3, estado = :4, mensaje_error = :5"
            w = "run_id = :6 AND task_id = :7 AND intento = :8 AND estado = :9"
        else:
            s = (f"finalizado_en = {m}, duracion_seg = {m}, filas_afectadas = {m}, "
                 f"estado = {m}, mensaje_error = {m}")
            w = f"run_id = {m} AND task_id = {m} AND intento = {m} AND estado = {m}"
        sql = f"UPDATE {self.tabla_bitacora} SET {s} WHERE {w}"
        try:
            hook.run(sql, parameters=(
                context["ts"], round(duracion, 3), filas, estado,
                (error or "")[:4000],
                ti.run_id, ti.task_id, ti.try_number, "EN_PROCESO",
            ), autocommit=True)
        except Exception as exc:
            self.log.warning("No se pudo registrar el fin: %s", exc)

    # ---------------------------------------------------------------- #

    def execute(self, context: Context) -> dict:
        # --- 1. ESTABLECER LA CONEXION -----------------------------------
        # Aqui es donde se resuelve todo: servidor, credenciales y motor salen
        # de la Connection. En el DAG solo viaja el nombre 'conn_id'.
        conn = BaseHook.get_connection(self.conn_id)
        dial = self._dialecto(conn.conn_type)
        hook = conn.get_hook()

        self.log.info("Conexion establecida | motor=%s servidor=%s base=%s",
                      conn.conn_type, conn.host, conn.schema)
        self.log.info("Procedimiento a ejecutar: %s", self.nombre_sp)
        if self.parametros:
            self.log.info("Parametros: %s", list(self.parametros))

        # --- 2. BITACORA -------------------------------------------------
        self._asegurar_bitacora(hook, dial)
        self._registrar_inicio(hook, dial, context)

        # --- 3. EJECUTAR -------------------------------------------------
        sentencia, params = self._armar_llamada(dial)
        self.log.info("Sentencia: %s", sentencia)

        inicio = time.monotonic()
        filas: int | None = None
        try:
            conexion = hook.get_conn()
            try:
                cur = conexion.cursor()
                cur.execute(sentencia, params) if params else cur.execute(sentencia)

                # rowcount es -1 cuando el motor no lo informa. Se normaliza a
                # None para no guardar un -1 sin significado en la bitacora.
                filas = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else None

                # Algunos procedimientos devuelven filas y otros no. Intentarlo
                # y tolerar el fallo es mas simple que adivinar cual es cual.
                try:
                    muestra = cur.fetchmany(5)
                    if muestra:
                        self.log.info("El procedimiento devolvio filas. Primeras %d:", len(muestra))
                        for f in muestra:
                            self.log.info("   %s", f)
                except Exception:
                    pass

                conexion.commit()
            finally:
                conexion.close()

            duracion = time.monotonic() - inicio
            self.log.info("Completado en %.2f s | filas afectadas: %s", duracion, filas)
            self._registrar_fin(hook, dial, context, "OK", duracion, filas, None)

            return {
                "procedimiento": self.nombre_sp,
                "estado": "OK",
                "duracion_seg": round(duracion, 3),
                "filas_afectadas": filas,
            }

        except Exception as exc:
            duracion = time.monotonic() - inicio
            self.log.error("Fallo tras %.2f s: %s", duracion, exc)
            self._registrar_fin(hook, dial, context, "ERROR", duracion, None, str(exc))
            raise


# ===========================================================================

class ConsultarBitacoraOperator(BaseOperator):
    """Lee la bitacora y devuelve un resumen de la ejecucion actual.

    Util como ultima tarea del DAG: deja en el log de Airflow un resumen de
    todo lo que se ejecuto contra la base externa.
    """

    template_fields = ("tabla_bitacora",)
    ui_color = "#1565C0"
    ui_fgcolor = "#FFFFFF"

    def __init__(self, *, conn_id: str,
                 tabla_bitacora: str = "airflow_bitacora_sp", **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.tabla_bitacora = tabla_bitacora

    def execute(self, context: Context) -> dict:
        conn = BaseHook.get_connection(self.conn_id)
        dial = DIALECTOS.get(conn.conn_type, DIALECTOS["postgres"])
        hook = conn.get_hook()
        m = dial["marcador"] if dial["marcador"] != ":1" else ":1"

        filas = hook.get_records(
            f"""SELECT procedimiento, estado, duracion_seg, filas_afectadas
                FROM {self.tabla_bitacora}
                WHERE run_id = {m}
                ORDER BY iniciado_en""",
            parameters=(context["dag_run"].run_id,),
        )

        print("=" * 68)
        print(f"  BITACORA DE ESTA EJECUCION — {context['ds']}")
        print("=" * 68)
        ok = err = 0
        for proc, estado, dur, fils in filas:
            marca = "OK " if estado == "OK" else "ERR"
            ok, err = (ok + 1, err) if estado == "OK" else (ok, err + 1)
            print(f"  [{marca}] {str(proc):40} {dur or 0:>8.2f}s  {fils if fils is not None else '-':>10}")
        print("=" * 68)
        print(f"  {ok} correctos, {err} con error, {len(filas)} en total")
        print("=" * 68)

        return {"total": len(filas), "ok": ok, "error": err}
