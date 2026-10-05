"""
bt2sql_carga - Mitad de DESTINO: Parquet -> STG en SQL Server.

    verificar_parquet()  compuerta: mira si hay archivos del lote EN DISCO.
    ejecutar_carga()     sube cada parquet a su tabla STG.

EL PUNTO DE ENCUENTRO ES LA TABLA, NO UNA RUTA FIJA
---------------------------------------------------
La extraccion guardo la RUTA COMPLETA de cada archivo en
ctl_proceso_parquet.archivo_parquet, junto al batch_id. La carga lee esa tabla
(unida al catalogo, que dice a que tabla STG va cada origen) y sube desde ahi.
No hay ninguna ruta escrita en el codigo.

TODO PASA POR UNA TABLA DE STAGING
----------------------------------
El parquet nunca se inserta directamente en la tabla STG. Primero va a una
tabla intermedia y de ahi se aplica con UNA sentencia dentro de una
transaccion. Eso compra tres cosas:

  - La tabla destino nunca se ve a medias. Un consumidor que la lea mientras
    carga ve la version vieja completa o la nueva completa.
  - Si la carga muere a mitad del volcado, la destino no se toco siquiera.
  - El TRUNCATE ocurre cuando los datos nuevos YA estan en la base. Truncar
    primero y cargar despues deja la tabla vacia si el archivo resulta estar
    corrupto, que es el peor momento para descubrirlo.

Detalle de SQL Server que lo hace posible: aqui TRUNCATE TABLE es transaccional
y se puede deshacer con ROLLBACK. En MySQL o SingleStore no, porque alli es DDL
y hace commit implicito.

usar_staging=false en la Variable salta la tabla intermedia e inserta directo.
Es mas rapido y pierde las tres garantias de arriba.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime,date
from typing import Any
import jpype

from airflow.exceptions import AirflowException

from .bt2sql_comun import (
    ESTADO_ERROR,
    ESTADO_SIN_DATOS,
    ESTADO_TERMINADO,
    atar_hilo_a_jvm,
    conexion_sqlserver,
    config_carga,
    config_extraccion,
    corchetes,
    duracion,
    filas_nativas,
    host,
    lista_columnas,
    normalizar_batch_id,
    recortar,
    validar_identificador,
)


logger = logging.getLogger(__name__)

# sql_jobs devuelve EXACTAMENTE estas cuatro columnas y en este orden, porque
# el codigo las desempaqueta por posicion.
COLUMNAS_JOB = ("archivo_parquet", "tabla_destino", "batch_size", "commit_every")


# ============================================================================
# QUE HAY QUE CARGAR
# ============================================================================
def obtener_jobs(cur, config: dict[str, Any], batch_id: str | None,
                 permitir_vacio: bool = False) -> list[tuple]:
    """Lee de ctl_proceso_parquet que archivos subir y a que tabla.

    El token {BATCH_ID} de la Variable se sustituye por un parametro ENLAZADO,
    no por el valor concatenado: es un dato que viene de una corrida anterior y
    no tiene por que entrar en el texto del SQL.
    """
    sql = config["sql_jobs"]
    batch_id = normalizar_batch_id(batch_id)

    if batch_id:
        sql = sql.replace("{BATCH_ID}", "?")
        parametros = (batch_id,)
        logger.info("Cargando los archivos del batch %s", batch_id)
    else:
        # Sin batch: la ultima extraccion TERMINADA de cada tabla. Es el caso
        # de relanzar solo la carga tras un clear de esa tarea.
        sql = sql.replace("p.batch_id = {BATCH_ID}", (
            "p.batch_id = (SELECT MAX(p2.batch_id) FROM "
            f"{config['tabla_control_extraccion']} p2 "
            "WHERE p2.tabla_origen = p.tabla_origen AND p2.estado = 'TERMINADO')"
        ))
        parametros = ()
        logger.warning(
            "No se recibio batch_id: se cargara la ultima extraccion TERMINADA "
            "de cada tabla. Esto puede subir datos de un dia anterior."
        )

    cur.execute(sql, parametros) if parametros else cur.execute(sql)
    jobs = cur.fetchall()
    jobs_normalizados = []

    for ruta, destino, batch_size, commit_every in jobs:
        jobs_normalizados.append(
            (
                str(ruta) if ruta is not None else None,
                str(destino) if destino is not None else None,
                batch_size,
                commit_every
            )
        )

    jobs = jobs_normalizados


    if not jobs and not permitir_vacio:
        raise AirflowException(
            f"No hay nada que cargar para el batch {batch_id!r}.\n"
            f"Revisa que la extraccion haya dejado filas en TERMINADO y que el "
            f"catalogo tenga tabla_destino para esos origenes."
        )

    for job in jobs:
        if len(job) != len(COLUMNAS_JOB):
            raise AirflowException(
                f"sql_jobs debe devolver {len(COLUMNAS_JOB)} columnas "
                f"({', '.join(COLUMNAS_JOB)}) y devolvio {len(job)}. "
                f"El codigo las desempaqueta por posicion."
            )
        if not job[0]:
            raise AirflowException(
                "sql_jobs devolvio una fila con archivo_parquet vacio. La "
                "extraccion deberia haber guardado ahi la ruta completa."
            )
    return list(jobs)


def verificar_parquet(batch_id: str | None = None) -> bool:
    """Compuerta del DAG: hay archivos del lote EN DISCO?

        hay archivos  -> True  -> la carga se ejecuta
        no hay        -> False -> la carga queda en SKIPPED, no en failed

    Comprueba el disco y no solo la bitacora, a proposito: el caso que atrapa
    es que la carpeta externa no este montada en el worker que tomo la tarea.
    Ahi la bitacora dice que el archivo existe y el disco dice que no.
    """
    config = config_carga()
    conn = conexion_sqlserver(config)
    try:
        cur = conn.cursor()
        jobs = obtener_jobs(cur, config, batch_id, permitir_vacio=True)
    finally:
        conn.close()

    if not jobs:
        logger.info(
            "No hay archivos registrados%s. La carga se salta: no es un error, "
            "es que no habia nada que extraer.",
            f" para el batch {batch_id}" if batch_id else "",
        )
        return False

    existen, faltan = [], []
    for ruta, destino, _, _ in jobs:
        logger.info("TIPO_RUTA=%s", type(ruta))
        logger.info("RUTA=%s", ruta)
        (existen if os.path.isfile(ruta) else faltan).append((ruta, destino))

    logger.info("Archivos registrados para el batch %s:", batch_id or "(ultimo)")
    for ruta, destino in existen:
        tam = os.path.getsize(ruta)
        logger.info("  OK     %-28s -> %-28s  %s bytes", os.path.basename(ruta), destino, f"{tam:,}")
    for ruta, destino in faltan:
        logger.error("  FALTA  %-28s -> %-28s  %s", os.path.basename(ruta), destino, ruta)

    if not existen:
        raise AirflowException(
            f"La bitacora registra {len(jobs)} archivo(s) del batch {batch_id}, pero "
            f"NINGUNO esta en disco.\n"
            f"Lo mas habitual es que la carpeta externa no este montada en el worker "
            f"que tomo esta tarea. Comprueba que la ruta de parquet este montada en "
            f"TODOS los servicios del docker-compose, no solo en el scheduler."
        )

    if faltan:
        raise AirflowException(
            f"Faltan {len(faltan)} de {len(jobs)} archivos del batch {batch_id} en disco. "
            f"Cargar solo una parte dejaria el dia incompleto sin que nada lo advirtiera.\n"
            f"Faltan: {', '.join(os.path.basename(r) for r, _ in faltan)}"
        )

    logger.info("Los %s archivos del batch estan en disco. Se procede a cargar.", len(existen))
    return True


# ============================================================================
# STAGING Y APLICACION
# ============================================================================
def columnas_del_parquet(ruta: str) -> list[str]:
    import pyarrow.parquet as pq

    return list(pq.ParquetFile(ruta).schema_arrow.names)


def nombre_staging(config: dict[str, Any], tabla_destino: str) -> str:
    esquema, tabla = _partir_destino(tabla_destino)
    return corchetes(esquema, f"{tabla}{config['sufijo_staging']}")


def _partir_destino(tabla_destino: str) -> tuple[str, str]:
    """'dbo.STG_FSR011' -> ('dbo', 'STG_FSR011').  Sin esquema -> dbo."""
    partes = str(tabla_destino).replace("[", "").replace("]", "").split(".")
    if len(partes) == 1:
        return "dbo", validar_identificador(partes[0], "tabla_destino")
    return (validar_identificador(partes[0], "tabla_destino.esquema"),
            validar_identificador(partes[1], "tabla_destino.tabla"))


def recrear_staging(conn, config: dict[str, Any], tabla_destino: str,
                    columnas: list[str]) -> str:
    """Crea la staging con los MISMOS tipos que la tabla destino.

    Se usa 'SELECT TOP 0 ... INTO' en vez de escribir un CREATE TABLE a mano:
    asi los tipos, longitudes y precisiones los copia el motor de la destino
    real. Si se declararan aqui, un VARCHAR(50) en destino contra un
    VARCHAR(MAX) en staging pasaria desapercibido hasta que una fila larga
    reventara el INSERT final, ya dentro de la transaccion.
    """
    destino = corchetes(*_partir_destino(tabla_destino))
    staging = nombre_staging(config, tabla_destino)
    cols = lista_columnas(columnas, "columnas del parquet")

    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {staging}")
    cur.execute(f"SELECT TOP 0 {cols} INTO {staging} FROM {destino}")
    conn.commit()
    logger.info("staging %s recreada con %s columnas", staging, len(columnas))
    return staging


def volcar_parquet(conn, config: dict[str, Any], ruta: str, tabla: str,
                   columnas: list[str], batch_size: int) -> int:
    """Lee el parquet por lotes y los inserta.

    Con JDBC no hay fast_executemany: ese acelerador es de pyodbc. Aqui el
    equivalente lo da el PreparedStatement, que jaydebeapi reutiliza en
    executemany agrupando el lote. Lo que manda es batch_size.
    """
    import pyarrow.parquet as pq

    cols = lista_columnas(columnas, "columnas del parquet")
    marcadores = ", ".join("?" * len(columnas))
    sql = f"INSERT INTO {tabla} ({cols}) VALUES ({marcadores})"

    cur = conn.cursor()
    escritas = 0
    for lote in pq.ParquetFile(ruta).iter_batches(batch_size=batch_size):
        # filas_nativas hace dos cosas imprescindibles con JDBC: convierte los
        # tipos de numpy a nativos de Python (JPype no sabe que es un
        # numpy.int64) y pasa los NaN/NaT a None.
        df = lote.to_pandas()
        if "FECHA_PROCESO" in df.columns:
            df["FECHA_PROCESO"] = df["FECHA_PROCESO"].apply(lambda x: (
            jpype.java.sql.Date.valueOf(x.strftime("%Y-%m-%d"))
            if isinstance(x, date)
            else x
        ))
        filas = filas_nativas(df)
        # filas = filas_nativas(lote.to_pandas())
        if not filas:
            continue
        cur.executemany(sql, filas)
        escritas += len(filas)
        logger.info("  %s filas", f"{escritas:,}")
    conn.commit()
    return escritas


def aplicar_a_destino(conn, config: dict[str, Any], tabla_destino: str,
                      staging: str, columnas: list[str]) -> None:
    """TRUNCATE + INSERT desde la staging, en UNA transaccion.

    NO se escribe BEGIN TRANSACTION: la conexion va con autocommit=False, asi
    que el driver ya mantiene una transaccion abierta. Los dos a la vez dejan
    @@TRANCOUNT en 2 y producen "The COMMIT TRANSACTION request has no
    corresponding BEGIN TRANSACTION" justo cuando algo ya fue mal.
    """
    destino = corchetes(*_partir_destino(tabla_destino))
    cols = lista_columnas(columnas, "columnas del parquet")
    cur = conn.cursor()
    try:
        # TRUNCATE en vez de DELETE: no registra fila por fila en el log de
        # transacciones. Requiere permiso ALTER y falla si alguna FOREIGN KEY
        # referencia la tabla; en ese caso hay que cambiarlo por DELETE aqui.
        cur.execute(f"TRUNCATE TABLE {destino}")
        cur.execute(f"INSERT INTO {destino} ({cols}) SELECT {cols} FROM {staging}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ============================================================================
# BITACORA DE CARGA (ctl_carga_stg)
# ============================================================================
def _abrir_log(conn, config: dict[str, Any], batch_id: str | None,
               ruta: str, tabla_destino: str) -> int:
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO {config['tabla_control']} "
        f"(nom_proceso, batch_id, archivo_parquet, tabla_destino, "
        f" fec_inicio, estado, host_name) "
        f"OUTPUT INSERTED.id_log "
        f"VALUES (?, ?, ?, ?, SYSDATETIME(), ?, ?)",
        (config["nom_proceso"], batch_id, ruta, str(tabla_destino),
         config["estado_iniciado"], host()),
    )
    id_log = int(cur.fetchone()[0])
    conn.commit()
    return id_log


def _cerrar_log(conn, config: dict[str, Any], id_log: int, estado: str, **campos) -> None:
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE {config['tabla_control']} SET "
            f"  estado = ?, fec_termino = SYSDATETIME(), duracion_segundos = ?, "
            f"  filas_cargadas = COALESCE(?, filas_cargadas), msg_error = ? "
            f"WHERE id_log = ?",
            (estado, campos.get("segundos"), campos.get("filas"),
             campos.get("error"), id_log),
        )
        conn.commit()
    except Exception as exc:
        logger.error("No se pudo cerrar la fila %s de la bitacora de carga: %s", id_log, exc)


# ============================================================================
# ENTRADA DEL DAG
# ============================================================================
def ejecutar_carga(batch_id: str | None = None) -> dict[str, Any]:
    """Sube a STG todos los parquet del lote.

    Cada tabla se aplica de forma independiente: una que falla no impide que
    las demas se carguen. Al final lanza si hubo errores, para que la corrida
    no salga verde con tablas sin cargar.
    """
    # Airflow no garantiza que el callable corra en el hilo principal. Aqui es
    # un no-op si ya lo esta, y evita que la JVM aborte el proceso si no.
    atar_hilo_a_jvm()

    config = config_carga()
    limite_error = int(config["error_size_limit"])

    conn = conexion_sqlserver(config)
    conn_log = conexion_sqlserver(config, autocommit=True)
    resultados: list[dict] = []

    try:
        cur = conn.cursor()
        jobs = obtener_jobs(cur, config, batch_id)
        logger.info("%s archivo(s) por cargar en el batch %s", len(jobs), batch_id or "(ultimo)")

        for ruta, tabla_destino, batch_size, _commit_every in jobs:
            inicio = datetime.now()
            id_log = _abrir_log(conn_log, config, batch_id, ruta, tabla_destino)
            nombre = os.path.basename(ruta)
            try:
                if not os.path.isfile(ruta):
                    raise AirflowException(
                        f"La bitacora registra {ruta} pero el archivo no esta ahi. "
                        f"Lo mas habitual es que la carpeta externa no este montada "
                        f"en este worker."
                    )

                columnas = columnas_del_parquet(ruta)
                if not columnas:
                    raise AirflowException(f"El parquet {ruta} no declara columnas.")

                tamano = int(batch_size or config["batch_default"])

                if config.get("usar_staging", True):
                    staging = recrear_staging(conn, config, tabla_destino, columnas)
                    escritas = volcar_parquet(conn, config, ruta, staging, columnas, tamano)
                    if escritas:
                        aplicar_a_destino(conn, config, tabla_destino, staging, columnas)
                else:
                    destino = corchetes(*_partir_destino(tabla_destino))
                    cur.execute(f"TRUNCATE TABLE {destino}")
                    escritas = volcar_parquet(conn, config, ruta, destino, columnas, tamano)

                _cerrar_log(conn_log, config, id_log, config["estado_finalizado"],
                            filas=escritas, segundos=duracion(inicio))
                logger.info("%-28s -> %-28s  %s filas  %.2f s",
                            nombre, tabla_destino, f"{escritas:,}", duracion(inicio))
                resultados.append({"archivo": nombre, "destino": tabla_destino,
                                   "filas": escritas, "estado": ESTADO_TERMINADO})

            except Exception as exc:
                _cerrar_log(conn_log, config, id_log, config["estado_error"],
                            error=recortar(exc, limite_error), segundos=duracion(inicio))
                logger.error("%-28s -> %-28s  ERROR: %s", nombre, tabla_destino, exc)
                resultados.append({"archivo": nombre, "destino": tabla_destino,
                                   "filas": 0, "estado": ESTADO_ERROR, "error": str(exc)})
    finally:
        conn.close()
        conn_log.close()

    ok = [r for r in resultados if r["estado"] == ESTADO_TERMINADO]
    errores = [r for r in resultados if r["estado"] == ESTADO_ERROR]
    total = sum(r["filas"] for r in resultados)

    resumen = {"batch_id": batch_id, "tablas_ok": len(ok), "tablas_error": len(errores),
               "filas_cargadas": total}
    logger.info("Carga %s: %s ok, %s con error | %s filas",
                batch_id, len(ok), len(errores), f"{total:,}")

    if errores:
        detalle = "\n".join(f"  - {r['destino']}: {r.get('error', '')[:200]}" for r in errores)
        raise AirflowException(
            f"La carga del batch {batch_id} termino con {len(errores)} tabla(s) en "
            f"error:\n{detalle}\n"
            f"Las {len(ok)} correctas SI se cargaron. El detalle completo esta en "
            f"{config['tabla_control']} para este batch_id."
        )
    return resumen
