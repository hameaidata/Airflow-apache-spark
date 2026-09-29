"""
s2sql_carga - Mitad de DESTINO del pipeline: Parquet -> SQL Server 2022.

    cargar_tabla()   aplica UN parquet a su tabla destino, segun el modo que
                     declara el catalogo.
    cerrar_lote()    resume la corrida y la marca TERMINADO o ERROR.

LOS TRES MODOS
--------------
REEMPLAZO     La tabla destino queda exactamente igual que el origen.
              TRUNCATE + INSERT dentro de UNA transaccion.
INCREMENTAL   Se anaden solo las filas nuevas (las que la extraccion trajo
              filtrando por la marca de agua). No corrige filas que cambiaron.
MERGE         Actualiza las que ya estaban e inserta las nuevas, por
              claves_merge. Es lo que se quiere cuando el origen corrige el
              pasado.

TODOS PASAN POR UNA TABLA DE STAGING
------------------------------------
El parquet nunca se inserta directamente en la tabla destino. Primero va a
[STG].[S2SQL_<destino>], y de ahi se aplica a la destino con UNA sentencia
dentro de una transaccion. Esto compra tres cosas:

  - La tabla destino nunca se ve a medias. Un consumidor que la lea mientras
    carga ve la version vieja completa o la nueva completa, nunca 40.000 filas
    de un archivo de 100.000.
  - Si la carga muere a mitad del volcado, la destino no se toco siquiera.
  - En REEMPLAZO, el TRUNCATE ocurre cuando los datos nuevos YA estan en la
    base. Truncar primero y cargar despues deja la tabla vacia si el archivo
    resulta estar corrupto, que es el peor momento posible para descubrirlo.

Detalle de SQL Server que lo hace posible: aqui TRUNCATE TABLE es transaccional
y se puede deshacer con ROLLBACK. En MySQL o SingleStore no, porque alli es DDL
y hace commit implicito. Este modulo depende de esa diferencia.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from airflow.exceptions import AirflowException

from .s2sql_comun import (
    ESTADO_ERROR,
    ESTADO_SIN_DATOS,
    ESTADO_TERMINADO,
    MODO_INCREMENTAL,
    MODO_MERGE,
    MODO_REEMPLAZO,
    cargar_config,
    conexion_sqlserver,
    corchetes,
    duracion,
    filas_nativas,
    lista_columnas,
    nombre_control,
    nombre_staging,
    recortar,
    validar_identificador,
)
from .s2sql_extraccion import _cerrar_log


logger = logging.getLogger(__name__)


# ============================================================================
# STAGING
# ============================================================================
def columnas_del_parquet(ruta: str) -> list[str]:
    import pyarrow.parquet as pq

    return list(pq.ParquetFile(ruta).schema_arrow.names)


def recrear_staging(conn, config: dict[str, Any], trabajo: dict, columnas: list[str]) -> str:
    """Crea [STG].[S2SQL_<destino>] con los MISMOS tipos que la tabla destino.

    Se usa 'SELECT TOP 0 ... INTO' en vez de escribir un CREATE TABLE a mano:
    asi los tipos, longitudes y precisiones los copia el motor de la tabla
    destino real. Si se declararan aqui, un VARCHAR(50) en destino contra un
    VARCHAR(MAX) en staging pasaria desapercibido hasta que una fila larga
    reventara el INSERT final, ya dentro de la transaccion.

    Se recrea en cada carga: una staging vieja con otras columnas es una fuente
    de errores raros dificiles de rastrear.
    """
    esquema_stg, tabla_stg = nombre_staging(config, trabajo["tabla_destino"])
    # Salen de la Variable, no del catalogo, pero acaban concatenados en el SQL
    # igual que los del catalogo: se validan por el mismo motivo.
    esquema_stg = validar_identificador(esquema_stg, "escritura.esquema_staging")
    tabla_stg = validar_identificador(tabla_stg, "escritura.prefijo_staging")

    destino = corchetes(trabajo["esquema_destino"], trabajo["tabla_destino"])
    staging = corchetes(esquema_stg, tabla_stg)
    cols = lista_columnas(columnas, "columnas del parquet")

    cur = conn.cursor()
    # Sin parametros enlazados a proposito: el nombre ya esta validado, y una
    # sentencia con ? dentro de un EXEC() dinamico se comporta distinto segun
    # el driver. Aqui interesa que haga lo mismo en todos.
    cur.execute(
        f"IF SCHEMA_ID('{esquema_stg}') IS NULL "
        f"EXEC('CREATE SCHEMA [{esquema_stg}]')"
    )
    cur.execute(f"DROP TABLE IF EXISTS {staging}")
    cur.execute(f"SELECT TOP 0 {cols} INTO {staging} FROM {destino}")
    conn.commit()
    logger.info("[%s] staging %s recreada con %s columnas",
                trabajo["tabla_origen"], staging, len(columnas))
    return staging


def volcar_parquet_en_staging(conn, config, trabajo: dict, staging: str,
                              columnas: list[str]) -> int:
    """Lee el parquet por lotes y los inserta en la staging.

    Con JDBC no hay fast_executemany: ese acelerador es de pyodbc. Aqui el
    equivalente lo da el PreparedStatement, que jaydebeapi reutiliza en
    executemany agrupando el lote con addBatch/executeBatch. Lo que manda es
    escritura.batch_filas, no un flag.
    """
    import pyarrow.parquet as pq

    esc = config["escritura"]
    tamano = int(trabajo.get("batch_filas") or esc["batch_filas"])
    cols = lista_columnas(columnas, "columnas del parquet")
    marcadores = ", ".join("?" * len(columnas))
    sql = f"INSERT INTO {staging} ({cols}) VALUES ({marcadores})"

    cur = conn.cursor()

    escritas = 0
    for lote in pq.ParquetFile(trabajo["ruta_parquet"]).iter_batches(batch_size=tamano):
        # filas_nativas hace dos cosas imprescindibles con JDBC: convierte los
        # tipos de numpy a tipos nativos de Python (JPype no sabe que es un
        # numpy.int64) y pasa los NaN/NaT a None. Ver el detalle en
        # s2sql_comun.filas_nativas.
        filas = filas_nativas(lote.to_pandas())
        if not filas:
            continue
        cur.executemany(sql, filas)
        escritas += len(filas)
        logger.info("[%s] %s filas en staging", trabajo["tabla_origen"], f"{escritas:,}")

    conn.commit()
    return escritas


# ============================================================================
# LOS TRES MODOS
# ============================================================================
# ----------------------------------------------------------------------------
# SOBRE LA TRANSACCION
# ----------------------------------------------------------------------------
# Aqui NO se escribe "BEGIN TRANSACTION". La conexion JDBC se abre con
# setAutoCommit(false), asi que el driver ya mantiene una transaccion abierta
# que se cierra con conn.commit() o conn.rollback().
#
# Anadir un BEGIN TRANSACTION explicito encima de eso deja @@TRANCOUNT en 2 y
# rompe la contabilidad que lleva el driver: el sintoma tipico es
# "The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION"
# justo cuando algo ya fue mal, o sea en el peor momento para tener encima un
# segundo error que tapa el primero.
#
# Cada una de estas tres funciones corre entonces dentro de la transaccion que
# empieza en su primera sentencia y termina en el commit o el rollback del
# final. Es el mismo alcance que antes, sin el conflicto.
# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
# SOBRE CONTAR FILAS
# ----------------------------------------------------------------------------
# Ninguna de estas funciones usa cursor.rowcount. Con JDBC (jaydebeapi) ese
# valor es -1 casi siempre, asi que informar a partir de el daria un log que
# miente. Las cuentas salen de datos que ya conocemos o de consultas
# explicitas:
#
#   REEMPLAZO / INCREMENTAL  insertadas = las filas que entraron en la staging,
#                            porque el INSERT las copia todas.
#   MERGE                    se cuenta ANTES cuantas claves de la staging ya
#                            existen en el destino. Esas seran UPDATE; el resto,
#                            INSERT. Es exacto porque la unicidad de la clave
#                            ya se verifico.
#
# Contar asi ademas evita depender de leer varios result sets seguidos, que es
# otra cosa que jaydebeapi soporta a medias.
# ----------------------------------------------------------------------------
def aplicar_reemplazo(conn, trabajo: dict, staging: str, columnas: list[str],
                      escritas: int) -> dict[str, int]:
    destino = corchetes(trabajo["esquema_destino"], trabajo["tabla_destino"])
    cols = lista_columnas(columnas, "columnas del parquet")
    cur = conn.cursor()
    try:
        # TRUNCATE en vez de DELETE: no registra fila por fila en el log de
        # transacciones y reinicia IDENTITY. Requiere permiso ALTER sobre la
        # tabla y falla si alguna FOREIGN KEY la referencia; en ese caso hay
        # que cambiarlo por DELETE en este punto.
        cur.execute(f"TRUNCATE TABLE {destino}")
        cur.execute(f"INSERT INTO {destino} ({cols}) SELECT {cols} FROM {staging}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"insertadas": escritas, "actualizadas": 0}


def aplicar_incremental(conn, trabajo: dict, staging: str, columnas: list[str],
                        escritas: int) -> dict[str, int]:
    destino = corchetes(trabajo["esquema_destino"], trabajo["tabla_destino"])
    cols = lista_columnas(columnas, "columnas del parquet")
    cur = conn.cursor()
    try:
        cur.execute(f"INSERT INTO {destino} ({cols}) SELECT {cols} FROM {staging}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"insertadas": escritas, "actualizadas": 0}


def verificar_claves_unicas(conn, trabajo: dict, staging: str) -> None:
    """MERGE exige que la clave sea unica en el origen. Se comprueba antes.

    Si hay duplicados, SQL Server aborta con el error 8672, cuyo texto habla de
    'multiple source rows' y no dice ni que tabla ni que clave. Comprobarlo
    aqui cuesta una consulta y convierte ese error en una frase accionable.
    """
    claves = lista_columnas(trabajo["claves_merge"], "claves_merge")
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM (SELECT {claves} FROM {staging} "
        f"GROUP BY {claves} HAVING COUNT(*) > 1) d"
    )
    duplicados = int(cur.fetchone()[0])
    if duplicados:
        raise AirflowException(
            f"[{trabajo['tabla_origen']}] MERGE cancelado: el origen trae {duplicados} "
            f"valor(es) repetido(s) de la clave ({', '.join(trabajo['claves_merge'])}). "
            f"MERGE no puede decidir cual de las filas repetidas gana. Corrige la clave "
            f"en CTL_S2SQL_CATALOGO o filtra el origen con filtro_where."
        )


def contar_coincidencias(conn, trabajo: dict, staging: str) -> int:
    """Cuantas claves de la staging ya existen en el destino.

    Esas filas seran UPDATE y las demas INSERT. Se cuenta ANTES del MERGE, que
    es el unico momento en que se puede: despues ya estan todas.

    Sustituye al truco de 'OUTPUT $action INTO @tabla' seguido de un SELECT.
    Aquello funcionaba con ODBC, pero obliga a leer un segundo result set
    dentro del mismo lote, y jaydebeapi soporta eso a medias. Esta consulta da
    el mismo numero y no depende del driver.
    """
    destino = corchetes(trabajo["esquema_destino"], trabajo["tabla_destino"])
    condicion = " AND ".join(f"d.[{c}] = o.[{c}]" for c in trabajo["claves_merge"])
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {staging} o JOIN {destino} d ON {condicion}")
    fila = cur.fetchone()
    return int(fila[0]) if fila else 0


def aplicar_merge(conn, trabajo: dict, staging: str, columnas: list[str],
                  escritas: int) -> dict[str, int]:
    verificar_claves_unicas(conn, trabajo, staging)
    coincidencias = contar_coincidencias(conn, trabajo, staging)

    destino = corchetes(trabajo["esquema_destino"], trabajo["tabla_destino"])
    claves = trabajo["claves_merge"]
    no_clave = [c for c in columnas if c not in claves]
    if not no_clave:
        raise AirflowException(
            f"[{trabajo['tabla_origen']}] MERGE cancelado: todas las columnas son clave, "
            f"asi que no hay nada que actualizar. Ese caso se resuelve con "
            f"modo_carga=INCREMENTAL."
        )

    condicion = " AND ".join(f"destino.[{c}] = origen.[{c}]" for c in claves)
    asignaciones = ", ".join(f"destino.[{c}] = origen.[{c}]" for c in no_clave)
    cols = lista_columnas(columnas, "columnas del parquet")
    valores = ", ".join(f"origen.[{c}]" for c in columnas)

    # WITH (HOLDLOCK) no es opcional. Sin el, MERGE tiene una condicion de
    # carrera entre el momento en que comprueba si la fila existe y el momento
    # en que la inserta, y dos cargas simultaneas acaban violando la clave
    # primaria. Es la recomendacion estandar para MERGE en SQL Server.
    #
    # El MERGE va como UNA sola sentencia, sin DECLARE ni SELECT detras: las
    # cuentas ya las tenemos de contar_coincidencias. Un lote de varias
    # sentencias obligaria a recorrer varios result sets, que es justo lo que
    # jaydebeapi hace mal.
    sql = f"""
        MERGE {destino} WITH (HOLDLOCK) AS destino
        USING {staging} AS origen
           ON {condicion}
        WHEN MATCHED THEN
            UPDATE SET {asignaciones}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({cols}) VALUES ({valores});
    """

    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"insertadas": escritas - coincidencias, "actualizadas": coincidencias}


APLICADORES = {
    MODO_REEMPLAZO: aplicar_reemplazo,
    MODO_INCREMENTAL: aplicar_incremental,
    MODO_MERGE: aplicar_merge,
}


# ============================================================================
# CARGA DE UNA TABLA
# ============================================================================
def cargar_tabla(trabajo: dict[str, Any]) -> dict[str, Any]:
    """Aplica el parquet de UN trabajo a su tabla destino.

    Recibe por XCom lo que devolvio extraer_tabla. Si esa extraccion no trajo
    filas, no hay archivo y no hay nada que hacer: se informa y se sale sin
    tocar la tabla destino. Truncarla porque el origen no cambio seria
    destruir datos buenos.
    """
    config = cargar_config()
    inicio = datetime.now()
    tabla = trabajo["tabla_origen"]
    limite_error = int(config["limites"]["mensaje_error"])

    if trabajo.get("estado") == ESTADO_SIN_DATOS or not trabajo.get("ruta_parquet"):
        logger.info("[%s] la extraccion no trajo filas. No se toca %s.",
                    tabla, trabajo["tabla_destino"])
        return {**trabajo, "estado_carga": ESTADO_SIN_DATOS, "insertadas": 0, "actualizadas": 0}

    ruta = trabajo["ruta_parquet"]
    if not os.path.isfile(ruta):
        raise AirflowException(
            f"[{tabla}] la extraccion registro {ruta} pero el archivo no esta ahi.\n"
            f"Lo mas habitual es que la carpeta externa no este montada en el worker que "
            f"tomo esta tarea. Comprueba que S2SQL_PARQUET_HOST_DIR este montado en "
            f"TODOS los servicios del docker-compose, no solo en el scheduler."
        )

    conn = conexion_sqlserver(config)
    conn_log = conexion_sqlserver(config)
    id_log = trabajo.get("id_log")

    try:
        columnas = columnas_del_parquet(ruta)
        if not columnas:
            raise AirflowException(f"[{tabla}] el parquet {ruta} no declara columnas.")

        staging = recrear_staging(conn, config, trabajo, columnas)
        escritas = volcar_parquet_en_staging(conn, config, trabajo, staging, columnas)

        if not escritas:
            logger.warning("[%s] el parquet existe pero no tiene filas. No se aplica nada.", tabla)
            resultado = {"insertadas": 0, "actualizadas": 0}
        else:
            aplicador = APLICADORES[trabajo["modo_carga"]]
            resultado = aplicador(conn, trabajo, staging, columnas, escritas)

        # La staging se deja en pie a proposito: tras una carga sospechosa es
        # exactamente lo que hay que mirar para comparar con el destino. Se
        # recrea en la siguiente corrida, asi que no se acumula.
        if id_log:
            _cerrar_log(conn_log, config, id_log, ESTADO_TERMINADO,
                        filas_escritas=escritas,
                        filas_insertadas=resultado["insertadas"],
                        filas_actualizadas=resultado["actualizadas"],
                        segundos=duracion(inicio))

        logger.info(
            "[%s] %s -> %s.%s | modo=%s | %s insertadas, %s actualizadas | %.2f s",
            tabla, os.path.basename(ruta), trabajo["esquema_destino"], trabajo["tabla_destino"],
            trabajo["modo_carga"], f"{resultado['insertadas']:,}",
            f"{resultado['actualizadas']:,}", duracion(inicio),
        )
        return {**trabajo, "estado_carga": ESTADO_TERMINADO, "filas_escritas": escritas, **resultado}

    except Exception as exc:
        if id_log:
            _cerrar_log(conn_log, config, id_log, ESTADO_ERROR,
                        error=recortar(exc, limite_error), segundos=duracion(inicio))
        raise
    finally:
        conn.close()
        conn_log.close()


# ============================================================================
# CIERRE DEL LOTE
# ============================================================================
def cerrar_lote(batch_id: str | None = None) -> dict[str, Any]:
    """Resume la corrida y decide si fue buena.

    Corre con trigger_rule="all_done", o sea tambien cuando alguna tabla fallo.
    Por eso LANZA si encuentra errores: si se limitara a informar, la corrida
    saldria verde en la interfaz con tablas sin cargar, que es la peor forma de
    no enterarse.

    La cuenta sale del log en SQL Server y no del estado de las tareas de
    Airflow: es la misma fuente que va a consultar manana quien audite, asi que
    si las dos no coinciden, la que manda es esta.
    """
    config = cargar_config()
    if not batch_id:
        raise AirflowException(
            "cerrar_lote no recibio batch_id. Deberia llegar por XCom desde "
            "preparar_lote; revisa que el DAG tenga render_template_as_native_obj."
        )

    conn = conexion_sqlserver(config)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT estado, COUNT(*), SUM(COALESCE(filas_insertadas,0)), "
            f"       SUM(COALESCE(filas_actualizadas,0)) "
            f"FROM {nombre_control(config, 'log')} WHERE batch_id = ? GROUP BY estado",
            (batch_id,),
        )
        por_estado = {str(e): (int(n), int(i or 0), int(a or 0)) for e, n, i, a in cur.fetchall()}

        ok = por_estado.get(ESTADO_TERMINADO, (0, 0, 0))[0]
        sin_datos = por_estado.get(ESTADO_SIN_DATOS, (0, 0, 0))[0]
        con_error = por_estado.get(ESTADO_ERROR, (0, 0, 0))[0]
        insertadas = sum(v[1] for v in por_estado.values())
        actualizadas = sum(v[2] for v in por_estado.values())

        estado_lote = ESTADO_ERROR if con_error else ESTADO_TERMINADO
        cur.execute(
            f"UPDATE {nombre_control(config, 'lote')} SET "
            f"  fec_fin = SYSDATETIME(), estado = ?, tablas_ok = ?, tablas_error = ? "
            f"WHERE batch_id = ?",
            (estado_lote, ok + sin_datos, con_error, batch_id),
        )
        conn.commit()

        if con_error:
            cur.execute(
                f"SELECT tabla_origen, LEFT(msg_error, 300) FROM {nombre_control(config, 'log')} "
                f"WHERE batch_id = ? AND estado = ? ORDER BY tabla_origen",
                (batch_id, ESTADO_ERROR),
            )
            detalle = cur.fetchall()
    finally:
        conn.close()

    resumen = {
        "batch_id": batch_id, "estado": estado_lote, "tablas_ok": ok,
        "tablas_sin_datos": sin_datos, "tablas_error": con_error,
        "filas_insertadas": insertadas, "filas_actualizadas": actualizadas,
    }
    logger.info(
        "Lote %s: %s | %s ok, %s sin datos, %s con error | %s insertadas, %s actualizadas",
        batch_id, estado_lote, ok, sin_datos, con_error,
        f"{insertadas:,}", f"{actualizadas:,}",
    )

    if con_error:
        lineas = "\n".join(f"  - {t}: {m}" for t, m in detalle)
        raise AirflowException(
            f"El lote {batch_id} termino con {con_error} tabla(s) en error:\n{lineas}\n"
            f"Las {ok} tabla(s) correctas SI se cargaron. Para reintentar solo las que "
            f"fallaron, usa el parametro solo_tablas al relanzar el DAG."
        )
    return resumen
