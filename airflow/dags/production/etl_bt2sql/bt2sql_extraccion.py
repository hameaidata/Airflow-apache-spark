"""
bt2sql_extraccion - Mitad de ORIGEN: Bantotal (IBM i) -> Parquet.

    ejecutar_extraccion()  lee el catalogo, extrae cada tabla activa de
                           Bantotal y deja un parquet por tabla, en su propia
                           carpeta del dia.
    limpiar_parquet()      borra las carpetas de dias anteriores a la retencion.

LA CARPETA
----------
    /data/bt2sql/<yyyyMMdd>/<TABLA>/<TABLA>.parquet

Una carpeta por dia y, dentro, una por tabla. El detalle de por que esta en
bt2sql_comun.py, junto a las funciones que arman la ruta.

EL RASTRO DE AUDITORIA
----------------------
Tres tablas en SQL Server, las mismas que ya usa el pipeline de SingleStore:

    CTL_PARAMETROS_PARQUET   que tablas se extraen y con que filtro
    ctl_proceso_parquet      una fila POR TABLA Y POR CORRIDA: cuando empezo,
                             cuando termino, cuantas filas, que archivo, que
                             error. Es el rastro que pide auditoria.
    ctl_carga_stg            lo mismo para la carga (ver bt2sql_carga.py)

De ctl_proceso_parquet sale ademas la ruta que la carga abre despues: la tabla
es el punto de encuentro entre las dos mitades, no hay rutas fijas en el codigo.

POR QUE PASA POR PARQUET Y NO VA DIRECTO AL DESTINO
---------------------------------------------------
Origen y destino quedan desacoplados. Si SQL Server esta caido o una carga
falla a mitad, el parquet del dia ya esta escrito: se reintenta la carga sin
volver a leer el core, que es la parte cara y la que molesta al AS/400.
"""

from __future__ import annotations

import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowException

from .bt2sql_comun import (
    ESTADO_EJECUTANDO,
    ESTADO_ERROR,
    ESTADO_SIN_DATOS,
    ESTADO_TERMINADO,
    atar_hilo_a_jvm,
    calificar,
    carpeta_de_tabla,
    carpeta_del_dia,
    conexion_bantotal,
    conexion_sqlserver,
    config_extraccion,
    duracion,
    host,
    lista_columnas,
    nuevo_batch_id,
    recortar,
    ruta_parquet,
    separar_lista,
    validar_identificador,
)


logger = logging.getLogger(__name__)

# Columnas que devuelve sql_parametros_parquet, en este orden. Se listan aqui
# para no depender de como haya quedado el SELECT de la Variable: si alguien
# cambia el orden, se detecta al desempaquetar y no meses despues con datos
# en la columna equivocada.
COLUMNAS_CATALOGO = ("ESQUEMA", "TABLA", "COLUMNAS", "FILTRO", "ACTIVO",
                     "NOMBRE_PARQUET", "TIPOS")


# ============================================================================
# CATALOGO Y FECHA DE PROCESO
# ============================================================================
def leer_fecha_proceso(cur, config: dict[str, Any]) -> date:
    """Ejecuta sql_fecha y devuelve la fecha de proceso del negocio.

    OJO: esta consulta va contra BANTOTAL, no contra SQL Server, y es la unica
    diferencia de fondo con el pipeline de SingleStore.

    La razon es el huevo y la gallina. Alli sql_fecha lee STG_FST017 del propio
    DataHub, que ya existe porque otro proceso la cargo antes. Aqui STG_FST017
    en SQL Server la llena ESTE pipeline: el primer dia estaria vacia y la
    extraccion moriria con "no devolvio ninguna fecha" sin haber extraido nada.

    Leyendola del core se acaba el problema: FST017 siempre esta, siempre tiene
    la fecha de cierre al dia, y es la fuente de la que sale la copia que
    acabara en STG de todos modos.

    La fecha es de NEGOCIO, no la del reloj. Es la que nombra la carpeta del dia
    y la que queda en la bitacora, asi que una corrida que se lanza a las 2 de
    la madrugada sigue archivandose bajo el dia contable correcto.
    """
    cur.execute(config["sql_fecha"])
    fila = cur.fetchone()
    if not fila or fila[0] is None:
        raise AirflowException(
            f"sql_fecha no devolvio ninguna fecha de proceso.\n"
            f"  Consulta: {config['sql_fecha']}\n"
            f"Sin fecha de proceso no se puede nombrar la carpeta ni registrar "
            f"la corrida."
        )

    valor = fila[0]
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor

    # Bantotal guarda las fechas como un numero yyyymmdd (PGFCIE = 20260926),
    # asi que se aceptan las dos formas: el entero del core y el ISO que
    # devuelve un CAST hecho en la propia consulta.
    texto = str(valor).strip()
    if texto.endswith(".0"):          # DECIMAL sin escala llega como 20260926.0
        texto = texto[:-2]
    for formato, recorte in (("%Y%m%d", 8), ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(texto[:recorte], formato).date()
        except ValueError:
            continue
    raise AirflowException(
        f"sql_fecha devolvio {valor!r}, que no es una fecha reconocible.\n"
        f"  Consulta: {config['sql_fecha']}\n"
        f"Se esperaba una fecha, un yyyymmdd de 8 digitos o un yyyy-mm-dd."
    )


def leer_catalogo(cur, config: dict[str, Any]) -> list[dict[str, Any]]:
    """Filas activas de CTL_PARAMETROS_PARQUET."""
    cur.execute(config["sql_parametros_parquet"])
    filas = cur.fetchall()
    if not filas:
        raise AirflowException(
            f"No hay ninguna tabla activa en el catalogo.\n"
            f"  Consulta: {config['sql_parametros_parquet']}\n"
            f"La corrida no se abre porque no habria nada que extraer."
        )
    return [dict(zip(COLUMNAS_CATALOGO, fila)) for fila in filas]


def filtrar_procesos(catalogo: list[dict], config: dict[str, Any],
                     tipo_ejecucion: str = "diario") -> list[dict]:
    """Cruza el catalogo de la base con la lista 'procesos' de la Variable.

    La Variable dice QUE procesos entran en una corrida diaria, semanal o
    mensual; la base dice COMO se extrae cada tabla. Una tabla tiene que estar
    en los dos sitios para entrar.
    """
    flag = {"diario": "estado_diario",
            "semanal": "estado_semanal",
            "mensual": "estado_mensual"}.get(tipo_ejecucion)
    if flag is None:
        raise AirflowException(
            f"tipo_ejecucion={tipo_ejecucion!r} no soportado. "
            f"Use diario, semanal o mensual."
        )

    activos = {
        (p["nombre_proceso"].upper(), p["nombre_esquema"].upper()): p
        for p in (config.get("procesos") or [])
        if int(p.get("estado", 0)) == 1 and int(p.get(flag, 0)) == 1
    }

    seleccion = []
    for fila in catalogo:
        if str(fila.get("ACTIVO", "")).strip().upper() != "S":
            continue
        nombre = str(fila["NOMBRE_PARQUET"] or fila["TABLA"]).upper()
        clave = (nombre, str(fila["ESQUEMA"]).upper())
        if clave not in activos:
            continue
        seleccion.append({**fila, "_prioridad": int(activos[clave].get("prioridad", 99))})

    if not seleccion:
        raise AirflowException(
            f"Ninguna tabla del catalogo coincide con los procesos activos de "
            f"la Variable para tipo_ejecucion={tipo_ejecucion!r}.\n"
            f"Revisa que NOMBRE_PARQUET y ESQUEMA de CTL_PARAMETROS_PARQUET "
            f"coincidan con nombre_proceso y nombre_esquema de la Variable."
        )
    return sorted(seleccion, key=lambda f: (f["_prioridad"], str(f["TABLA"])))


def construir_select(fila: dict[str, Any]) -> str:
    """Arma el SELECT contra Bantotal.

    Los nombres van concatenados porque ningun motor los admite como parametro,
    pero ya pasaron por validar_identificador.

    FILTRO es SQL libre escrito por el equipo de datos en el catalogo. No se
    puede validar sin un parser, asi que se documenta como lo que es: una
    columna con permisos de escribir SQL contra el core. Quien pueda editar
    CTL_PARAMETROS_PARQUET puede consultar cualquier cosa del origen.
    """
    esquema = validar_identificador(fila["ESQUEMA"], "CTL_PARAMETROS_PARQUET.ESQUEMA")
    tabla = validar_identificador(fila["TABLA"], "CTL_PARAMETROS_PARQUET.TABLA")
    columnas = lista_columnas(separar_lista(fila.get("COLUMNAS")),
                              "CTL_PARAMETROS_PARQUET.COLUMNAS", motor="db2i")

    sql = f"SELECT {columnas} FROM {calificar(esquema, tabla)}"
    filtro = (fila.get("FILTRO") or "").strip()
    if filtro:
        sql += f" WHERE {filtro}"
    return sql


# ============================================================================
# BITACORA (ctl_proceso_parquet, en SQL Server)
# ============================================================================
def _abrir_log(conn, config: dict[str, Any], fila: dict, batch_id: str,
               fecha_proceso: date) -> int:
    tabla = config["tb_proceso_parquet"]
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO {tabla} "
        f"(nom_proceso, esquema, tabla_origen, batch_id, fecha_proceso, "
        f" fec_inicio, estado, host_name) "
        f"OUTPUT INSERTED.id_log "
        f"VALUES (?, ?, ?, ?, ?, SYSDATETIME(), ?, ?)",
        ("EXTRACCION_BANTOTAL", str(fila["ESQUEMA"]), str(fila["TABLA"]),
         batch_id, fecha_proceso.strftime("%Y-%m-%d"), ESTADO_EJECUTANDO, host()),
    )
    id_log = int(cur.fetchone()[0])
    conn.commit()
    return id_log


def _cerrar_log(conn, config: dict[str, Any], id_log: int, estado: str, **campos) -> None:
    """Cierra la fila de la bitacora.

    Va envuelto en try/except a proposito: si esto fallara dentro del except de
    la extraccion, la excepcion del log taparia la real y el log de la tarea
    mostraria un problema de escritura en vez de la causa.
    """
    tabla = config["tb_proceso_parquet"]
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE {tabla} SET "
            f"  estado = ?, fec_termino = SYSDATETIME(), duracion_segundos = ?, "
            f"  filas_procesadas = COALESCE(?, filas_procesadas), "
            f"  archivo_parquet  = COALESCE(?, archivo_parquet), "
            f"  msg_error = ? "
            f"WHERE id_log = ?",
            (estado, campos.get("segundos"), campos.get("filas"),
             campos.get("archivo"), campos.get("error"), id_log),
        )
        conn.commit()
    except Exception as exc:
        logger.error("No se pudo cerrar la fila %s de la bitacora: %s", id_log, exc)



# ============================================================================
# TIPOS DECLARADOS EN EL CATALOGO  (columna TIPOS de CTL_PARAMETROS_PARQUET)
# ----------------------------------------------------------------------------
# POR QUE EXISTE ESTO
#
# El DB2 del core devuelve DECIMAL sin escala y campos de texto con espacios o
# cadenas vacias donde deberia haber NULL. pandas infiere el tipo bloque a
# bloque, asi que la MISMA columna puede salir int64 en el primer chunk y
# float64 en el siguiente por un solo nulo. Peor: si en los primeros
# chunk_size registros una columna viene entera a NULL, pyarrow la infiere de
# tipo 'null', y el bloque siguiente -con valores de verdad- ya no castea
# contra ese esquema. El ParquetWriter aborta a media escritura y deja un
# archivo sin footer.
#
# La columna TIPOS del catalogo esta justo para eso, y hasta ahora era codigo
# muerto: se leia del catalogo, se guardaba en el diccionario de la fila y no
# la usaba nadie.
#
# Formato:   COLUMNA:TIPO|COLUMNA:TIPO|...
# Ejemplo:   PGCOD:DECIMAL(3,0)|FSH005TCV:DECIMAL(17,8)|NOMBRE:VARCHAR(50)
# ============================================================================
def parsear_tipos(tipos_str: str | None, tabla: str = "") -> dict[str, dict]:
    """Convierte la cadena TIPOS en un mapa {columna: {type, precision, scale}}.

    Falla con un mensaje que nombra la tabla y el fragmento malo. Un catalogo
    mal escrito tiene que detenerse aqui, no producir un parquet con tipos
    distintos a los declarados.
    """
    import re

    mapa: dict[str, dict] = {}
    if not tipos_str or not str(tipos_str).strip():
        return mapa

    for item in str(tipos_str).split("|"):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise AirflowException(
                f"[{tabla}] TIPOS mal formado en CTL_PARAMETROS_PARQUET: {item!r}. "
                f"El formato es COLUMNA:TIPO separado por |, "
                f"por ejemplo  PGCOD:DECIMAL(3,0)|NOMBRE:VARCHAR(50)"
            )
        # split con limite 1: un tipo podria llevar dos puntos en el futuro y
        # la version anterior reventaba con un ValueError sin contexto.
        col, tipo = item.split(":", 1)
        col, tipo = col.strip(), tipo.strip().lower()

        if tipo.startswith(("char", "varchar", "nchar", "nvarchar")):
            mapa[col] = {"type": "string"}
        elif tipo.startswith(("decimal", "numeric")):
            m = re.search(r"\((\d+)\s*,\s*(\d+)\)", tipo)
            if not m:
                raise AirflowException(
                    f"[{tabla}] DECIMAL sin precision en TIPOS: {item!r}. "
                    f"Escriba DECIMAL(p,s), por ejemplo DECIMAL(17,8). "
                    f"Sin precision no se puede fijar el tipo del parquet."
                )
            mapa[col] = {"type": "decimal",
                         "precision": int(m.group(1)), "scale": int(m.group(2))}
        elif tipo in ("int", "integer", "smallint"):
            mapa[col] = {"type": "int"}
        elif tipo == "bigint":
            mapa[col] = {"type": "bigint"}
        elif tipo in ("float", "double", "real"):
            mapa[col] = {"type": "float"}
        else:
            # Lo desconocido va a texto: es la unica conversion que nunca
            # pierde informacion.
            logger.warning("[%s] TIPOS: tipo no reconocido %r en %r, se trata como texto.",
                           tabla, tipo, col)
            mapa[col] = {"type": "string"}
    return mapa


def aplicar_tipos(bloque, mapa: dict[str, dict], tabla: str):
    """Normaliza el bloque segun los tipos declarados, ANTES de pasarlo a Arrow.

    Lo que de verdad arregla aqui es la cadena vacia: el core devuelve '' en
    campos numericos sin valor, y '' no es NULL ni es cero. Sin esta pasada,
    pyarrow infiere texto para toda la columna y el parquet acaba con numeros
    guardados como cadenas.

    A diferencia de la version del pipeline antiguo, aqui BIGINT si se
    convierte: alla se declaraba en el mapa y luego la cadena de elif no lo
    contemplaba, asi que una columna BIGINT se quedaba sin tocar.
    """
    import pandas as pd
    from decimal import Decimal, InvalidOperation

    for col, cfg in mapa.items():
        if col not in bloque.columns:
            logger.warning(
                "[%s] TIPOS declara la columna %r, que no viene en el SELECT. "
                "Revise COLUMNAS y TIPOS en el catalogo: se ignora.", tabla, col)
            continue

        tipo = cfg["type"]

        if tipo == "string":

            def _a_string(v):
                if pd.isna(v):
                    return None
                return str(v)

            bloque[col] = bloque[col].apply(_a_string)

        elif tipo in ("int", "bigint", "float"):
            vacias = int((bloque[col] == "").sum()) if bloque[col].dtype == object else 0
            if vacias:
                bloque[col] = bloque[col].replace("", None)
            bloque[col] = pd.to_numeric(bloque[col], errors="coerce")
            if vacias:
                logger.info("[%s] %s: %s cadena(s) vacia(s) -> NULL", tabla, col, vacias)

        elif tipo == "decimal":
            vacias = int((bloque[col] == "").sum()) if bloque[col].dtype == object else 0
            if vacias:
                bloque[col] = bloque[col].replace("", None)

            def _a_decimal(v):
                if pd.isna(v):
                    return None
                try:
                    return Decimal(str(v))
                except (InvalidOperation, ValueError) as exc:
                    raise AirflowException(
                        f"[{tabla}] {col}: no se pudo convertir {v!r} a DECIMAL "
                        f"({exc}). Revise TIPOS en el catalogo o el dato de origen."
                    ) from exc

            bloque[col] = bloque[col].apply(_a_decimal)
            if vacias:
                logger.info("[%s] %s: %s cadena(s) vacia(s) -> NULL", tabla, col, vacias)

    return bloque


def esquema_fijo(tabla_arrow, mapa: dict[str, dict]):
    """Esquema de Arrow con los tipos DECLARADOS, no los inferidos.

    Esta es la pieza que evita el fallo al escribir el parquet. La version del
    pipeline antiguo solo sustituia el campo cuando el tipo era DECIMAL; para
    todo lo demas dejaba lo que pyarrow hubiera inferido del primer bloque, que
    es justo donde esta el problema: una columna entera a NULL en los primeros
    chunk_size registros se infiere como 'null' y el bloque siguiente ya no
    puede castearse contra ella.

    Aqui se fija el tipo de TODA columna declarada.
    """
    import pyarrow as pa

    equivalencias = {
        "int":    pa.int32(),
        "bigint": pa.int64(),
        "float":  pa.float64(),
        "string": pa.string(),
    }

    campos = []
    for campo in tabla_arrow.schema:
        cfg = mapa.get(campo.name)
        if cfg is None:
            campos.append(campo)
            continue
        if cfg["type"] == "decimal":
            tipo = pa.decimal128(cfg["precision"], cfg["scale"])
        else:
            tipo = equivalencias[cfg["type"]]
        campos.append(pa.field(campo.name, tipo, nullable=True))
    return pa.schema(campos)


# ============================================================================
# EXTRACCION DE UNA TABLA
# ============================================================================
def extraer_tabla(fila: dict[str, Any], config: dict[str, Any], batch_id: str,
                  fecha_proceso: date) -> dict[str, Any]:
    """Consulta Bantotal, escribe el parquet y registra el resultado.

    Cada tabla abre sus propias conexiones porque se ejecutan en hilos: una
    conexion JDBC compartida entre hilos da errores intermitentes que no se
    reproducen al depurar.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Primera linea del hilo, antes de tocar nada de Java. Sin esto la JVM
    # aborta el proceso del worker entero en vez de lanzar una excepcion. El
    # porque esta explicado en atar_hilo_a_jvm().
    atar_hilo_a_jvm()

    inicio = datetime.now()
    tabla = str(fila["TABLA"])
    ruta = ruta_parquet(config, fecha_proceso, tabla)
    limite_error = int(config["error_size_limit"])

    conn_log = conexion_sqlserver(config)
    id_log = _abrir_log(conn_log, config, fila, batch_id, fecha_proceso)

    conn_bt = None
    escritor = None
    filas_totales = 0

    try:
        os.makedirs(carpeta_de_tabla(config, fecha_proceso, tabla), exist_ok=True)
        sql = construir_select(fila)
        logger.info("[%s] %s", tabla, sql)

        conn_bt = conexion_bantotal(config)
        chunk = int(config["chunk_size"])
        esquema_arrow: pa.Schema | None = None

        # Tipos declarados en el catalogo. Si la columna TIPOS viene vacia el
        # mapa queda vacio y todo se comporta como antes: pyarrow infiere.
        mapa_tipos = parsear_tipos(fila.get("TIPOS"), tabla)
        if mapa_tipos:
            logger.info("[%s] TIPOS declarados para %s columna(s): %s",
                        tabla, len(mapa_tipos), ", ".join(sorted(mapa_tipos)))

        for bloque in pd.read_sql(sql, conn_bt, chunksize=chunk):
            if bloque.empty:
                continue

            # Dos columnas que no vienen del core y que se inyectan aqui, con
            # el mismo criterio que el pipeline de SingleStore: FECHA_PROCESO
            # la PRIMERA y BATCH_ID la ULTIMA.
            #
            # No son decorativas. La bitacora dice que paso en cada corrida;
            # estas dos lo dicen desde los DATOS, que es lo que hace falta
            # cuando hay que arreglar algo:
            #
            #   DELETE FROM STG_FSH005 WHERE BATCH_ID = '20260926041500'
            #
            # deshace exactamente una corrida sin tocar el resto de la tabla.
            # Sin esa columna, la unica forma de volver atras es recargar todo.
            #
            # Se insertan en cada bloque y no al final porque el parquet se
            # escribe en trozos: no hay un momento en el que el DataFrame
            # completo este en memoria, que es justo lo que se evita con
            # chunksize en una tabla de millones de filas.
            # Normalizar ANTES de inyectar las dos columnas propias y antes
            # de pasar a Arrow: las cadenas vacias del core tienen que ser NULL
            # aqui, no en la carga.
            bloque = aplicar_tipos(bloque, mapa_tipos, tabla)


            bloque.insert(0, "FECHA_PROCESO", fecha_proceso)
            bloque["BATCH_ID"] = batch_id

            if esquema_arrow is None:
                # El esquema se fija con el PRIMER bloque y los siguientes se
                # convierten a el. Sin esto, pandas puede inferir int64 en un
                # bloque y float64 en el siguiente (por un solo NULL) y
                # ParquetWriter aborta a mitad del archivo, dejando un parquet
                # corrupto.
                tabla_arrow = pa.Table.from_pandas(bloque, preserve_index=False)
                # El esquema lo mandan los TIPOS del catalogo, no la inferencia
                # del primer bloque. Las columnas sin declarar conservan lo
                # inferido.
                esquema_arrow = esquema_fijo(tabla_arrow, mapa_tipos)
                if not tabla_arrow.schema.equals(esquema_arrow):
                    tabla_arrow = tabla_arrow.cast(esquema_arrow)
                escritor = pq.ParquetWriter(ruta, esquema_arrow, compression="snappy")
            else:
                tabla_arrow = pa.Table.from_pandas(
                    bloque, schema=esquema_arrow, preserve_index=False, safe=False)
            escritor.write_table(tabla_arrow)
            filas_totales += len(bloque)
            logger.info("[%s] %s filas acumuladas", tabla, f"{filas_totales:,}")

        if escritor is not None:
            escritor.close()
            escritor = None

        if filas_totales:
            estado = ESTADO_TERMINADO
        else:
            # Sin filas no se escribe archivo. Dejar un parquet vacio obligaria
            # a la carga a distinguir "vacio" de "corrupto", y una tabla vacia
            # truncaria la destino sin motivo.
            estado = ESTADO_SIN_DATOS
            ruta = None
            logger.info("[%s] sin filas. No se escribe parquet.", tabla)

        _cerrar_log(conn_log, config, id_log, estado, filas=filas_totales,
                    archivo=ruta, segundos=duracion(inicio))
        logger.info("[%s] %s | %s filas | %.2f s", tabla, estado,
                    f"{filas_totales:,}", duracion(inicio))
        return {"tabla": tabla, "esquema": str(fila["ESQUEMA"]), "estado": estado,
                "filas": filas_totales, "archivo": ruta, "id_log": id_log}

    except Exception as exc:
        if escritor is not None:
            try:
                escritor.close()
            except Exception:
                pass
        # Un parquet a medias es peor que ninguno: la carga lo abriria y subiria
        # una tabla incompleta sin que nada lo advirtiera.
        if ruta and os.path.exists(ruta):
            try:
                os.remove(ruta)
                logger.warning("[%s] se borro el parquet incompleto %s", tabla, ruta)
            except OSError:
                pass
        _cerrar_log(conn_log, config, id_log, ESTADO_ERROR, filas=filas_totales,
                    error=recortar(exc, limite_error), segundos=duracion(inicio))
        logger.error("[%s] ERROR: %s", tabla, exc)
        return {"tabla": tabla, "esquema": str(fila["ESQUEMA"]), "estado": ESTADO_ERROR,
                "filas": filas_totales, "archivo": None, "id_log": id_log,
                "error": str(exc)}
    finally:
        if conn_bt is not None:
            try:
                conn_bt.close()
            except Exception:
                pass
        conn_log.close()


# ============================================================================
# ENTRADA DEL DAG
# ============================================================================
def ejecutar_extraccion(tipo_ejecucion: str = "diario") -> str:
    """Extrae todas las tablas activas y devuelve el batch_id de la corrida.

    El batch_id viaja por XCom a la carga, para que suba exactamente lo que se
    acaba de extraer. Sin eso, si la extraccion de hoy falla, la carga podria
    recargar los archivos de ayer.

    Lanza si alguna tabla fallo, pero DESPUES de haber intentado todas: una
    tabla rota no debe impedir que las otras 37 se extraigan.
    """
    config = config_extraccion()
    batch_id = nuevo_batch_id()

    # Dos origenes distintos a proposito:
    #   la FECHA sale del core, que es quien la define (ver leer_fecha_proceso)
    #   el CATALOGO sale de SQL Server, que es donde se administra
    conn_bt = conexion_bantotal(config)
    try:
        fecha_proceso = leer_fecha_proceso(conn_bt.cursor(), config)
    finally:
        conn_bt.close()

    conn = conexion_sqlserver(config)
    try:
        cur = conn.cursor()
        catalogo = leer_catalogo(cur, config)
        seleccion = filtrar_procesos(catalogo, config, tipo_ejecucion)
    finally:
        conn.close()

    os.makedirs(carpeta_del_dia(config, fecha_proceso), exist_ok=True)
    logger.info(
        "Lote %s | fecha_proceso=%s | tipo=%s | %s tabla(s): %s",
        batch_id, fecha_proceso, tipo_ejecucion, len(seleccion),
        ", ".join(str(f["TABLA"]) for f in seleccion),
    )
    logger.info("Carpeta del dia: %s", carpeta_del_dia(config, fecha_proceso))

    max_worker = max(int(config["max_worker"]), 1)
    resultados: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_worker) as pool:
        futuros = {
            pool.submit(extraer_tabla, fila, config, batch_id, fecha_proceso): fila
            for fila in seleccion
        }
        for futuro in as_completed(futuros):
            resultados.append(futuro.result())

    ok = [r for r in resultados if r["estado"] == ESTADO_TERMINADO]
    sin_datos = [r for r in resultados if r["estado"] == ESTADO_SIN_DATOS]
    errores = [r for r in resultados if r["estado"] == ESTADO_ERROR]
    total_filas = sum(r["filas"] for r in resultados)

    logger.info(
        "Extraccion %s: %s ok, %s sin datos, %s con error | %s filas",
        batch_id, len(ok), len(sin_datos), len(errores), f"{total_filas:,}",
    )

    if errores:
        detalle = "\n".join(f"  - {r['tabla']}: {r.get('error', '')[:200]}" for r in errores)
        raise AirflowException(
            f"La extraccion del lote {batch_id} termino con {len(errores)} "
            f"tabla(s) en error:\n{detalle}\n"
            f"Las {len(ok)} correctas SI se extrajeron y estan en disco: la carga "
            f"puede subirlas. El detalle completo esta en "
            f"{config['tb_proceso_parquet']} para este batch_id."
        )
    return batch_id


# ============================================================================
# LIMPIEZA
# ============================================================================
def limpiar_parquet(dias_retencion: int | None = None) -> dict[str, Any]:
    """Borra las carpetas /data/bt2sql/<yyyyMMdd>/ mas viejas que la retencion.

    Solo toca carpetas cuyo nombre son 8 digitos que forman una fecha valida.
    Cualquier otra cosa dentro del directorio se deja intacta: la carpeta esta
    FUERA del contenedor y puede tener vecinos que no son de este pipeline.
    Con retencion 0 no borra nada.
    """
    config = config_extraccion()
    dias = int(config["retencion_dias"] if dias_retencion is None else dias_retencion)
    raiz = config["output_dir"]

    resumen = {"directorio": raiz, "dias_retencion": dias, "borradas": [], "conservadas": 0}
    if dias <= 0:
        logger.info("Retencion desactivada (dias=%s). No se borra nada.", dias)
        return resumen
    if not os.path.isdir(raiz):
        logger.warning("No existe %s. Nada que limpiar.", raiz)
        return resumen

    corte = date.today() - timedelta(days=dias)
    for nombre in sorted(os.listdir(raiz)):
        ruta = os.path.join(raiz, nombre)
        if not os.path.isdir(ruta) or len(nombre) != 8 or not nombre.isdigit():
            continue
        try:
            dia = datetime.strptime(nombre, "%Y%m%d").date()
        except ValueError:
            continue
        if dia < corte:
            shutil.rmtree(ruta, ignore_errors=True)
            resumen["borradas"].append(nombre)
        else:
            resumen["conservadas"] += 1

    logger.info(
        "Limpieza en %s: %s carpeta(s) borrada(s) %s, %s conservada(s) (retencion %s dias).",
        raiz, len(resumen["borradas"]), resumen["borradas"], resumen["conservadas"], dias,
    )
    return resumen
