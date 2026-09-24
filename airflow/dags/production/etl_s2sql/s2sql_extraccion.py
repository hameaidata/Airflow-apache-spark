"""
s2sql_extraccion - Mitad de ORIGEN del pipeline: SingleStore -> Parquet.

    preparar_lote()    abre el lote, lee el catalogo y devuelve una lista de
                       trabajos, uno por tabla activa. El DAG crea una tarea
                       por elemento de esa lista.

    extraer_tabla()    ejecuta UN trabajo: consulta SingleStore, escribe el
                       parquet y registra el resultado en el log de SQL Server.

    limpiar_parquet()  borra las carpetas de dias anteriores a la retencion.

POR QUE PASA POR PARQUET Y NO VA DIRECTO
----------------------------------------
Origen y destino quedan desacoplados. Si SQL Server esta caido o una carga
falla a mitad, el parquet del dia ya esta escrito: se reintenta la carga sin
volver a leer SingleStore, que es la parte cara y la que molesta al origen.
El archivo es ademas la evidencia de que se extrajo, y su ruta completa queda
guardada en CTL_S2SQL_LOG_CARGA.

LA MARCA DE AGUA
----------------
Para las tablas INCREMENTAL y MERGE, el punto de corte NO se guarda en una
tabla aparte: se deduce del propio log.

    SELECT MAX(marca_hasta) FROM CTL.CTL_S2SQL_LOG_CARGA
    WHERE tabla_origen = ? AND estado = 'TERMINADO'

Asi no hay dos sitios que puedan discrepar. Si una corrida falla, no escribe
marca_hasta, y la siguiente vuelve a arrancar desde donde arranco esta: no se
pierden filas por un fallo intermedio.
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import date, datetime, timedelta
from typing import Any

from airflow.exceptions import AirflowException

from .s2sql_comun import (
    ESTADO_EJECUTANDO,
    ESTADO_ERROR,
    ESTADO_SIN_DATOS,
    ESTADO_TERMINADO,
    MODOS_VALIDOS,
    MODO_INCREMENTAL,
    MODO_MERGE,
    MODO_REEMPLAZO,
    cargar_config,
    carpeta_del_lote,
    conexion_singlestore,
    conexion_sqlserver,
    corchetes,
    duracion,
    host,
    lista_columnas,
    nombre_control,
    nuevo_batch_id,
    recortar,
    ruta_parquet,
    separar_lista,
    validar_identificador,
)


logger = logging.getLogger(__name__)


# ============================================================================
# CATALOGO
# ============================================================================
COLUMNAS_CATALOGO = (
    "esquema_origen", "tabla_origen", "esquema_destino", "tabla_destino",
    "modo_carga", "columnas", "filtro_where", "columna_marca", "claves_merge",
    "batch_filas", "orden",
)


def leer_catalogo(cur, config: dict[str, Any], solo_tablas: list[str] | None = None) -> list[dict]:
    """Lee las filas activas de CTL_S2SQL_CATALOGO.

    Se listan las columnas una a una en vez de SELECT *: asi el orden de
    desempaquetado no depende de como quedo el CREATE TABLE, y anadir una
    columna al catalogo manana no rompe esta funcion en silencio.
    """
    sql = (
        f"SELECT {', '.join(COLUMNAS_CATALOGO)} "
        f"FROM {nombre_control(config, 'catalogo')} "
        f"WHERE activo = 1 "
        f"ORDER BY orden, tabla_origen"
    )
    cur.execute(sql)
    filas = [dict(zip(COLUMNAS_CATALOGO, fila)) for fila in cur.fetchall()]

    if solo_tablas:
        pedidas = {t.strip().upper() for t in solo_tablas}
        encontradas = {f["tabla_origen"].upper() for f in filas}
        faltan = pedidas - encontradas
        if faltan:
            raise AirflowException(
                f"Se pidio exportar {sorted(faltan)}, pero esas tablas no estan activas "
                f"en {nombre_control(config, 'catalogo')}."
            )
        filas = [f for f in filas if f["tabla_origen"].upper() in pedidas]

    return filas


def validar_fila_catalogo(fila: dict[str, Any]) -> dict[str, Any]:
    """Convierte una fila del catalogo en un trabajo, o explica por que no puede.

    Se valida aqui, con el catalogo entero a la vista, y no dentro de la tarea
    de cada tabla: una fila mal configurada se descubre al abrir el lote, no
    veinte minutos despues cuando le toca el turno.
    """
    donde = f"CTL_S2SQL_CATALOGO.{fila.get('tabla_origen')!r}"

    trabajo = {
        "esquema_origen": validar_identificador(fila["esquema_origen"], f"{donde}.esquema_origen"),
        "tabla_origen": validar_identificador(fila["tabla_origen"], f"{donde}.tabla_origen"),
        "esquema_destino": validar_identificador(fila["esquema_destino"], f"{donde}.esquema_destino"),
        "tabla_destino": validar_identificador(fila["tabla_destino"], f"{donde}.tabla_destino"),
        "modo_carga": str(fila["modo_carga"] or "").strip().upper(),
        "columnas": separar_lista(fila.get("columnas")),
        "filtro_where": (fila.get("filtro_where") or "").strip(),
        "columna_marca": (fila.get("columna_marca") or "").strip(),
        "claves_merge": separar_lista(fila.get("claves_merge")),
        "batch_filas": int(fila["batch_filas"]) if fila.get("batch_filas") else None,
    }

    if trabajo["modo_carga"] not in MODOS_VALIDOS:
        raise AirflowException(
            f"{donde}: modo_carga={trabajo['modo_carga']!r} no valido. "
            f"Use uno de {', '.join(MODOS_VALIDOS)}."
        )

    for col in trabajo["columnas"]:
        validar_identificador(col, f"{donde}.columnas")

    if trabajo["columna_marca"]:
        validar_identificador(trabajo["columna_marca"], f"{donde}.columna_marca")
    elif trabajo["modo_carga"] == MODO_INCREMENTAL:
        raise AirflowException(
            f"{donde}: modo_carga=INCREMENTAL exige columna_marca. Sin ella no hay "
            f"forma de saber desde donde continuar, y cada corrida duplicaria la tabla."
        )

    if trabajo["modo_carga"] == MODO_MERGE:
        if not trabajo["claves_merge"]:
            raise AirflowException(
                f"{donde}: modo_carga=MERGE exige claves_merge. Sin clave, el MERGE no "
                f"puede distinguir una fila nueva de una que cambio."
            )
        for col in trabajo["claves_merge"]:
            validar_identificador(col, f"{donde}.claves_merge")
        if trabajo["columnas"]:
            faltan = [c for c in trabajo["claves_merge"] if c not in trabajo["columnas"]]
            if faltan:
                raise AirflowException(
                    f"{donde}: las claves de MERGE {faltan} no estan en la lista de "
                    f"columnas. El parquet no las llevaria y el MERGE fallaria."
                )

    return trabajo


def marca_anterior(cur, config: dict[str, Any], tabla_origen: str) -> Any:
    """Ultimo valor cargado con exito para esta tabla, o None la primera vez."""
    cur.execute(
        f"SELECT MAX(marca_hasta) FROM {nombre_control(config, 'log')} "
        f"WHERE tabla_origen = ? AND estado = ? AND marca_hasta IS NOT NULL",
        (tabla_origen, ESTADO_TERMINADO),
    )
    fila = cur.fetchone()
    return fila[0] if fila else None


# ============================================================================
# APERTURA DEL LOTE
# ============================================================================
def preparar_lote(
    fecha_proceso: str | None = None,
    solo_tablas: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Abre el lote y devuelve un trabajo por tabla activa.

    El DAG mapea una tarea sobre cada elemento de la lista devuelta, asi que
    esta funcion decide la forma del grafo de la corrida. Por eso valida TODO
    el catalogo antes de devolver nada: es preferible no abrir el lote a
    abrirlo con una tabla que va a fallar seguro.

    fecha_proceso llega como 'YYYY-MM-DD' desde los params del DAG; si viene
    vacia se usa la fecha logica de la corrida, que el DAG ya resuelve.
    """
    config = cargar_config()
    batch_id = nuevo_batch_id()
    fecha = (
        datetime.strptime(fecha_proceso, "%Y-%m-%d").date()
        if fecha_proceso else date.today()
    )

    conn = conexion_sqlserver(config)
    try:
        cur = conn.cursor()
        filas = leer_catalogo(cur, config, solo_tablas)
        if not filas:
            raise AirflowException(
                f"No hay ninguna tabla con activo=1 en {nombre_control(config, 'catalogo')}. "
                f"El lote no se abre porque no habria nada que exportar."
            )

        tope = int(config["limites"]["tablas_por_corrida"])
        if len(filas) > tope:
            raise AirflowException(
                f"El catalogo tiene {len(filas)} tablas activas y el tope por corrida es "
                f"{tope}. Airflow crearia {len(filas)} tareas dinamicas de golpe. Sube "
                f"limites.tablas_por_corrida en la Variable S2SQL_EXPORT_CONFIG si de "
                f"verdad son todas."
            )

        trabajos = []
        for fila in filas:
            trabajo = validar_fila_catalogo(fila)
            trabajo.update(
                batch_id=batch_id,
                fecha_proceso=fecha.isoformat(),
                ruta_parquet=ruta_parquet(config, fecha, trabajo["tabla_origen"]),
            )
            if trabajo["columna_marca"]:
                previa = marca_anterior(cur, config, trabajo["tabla_origen"])
                # Viaja por XCom, asi que tiene que ser serializable a JSON.
                trabajo["marca_desde"] = str(previa) if previa is not None else None
            else:
                trabajo["marca_desde"] = None
            trabajos.append(trabajo)

        os.makedirs(carpeta_del_lote(config, fecha), exist_ok=True)

        cur.execute(
            f"INSERT INTO {nombre_control(config, 'lote')} "
            f"(batch_id, fecha_proceso, fec_inicio, estado, tablas_total, host_name) "
            f"VALUES (?, ?, SYSDATETIME(), ?, ?, ?)",
            (batch_id, fecha, ESTADO_EJECUTANDO, len(trabajos), host()),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(
        "Lote %s abierto | fecha_proceso=%s | %s tabla(s): %s",
        batch_id, fecha, len(trabajos), ", ".join(t["tabla_origen"] for t in trabajos),
    )
    for t in trabajos:
        logger.info(
            "  %-30s modo=%-11s desde=%s -> %s",
            t["tabla_origen"], t["modo_carga"], t["marca_desde"] or "(todo)", t["ruta_parquet"],
        )
    return trabajos


# ============================================================================
# EXTRACCION DE UNA TABLA
# ============================================================================
def construir_select(trabajo: dict[str, Any]) -> tuple[str, list]:
    """Arma el SELECT contra SingleStore y sus parametros enlazados.

    Los nombres van concatenados porque ningun motor los admite como
    parametro, pero ya pasaron por validar_identificador. El VALOR de la marca
    de agua si va enlazado: es el unico dato de la consulta que viene de una
    corrida anterior.

    filtro_where es SQL libre escrito por el equipo de datos en el catalogo.
    No se puede validar sin un parser, asi que se documenta como lo que es:
    una columna con permisos de escribir SQL. Quien pueda editar el catalogo
    puede escribir cualquier consulta contra el origen.
    """
    columnas = lista_columnas(trabajo["columnas"], "columnas") if trabajo["columnas"] else "*"
    sql = f"SELECT {columnas} FROM {corchetes(trabajo['esquema_origen'], trabajo['tabla_origen'])}"

    condiciones, parametros = [], []
    if trabajo["filtro_where"]:
        condiciones.append(f"({trabajo['filtro_where']})")
    if trabajo["columna_marca"] and trabajo["marca_desde"] is not None:
        condiciones.append(f"[{trabajo['columna_marca']}] > %s")
        parametros.append(trabajo["marca_desde"])

    if condiciones:
        sql += " WHERE " + " AND ".join(condiciones)
    if trabajo["columna_marca"]:
        # Ordenar por la marca hace que, si la corrida muere a mitad, lo que
        # quedo escrito sea un prefijo contiguo y no un conjunto salteado.
        sql += f" ORDER BY [{trabajo['columna_marca']}]"
    return sql, parametros


def extraer_tabla(trabajo: dict[str, Any]) -> dict[str, Any]:
    """Consulta SingleStore, escribe el parquet y registra el resultado.

    Devuelve el mismo trabajo enriquecido con filas, ruta y marca_hasta, que es
    lo que la tarea de carga recibe por XCom. Si la tabla no trajo filas, marca
    SIN_DATOS y lo dice: no es un error, es informacion.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    config = cargar_config()
    inicio = datetime.now()
    tabla = trabajo["tabla_origen"]
    ruta = trabajo["ruta_parquet"]
    limite_error = int(config["limites"]["mensaje_error"])

    conn_log = conexion_sqlserver(config)
    id_log = _abrir_log(conn_log, config, trabajo)

    conn_s2 = None
    escritor = None
    filas_totales = 0
    marca_hasta = None

    try:
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        sql, parametros = construir_select(trabajo)
        logger.info("[%s] %s", tabla, sql)

        conn_s2 = conexion_singlestore(config)
        chunk = int(config["lectura"]["chunk_filas"])
        esquema: pa.Schema | None = None

        for bloque in pd.read_sql(sql, conn_s2, params=parametros or None, chunksize=chunk):
            if bloque.empty:
                continue

            if esquema is None:
                # El esquema se fija con el PRIMER bloque y los siguientes se
                # convierten a el. Sin esto, pandas puede inferir int64 en un
                # bloque y float64 en el siguiente (por un solo NULL) y
                # ParquetWriter aborta a mitad del archivo con un error de
                # esquema incompatible, dejando un parquet corrupto.
                tabla_arrow = pa.Table.from_pandas(bloque, preserve_index=False)
                esquema = tabla_arrow.schema
                escritor = pq.ParquetWriter(
                    ruta, esquema, compression=config["parquet"]["compresion"]
                )
            else:
                tabla_arrow = pa.Table.from_pandas(
                    bloque, schema=esquema, preserve_index=False, safe=False
                )

            escritor.write_table(tabla_arrow)
            filas_totales += len(bloque)

            if trabajo["columna_marca"]:
                maximo = bloque[trabajo["columna_marca"]].max()
                if pd.notna(maximo):
                    valor = str(maximo)
                    marca_hasta = valor if marca_hasta is None else max(marca_hasta, valor)

            logger.info("[%s] %s filas acumuladas", tabla, f"{filas_totales:,}")

        if escritor is not None:
            escritor.close()
            escritor = None

        estado = ESTADO_TERMINADO if filas_totales else ESTADO_SIN_DATOS
        if not filas_totales:
            # Sin filas no se escribe archivo. Dejar un parquet vacio obligaria
            # a la carga a distinguir "vacio" de "corrupto", y en modo
            # REEMPLAZO un archivo vacio truncaria la tabla destino.
            logger.info(
                "[%s] sin filas nuevas%s. No se escribe parquet y la carga se saltara.",
                tabla,
                f" desde {trabajo['marca_desde']}" if trabajo["marca_desde"] else "",
            )
            ruta = None

        _cerrar_log(
            conn_log, config, id_log, estado,
            filas_leidas=filas_totales,
            marca_hasta=marca_hasta,
            archivo=ruta,
            segundos=duracion(inicio),
        )
        logger.info(
            "[%s] %s | %s filas | %.2f s | marca_hasta=%s",
            tabla, estado, f"{filas_totales:,}", duracion(inicio), marca_hasta,
        )
        return {**trabajo, "estado": estado, "filas_leidas": filas_totales,
                "ruta_parquet": ruta, "marca_hasta": marca_hasta, "id_log": id_log}

    except Exception as exc:
        if escritor is not None:
            try:
                escritor.close()
            except Exception:
                pass
        # Un parquet a medias es peor que ninguno: la carga lo abriria y
        # cargaria una tabla incompleta sin que nada lo advirtiera.
        if os.path.exists(ruta or ""):
            try:
                os.remove(ruta)
                logger.warning("[%s] se borro el parquet incompleto %s", tabla, ruta)
            except OSError:
                pass
        _cerrar_log(
            conn_log, config, id_log, ESTADO_ERROR,
            filas_leidas=filas_totales,
            error=recortar(exc, limite_error),
            segundos=duracion(inicio),
        )
        raise
    finally:
        if conn_s2 is not None:
            try:
                conn_s2.close()
            except Exception:
                pass
        conn_log.close()


# ============================================================================
# LOG (en SQL Server, el destino)
# ============================================================================
def _abrir_log(conn, config: dict[str, Any], trabajo: dict[str, Any]) -> int:
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO {nombre_control(config, 'log')} "
        f"(batch_id, tabla_origen, tabla_destino, modo_carga, fec_inicio, estado, "
        f" marca_desde, host_name) "
        f"OUTPUT INSERTED.id_log "
        f"VALUES (?, ?, ?, ?, SYSDATETIME(), ?, ?, ?)",
        (trabajo["batch_id"], trabajo["tabla_origen"], trabajo["tabla_destino"],
         trabajo["modo_carga"], ESTADO_EJECUTANDO, trabajo["marca_desde"], host()),
    )
    id_log = int(cur.fetchone()[0])
    conn.commit()
    return id_log


def _cerrar_log(conn, config: dict[str, Any], id_log: int, estado: str, **campos) -> None:
    """Cierra la fila del log.

    Va envuelto en try/except a proposito: si esto fallara dentro del except de
    extraer_tabla, la excepcion del log taparia la excepcion real y el log de
    la tarea mostraria un problema de escritura en vez de la causa.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE {nombre_control(config, 'log')} SET "
            f"  estado = ?, fec_fin = SYSDATETIME(), duracion_segundos = ?, "
            f"  filas_leidas = COALESCE(?, filas_leidas), "
            f"  filas_escritas = COALESCE(?, filas_escritas), "
            f"  filas_insertadas = COALESCE(?, filas_insertadas), "
            f"  filas_actualizadas = COALESCE(?, filas_actualizadas), "
            f"  marca_hasta = COALESCE(?, marca_hasta), "
            f"  archivo_parquet = COALESCE(?, archivo_parquet), "
            f"  msg_error = ? "
            f"WHERE id_log = ?",
            (estado, campos.get("segundos"),
             campos.get("filas_leidas"), campos.get("filas_escritas"),
             campos.get("filas_insertadas"), campos.get("filas_actualizadas"),
             campos.get("marca_hasta"), campos.get("archivo"),
             campos.get("error"), id_log),
        )
        conn.commit()
    except Exception as exc:
        logger.error("No se pudo cerrar la fila %s del log: %s", id_log, exc)


# ============================================================================
# LIMPIEZA
# ============================================================================
def limpiar_parquet(dias_retencion: int | None = None) -> dict[str, Any]:
    """Borra las carpetas /data/s2sql/<yyyyMMdd>/ mas viejas que la retencion.

    Solo toca carpetas cuyo nombre son 8 digitos que forman una fecha valida.
    Cualquier otra cosa dentro del directorio se deja intacta: la carpeta esta
    FUERA del contenedor y puede tener vecinos que no son de este pipeline.
    Con retencion 0 no borra nada.
    """
    config = cargar_config()
    dias = int(config["parquet"]["retencion_dias"] if dias_retencion is None else dias_retencion)
    raiz = config["parquet"]["directorio"]

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
