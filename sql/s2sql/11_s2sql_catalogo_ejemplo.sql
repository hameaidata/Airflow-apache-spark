/* ===========================================================================
   S2SQL - Filas de ejemplo del catalogo, una por modo de carga
   ---------------------------------------------------------------------------
   Ejecutar DESPUES de 10_s2sql_control_sqlserver.sql, en la base destino.

   Las tres filas entran con activo = 0 a proposito: se activan una a una,
   despues de comprobar que la tabla destino existe y que las columnas
   coinciden. Activar las tres de golpe y lanzar el DAG es la forma mas rapida
   de acabar con tres errores distintos a la vez.

       UPDATE CTL.CTL_S2SQL_CATALOGO SET activo = 1 WHERE tabla_origen = 'TIPO_CAMBIO';

   ANTES DE ACTIVAR UNA FILA
   -------------------------
   La tabla DESTINO tiene que existir ya, con las columnas que el parquet va a
   traer y con tipos capaces de recibirlas. El pipeline NO crea tablas de
   negocio: crear la destino implica decidir tipos, claves e indices, y eso es
   una decision de modelado, no algo que deba inventar un proceso automatico.
   La tabla de staging si la crea (copiando los tipos de la destino con
   SELECT TOP 0 ... INTO), y por eso la destino es la que manda.
=========================================================================== */

USE [PON_AQUI_TU_BASE];
GO

SET NOCOUNT ON;
GO

DELETE FROM CTL.CTL_S2SQL_CATALOGO
WHERE tabla_origen IN ('TIPO_CAMBIO', 'MOVIMIENTO_DIARIO', 'CLIENTE');
GO


/* ---------------------------------------------------------------------------
   1. REEMPLAZO - tabla pequena que se vuelve a traer entera cada dia
   ---------------------------------------------------------------------------
   Es el modo por defecto y el que menos puede salir mal: la destino queda
   exactamente igual que el origen, sin que importe lo que paso antes. Deja de
   ser razonable cuando la tabla es grande, porque mueve todo cada vez.
--------------------------------------------------------------------------- */
INSERT INTO CTL.CTL_S2SQL_CATALOGO
    (esquema_origen, tabla_origen, esquema_destino, tabla_destino, modo_carga,
     columnas, filtro_where, columna_marca, claves_merge, batch_filas,
     activo, orden, descripcion)
VALUES
    ('BDS', 'TIPO_CAMBIO', 'dbo', 'TIPO_CAMBIO', 'REEMPLAZO',
     'FECHA, MONEDA, COMPRA, VENTA',     -- lista explicita, ver nota al final
     NULL,
     NULL,
     NULL,
     NULL,
     0, 1, 'Tipo de cambio diario. Pocas filas: se reemplaza entera.');


/* ---------------------------------------------------------------------------
   2. INCREMENTAL - tabla de movimientos que solo crece
   ---------------------------------------------------------------------------
   Cada corrida trae unicamente  columna_marca > ultima marca cargada. Mucho
   menos trafico, pero NO corrige filas que cambiaron en el origen: si un
   movimiento se anula editando la fila en vez de insertando una nueva, esa
   correccion no llega nunca. Para ese caso, MERGE.
--------------------------------------------------------------------------- */
INSERT INTO CTL.CTL_S2SQL_CATALOGO
    (esquema_origen, tabla_origen, esquema_destino, tabla_destino, modo_carga,
     columnas, filtro_where, columna_marca, claves_merge, batch_filas,
     activo, orden, descripcion)
VALUES
    ('BDS', 'MOVIMIENTO_DIARIO', 'dbo', 'MOVIMIENTO_DIARIO', 'INCREMENTAL',
     'ID_MOVIMIENTO, FECHA_MOVIMIENTO, ID_CUENTA, MONTO, MONEDA, ESTADO',
     'ESTADO <> ''ANULADO''',            -- SQL libre; ver el aviso de seguridad
     'FECHA_MOVIMIENTO',                 -- obligatoria en este modo
     NULL,
     10000,                              -- lotes mas grandes: tabla estrecha
     0, 2, 'Movimientos. Solo crece, se trae por fecha.');


/* ---------------------------------------------------------------------------
   3. MERGE - maestro que se corrige hacia atras
   ---------------------------------------------------------------------------
   Actualiza las filas que ya estaban e inserta las nuevas, comparando por
   claves_merge. Es lo que se quiere cuando el origen edita el pasado.

   La clave TIENE que ser unica en el origen. Si no, el pipeline cancela antes
   de tocar nada y lo dice; sin esa comprobacion, SQL Server aborta con el
   error 8672, cuyo texto no menciona ni la tabla ni la clave.

   Combinar columna_marca con MERGE es util y esta permitido: se traen solo las
   filas modificadas desde la ultima corrida, y de esas se actualizan las que
   ya existian. Requiere que el origen mantenga una FECHA_MODIFICACION fiable.
--------------------------------------------------------------------------- */
INSERT INTO CTL.CTL_S2SQL_CATALOGO
    (esquema_origen, tabla_origen, esquema_destino, tabla_destino, modo_carga,
     columnas, filtro_where, columna_marca, claves_merge, batch_filas,
     activo, orden, descripcion)
VALUES
    ('BDS', 'CLIENTE', 'dbo', 'CLIENTE', 'MERGE',
     'ID_CLIENTE, NOMBRE, DOCUMENTO, SEGMENTO, FECHA_MODIFICACION',
     NULL,
     'FECHA_MODIFICACION',               -- opcional aqui: reduce lo que se mueve
     'ID_CLIENTE',                       -- obligatoria en este modo
     NULL,
     0, 3, 'Maestro de clientes. El origen corrige hacia atras.');
GO


/* ===========================================================================
   COMPROBACIONES ANTES DE ACTIVAR
=========================================================================== */

-- 1. La tabla destino existe?
/*
SELECT  c.tabla_origen, c.esquema_destino, c.tabla_destino,
        CASE WHEN OBJECT_ID(c.esquema_destino + '.' + c.tabla_destino) IS NULL
             THEN 'NO EXISTE - creala antes de activar'
             ELSE 'ok' END AS destino
FROM    CTL.CTL_S2SQL_CATALOGO c;
*/

-- 2. Las columnas del catalogo existen en la destino?
--    Esta es la comprobacion que evita el error mas comun: una columna bien
--    escrita en el origen y mal escrita en el catalogo. El pipeline lo
--    detectaria al cargar, pero despues de haber movido el archivo entero.
/*
SELECT  c.tabla_origen, LTRIM(v.value) AS columna,
        CASE WHEN COL_LENGTH(c.esquema_destino + '.' + c.tabla_destino, LTRIM(v.value)) IS NULL
             THEN 'NO ESTA EN LA DESTINO'
             ELSE 'ok' END AS estado
FROM    CTL.CTL_S2SQL_CATALOGO c
CROSS   APPLY STRING_SPLIT(c.columnas, ',') v
WHERE   c.columnas IS NOT NULL
  AND   COL_LENGTH(c.esquema_destino + '.' + c.tabla_destino, LTRIM(v.value)) IS NULL;
*/

-- 3. La clave de MERGE es realmente unica en la destino?
--    Si no lo es en la destino, tampoco lo sera en el origen.
/*
SELECT COUNT(*) AS filas, COUNT(DISTINCT ID_CLIENTE) AS claves_distintas
FROM   dbo.CLIENTE;
*/


/* ===========================================================================
   NOTAS
=========================================================================== */
--
-- SOBRE columnas
--   Dejarla NULL equivale a SELECT *. Funciona, pero acopla la carga a la
--   forma del origen: el dia que alguien anada una columna en SingleStore,
--   el parquet la traera, la destino no la tendra y la carga fallara sin
--   que nadie hubiera tocado este pipeline. Con lista explicita, esa columna
--   simplemente se ignora hasta que se decida incorporarla.
--
-- SOBRE filtro_where
--   Es SQL que se concatena al WHERE del origen. No se puede validar sin un
--   parser. Quien tenga UPDATE sobre CTL_S2SQL_CATALOGO puede escribir
--   cualquier consulta contra SingleStore, asi que ese permiso equivale a
--   lectura sobre toda la base de origen. Conviene que el catalogo lo edite
--   un rol distinto del que consume los datos.
--
-- SOBRE LOS TIPOS
--   El parquet conserva los tipos que devolvio SingleStore. La tabla de
--   staging se crea copiando los tipos de la DESTINO, asi que es ahi donde se
--   resuelve la conversion. Los dos desajustes que aparecen en la practica:
--     - DECIMAL con mas precision en el origen que en la destino: se trunca o
--       falla segun el caso. Iguala las precisiones.
--     - Texto: si la destino tiene VARCHAR(50) y el origen trae 80
--       caracteres, la carga falla dentro de la transaccion y no se aplica
--       nada. Es el comportamiento correcto, pero el mensaje de SQL Server no
--       dice que columna: si pasa, mira la staging, que si tiene los datos.
--
-- SOBRE EL PRIMER ARRANQUE DE UNA TABLA INCREMENTAL
--   Sin filas previas en el log, marca_desde es NULL y la primera corrida se
--   trae la tabla ENTERA. Si eso es demasiado, carga el historico una vez por
--   fuera y despues inserta a mano una fila TERMINADO en CTL_S2SQL_LOG_CARGA
--   con la marca_hasta correspondiente; la siguiente corrida arrancara desde
--   ahi.
