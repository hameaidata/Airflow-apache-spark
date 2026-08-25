-- ===========================================================================
-- Procedimientos de prueba para el DAG demo_completo
-- ---------------------------------------------------------------------------
-- Sirven para probar el DAG HOY, sin esperar a que TI abra SQL Server o DB2.
-- Se crean en una base aparte dentro del mismo PostgreSQL que ya corre.
--
-- IMPORTANTE: se crea una base NUEVA llamada 'negocio_pruebas'. NO se toca la
-- base de metadatos de Airflow. Mezclar tablas de negocio con el esquema de
-- Airflow es un problema al actualizar y complica cualquier restauracion.
--
-- COMO EJECUTARLO
--
--   1. Crear la base (una sola vez):
--        docker compose -f docker-compose.windows.yml exec airflow-postgres \
--          psql -U airflow -d postgres -c "CREATE DATABASE negocio_pruebas"
--
--   2. Cargar este archivo:
--        docker compose -f docker-compose.windows.yml exec -T airflow-postgres \
--          psql -U airflow -d negocio_pruebas < scripts/sql/demo_procedimientos_postgres.sql
--
--   3. Crear la Connection en Airflow (Admin -> Connections):
--        Conn Id    : bd_negocio
--        Conn Type  : Postgres
--        Host       : postgres
--        Schema     : negocio_pruebas
--        Login      : airflow
--        Password   : (el POSTGRES_PASSWORD de su .env)
--        Port       : 5432
--
--      Ojo: Host es 'postgres', no 'localhost'. Es el nombre del servicio
--      dentro de la red de contenedores.
-- ===========================================================================

-- --- Tablas de negocio de mentira ------------------------------------------

CREATE TABLE IF NOT EXISTS ventas (
    id            BIGSERIAL PRIMARY KEY,
    fecha         DATE NOT NULL,
    sucursal      VARCHAR(50),
    monto         NUMERIC(14,2),
    procesado_en  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clientes (
    id            BIGSERIAL PRIMARY KEY,
    fecha         DATE NOT NULL,
    documento     VARCHAR(20),
    nombre        VARCHAR(200),
    procesado_en  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS resumen_diario (
    fecha         DATE PRIMARY KEY,
    total_ventas  NUMERIC(16,2),
    num_clientes  INTEGER,
    generado_en   TIMESTAMP DEFAULT NOW()
);


-- ===========================================================================
-- sp_demo_ventas — carga ventas del dia
--
-- Idempotente a proposito: borra antes de insertar. Ejecutarlo dos veces con
-- la misma fecha deja el mismo resultado. Sin eso, un reintento duplicaria.
-- ===========================================================================

CREATE OR REPLACE PROCEDURE public.sp_demo_ventas(p_fecha DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_insertadas INTEGER;
BEGIN
    DELETE FROM ventas WHERE fecha = p_fecha;

    INSERT INTO ventas (fecha, sucursal, monto)
    SELECT p_fecha,
           'SUC-' || LPAD((n % 5 + 1)::TEXT, 2, '0'),
           ROUND((RANDOM() * 10000)::NUMERIC, 2)
    FROM generate_series(1, 250) AS n;

    GET DIAGNOSTICS v_insertadas = ROW_COUNT;
    RAISE NOTICE 'sp_demo_ventas: % filas para %', v_insertadas, p_fecha;
END;
$$;


-- ===========================================================================
-- sp_demo_clientes — carga clientes del dia
-- ===========================================================================

CREATE OR REPLACE PROCEDURE public.sp_demo_clientes(p_fecha DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_insertadas INTEGER;
BEGIN
    DELETE FROM clientes WHERE fecha = p_fecha;

    INSERT INTO clientes (fecha, documento, nombre)
    SELECT p_fecha,
           LPAD((10000000 + n)::TEXT, 8, '0'),
           'Cliente de prueba ' || n
    FROM generate_series(1, 80) AS n;

    GET DIAGNOSTICS v_insertadas = ROW_COUNT;
    RAISE NOTICE 'sp_demo_clientes: % filas para %', v_insertadas, p_fecha;
END;
$$;


-- ===========================================================================
-- sp_demo_resumen — consolida. Depende de los dos anteriores.
-- ===========================================================================

CREATE OR REPLACE PROCEDURE public.sp_demo_resumen(p_fecha DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM resumen_diario WHERE fecha = p_fecha;

    INSERT INTO resumen_diario (fecha, total_ventas, num_clientes)
    SELECT p_fecha,
           COALESCE((SELECT SUM(monto)  FROM ventas   WHERE fecha = p_fecha), 0),
           COALESCE((SELECT COUNT(*)    FROM clientes WHERE fecha = p_fecha), 0);

    RAISE NOTICE 'sp_demo_resumen: consolidado %', p_fecha;
END;
$$;


-- ===========================================================================
-- sp_demo_falla — falla siempre, a proposito
--
-- Anadalo a la lista para comprobar que:
--   - la tarea que falla NO detiene a las demas
--   - el error queda registrado en la bitacora
--   - la ramificacion toma el camino 'hubo_errores'
-- ===========================================================================

CREATE OR REPLACE PROCEDURE public.sp_demo_falla(p_fecha DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Fallo intencional para probar el manejo de errores (fecha %)', p_fecha;
END;
$$;


-- ===========================================================================
-- Comprobar que quedaron creados
-- ===========================================================================
--   SELECT n.nspname, p.proname
--   FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
--   WHERE p.proname LIKE 'sp_demo%';
--
-- Y despues de ejecutar el DAG, la bitacora:
--   SELECT procedimiento, estado, duracion_seg, filas_afectadas, mensaje_error
--   FROM airflow_bitacora_sp ORDER BY iniciado_en DESC;
