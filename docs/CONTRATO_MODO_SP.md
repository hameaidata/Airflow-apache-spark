# Contrato de modo de los stored procedures de BDS

Los siete procedimientos de BDS reciben **los mismos tres parámetros, en el
mismo orden, con el mismo significado**. Quien sepa llamar a uno sabe llamar a
todos.

```
P_MODO       VARCHAR(10)   'DIARIO' | 'RANGO' | 'MES'
P_FECHA_INI  DATE
P_FECHA_FIN  DATE
```

Cada procedimiento puede añadir parámetros propios **después** de estos tres
—`P_DIAS`, `P_OPCION`— pero nunca antes y nunca en otro orden.

---

## Lo que hace cada modo

Los tres modos resuelven lo mismo: **un rango cerrado de fechas
`[V_DESDE, V_HASTA]`**. A partir de ahí, cada procedimiento hace su trabajo
sobre ese rango. La diferencia entre los modos es sólo cómo se calcula.

| `P_MODO` | `P_FECHA_INI` | `P_FECHA_FIN` | Rango resultante |
|---|---|---|---|
| `DIARIO` | la fecha, o `NULL` | debe ser `NULL` | un solo día: `[INI, INI]` |
| `RANGO` | obligatoria | obligatoria | `[INI, FIN]`, ambos incluidos |
| `MES` | cualquier día del mes | debe ser `NULL` | del día 1 al último día de ese mes |

**`DIARIO` con `P_FECHA_INI` en `NULL`** es el caso de la corrida automática: el
procedimiento toma por su cuenta el `MAX(FECHA_PROCESO)` de su tabla origen. Así
el JSON del DAG queda estático —`["DIARIO", null, null]`— y no envejece. Cada
procedimiento documenta en su cabecera de qué tabla la saca.

**`MES` toma el mes de `P_FECHA_INI`, no el mes anterior.** `'2026-09-14'` procesa
del 1 al 30 de septiembre. Para reprocesar agosto se pasa cualquier día de
agosto. No hay un modo «mes anterior»: era ambiguo el último día del mes y se
expresa igual de bien con `MES` y la fecha correspondiente.

**`MES` sobre el mes en curso no se recorta.** Si hoy es 15 de septiembre y pide
`MES` con `'2026-09-15'`, el rango es del 1 al 30. Los días que aún no existen en
el origen no producen filas: el `DELETE` no borra nada porque no hay nada, y el
`INSERT` no inserta nada. Es inofensivo y hace que `MES` y `RANGO` se comporten
**exactamente igual** para las mismas fechas, que es lo que evita sorpresas.

---

## Lo que se rechaza, y con qué mensaje

Toda combinación inválida falla **antes** de tocar una sola fila, con un mensaje
que nombra el parámetro y el valor que llegó. Ninguna se ignora en silencio.

| Situación | Mensaje |
|---|---|
| `P_MODO` no es uno de los tres | `P_MODO='X' no existe. Use DIARIO, RANGO o MES.` |
| `P_MODO` es `NULL` | el mismo, con `NULL` |
| `RANGO` sin `P_FECHA_INI` o sin `P_FECHA_FIN` | `En modo RANGO, P_FECHA_INI y P_FECHA_FIN son obligatorias.` |
| `RANGO` con inicio posterior al fin | `El rango esta invertido: inicio=… fin=…` |
| `MES` con `P_FECHA_INI` en `NULL` | `En modo MES, P_FECHA_INI es obligatoria: cualquier dia del mes a procesar.` |
| `DIARIO` o `MES` con `P_FECHA_FIN` informada | `En modo …, P_FECHA_FIN debe ir en NULL. Para procesar varios dias use RANGO.` |
| `DIARIO` con `NULL` y origen vacío | `No hay ninguna FECHA_PROCESO en <tabla>. No se puede resolver la corrida diaria.` |

Esa última fila es la que más importa. La versión anterior de
`SP_CREAR_VARIACION_CONTABLE` terminaba en un `RAISE;` **sin mensaje** cuando la
fecha no se podía resolver, y el operador veía un error genérico que no decía
qué había pasado ni dónde mirar.

El rango también tiene un tope: **más de 366 días se rechaza**. No es una
limitación técnica, es una red: un reproceso de varios años suele ser un error de
tecleo, y descubrirlo cuatro horas después con las tablas a medias es caro.

---

## Por qué se cambió el esquema anterior

`SP_CREAR_VARIACION_CONTABLE` usaba `P_OPCION INT` con los valores 2, 3 y 4.
Revisándolo a fondo aparecieron cuatro problemas, y este contrato existe para no
propagarlos a los otros seis procedimientos.

**Las opciones 2 y 4 eran la misma rama.** Las dos ejecutaban

```sql
SELECT MAX(FECHA_PROCESO) INTO V_FECHA_VARIACION
FROM BDS_SALDOS_CIERRE_JARED
WHERE FECHA_PROCESO < P_FECHA
  AND DAY(FECHA_PROCESO) = DAY(LAST_DAY(FECHA_PROCESO));
```

byte por byte. Lo único que las distinguía era el comentario de encima. Dos
códigos para una conducta es una invitación a que alguien cambie uno de los dos
creyendo que son independientes.

**No existía la opción 1 y no había `ELSE`.** Con `P_OPCION` en 1, 0, 5 o `NULL`
no corría ninguna rama, la variable quedaba en `NULL` y se llegaba al `RAISE;`
pelado. Un código inválido y una tabla vacía daban **el mismo error**.

**El reproceso era de un solo día.** El `DELETE` era
`WHERE FECHA_PROCESO = P_FECHA`, así que un mes eran treinta llamadas, cada una
con su `DELETE` y sus tres `INSERT`.

**Los códigos numéricos no dicen nada en el sitio donde se leen.** En el JSON del
DAG, `"PARAMETROS": [3, "2026-09-30"]` obliga a abrir el procedimiento para
saber qué es 3. `["DIARIO", null, null]` se entiende sin salir del archivo.

---

## Cómo se llama

Desde el JSON del DAG, para la corrida diaria automática:

```json
{
  "STORED_PROCEDURE": "SP_BDS_SALDOS_OPERATIVOS",
  "PARAMETROS": ["DIARIO", null, null],
  "TIMEOUT_MINUTOS": 90
}
```

A mano, para reprocesar:

```sql
-- un día suelto
CALL SP_BDS_SALDOS_OPERATIVOS('DIARIO', '2026-09-14', NULL);

-- una semana
CALL SP_BDS_SALDOS_OPERATIVOS('RANGO', '2026-09-08', '2026-09-14');

-- septiembre entero
CALL SP_BDS_SALDOS_OPERATIVOS('MES', '2026-09-01', NULL);

-- agosto entero: cualquier día de agosto sirve
CALL SP_BDS_SALDOS_OPERATIVOS('MES', '2026-08-17', NULL);
```

En SQL Server la llamada es `EXEC` y los parámetros van con nombre, que es más
claro todavía:

```sql
EXEC dbo.SP_BDS_SALDOS_OPERATIVOS
     @P_MODO = 'MES', @P_FECHA_INI = '2026-08-17', @P_FECHA_FIN = NULL;
```

---

## Reproceso e idempotencia

Los siete procedimientos borran su propio rango antes de escribirlo:

```sql
DELETE FROM <destino> WHERE FECHA_PROCESO BETWEEN V_DESDE AND V_HASTA;
```

Eso es lo que permite relanzar cualquier modo las veces que haga falta sin
duplicar. Es también la diferencia con los procedimientos originales, en los que
ese `DELETE` **estaba comentado** mientras el `INSERT` no: reejecutar duplicaba
todo el rango.

Dos consecuencias que conviene tener presentes. El `DELETE` alcanza al rango
completo, así que un `MES` sobre un mes ya cerrado **borra y rehace** los treinta
días, no sólo los que cambiaron; es más lento pero es la única forma de que el
resultado no dependa de lo que hubiera antes. Y mientras el procedimiento corre,
el rango está incompleto para quien lo consulte: no hay versionado, así que un
reproceso de producción se hace en ventana.

---

## El bloque de validación

Es idéntico en los siete procedimientos y va **antes de cualquier otra cosa**.
Se copia tal cual; si hay que cambiarlo, se cambia en los siete.

### SingleStore

```sql
    V_MODO = UPPER(COALESCE(P_MODO, ''));

    IF V_MODO NOT IN ('DIARIO', 'RANGO', 'MES') THEN
        RAISE USER_EXCEPTION(CONCAT(
            'P_MODO=', COALESCE(P_MODO, 'NULL'),
            ' no existe. Use DIARIO, RANGO o MES.'));
    END IF;

    IF V_MODO IN ('DIARIO', 'MES') AND P_FECHA_FIN IS NOT NULL THEN
        RAISE USER_EXCEPTION(CONCAT(
            'En modo ', V_MODO, ', P_FECHA_FIN debe ir en NULL. ',
            'Para procesar varios dias use RANGO.'));
    END IF;

    IF V_MODO = 'DIARIO' THEN
        IF P_FECHA_INI IS NULL THEN
            SELECT MAX(FECHA_PROCESO) INTO V_DESDE FROM <TABLA_ORIGEN>;
            IF V_DESDE IS NULL THEN
                RAISE USER_EXCEPTION(
                    'No hay ninguna FECHA_PROCESO en <TABLA_ORIGEN>. '
                    'No se puede resolver la corrida diaria.');
            END IF;
        ELSE
            V_DESDE = P_FECHA_INI;
        END IF;
        V_HASTA = V_DESDE;

    ELSIF V_MODO = 'RANGO' THEN
        IF P_FECHA_INI IS NULL OR P_FECHA_FIN IS NULL THEN
            RAISE USER_EXCEPTION(
                'En modo RANGO, P_FECHA_INI y P_FECHA_FIN son obligatorias.');
        END IF;
        IF P_FECHA_INI > P_FECHA_FIN THEN
            RAISE USER_EXCEPTION(CONCAT(
                'El rango esta invertido: inicio=', P_FECHA_INI,
                ' fin=', P_FECHA_FIN, '.'));
        END IF;
        V_DESDE = P_FECHA_INI;
        V_HASTA = P_FECHA_FIN;

    ELSE  -- MES
        IF P_FECHA_INI IS NULL THEN
            RAISE USER_EXCEPTION(
                'En modo MES, P_FECHA_INI es obligatoria: '
                'cualquier dia del mes a procesar.');
        END IF;
        V_DESDE = DATE(DATE_FORMAT(P_FECHA_INI, '%Y-%m-01'));
        V_HASTA = LAST_DAY(P_FECHA_INI);
    END IF;

    IF DATEDIFF(V_HASTA, V_DESDE) > 366 THEN
        RAISE USER_EXCEPTION(CONCAT(
            'El rango ', V_DESDE, '..', V_HASTA, ' abarca ',
            DATEDIFF(V_HASTA, V_DESDE) + 1,
            ' dias y el tope son 366. Parta el reproceso.'));
    END IF;
```

### SQL Server

```sql
    SET @V_MODO = UPPER(ISNULL(@P_MODO, ''));

    IF @V_MODO NOT IN ('DIARIO', 'RANGO', 'MES')
        THROW 50001, 'P_MODO no existe. Use DIARIO, RANGO o MES.', 1;

    IF @V_MODO IN ('DIARIO', 'MES') AND @P_FECHA_FIN IS NOT NULL
        THROW 50002, 'En modo DIARIO o MES, P_FECHA_FIN debe ir en NULL. Para procesar varios dias use RANGO.', 1;

    IF @V_MODO = 'DIARIO'
    BEGIN
        IF @P_FECHA_INI IS NULL
        BEGIN
            SELECT @V_DESDE = MAX(FECHA_PROCESO) FROM <TABLA_ORIGEN>;
            IF @V_DESDE IS NULL
                THROW 50003, 'No hay ninguna FECHA_PROCESO en el origen. No se puede resolver la corrida diaria.', 1;
        END
        ELSE SET @V_DESDE = @P_FECHA_INI;
        SET @V_HASTA = @V_DESDE;
    END
    ELSE IF @V_MODO = 'RANGO'
    BEGIN
        IF @P_FECHA_INI IS NULL OR @P_FECHA_FIN IS NULL
            THROW 50004, 'En modo RANGO, P_FECHA_INI y P_FECHA_FIN son obligatorias.', 1;
        IF @P_FECHA_INI > @P_FECHA_FIN
            THROW 50005, 'El rango esta invertido.', 1;
        SET @V_DESDE = @P_FECHA_INI;
        SET @V_HASTA = @P_FECHA_FIN;
    END
    ELSE
    BEGIN
        IF @P_FECHA_INI IS NULL
            THROW 50006, 'En modo MES, P_FECHA_INI es obligatoria: cualquier dia del mes a procesar.', 1;
        SET @V_DESDE = DATEFROMPARTS(YEAR(@P_FECHA_INI), MONTH(@P_FECHA_INI), 1);
        SET @V_HASTA = EOMONTH(@P_FECHA_INI);
    END

    IF DATEDIFF(DAY, @V_DESDE, @V_HASTA) > 366
        THROW 50007, 'El rango abarca mas de 366 dias. Parta el reproceso.', 1;
```

`THROW` con un número de error propio por cada causa hace que el mensaje se
pueda buscar. `RAISERROR` no sirve aquí: no aborta el lote por sí solo y el
procedimiento seguiría ejecutándose después del error.

`LAST_DAY` en SingleStore y `EOMONTH` en SQL Server resuelven febrero y los
bisiestos por sí solos. No se calcula el fin de mes a mano en ninguno de los dos.
