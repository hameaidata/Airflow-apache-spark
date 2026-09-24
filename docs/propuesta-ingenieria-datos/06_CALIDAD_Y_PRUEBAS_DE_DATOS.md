# 06 · Calidad y pruebas de datos

---

## 1. Dos cosas distintas que suelen confundirse

**Probar el código** responde: ¿la transformación hace lo que dice que hace? Se
prueba con datos que uno inventa, corre en segundos y su resultado es siempre el
mismo.

**Probar el dato** responde: ¿lo que llegó hoy tiene sentido? Se ejecuta sobre datos
reales, corre en cada carga y su resultado cambia todos los días.

Las dos hacen falta y se ejecutan en momentos distintos. Confundirlas lleva a dos
errores caros: pipelines que validan reglas de negocio contra datos de producción
en el CI (lentos, frágiles, y fallan por razones que no son del cambio), o cargas
que no validan nada porque "ya se probó en desarrollo".

```
Probar el CÓDIGO                        Probar el DATO
├── Se ejecuta en el CI                 ├── Se ejecuta en cada carga
├── Datos inventados                    ├── Datos reales
├── Corta el pull request               ├── Corta la publicación
└── Resultado determinista              └── Resultado cambia a diario
```

---

## 2. Pirámide de pruebas de código

| Nivel | Qué prueba | Dónde corre | Cuánto tarda | Cuántas |
|---|---|---|---|---|
| Unitarias | Una función de transformación aislada | CI, sin cluster | Segundos | Muchas |
| Configuración | `catalogo_tablas.yml` y contratos contra su esquema | CI, sin cluster | Segundos | Una por archivo |
| Integración | Un pipeline completo con datos sintéticos | CI, sobre DEV | Minutos | Una por flujo |
| Contrato | El esquema producido coincide con el declarado | CI, sobre DEV | Minutos | Una por tabla de frontera |

### Unitarias

La lógica de negocio se extrae a funciones puras que reciben y devuelven
DataFrames, sin leer ni escribir. Eso permite probarlas sin cluster y en segundos.

```python
def test_saldo_ajustado_resta_el_ajuste():
    entrada = spark.createDataFrame([
        ("001", 1000.0, 50.0),
        ("002", 2000.0, 0.0),
        ("003", 500.0, -25.0),      # ajuste negativo: caso real, no hipotético
    ], "cuenta string, saldo_original double, ajuste double")

    salida = calcular_saldo_ajustado(entrada)

    assert salida.filter("cuenta = '001'").first().saldo_ajustado == 950.0
    assert salida.filter("cuenta = '003'").first().saldo_ajustado == 525.0
```

La regla que hace útil este nivel: **una transformación que no se puede probar sin
un cluster está mal estructurada.** Si la función lee de una tabla por su nombre en
lugar de recibir un DataFrame, hay que arreglar la función, no aceptar que no se
puede probar.

### Contrato

Comprueba que lo que el código produce coincide con lo que el contrato declara. Es
la prueba que impide el cambio silencioso de esquema: alguien agrega una columna en
Silver, no actualiza el contrato, y tres semanas después un tablero deja de cuadrar
sin que nadie relacione las dos cosas.

```python
def test_silver_cliente_cumple_su_contrato():
    contrato = cargar_contrato("silver/contratos/cliente.yml")
    real = spark.table("datahub_dev.silver.cliente").schema

    faltantes = [c for c in contrato.columnas_obligatorias if c not in real.names]
    assert not faltantes, f"El contrato declara columnas que no existen: {faltantes}"

    nuevas = [c for c in real.names if c not in contrato.columnas_declaradas]
    assert not nuevas, (
        f"Columnas nuevas sin declarar en el contrato: {nuevas}. "
        f"Actualiza silver/contratos/cliente.yml en este mismo PR y avisa a "
        f"los consumidores declarados."
    )
```

El mensaje de error nombra el archivo que hay que tocar y recuerda avisar a los
consumidores. Un test cuyo mensaje obliga a buscar qué hacer es un test a medias.

---

## 3. Calidad del dato en ejecución

Las 30 reglas de calidad y los 20 perfilamientos del alcance se ejecutan en cada
carga, dentro de los pipelines declarativos de Lakeflow, como *expectations*.

### Los tres niveles de severidad

| Severidad | Qué hace con la fila | Qué hace con la carga | Para qué |
|---|---|---|---|
| `warn` | La deja pasar | Sigue | Perfilamiento y tendencias |
| `drop` | La descarta y la registra | Sigue | Basura conocida del origen |
| `fail` | — | **Detiene la carga** | Invariantes del negocio |

El criterio para elegir severidad es una sola pregunta: **¿preferiría no publicar
nada antes que publicar esto?** Si la respuesta es sí, es `fail`.

Ejemplos de esa frontera en este dominio: un saldo que no cuadra contra MIS por más
de la tolerancia acordada es `fail`, porque un balance descuadrado publicado es peor
que un tablero sin actualizar. Un cliente sin segmento es `drop` o `warn`, porque el
balance sigue siendo correcto sin él. Una fecha de proceso futura es `fail`, porque
indica que algo está mal en el origen y todo lo que venga detrás es sospechoso.

```python
@dlt.expect_all_or_fail({
    "clave_no_nula":     "id_cuenta IS NOT NULL",
    "fecha_no_futura":   "fecha_proceso <= current_date()",
    "moneda_conocida":   "moneda IN (SELECT codigo FROM silver.dim_moneda)",
})
@dlt.expect_all_or_drop({
    "importe_numerico":  "importe IS NOT NULL",
})
@dlt.expect_all({
    "segmento_informado": "segmento IS NOT NULL",   # solo se mide
})
def silver_movimiento_contable():
    ...
```

### Cuadre contra MIS

Es la regla que justifica las tres semanas de paralelo, y merece tratarse aparte de
las demás: no valida una fila, valida un total.

```sql
-- Se ejecuta al final de Gold Enterprise, antes de publicar
WITH hub AS (
    SELECT fecha_proceso, cuenta_contable, SUM(saldo_ajustado) AS saldo
    FROM   gold_enterprise.fact_saldo_diario
    WHERE  fecha_proceso = :fecha
    GROUP  BY fecha_proceso, cuenta_contable
),
mis AS (   -- por federación: no se copia MIS, se consulta
    SELECT fecha, cuenta, saldo FROM mis_federado.balances
    WHERE  fecha = :fecha
)
SELECT h.cuenta_contable,
       h.saldo AS saldo_hub,
       m.saldo AS saldo_mis,
       ABS(h.saldo - m.saldo) AS diferencia
FROM   hub h FULL OUTER JOIN mis m
       ON h.cuenta_contable = m.cuenta AND h.fecha_proceso = m.fecha
WHERE  ABS(COALESCE(h.saldo,0) - COALESCE(m.saldo,0)) > :tolerancia
```

El `FULL OUTER JOIN` es deliberado. Un `INNER JOIN` no vería el caso que más
importa: una cuenta que existe en MIS y **no** llegó al Data Hub. Esa cuenta
ausente no genera diferencia en un inner join, simplemente desaparece, y el cuadre
daría correcto mientras falta un pedazo del balance.

La tolerancia se declara por cuenta en configuración, no se codifica: hay cuentas
donde un centavo importa y otras donde no.

---

## 4. Qué detiene qué

```
Pull request
  └── falla una prueba unitaria, de configuración o de contrato
        → el PR no se puede fusionar

Carga diaria en producción
  ├── falla una expectativa `fail` en Silver
  │     → no se propaga a Gold; Gold conserva el dato de ayer, que es correcto
  ├── falla el cuadre contra MIS
  │     → no se publica a Gold Analytics; el tablero muestra el día anterior
  └── falla el refresco de Power BI
        → alerta; los datos en Gold están bien, solo no llegaron al tablero
```

El principio detrás de los tres casos: **ante la duda, no publicar.** Un tablero que
muestra el dato de ayer y lo dice es un inconveniente. Un tablero que muestra un
dato incorrecto de hoy sin decirlo es un problema de control interno.

Eso exige que el tablero muestre siempre la fecha del dato que está mostrando, de
forma visible. Es un requisito de los cuatro tableros, no un detalle de diseño.

---

## 5. Datos de prueba

**DEV: sintéticos.** Generador versionado en `tests/datos/generadores/`. Conserva
esquema, cardinalidad y distribuciones, y permite fabricar a voluntad los casos
difíciles: un saldo negativo, una fecha nula, un cliente sin segmento, un cambio de
año en la marca de agua. Esperar a que esos casos aparezcan solos en datos reales
es esperar a que aparezcan en producción.

**QA: subconjunto enmascarado de producción.** Ver
[04 · Entornos](04_ENTORNOS_Y_PROMOCION.md#4-qué-datos-viven-en-dev-y-qa) y
[ADR-004](10_DECISIONES_ADR.md#adr-004).

**Casos de regresión.** Cada incidente de producción deja un caso de prueba con los
datos que lo provocaron, anonimizados. Es lo que impide que el mismo error vuelva
dentro de seis meses, cuando nadie recuerde por qué esa línea estaba escrita así.

---

## 6. Observabilidad de la calidad

Los resultados de las expectativas se guardan en `ctl.ctl_calidad` y se publican en
un tablero operativo que responde tres preguntas:

**¿Cómo viene la calidad en el tiempo?** Una regla que pasa de 0,1% a 3% de
descartes no dispara ninguna alerta, pero está diciendo que algo cambió en el
origen. Esa tendencia se ve o no se ve; no hay término medio.

**¿Qué reglas fallan más?** Una regla que falla todos los días o está mal escrita o
describe un problema real del origen que nadie está atendiendo. Las dos merecen una
conversación, y ninguna de las dos se resuelve sola.

**¿Qué tablas no tienen ninguna regla?** Es el indicador más útil del tablero.
Una tabla de Gold sin control de calidad es una tabla en la que se confía sin
motivo, y suele ser la que rompe el cierre.

---

## Documentos relacionados

- [03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md) — dónde encaja cada prueba en el pipeline
- [05 · Estándares y convenciones](05_ESTANDARES_Y_CONVENCIONES.md) — contratos entre capas
- [08 · Operación y observabilidad](08_OPERACION_OBSERVABILIDAD_FINOPS.md) — alertas y runbooks
