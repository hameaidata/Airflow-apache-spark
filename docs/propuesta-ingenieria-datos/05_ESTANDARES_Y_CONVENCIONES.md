# 05 · Estándares y convenciones

> Un estándar que no se verifica automáticamente es una sugerencia. Cada regla de
> este documento dice cómo se comprueba; las que no se pueden comprobar en el CI se
> verifican en revisión de código y así se indica.

---

## 1. Nomenclatura

### Catálogos y esquemas

```
datahub_<entorno>.<capa>.<objeto>

datahub_dev    datahub_qa    datahub_prod    sandbox
       └── bronze · silver · gold_enterprise · gold_analytics · ctl
```

El esquema `ctl` guarda las tablas de control del propio pipeline: bitácora de
lotes, marcas de agua, resultados de reglas de calidad. Separarlo de las capas de
negocio evita que alguien lo confunda con un dato de Finanzas.

### Tablas

| Capa | Patrón | Ejemplo |
|---|---|---|
| Bronze | `<origen>_<tabla_original>` | `bantotal_fsd010`, `mis_saldos_diarios` |
| Silver | `<entidad_de_negocio>` | `cliente`, `cuenta`, `movimiento_contable` |
| Gold Enterprise | `fact_<hecho>` · `dim_<dimensión>` | `fact_saldo_diario`, `dim_producto` |
| Gold Analytics | `cu_<modelo>` | `cu_contabilidad`, `cu_resultados` |
| Control | `ctl_<propósito>` | `ctl_lote`, `ctl_marca_agua`, `ctl_calidad` |

En Bronze el nombre conserva el del origen, aunque sea críptico. `fsd010` no dice
nada, y renombrarlo a `saldos_diarios` en Bronze parece una mejora hasta el día que
alguien tiene que rastrear un dato hasta el core y no encuentra la correspondencia.
El nombre legible aparece en Silver, que es donde empieza el lenguaje de negocio.

### Columnas

`snake_case`, en español, sin abreviaturas inventadas. Los metadatos que agrega el
pipeline llevan prefijo `_` para que nunca se confundan con un dato de negocio:

```
_fecha_proceso      fecha de negocio del lote
_batch_id           identificador de la corrida
_origen             sistema de procedencia
_fecha_carga        marca de tiempo de la extracción
_es_actual          SCD2: si la fila es la versión vigente
_valido_desde       SCD2
_valido_hasta       SCD2
```

### Trabajos y pipelines

```
<capa>_<dominio>_<acción>

ingesta_bantotal_diaria
bronze_bantotal_carga
silver_contabilidad_conformado
gold_contabilidad_saldos
```

El bundle prefija automáticamente con el entorno, así que el nombre no lo incluye.
En desarrollo, `mode: development` agrega además el usuario, de forma que dos
ingenieros no se pisan.

**Verificación:** un test del CI comprueba estos patrones sobre el bundle
desplegado a DEV. Un nombre fuera de patrón corta el pull request.

---

## 2. Estructura del repositorio

Definida en [03 · CI/CD](03_CICD_Y_DATAOPS.md#3-estructura-del-repositorio). Tres
reglas sobre cómo se usa:

**Un archivo de recursos por dominio, no uno gigante.** `resources/` se organiza por
grupo de recursos, de manera que dos personas trabajando en dominios distintos rara
vez tocan el mismo archivo. Los conflictos de fusión en YAML son especialmente
molestos porque el archivo sigue siendo válido después de un merge mal resuelto.

**El código de transformación no sabe en qué entorno corre.** Recibe el catálogo por
parámetro. Un `if entorno == "prod"` dentro de una transformación significa que lo
que se probó en QA no es lo que corre en producción, y entonces QA no prueba nada.

**`src/comun/` es para lo que se usa en tres sitios o más.** Menos que eso, se
duplica. Una utilidad compartida que usan dos procesos se convierte en una
dependencia que hay que coordinar cada vez que cambia, y el costo de esa
coordinación supera al de las veinte líneas duplicadas.

---

## 3. La configuración de las 38 tablas

El framework de ingesta por metadatos se apoya en un solo archivo.

```yaml
# conf/catalogo_tablas.yml
tablas:
  - id: bantotal_fsd010
    origen:
      sistema: bantotal
      esquema: BANTOTAL
      tabla: FSD010
    destino:
      capa: bronze
      tabla: bantotal_fsd010
    carga:
      modo: incremental          # completa | incremental
      columna_marca: FPROCESO    # obligatoria si modo = incremental
      clave: [PGCOD, PPCOD, SCCOD]
    calidad:
      reglas: [no_nulos_clave, fecha_valida, importe_no_negativo]
    activo: true
```

Agregar la tabla 39 es una entrada en este archivo revisada en un pull request. No
hay notebook nuevo, no hay trabajo nuevo que declarar.

**Verificación:** el CI valida este archivo contra un esquema JSON antes que
cualquier otra cosa. Comprueba que el modo sea válido, que `incremental` traiga
`columna_marca`, que la clave no esté vacía, que no haya identificadores repetidos y
que cada regla de calidad referenciada exista en `src/calidad/`.

Esa validación atrapa la clase de error más cara del proyecto: una tabla en modo
incremental sin columna de corte se cargaría entera cada día y duplicaría el
destino, y el síntoma aparece de madrugada, en producción, cuando no hay nadie
mirando.

---

## 4. Estilo de código

| Herramienta | Para qué | Corta el PR |
|---|---|---|
| `black` | Formato de Python | Sí |
| `ruff` | Linting | Sí |
| `sqlfluff` | Formato y linting de SQL (dialecto `databricks`) | Sí |
| `mypy` | Tipos en `src/comun/` | Advertencia |

Convenciones que las herramientas no cubren y se revisan en el pull request:

**Nada de rutas fijas.** Ni catálogos, ni volúmenes, ni identificadores de
warehouse. Todo entra por parámetro desde `conf/`.

**Nada de `SELECT *` en Silver ni en Gold.** En Bronze está bien: la capa es una
réplica y las columnas del origen son las que son. De Silver en adelante, la lista
explícita es lo que hace que una columna nueva en el origen no cambie la forma de
una tabla de negocio sin que nadie lo haya decidido.

**Toda transformación es idempotente.** Volver a correr el día D produce el mismo
resultado. Es la condición para que el pipeline se pueda reintentar solo, y la
condición para que la recuperación ante un fallo no sea una intervención manual.

**Los comentarios explican el porqué, no el qué.** `# suma los saldos` sobre una
línea que suma saldos no aporta nada. `# se excluye la moneda 999 porque el core la
usa como marcador de ajuste interno, no es una moneda real` es lo que evita que
alguien "corrija" esa exclusión en seis meses.

---

## 5. Contratos entre capas

Cada tabla que cruza una frontera de capa tiene un contrato explícito: esquema,
claves, obligatoriedad y significado.

```yaml
# src/transformacion/silver/contratos/cliente.yml
tabla: silver.cliente
descripcion: Maestro de clientes conformado, con historia SCD2
clave_negocio: [id_cliente]
columnas:
  - nombre: id_cliente
    tipo: string
    obligatoria: true
    descripcion: Identificador único del cliente en Bantotal
  - nombre: segmento
    tipo: string
    obligatoria: false
    valores_permitidos: [PERSONA, EMPRESA, CORPORATIVO]
consumidores:
  - gold_enterprise.dim_cliente
  - gold_analytics.cu_contabilidad
```

El campo `consumidores` es el que hace trabajar al contrato. Cuando alguien propone
cambiar `silver.cliente`, el revisor sabe de inmediato qué se puede romper, sin
buscarlo. Y el CI comprueba que el esquema producido coincide con el declarado: un
cambio de contrato exige actualizar el contrato en el mismo pull request, que es el
momento en que la conversación con los consumidores tiene que ocurrir.

---

## 6. Git

**Commits en formato convencional**, para que el historial se pueda leer y la nota
de versión se genere sola:

```
feat(silver): agregar SCD2 al maestro de productos
fix(bronze): corregir marca de agua de fsd010 en cambio de año
docs(adr): registrar decisión sobre entornos
refactor(gold): extraer cálculo de saldo ajustado a función común
```

**Un pull request, un propósito.** Un PR que cambia una regla de negocio y de paso
reordena imports es un PR que nadie revisa bien: el revisor se pierde entre el ruido
y aprueba lo que no miró.

**Toda ampliación de funcionalidad viene con su prueba.** No como norma moral: sin
prueba, el CI no puede impedir que alguien la rompa el mes que viene, y entonces la
funcionalidad depende de que nadie toque nada.

Ver [`plantillas/pull_request_template.md`](plantillas/pull_request_template.md).

---

## 7. Revisión de código

Mínimo un aprobador. Dos para cambios en Gold Enterprise, porque ahí vive el número
que Finanzas defiende ante el Directorio.

Lo que el revisor mira, más allá de que el código funcione:

¿El cambio es idempotente? ¿Qué pasa si esto corre dos veces? ¿Rompe algún contrato
declarado? ¿Tiene pruebas que fallarían si alguien deshace el cambio? Si esto sale
mal en producción, ¿cómo se revierte? ¿La documentación afectada se actualizó en
este mismo PR?

Esa última pregunta es la que sostiene el resto del conjunto documental. Un cambio
de arquitectura que no actualiza su documento se rechaza, porque la documentación
que vive aparte del código siempre termina describiendo un sistema que ya no existe.

---

## Documentos relacionados

- [03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md)
- [06 · Calidad y pruebas de datos](06_CALIDAD_Y_PRUEBAS_DE_DATOS.md)
