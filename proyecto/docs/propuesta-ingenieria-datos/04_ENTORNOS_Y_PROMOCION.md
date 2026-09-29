# 04 · Entornos y promoción

---

## 1. La contradicción que hay que resolver primero

Los tres documentos de diseño dicen cosas distintas sobre cuántos entornos hay:

| Documento | Dice |
|---|---|
| Diagrama de arquitectura | *Entornos: 2 workspaces (No-Prod DEV/QA · PRD), 1 metastore* |
| Tabla de alcance, fila Landing zone | *1 workspace · 3 catálogos* |
| Diagrama de CI/CD | Tres entornos Databricks separados: DEV, QA y PROD |

No es una discrepancia de redacción. Cada opción tiene un costo, un nivel de
aislamiento y un riesgo distintos, y la landing zone se construye una sola vez.

La recomendación está desarrollada en [ADR-001](10_DECISIONES_ADR.md#adr-001).
El resto de este documento asume esa recomendación: **dos workspaces y tres
catálogos**.

---

## 2. Modelo propuesto

```
                  ┌─────────────────────────────────────────┐
                  │   METASTORE ÚNICO DE UNITY CATALOG      │
                  │   (una región, gobierno transversal)    │
                  └─────────────────────────────────────────┘
                                     │
          ┌──────────────────────────┴──────────────────────────┐
          │                                                     │
┌─────────────────────────────────┐          ┌──────────────────────────────┐
│   WORKSPACE NO-PRODUCTIVO       │          │   WORKSPACE PRODUCTIVO       │
│                                 │          │                              │
│  Catálogo  datahub_dev          │          │  Catálogo  datahub_prod      │
│  Catálogo  datahub_qa           │          │                              │
│  Catálogo  sandbox              │          │  Acceso: solo service         │
│                                 │          │  principal + lectura a        │
│  Acceso: ingenieros de datos    │          │  Finanzas sobre Gold          │
└─────────────────────────────────┘          └──────────────────────────────┘
```

**Un metastore.** Es lo que permite que el linaje sea de punta a punta y que el
glosario y las etiquetas gobernadas sean las mismas en todos los entornos. Un
metastore por entorno rompe el linaje justo donde más se necesita.

**Dos workspaces.** La frontera dura —la que separa quién puede entrar— va entre
producción y todo lo demás. Un ingeniero de datos tiene acceso al workspace no
productivo y no lo tiene al productivo. En producción solo actúa el service
principal del pipeline.

**Tres catálogos productivos más un sandbox.** La frontera entre DEV y QA es de
datos y permisos, no de infraestructura: separarlos en catálogos da el aislamiento
necesario sin duplicar workspaces, redes ni warehouses.

El `sandbox` es el catálogo gobernado que menciona la arquitectura para la
habilitación de ML e IA. Vive en el workspace no productivo, tiene su propia cuota
de costo y **no puede ser origen de nada que llegue a producción**.

### Por qué no un workspace por entorno

Es la opción del diagrama de CI/CD y es la más aislada, pero triplica la landing
zone: tres VNet, tres conjuntos de Private Link, tres configuraciones de red contra
el data center, tres warehouses. Para 38 tablas y cuatro tableros, ese aislamiento
extra no compra riesgo evitado proporcional al costo y a la carga operativa.

Si Seguridad exige separación a nivel de workspace también entre DEV y QA, la
decisión es suya y es legítima; lo que pide este documento es que sea una decisión
consciente y no el resultado de que tres diagramas dijeran cosas distintas.

---

## 3. Qué aísla cada entorno

| | DEV | QA | PROD |
|---|---|---|---|
| Propósito | Construir y romper | Validar y aceptar | Operar |
| Catálogo | `datahub_dev` | `datahub_qa` | `datahub_prod` |
| Quién despliega | Pipeline, en cada merge | Pipeline, en cada merge | Pipeline, con aprobación |
| Quién puede escribir a mano | Ingenieros | Nadie | Nadie |
| Origen de datos | Sintéticos | Subconjunto enmascarado | Bantotal y MIS reales |
| Volumen | Mínimo | Representativo | Completo |
| Programación de cargas | En pausa | Diaria, en horario laboral | Diaria, en ventana nocturna |
| Retención de time travel | 7 días | 7 días | 35 días |
| Acceso de Finanzas | No | UAT, durante certificación | Lectura sobre Gold |
| Alertas a guardia | No | No | Sí |

Dos filas concentran casi todo el valor del aislamiento.

**"Quién puede escribir a mano: nadie"** en QA y PROD. Si un ingeniero puede
corregir una tabla de QA desde un notebook, lo que se certifica deja de ser lo que
produce el pipeline, y la certificación pierde su sentido. Se aplica con permisos
de Unity Catalog, no con una norma escrita.

**"Origen de datos"**, que es la cuarta decisión pendiente y la trata el punto
siguiente.

---

## 4. Qué datos viven en DEV y QA

Ninguno de los documentos de diseño lo define. En un banco esa omisión termina
resolviéndose sola de la peor manera: alguien necesita probar con datos reales,
copia una tabla de producción a QA, funciona, y la práctica se instala.

Esa copia es un hallazgo de auditoría. Los datos de un core bancario en un entorno
donde los ingenieros tienen acceso directo y los controles son más laxos es
exactamente el escenario que la normativa de protección de datos busca evitar.

Lo que se propone:

**DEV usa datos sintéticos.** Generados por un script versionado en
`tests/datos/generadores/`. Conservan la forma —esquema, cardinalidad,
distribuciones, casos borde— y no contienen ningún dato real. Ventaja adicional:
los casos difíciles (un saldo negativo, una fecha nula, un cliente sin segmento) se
pueden fabricar a voluntad en lugar de esperar a que aparezcan.

**QA usa un subconjunto enmascarado de producción.** Es lo que hace la
certificación creíble: mismo esquema, mismos volúmenes relativos, mismas
distribuciones. El enmascaramiento se ejecuta con un proceso automatizado, no
manual, aplicando las políticas de Unity Catalog:

| Tipo de dato | Tratamiento en QA |
|---|---|
| Identificadores de cliente | Seudonimizados con hash estable, para que los joins sigan funcionando |
| Nombres y direcciones | Sustituidos por datos ficticios |
| Números de cuenta y tarjeta | Sustituidos, conservando el formato |
| Importes y saldos | Se conservan: son el objeto de la validación contable |
| Fechas | Se conservan: el cierre depende de ellas |

Los importes se conservan a propósito. Enmascararlos haría imposible la
reconciliación contra MIS, que es justamente lo que QA tiene que validar. El riesgo
se controla quitando la identificación del titular, no el monto.

**La refrescada de QA es un proceso versionado y auditado**, que corre bajo demanda
con aprobación, deja registro de quién la pidió y cuándo, y nunca al revés: de QA a
producción no se mueve ningún dato jamás.

---

## 5. Parametrización entre entornos

Lo que cambia entre entornos se declara, nunca se codifica.

```yaml
# conf/prod.yml
catalogo: datahub_prod
warehouse_id: ${var.warehouse_prod}
retencion_time_travel_dias: 35
ventana_carga: "0 0 2 * * ?"          # 02:00
alertas_destino: guardia-datos@banco.com
volumen_landing: /Volumes/datahub_prod/landing/archivos
calidad_modo: bloquear                 # una regla rota detiene la carga
```

```yaml
# conf/dev.yml
catalogo: datahub_dev
warehouse_id: ${var.warehouse_nonprod}
retencion_time_travel_dias: 7
ventana_carga: null                    # sin programación
alertas_destino: equipo-datos@banco.com
volumen_landing: /Volumes/datahub_dev/landing/archivos
calidad_modo: advertir                 # una regla rota avisa y sigue
```

`calidad_modo` merece explicación. En desarrollo, una regla que falla debe avisar
pero dejar seguir: el ingeniero está justamente trabajando sobre datos incompletos.
En producción, la misma regla detiene la carga. Es la misma regla, el mismo código,
distinto comportamiento por configuración. Sin esta separación pasa una de dos
cosas: o desarrollo se vuelve inviable, o alguien relaja la regla en producción para
poder trabajar y nadie la vuelve a apretar.

---

## 6. Promoción entre entornos

Lo que se promueve es **código y configuración**. Nunca datos.

```
  main ──► DEV ──► QA ──► (aprobación) ──► PROD
             │       │                        │
         sintéticos  │                    datos reales
                 enmascarados
```

Cada flecha es el mismo artefacto con distinto target. Un cambio no puede entrar a
QA sin haber pasado por DEV, ni a producción sin haber pasado por QA. La única
excepción es el hotfix, que tiene su propio camino:

**Hotfix.** Se ramifica del tag productivo, pasa el CI completo, se despliega a QA
para una validación mínima y se promueve a producción con aprobación. Después se
fusiona a `main` obligatoriamente. Un hotfix que no vuelve a `main` es la forma más
rápida de que el siguiente despliegue normal deshaga la corrección de madrugada.

---

## 7. Lo que hay que construir para sostener esto

| Elemento | Dónde | Responsable |
|---|---|---|
| Dos workspaces con su red | Terraform, `infra/` | Plataforma |
| Metastore y tres catálogos | Terraform | Plataforma |
| Service principals por entorno | Entra ID + Terraform | Seguridad + Plataforma |
| Generador de datos sintéticos | `tests/datos/generadores/` | Ingeniería de Datos |
| Proceso de enmascaramiento a QA | `src/comun/enmascarar/` | Ingeniería de Datos |
| Políticas de enmascaramiento de UC | Bundle | Ingeniería de Datos + Gobierno |
| Aprobación de despliegue a PROD | Azure DevOps Environments | DevOps |

---

## Documentos relacionados

- [03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md) — el flujo de despliegue completo
- [07 · Gobierno, seguridad y redes](07_GOBIERNO_SEGURIDAD_Y_REDES.md) — permisos y enmascaramiento en detalle
- [ADR-001](10_DECISIONES_ADR.md#adr-001) y [ADR-004](10_DECISIONES_ADR.md#adr-004) — las decisiones que este documento asume
