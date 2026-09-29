# Propuesta de Ingeniería de Datos y DataOps — Data Hub Finanzas

**Área:** Ingeniería y Analítica · Ingeniería de Datos · Arquitectura de Datos · DevOps
**Plataforma:** Microsoft Azure + Databricks (Unity Catalog, Lakeflow)
**Estado:** Propuesta para revisión
**Versión:** 1.0

---

## Qué es este documento

Un conjunto de documentos de **arquitectura y diseño técnico** para la práctica de
Ingeniería de Datos que va a construir y operar el Data Hub de Finanzas.

No es una propuesta comercial. No trae precios ni cronograma de facturación. Trae
las decisiones de ingeniería, la forma de trabajo y los estándares con los que el
área va a construir, probar, desplegar y operar la plataforma.

El eje del conjunto es el **CI/CD aplicado a datos**, porque es lo que diferencia
un data hub que se sostiene de uno que funciona el primer trimestre y después nadie
se atreve a tocar.

---

## Cómo leerlo

El material está separado por audiencia. Los dos bloques se sostienen por separado:
quien lee el primero no necesita el segundo para decidir, y quien lee el segundo no
necesita el primero para construir.

### Para el comité y la gerencia

| Documento | Qué contesta | Lectura |
|---|---|---|
| [01 · Resumen ejecutivo](01_RESUMEN_EJECUTIVO.md) | Qué se propone, qué cambia, qué se pide aprobar y qué pasa si no se hace | 10 min |
| [10 · Decisiones de arquitectura](10_DECISIONES_ADR.md) | Las decisiones que requieren aprobación, con sus alternativas | 15 min |
| [11 · Plan de adopción](11_PLAN_DE_ADOPCION.md) | En qué orden se construye y cómo se sabe que cada fase terminó | 10 min |

### Para Ingeniería de Datos, Arquitectura y DevOps

| Documento | Qué contesta |
|---|---|
| [02 · Arquitectura de referencia](02_ARQUITECTURA_DE_REFERENCIA.md) | Cómo está compuesta la plataforma y por qué cada pieza está donde está |
| [03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md) | **Documento central.** Cómo se versiona, prueba, despliega y revierte |
| [04 · Entornos y promoción](04_ENTORNOS_Y_PROMOCION.md) | Cuántos entornos, qué aísla cada uno y qué datos vive en cada uno |
| [05 · Estándares y convenciones](05_ESTANDARES_Y_CONVENCIONES.md) | Estructura del repositorio, nombres, estilo, parametrización |
| [06 · Calidad y pruebas de datos](06_CALIDAD_Y_PRUEBAS_DE_DATOS.md) | Qué se prueba, en qué capa, y qué detiene un despliegue |
| [07 · Gobierno, seguridad y redes](07_GOBIERNO_SEGURIDAD_Y_REDES.md) | Unity Catalog, permisos, secretos, conectividad, auditoría |
| [08 · Operación, observabilidad y FinOps](08_OPERACION_OBSERVABILIDAD_FINOPS.md) | Acuerdos de servicio, alertas, runbooks y control de costos |
| [09 · Roles y modelo operativo](09_ROLES_Y_MODELO_OPERATIVO.md) | Quién hace qué, y qué perfiles hacen falta |

### Plantillas listas para usar

| Archivo | Para qué |
|---|---|
| [`plantillas/databricks.yml`](plantillas/databricks.yml) | Asset Bundle con los tres entornos |
| [`plantillas/azure-pipelines-ci.yml`](plantillas/azure-pipelines-ci.yml) | Pipeline de integración continua |
| [`plantillas/azure-pipelines-cd.yml`](plantillas/azure-pipelines-cd.yml) | Pipeline de despliegue con aprobación |
| [`plantillas/pull_request_template.md`](plantillas/pull_request_template.md) | Plantilla de pull request |
| [`plantillas/CHECKLIST_PASE_A_PRODUCCION.md`](plantillas/CHECKLIST_PASE_A_PRODUCCION.md) | Lo que se verifica antes de cada pase |

---

## La idea en un párrafo

El Data Hub mueve 38 tablas de Bantotal y MIS hacia un lakehouse en capas
(Bronze → Silver → Gold), y de ahí a cuatro tableros de Power BI que Finanzas usa
para cerrar. Ese flujo ya está diseñado. Lo que esta propuesta agrega es la
**disciplina de ingeniería** alrededor: que todo el flujo viva en Git, que cada
cambio pase por revisión y pruebas automáticas, que el despliegue a producción sea
un artefacto versionado y no una edición manual en la interfaz, y que la calidad
del dato se verifique antes de que un tablero la muestre, no después de que alguien
la reclame.

---

## Lo que hay que decidir antes de construir

Revisando los diagramas de arquitectura y las tablas de alcance aparecieron
**cuatro contradicciones** entre documentos. No son errores de redacción: cada una
implica una decisión distinta de plataforma, costo y riesgo. Están planteadas con
sus opciones en [10 · Decisiones de arquitectura](10_DECISIONES_ADR.md):

| # | Tema | Contradicción |
|---|---|---|
| ADR-001 | Número de entornos | El diagrama de arquitectura dice *2 workspaces (No-Prod DEV/QA · PRD)*; el de CI/CD muestra *3 entornos separados*; la tabla de alcance dice *1 workspace · 3 catálogos* |
| ADR-002 | Modo de Power BI | Arquitectura dice *"Sin DirectQuery"*; el diagrama de CI/CD dice *"Import · DirectQuery"* |
| ADR-003 | CDC | Arquitectura marca *CDC tiempo real: fuera de alcance*; el diagrama de CI/CD pone *Fivetran CDC* como origen en los tres entornos |
| ADR-004 | Datos en entornos bajos | Ningún documento dice qué datos viven en DEV y QA. En un banco esa omisión es un hallazgo de auditoría esperando |

Resolverlas antes de la Fase 1 cuesta una reunión. Resolverlas después cuesta
rehacer la landing zone.

---

## Convenciones de este conjunto

Los documentos se versionan con el código, en el mismo repositorio y bajo el mismo
flujo de pull request. Un cambio de arquitectura que no actualiza su documento en
el mismo PR se rechaza en revisión: la documentación que vive aparte del código
siempre termina describiendo un sistema que ya no existe.

Las decisiones de arquitectura se registran como **ADR** (Architecture Decision
Record) numeradas y nunca se borran. Una decisión que se revierte se marca como
*Reemplazada por ADR-NNN*, porque saber por qué se intentó algo y por qué se
abandonó vale tanto como la decisión vigente.
