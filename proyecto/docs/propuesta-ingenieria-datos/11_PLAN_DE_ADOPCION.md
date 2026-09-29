# 11 · Plan de adopción

> Cada fase tiene un **criterio de salida verificable**. No "se terminó la fase",
> sino "se puede demostrar esto". Una fase sin criterio de salida se declara
> terminada por calendario, que es como se acumula la deuda que aparece en
> producción.

---

## Fase 0 · Decisiones y cimientos

**Objetivo:** que nadie construya sobre un supuesto equivocado.

Resolver las cuatro decisiones de [ADR](10_DECISIONES_ADR.md) en una sesión con
Arquitectura, Seguridad y Gerencia. Corregir los diagramas de diseño para que digan
lo mismo entre sí. Crear el repositorio con su estructura, las ramas protegidas y
esta documentación.

**Criterio de salida**

Las cuatro ADR están en estado *Aceptada* con firma de quien decide. Los diagramas
corregidos ya no se contradicen. El repositorio existe, `main` está protegida y
ningún cambio puede entrar sin pull request aprobado.

**Por qué va primero:** la landing zone se construye una vez. Rehacerla después
significa recrear redes, Private Link y permisos con la plataforma ya en uso.

---

## Fase 1 · Plataforma y CI/CD, sin datos todavía

**Objetivo:** que el camino de despliegue exista antes de que haya algo que
desplegar.

Se construye con Terraform la landing zone completa: workspaces, redes, metastore,
catálogos, ADLS, Key Vault, grupos de Entra ID y service principals. Se arma el
bundle con sus tres targets, y los pipelines de CI y CD de Azure DevOps.

Para probar que todo funciona se despliega un pipeline trivial —una tabla de dos
columnas— por el camino completo: pull request, CI, DEV, QA, aprobación, producción.

**Criterio de salida**

Un cambio de una línea recorre el camino completo hasta producción sin que nadie
toque la interfaz de Databricks. El despliegue a producción queda registrado con su
aprobación. Se puede revertir ese cambio desplegando el tag anterior, y se demuestra.

**El error más común es saltarse esta fase.** La tentación de empezar por los datos
—que es lo que se ve— y dejar el CI/CD "para cuando haya algo que desplegar" es
fuerte. El problema es que para entonces ya hay veinte objetos creados a mano que
nadie sabe cómo reproducir, y montar el CI/CD encima de eso es un proyecto de
migración en lugar de un punto de partida.

---

## Fase 2 · Ingesta y Bronze

**Objetivo:** las 38 tablas replicadas, con el framework por metadatos funcionando.

Se construye el framework de ingesta que consume `catalogo_tablas.yml`, la
conectividad a Bantotal, MIS y archivos, la carga inicial histórica y la carga
diaria incremental. En paralelo, el generador de datos sintéticos y el proceso de
enmascaramiento de QA, que son requisito de [ADR-004](10_DECISIONES_ADR.md#adr-004)
y suelen olvidarse en la planificación.

**Criterio de salida**

Las 38 tablas cargan a diario sin intervención. Agregar la tabla 39 es una entrada
en `catalogo_tablas.yml` revisada en un pull request, y se demuestra agregando una.
DEV corre con datos sintéticos y QA con datos enmascarados. La bitácora de lotes
responde qué cargó, cuándo y cuántas filas.

---

## Fase 3 · Silver y calidad

**Objetivo:** el dato conformado, con las reglas que lo protegen.

Limpieza, tipificación y homologación; maestros con SCD2; el registro de ajustes
versionado; las 30 reglas de calidad y los 20 perfilamientos; y el tablero
operativo de calidad.

**Criterio de salida**

Una regla de severidad `fail` detiene efectivamente la propagación a Gold, y se
demuestra provocándola a propósito en QA. El tablero de calidad muestra la
tendencia. Cada tabla de Silver tiene su contrato declarado y el CI lo verifica.

---

## Fase 4 · Gold y reconciliación

**Objetivo:** el número que Finanzas defiende.

Gold Enterprise con saldos en sus tres versiones, hechos y dimensiones; la
propagación automática del ajuste de balance con sus controles y bitácora; el cuadre
contra MIS; y Gold Analytics con las tablas CU.

**Criterio de salida**

El cuadre contra MIS corre en cada carga y detiene la publicación cuando la
diferencia supera la tolerancia. La propagación de un ajuste se puede rastrear de
punta a punta: qué ajuste, quién lo cargó, a qué saldos llegó. El linaje de columna
responde de dónde sale cada campo de las tablas CU.

**Es la fase más delicada del proyecto.** La propagación del ajuste es la pieza
funcional que más puede contaminar aguas abajo sin ser evidente a simple vista.
Merece revisión doble y casos de prueba escritos por Finanzas.

---

## Fase 5 · Power BI

**Objetivo:** los cuatro tableros sobre Gold, con su propio ciclo de despliegue.

Modelos semánticos en modo Import con refresco incremental, disparado por Lakeflow
al terminar la carga; medidas y pestañas; seguridad por filas donde aplique; y el
despliegue de los tableros por su propio flujo con sus entornos.

**Criterio de salida**

Los cuatro tableros se refrescan solos al terminar la carga. Cada uno muestra de
forma visible la fecha del dato que está mostrando. Un tablero de QA apunta al
warehouse de QA y no hay forma de que apunte al de producción. El despliegue de un
cambio de tablero sigue un flujo con aprobación, igual que el de un pipeline.

---

## Fase 6 · Operación y paralelo

**Objetivo:** que la plataforma se sostenga sin sus constructores.

Alertas y escalamiento; los runbooks; el tablero de costos sobre `system.billing`
con sus budget policies; las tres semanas de corrida en paralelo con cuadre diario
contra MIS; y los cuatro talleres de transferencia a TI y a Finanzas.

**Criterio de salida**

Las tres semanas de paralelo cierran con las diferencias explicadas y aceptadas por
Finanzas. La guardia puede atender un incidente siguiendo el runbook, sin llamar a
quien construyó el pipeline, y se verifica con un simulacro. El tablero de costos
muestra gasto contra presupuesto con proyección.

**La prueba de recuperación es parte de esta fase:** revertir una tabla Gold en QA y
medir cuánto tardó. Un procedimiento de reversión escrito y nunca ejecutado es una
hipótesis.

---

## Fase 7 · Habilitación de ML e IA

**Objetivo:** dejar el andamio listo, sin construir modelos todavía.

Catálogo `sandbox` gobernado con su cuota de costo, MLflow, Feature Engineering en
Unity Catalog, Model Serving serverless, y un espacio Genie piloto sobre Gold.

**Criterio de salida**

Un científico de datos puede trabajar en `sandbox` sin pedir permisos ad hoc y sin
poder tocar producción. El gasto del sandbox se ve por separado y tiene tope.

**La restricción que hay que fijar ahora y no después:** nada que nazca en `sandbox`
llega a producción sin pasar por el flujo normal de ingeniería. Un modelo entrenado
en un notebook y publicado a mano es exactamente lo que todo este conjunto de
documentos existe para evitar.

---

## Orden y solapamiento

Las fases 0 y 1 son secuenciales y bloqueantes: nada empieza antes. De la 2 en
adelante hay solapamiento natural —mientras se estabiliza Silver se puede empezar
Gold de los dominios ya conformados—, pero **ninguna fase se declara cerrada sin su
criterio de salida demostrado**, aunque la siguiente ya haya empezado.

La Fase 6 es la que más se recorta cuando el calendario aprieta, y es la que decide
si en un año la plataforma la opera el banco o la operan las dos personas que la
construyeron.

---

## Qué rol hace falta en cada fase

| Fase | Ing. Datos | Arquitecto | DevOps | Gobierno | Finanzas |
|---|---|---|---|---|---|
| 0 · Decisiones | ○ | ● | ○ | ● | ○ |
| 1 · Plataforma y CI/CD | ○ | ○ | ● | ○ | — |
| 2 · Ingesta y Bronze | ● | ○ | ○ | ○ | — |
| 3 · Silver y calidad | ● | ○ | — | ○ | ○ |
| 4 · Gold y reconciliación | ● | ● | — | ○ | ● |
| 5 · Power BI | ○ | ○ | ○ | — | ● |
| 6 · Operación y paralelo | ● | ○ | ● | ○ | ● |
| 7 · ML e IA | ○ | ○ | ○ | ● | — |

● dedicación principal · ○ participación · — no participa

La lectura útil de esta tabla es que **DevOps es imprescindible al principio y al
final**, no en el medio. Planificar ese perfil solo para el arranque deja la Fase 6
sin quien la haga, que es donde se decide si la plataforma es operable.

---

## Documentos relacionados

- [10 · Decisiones de arquitectura](10_DECISIONES_ADR.md) — lo que resuelve la Fase 0
- [09 · Roles y modelo operativo](09_ROLES_Y_MODELO_OPERATIVO.md) — qué hace cada perfil
- [03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md) — lo que construye la Fase 1
