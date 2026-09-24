# 10 · Decisiones de arquitectura (ADR)

Registro de decisiones. Cada una dice qué se decidió, por qué, y qué se pierde al
decidirlo así. Ninguna se borra: una decisión revertida se marca como *Reemplazada
por ADR-NNN*, porque saber por qué se intentó algo y por qué se abandonó vale tanto
como la decisión vigente.

**Estados:** `Propuesta` · `Aceptada` · `Rechazada` · `Reemplazada`

---

## Decisiones pendientes de aprobación

Las cuatro primeras salen de **contradicciones reales entre los documentos de
diseño existentes**. No son temas nuevos: son temas que ya se definieron de dos
maneras distintas y hay que cerrar antes de construir la landing zone.

| ADR | Tema | Contradicción detectada |
|---|---|---|
| [001](#adr-001) | Número de entornos | *2 workspaces* vs *3 entornos* vs *1 workspace · 3 catálogos* |
| [002](#adr-002) | Modo de Power BI | *Sin DirectQuery* vs *Import · DirectQuery* |
| [003](#adr-003) | CDC en Fase 1 | *Fuera de alcance* vs *Fivetran CDC como origen* |
| [004](#adr-004) | Datos en entornos bajos | No está definido en ningún documento |

---

<a id="adr-001"></a>
## ADR-001 · Número y topología de entornos

**Estado:** Propuesta
**Fecha:** 2026-09-24
**Decide:** Arquitectura de Datos + Seguridad + Plataforma

### Contexto

Los documentos de diseño se contradicen:

| Documento | Dice |
|---|---|
| Diagrama de arquitectura | *2 workspaces (No-Prod DEV/QA · PRD), 1 metastore* |
| Tabla de alcance, fila Landing zone | *1 workspace · 3 catálogos* |
| Diagrama de CI/CD | Tres entornos Databricks completos y separados |

La landing zone se construye una vez y por infraestructura como código. Rehacerla
después significa recrear redes, Private Link y permisos con la plataforma ya en
uso.

### Opciones

**A · Un workspace, tres catálogos.** El más barato y el menos aislado. Un error de
permisos en un catálogo expone el resto, y cualquiera con acceso al workspace ve
producción. Para un core bancario no es defendible ante auditoría.

**B · Dos workspaces, tres catálogos.** La frontera dura va entre producción y el
resto. DEV y QA se separan por catálogo y permisos dentro del workspace no
productivo. Una sola landing zone productiva que proteger.

**C · Tres workspaces.** El más aislado. Triplica redes, Private Link, warehouses y
configuración contra el data center. Para 38 tablas y cuatro tableros, el
aislamiento adicional entre DEV y QA no compra riesgo evitado proporcional al costo
y a la carga operativa.

### Decisión propuesta

**Opción B.** Dos workspaces, tres catálogos, un metastore.

El metastore único no es negociable: es lo que hace posible el linaje de punta a
punta y que el glosario y las etiquetas gobernadas sean los mismos en todos lados.
Un metastore por entorno rompe el linaje justo donde más se necesita para auditoría.

### Consecuencias

Se acepta que un ingeniero con acceso al workspace no productivo pueda ver los
datos enmascarados de QA. Eso es aceptable **si y solo si** se aprueba también
[ADR-004](#adr-004): sin enmascaramiento, esta decisión deja datos reales al
alcance del equipo de desarrollo.

Si Seguridad exige separación a nivel de workspace también entre DEV y QA, la
decisión es legítima y se pasa a la opción C; lo que este ADR pide es que sea una
decisión consciente.

---

<a id="adr-002"></a>
## ADR-002 · Modo de conexión de Power BI

**Estado:** Propuesta
**Fecha:** 2026-09-24
**Decide:** Arquitectura de Datos + Finanzas + FinOps

### Contexto

| Documento | Dice |
|---|---|
| Diagrama de arquitectura | *Modo Import + refresco incremental* · *"Sin DirectQuery: el warehouse no queda encendido por consultas de usuarios"* |
| Tabla de servicios | *Power BI (modo Import)* · *"sin DirectQuery para no mantener cómputo encendido"* |
| Diagrama de CI/CD | *Power BI (Consumo) · Import · DirectQuery* |

Dos de tres documentos excluyen DirectQuery explícitamente y por una razón de
costo. El tercero lo incluye.

### Por qué importa

El SQL Warehouse serverless factura por tiempo encendido. Con Import, se enciende
en la ventana de refresco y se apaga a los cinco minutos; el costo es acotado y
predecible. Con DirectQuery, cada usuario que abre un tablero lo enciende, y el
costo pasa a depender de cuánta gente mire los tableros y a qué hora, que es
justamente lo que no se puede presupuestar.

El cierre contable concentra a muchos usuarios en pocos días. Es el peor patrón
posible para DirectQuery.

### Decisión propuesta

**Import con refresco incremental, disparado por el trabajo de Lakeflow al terminar
la carga.** DirectQuery queda como excepción que requiere justificación escrita,
aprobación de FinOps y un presupuesto asignado.

### Consecuencias

Los tableros muestran el dato del último refresco, no el del instante. Para un
cierre contable diario eso es lo correcto: un saldo que cambia mientras alguien lo
mira es un problema, no una ventaja.

Si más adelante aparece un caso real de necesidad de tiempo real, se evalúa con
número: cuántos usuarios, en qué horario, y cuánto cuesta tener el warehouse
encendido en esa ventana.

---

<a id="adr-003"></a>
## ADR-003 · CDC en tiempo real dentro de la Fase 1

**Estado:** Propuesta
**Fecha:** 2026-09-24
**Decide:** Arquitectura de Datos + Gerencia

### Contexto

| Documento | Dice |
|---|---|
| Diagrama de arquitectura, ingesta | *Fuera de alcance: CDC tiempo real (evolución futura)* |
| Tabla de servicios, Ingesta Bantotal | *Lakeflow Jobs + JDBC (JTOpen) / Fivetran CDC* |
| Diagrama de CI/CD | *Fivetran CDC (Incremental)* como origen en los tres entornos |

### Por qué importa

Son dos proyectos distintos, no dos formas de hacer lo mismo.

La ingesta por lote con marca de agua sobre JDBC usa componentes que el proyecto ya
contempla, no agrega licenciamiento y toca el core una vez al día, en ventana
acordada.

El CDC sobre un IBM Db2 for i exige habilitar el *journaling* en las tablas del
core, una herramienta de captura con su propia licencia, y una conversación con el
área que opera el AS/400 sobre el impacto en el sistema productivo. Además cambia
el modelo de la capa Bronze: deja de ser una réplica diaria y pasa a ser un flujo
de eventos con orden y deduplicación.

Descubrir esto a mitad de la Fase 1 es la forma habitual de que un proyecto de
datos se atrase un trimestre.

### Decisión propuesta

**CDC fuera de la Fase 1.** Se construye con ingesta por lote e incremental por
marca de agua, como dice el diagrama de arquitectura.

La arquitectura se deja preparada: Bronze se modela de forma que aceptar un flujo
CDC más adelante sea agregar un origen, no rehacer la capa.

### Consecuencias

La latencia del dato es de un día. Para un cierre contable diario es suficiente:
el proceso de Finanzas es diario, no continuo.

El diagrama de CI/CD debe corregirse para no mostrar Fivetran como origen, o el
equipo que lo lea va a construir contra un supuesto equivocado.

---

<a id="adr-004"></a>
## ADR-004 · Datos en entornos no productivos

**Estado:** Propuesta
**Fecha:** 2026-09-24
**Decide:** Seguridad + Gobierno de Datos + Arquitectura

### Contexto

Ningún documento de diseño define qué datos viven en DEV y en QA.

En la práctica, esa omisión se resuelve sola y de la peor manera: alguien necesita
probar con datos reales, copia una tabla de producción, funciona, y la práctica se
instala sin que nadie la haya aprobado.

Datos de un core bancario en un entorno donde los ingenieros tienen acceso directo
y los controles son más laxos es el hallazgo de auditoría más común en proyectos de
datos bancarios. Y es de los que no se pueden deshacer: una vez copiado, el dato
estuvo ahí.

### Decisión propuesta

**DEV con datos sintéticos.** Generados por un script versionado, conservando forma
y casos borde, sin ningún dato real.

**QA con un subconjunto enmascarado de producción**, refrescado por un proceso
automatizado y auditado. Se seudonimizan identificadores de cliente con hash
estable —para que los joins sigan funcionando—, se sustituyen nombres, direcciones
y números de cuenta, y **se conservan importes y fechas**, porque son el objeto
mismo de la validación contable y enmascararlos haría imposible la reconciliación
contra MIS.

**Ningún dato se mueve de QA hacia producción, nunca.**

### Consecuencias

Hay que construir dos cosas que no están en la tabla de alcance actual: el
generador de datos sintéticos y el proceso de enmascaramiento. Es esfuerzo real que
debe entrar en el plan.

A cambio, la certificación se hace sobre datos con la forma correcta, el riesgo
regulatorio baja sustancialmente, y la conversación con auditoría deja de ser un
problema.

Conservar los importes sin identificación del titular es una decisión que Seguridad
debe validar explícitamente. Es defendible —el riesgo está en identificar a la
persona, no en el monto agregado— pero tiene que quedar aprobado por escrito.

---

## Decisiones ya tomadas por la arquitectura existente

Se registran aquí para que queden trazadas, aunque no requieren nueva aprobación.

<a id="adr-005"></a>
### ADR-005 · Arquitectura medallion de tres capas

**Estado:** Aceptada

Bronze (STAGE) réplica fiel, Silver (ODS) conformado, Gold (BDS) modelado para
consumo. La separación permite reprocesar Silver y Gold sin volver a tocar el core
bancario, que es la operación más cara y la más molesta para el origen.

<a id="adr-006"></a>
### ADR-006 · Unity Catalog como capa única de gobierno

**Estado:** Aceptada

Catálogo, linaje, permisos ABAC, enmascaramiento y auditoría en un solo lugar, con
un metastore único. El linaje de punta a punta —desde la tabla de Bantotal hasta el
campo del tablero de Power BI— es lo que hace viable responder a auditoría sin
arqueología.

<a id="adr-007"></a>
### ADR-007 · Serverless para pipelines y warehouse

**Estado:** Aceptada

Evita administrar clústeres y pagar tiempo ocioso. Consecuencia que hay que
gestionar: el costo se vuelve variable y depende del comportamiento de uso, lo que
hace obligatorio el etiquetado y las budget policies de
[08 · FinOps](08_OPERACION_OBSERVABILIDAD_FINOPS.md).

---

## Decisiones que propone esta documentación

<a id="adr-008"></a>
### ADR-008 · Databricks Asset Bundles como unidad de despliegue

**Estado:** Propuesta

**Alternativas consideradas:** Terraform para todo; despliegue por API con scripts
propios; sincronización de carpetas de Git en el workspace.

Terraform es excelente para infraestructura y torpe para trabajos y pipelines que
cambian a diario: cada cambio de un notebook se vuelve un `plan` y un `apply` sobre
el estado compartido. Los scripts propios funcionan hasta que quien los escribió se
va. La sincronización de Git no versiona la configuración del trabajo, solo el
código.

Los Bundles son el mecanismo nativo, cubren trabajos, pipelines, permisos y
parámetros, y `bundle validate` da verificación previa en el CI.

**Decisión:** Bundles para la carga de trabajo, Terraform para la infraestructura.
La frontera está en la velocidad de cambio: lo que cambia en cada sprint va en
Bundles; lo que cambia por trimestre va en Terraform.

<a id="adr-009"></a>
### ADR-009 · Trunk-based development

**Estado:** Propuesta

**Alternativa considerada:** GitFlow con ramas de release.

Con ramas de release largas, el equipo mantiene dos versiones de las reglas de
negocio a la vez. En un dominio contable eso significa dos versiones del mismo
saldo, y la pregunta "¿cuál es la regla vigente?" deja de tener respuesta única.

**Decisión:** `main` siempre desplegable, ramas de uno a tres días, liberación por
tag.

<a id="adr-010"></a>
### ADR-010 · La configuración de las 38 tablas es dato, no código

**Estado:** Propuesta

Origen, destino, modo de carga, clave y columna de corte de cada tabla viven en
`conf/catalogo_tablas.yml`, validado por esquema en el CI.

Agregar la tabla 39 es una línea de configuración revisada en un pull request, no
un notebook nuevo. Es lo que hace real el *framework de ingesta por metadatos* que
menciona la tabla de alcance, en lugar de 38 notebooks parecidos que divergen con
el tiempo.

**Consecuencia:** el esquema de validación de ese archivo es código crítico. Un
error ahí se propaga a las 38 tablas a la vez, así que tiene sus propias pruebas.

---

## Plantilla para nuevas ADR

```markdown
## ADR-NNN · Título en una línea

**Estado:** Propuesta
**Fecha:** AAAA-MM-DD
**Decide:** rol o comité

### Contexto
Qué situación obliga a decidir. Hechos, no opiniones.

### Opciones
Las que se consideraron de verdad, con lo bueno y lo malo de cada una.

### Decisión
Cuál se eligió.

### Consecuencias
Qué se gana y, sobre todo, QUÉ SE PIERDE. Una ADR sin consecuencias negativas
es una ADR que no analizó nada.
```
