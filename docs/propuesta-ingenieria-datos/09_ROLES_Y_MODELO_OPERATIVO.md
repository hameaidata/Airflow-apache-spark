# 09 · Roles y modelo operativo

---

## 1. Los roles

Son **roles, no personas**. En un equipo pequeño una persona cubre varios; lo que
importa es que cada responsabilidad tenga dueño y que nadie apruebe su propio
trabajo.

### Ingeniero de Datos

Construye y mantiene los pipelines. Es el rol con más volumen de trabajo diario.

Escribe las transformaciones de las tres capas, las reglas de calidad y sus pruebas;
mantiene `catalogo_tablas.yml`; revisa el código de sus pares; y atiende los
incidentes de su dominio.

Necesita: Spark y SQL con soltura, Python de calidad productiva, Delta Lake, Git y
trabajo con pull requests, y capacidad de leer un plan de ejecución cuando algo va
lento. La parte que más suele faltar no es la técnica: es entender el dominio
contable lo suficiente para que una regla tenga sentido, y para saber cuándo
preguntar en vez de suponer.

### Arquitecto de Datos

Decide la forma. Es el rol que evita que la plataforma se convierta en la suma de
decisiones locales de cada sprint.

Define el modelo por capa y los contratos entre ellas; escribe y mantiene las ADR;
revisa los cambios que cruzan dominios; y es quien dice que no cuando un atajo
hipoteca la plataforma.

Necesita: modelado dimensional, criterio para decidir qué va en Gold Enterprise y
qué en Gold Analytics, y —sobre todo— la disciplina de escribir por qué se decidió
cada cosa. Un arquitecto que no documenta sus decisiones obliga a redescubrirlas
cada seis meses.

### Ingeniero DevOps / Plataforma

Sostiene el andamiaje. Es el rol que hace que los otros dos puedan trabajar.

Mantiene la infraestructura como código, los pipelines de CI/CD, las identidades y
el secreto de cada entorno, la observabilidad y el tablero de costos.

Necesita: Terraform, Azure DevOps, redes de Azure, y entender lo suficiente de
Databricks para que los bundles no sean una caja negra. La diferencia entre un
DevOps que sirve a una plataforma de datos y uno genérico está en comprender por qué
revertir datos no es lo mismo que revertir código.

### Especialista en Gobierno de Datos

Es el rol que más se posterga y el que más caro sale postergar en un banco.

Mantiene el catálogo y el glosario, define las políticas de clasificación y
enmascaramiento, revisa los permisos otorgados, y es el interlocutor de auditoría.

Puede ser una dedicación parcial. Lo que no puede es no existir: sin este rol, el
catálogo se llena de tablas sin descripción, los permisos se otorgan y nunca se
revisan, y la conversación con auditoría empieza de cero cada vez.

### Analista de Datos de Finanzas

No es parte del equipo de ingeniería, pero sin él la plataforma construye lo
equivocado con mucha disciplina.

Define las reglas de negocio, valida que el número sea el número, ejecuta la UAT y
acepta o rechaza el pase a producción de lo que afecta a su dominio.

---

## 2. Quién hace qué

`R` responsable · `A` aprueba · `C` se le consulta · `I` se le informa

| Actividad | Ing. Datos | Arquitecto | DevOps | Gobierno | Finanzas |
|---|---|---|---|---|---|
| Diseñar el modelo de una capa | C | **R/A** | I | C | C |
| Construir un pipeline | **R** | C | I | I | I |
| Definir una regla de negocio | C | C | I | I | **R/A** |
| Implementar una regla de calidad | **R** | C | I | C | **A** |
| Revisar un pull request | **R** | C | C | I | I |
| Aprobar un cambio en Gold Enterprise | C | **A** | I | C | **A** |
| Desplegar a producción | I | I | **R** | I | **A** |
| Otorgar un permiso | I | C | **R** | **A** | I |
| Atender un incidente | **R** | C | C | I | I |
| Decidir si se revierte una tabla | **R** | **A** | C | I | **C** |
| Escribir una ADR | C | **R/A** | C | C | I |
| Revisar el costo mensual | C | C | **R** | I | I |

Tres filas donde la doble aprobación es deliberada.

**Aprobar un cambio en Gold Enterprise** requiere arquitecto y Finanzas. Es donde
vive el número que se defiende ante el Directorio: ni la corrección técnica ni la
contable alcanzan por separado.

**Desplegar a producción** lo ejecuta DevOps pero lo aprueba Finanzas. Quien sufre
las consecuencias de un cambio decide cuándo entra.

**Otorgar un permiso** lo ejecuta DevOps pero lo aprueba Gobierno. Separar quien
tiene la capacidad técnica de quien tiene la autoridad es lo que evita que los
permisos se otorguen por conveniencia operativa.

---

## 3. Cómo se trabaja con Finanzas

El patrón que falla es el conocido: Finanzas pide, Ingeniería construye, Finanzas
recibe algo que no era, y se repite. Falla porque la conversación ocurre dos veces
—al principio y al final— y en el medio nadie valida nada.

Lo que se propone:

**Un requerimiento empieza con un ejemplo, no con una descripción.** "El saldo
ajustado debe considerar los ajustes manuales" admite cinco interpretaciones. Una
planilla con diez casos y el resultado esperado de cada uno admite una. Ese ejemplo
se convierte directamente en caso de prueba, así que el esfuerzo de escribirlo no se
pierde.

**Validación temprana en QA, no al final.** En cuanto hay algo que corre, aunque
sea parcial. Descubrir a las tres semanas que el criterio de agrupación era otro es
barato; descubrirlo en la corrida en paralelo, no.

**Un punto de contacto por dominio.** No un comité. Una persona de Finanzas que
puede decidir sobre contabilidad, otra sobre planeamiento. Las decisiones que
requieren comité son las de alcance, no las de una regla.

**La corrida en paralelo de tres semanas es validación, no descubrimiento.** Si
durante el paralelo aparecen reglas de negocio nuevas, el problema está aguas
arriba: se saltó la validación temprana. El paralelo sirve para confirmar que lo
acordado se cumple, no para averiguar qué se quería.

---

## 4. Ceremonias

Lo mínimo que hace falta, sin convertir la operación en una agenda.

| Cuándo | Qué | Quiénes | Duración |
|---|---|---|---|
| Diaria | Revisión de la carga de la noche y de las alertas | Ingeniería de Datos | 15 min |
| Semanal | Prioridades y bloqueos | Ingeniería + Arquitecto + Finanzas | 45 min |
| Quincenal | Revisión de ADR pendientes y deuda técnica | Arquitecto + Ingeniería | 1 h |
| Mensual | Costos, calidad, métricas del proceso | Todos + gerencia | 1 h |
| Por incidente | Análisis sin culpables | Quienes intervinieron | 1 h |

El análisis posterior a incidentes merece una nota. Se hace **sin buscar
culpables**, y no por cortesía: un equipo que sabe que el análisis termina en un
señalado deja de reportar los incidentes menores, que son justamente los que avisan
del grande. La pregunta correcta nunca es quién se equivocó, sino qué permitió que
ese error llegara a producción sin que nada lo detuviera.

---

## 5. Cómo entra alguien nuevo

Sin un camino definido, incorporar a alguien depende de quién esté libre para
explicarle, y lo que aprende depende de a quién le tocó.

**Primer día.** Accesos, clonar el repositorio, leer este conjunto de documentos en
el orden del [README](README.md).

**Primera semana.** Levantar el entorno de desarrollo, correr las pruebas, seguir
un pipeline de punta a punta con el depurador, y hacer un pull request pequeño y
real —arreglar un mensaje de error, agregar una prueba— para recorrer el flujo
completo.

**Primer mes.** Tomar un dominio acotado con acompañamiento, participar en revisión
de código como revisor, y estar presente en un incidente sin ser el responsable.

**Criterio para considerarlo autónomo:** puede desplegar a producción un cambio
suyo, explicar de dónde viene un número del tablero de Contabilidad, y sabe qué
hacer si la carga falla de madrugada.

---

## 6. Qué pasa si falta un rol

Vale la pena tenerlo escrito, porque la tentación de empezar sin algunos es fuerte.

| Rol ausente | Qué ocurre |
|---|---|
| Arquitecto | Cada sprint decide por su cuenta; en seis meses hay tres formas de hacer lo mismo y ninguna documentada |
| DevOps | Los ingenieros despliegan a mano, los entornos divergen y nadie sabe qué hay en producción |
| Gobierno | El catálogo se llena de tablas sin descripción y los permisos nunca se revisan; la primera auditoría lo encuentra |
| Analista de Finanzas | Se construye rápido y bien algo que no era lo que hacía falta |

El más fácil de postergar es Gobierno, porque su ausencia no duele en el corto
plazo. También es el que más caro sale recuperar: catalogar y clasificar hacia atrás
cuatrocientas tablas es un proyecto en sí mismo.

---

## Documentos relacionados

- [03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md) — el flujo en el que estos roles operan
- [08 · Operación y observabilidad](08_OPERACION_OBSERVABILIDAD_FINOPS.md) — guardia y escalamiento
- [11 · Plan de adopción](11_PLAN_DE_ADOPCION.md) — cuándo hace falta cada rol
