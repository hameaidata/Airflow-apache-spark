# 01 · Resumen ejecutivo

**Para:** Comité de Tecnología · Gerencia de Ingeniería y Analítica
**Asunto:** Práctica de Ingeniería de Datos y DataOps para el Data Hub de Finanzas
**Decisión que se pide:** aprobar el modelo de trabajo y resolver cuatro definiciones de arquitectura

---

## El problema que resuelve

El Data Hub de Finanzas ya tiene una arquitectura definida: 38 tablas de Bantotal y
MIS que se cargan a diario sobre Databricks en Azure, se transforman en tres capas y
alimentan cuatro tableros de Power BI que el área usa para cerrar.

Esa arquitectura describe **qué** se construye. No describe **cómo** se construye ni
cómo se sostiene. Y esa segunda mitad es la que decide si en dieciocho meses la
plataforma sigue siendo un activo o se ha convertido en algo que nadie quiere tocar.

La diferencia se ve en preguntas concretas que hoy no tienen respuesta escrita:

Cuando un analista cambia la regla de un saldo ajustado, ¿quién revisa ese cambio
antes de que llegue al tablero que ve el Directorio? Si el cierre de mañana sale mal
por ese cambio, ¿cuánto tarda en volver atrás? ¿Se puede volver atrás? Cuando
auditoría pregunte quién modificó el cálculo de provisiones el 14 de marzo y con qué
aprobación, ¿dónde está esa evidencia? Si alguien copia una tabla de producción al
entorno de desarrollo para probar, ¿quién se entera?

En una plataforma de datos de un banco, esas preguntas no son burocracia. Son el
control interno.

## Qué se propone

Aplicar al Data Hub la misma disciplina de ingeniería que ya se exige al software
del banco, adaptada a que aquí el artefacto no es solo código: es código **más
datos con estado**.

La propuesta se sostiene sobre cuatro principios, y cada uno tiene un documento
técnico que lo desarrolla.

**Todo vive en Git.** Pipelines, trabajos, reglas de calidad, configuración de
catálogos, permisos y esta documentación. Nada se crea editando la interfaz de
Databricks. Si no está en el repositorio, no existe en producción.

**El despliegue es un artefacto, no una persona.** Un mismo paquete versionado
(Databricks Asset Bundle) se despliega a desarrollo, a certificación y a producción
cambiando únicamente los parámetros del entorno. Lo que se probó en QA es
literalmente lo mismo que entra a producción.

**La calidad se verifica antes, no después.** Las reglas de calidad se ejecutan en
el pipeline y detienen la carga cuando algo no cuadra, en lugar de dejar que el
error llegue al tablero y que lo descubra el usuario de Finanzas al cierre.

**Todo cambio deja rastro.** Quién lo pidió, quién lo revisó, quién lo aprobó, qué
se desplegó y cuándo. No como un informe que alguien redacta después, sino como
subproducto automático del propio flujo de trabajo.

## Qué cambia respecto de cómo se trabaja hoy

| | Sin esta propuesta | Con esta propuesta |
|---|---|---|
| Un cambio en una regla de negocio | Se edita en la interfaz del entorno correspondiente | Se propone en un pull request, se revisa, pasa pruebas y se despliega |
| Pasar a producción | Se replica a mano lo que se hizo en desarrollo | Se despliega el mismo artefacto ya probado, con aprobación registrada |
| Un error en el cierre | Se corrige en caliente en producción | Se revierte al artefacto anterior; la corrección sigue el flujo normal |
| Evidencia para auditoría | Se reconstruye a mano cuando la piden | Existe desde el primer día, generada por el propio proceso |
| Un dato que no cuadra | Lo detecta Finanzas al usar el tablero | Lo detecta el pipeline y detiene la publicación |
| Onboarding de un ingeniero nuevo | Depende de quién esté disponible para explicarle | Repositorio, estándares y runbooks escritos |

## Lo que se pide aprobar

No se pide presupuesto adicional de plataforma: la propuesta usa los servicios que
el proyecto ya contempla. Lo que se pide es **decidir cuatro definiciones** que hoy
están contradichas entre los propios documentos de diseño, y **aceptar el modelo de
trabajo** que impone disciplina sobre cómo se cambia la plataforma.

Las cuatro definiciones, con sus opciones y la recomendación técnica de cada una,
están en [10 · Decisiones de arquitectura](10_DECISIONES_ADR.md). En resumen:

**Cuántos entornos.** Los documentos dicen dos, tres y uno según cuál se lea. La
recomendación es tres catálogos sobre dos workspaces: separa desarrollo de
certificación sin duplicar el costo de infraestructura.

**Si Power BI usa DirectQuery.** Un documento lo excluye y otro lo incluye. Tienen
consecuencias opuestas sobre el costo: DirectQuery mantiene cómputo encendido cada
vez que un usuario abre un tablero. La recomendación es Import con refresco
incremental, y DirectQuery solo como excepción justificada.

**Si entra CDC en esta fase.** Un documento lo declara fuera de alcance y otro lo
dibuja como el origen principal. Son dos proyectos distintos en esfuerzo y en
licenciamiento. La recomendación es mantenerlo fuera de la Fase 1 y dejar la
arquitectura preparada.

**Qué datos viven en desarrollo y certificación.** Ningún documento lo dice. Es la
más importante de las cuatro: copiar datos de producción a un entorno con controles
más laxos es el hallazgo de auditoría más común en proyectos de datos bancarios.

## Qué pasa si no se hace

La plataforma va a funcionar igual el primer trimestre. El costo aparece después, y
es acumulativo.

Sin control de versiones, los entornos divergen en silencio y llega el día en que
lo que está en producción no se parece a nada de lo que hay documentado. Sin
pruebas automáticas, cada cambio es una apuesta y el equipo aprende a no tocar lo
que funciona, que es como mueren las plataformas de datos: no por fallar, sino por
volverse intocables. Sin trazabilidad, cada requerimiento de auditoría se convierte
en una semana de arqueología. Y sin estándares escritos, la operación depende de
las dos o tres personas que saben cómo está armado, que es un riesgo de
concentración que el propio banco no aceptaría en ningún otro proceso crítico.

## Alcance de esta propuesta

Cubre la ingeniería de datos y la operación de la plataforma: repositorio, pruebas,
despliegue, calidad, gobierno técnico, observabilidad y control de costos.

No cubre el diseño funcional del modelo contable, las reglas de negocio de Finanzas
ni el contenido de los tableros. Esos son insumos que la propuesta consume, no
produce.

---

**Siguiente paso sugerido:** una sesión de dos horas con Arquitectura y Seguridad
para cerrar las cuatro decisiones de [ADR](10_DECISIONES_ADR.md). Todo lo demás
puede avanzar en paralelo.
