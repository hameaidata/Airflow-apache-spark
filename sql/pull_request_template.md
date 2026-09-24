<!--
  Plantilla de pull request · Data Hub Finanzas
  Se copia a .azuredevops/pull_request_template.md en la raiz del repositorio.

  Un PR que cambia una regla de negocio y de paso reordena imports es un PR que
  nadie revisa bien: el revisor se pierde entre el ruido y aprueba lo que no
  miro. Un PR, un proposito.
-->

## Qué cambia

<!-- Una o dos frases. Si necesitas más de un párrafo, probablemente son dos PR. -->

## Por qué

<!-- El problema que resuelve, no la solución. Enlaza el ticket. -->

Ticket: DH-

## Capas afectadas

- [ ] Ingesta
- [ ] Bronze
- [ ] Silver
- [ ] Gold Enterprise ← **requiere dos aprobadores**
- [ ] Gold Analytics
- [ ] Power BI
- [ ] Infraestructura / CI-CD
- [ ] Solo documentación

---

## Lista de verificación del autor

### Correctitud

- [ ] **Es idempotente**: correr esto dos veces sobre el mismo día produce el mismo resultado
- [ ] No hay rutas, catálogos ni identificadores fijos en el código
- [ ] No hay `if entorno == "prod"` dentro de una transformación
- [ ] Si toca Silver o Gold, la lista de columnas es explícita (sin `SELECT *`)

### Pruebas

- [ ] Hay una prueba que **falla si alguien deshace este cambio**
- [ ] Las pruebas unitarias pasan en local
- [ ] Si cambia una regla de negocio, el caso de ejemplo de Finanzas está como prueba

### Contratos y configuración

- [ ] Si cambia el esquema de una tabla de frontera, el contrato se actualizó **en este mismo PR**
- [ ] Los consumidores declarados en el contrato están avisados
- [ ] Si agrega o cambia una tabla, `conf/catalogo_tablas.yml` está actualizado

### Documentación

- [ ] La documentación afectada se actualizó en este mismo PR
- [ ] Si es una decisión de arquitectura, hay una ADR

### Operación

- [ ] Sé cómo revertir esto si sale mal, y está escrito abajo
- [ ] No requiere intervención manual en producción para funcionar

---

## Cómo se revierte

<!--
  Obligatorio para cambios en Silver, Gold o ingesta.

  Ejemplo:
    1. Desplegar el tag anterior.
    2. La tabla gold_analytics.cu_contabilidad hay que restaurarla a la versión
       previa a la carga del día, porque este cambio la reescribe.
    3. Volver a correr el job gold_contabilidad_saldos.
-->

## Qué mirar después de desplegar

<!--
  Qué señal confirma que funcionó. No "revisar que esté bien", sino algo
  concreto: qué tabla, qué número, qué tablero.
-->

---

## Para el revisor

Más allá de que el código funcione:

- ¿Qué pasa si esto corre dos veces?
- ¿Rompe algún contrato declarado?
- ¿La prueba realmente fallaría si alguien deshace el cambio, o solo verifica lo obvio?
- Si sale mal en producción a las 3 de la mañana, ¿el procedimiento de arriba es suficiente?
- ¿La documentación quedó diciendo lo que el código hace?
