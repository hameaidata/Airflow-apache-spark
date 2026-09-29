# 08 · Operación, observabilidad y FinOps

---

## 1. El acuerdo de servicio

Lo primero que hay que fijar, porque sin esto no se puede decidir nada más: si no
está escrito a qué hora tiene que estar el dato, no hay forma de saber si el proceso
va bien o va mal.

| Concepto | Compromiso |
|---|---|
| Ventana de carga | 02:00 a 06:00 |
| Dato disponible en Power BI | 07:00 |
| Puntualidad | 99% de los días hábiles dentro de ventana |
| Tiempo de detección de un fallo | Menos de 15 minutos |
| Tiempo de recuperación | Menos de 4 horas dentro del día hábil |
| Ventana de cierre mensual | Prioridad máxima; se escala de inmediato |

Los números son una propuesta de partida: hay que acordarlos con Finanzas, no
imponerlos. Lo que no es negociable es que existan y estén escritos.

**Una tabla Gold que no cargó no se publica vacía ni a medias.** Conserva el dato
del día anterior y el tablero muestra la fecha del dato de forma visible. Ver
[06 · Calidad](06_CALIDAD_Y_PRUEBAS_DE_DATOS.md#4-qué-detiene-qué).

---

## 2. Qué se monitorea

Cuatro capas, de más técnica a más cercana al negocio. Las de abajo detectan
problemas; las de arriba detectan que el problema importa.

| Capa | Qué mide | Fuente |
|---|---|---|
| Ejecución | Trabajos fallidos, duración, reintentos | Lakeflow + `system.lakeflow` |
| Datos | Expectativas, cuadre con MIS, volumetría | `ctl.ctl_calidad` |
| Consumo | Refresco de Power BI, uso del warehouse | Power BI + `system.access` |
| Costo | DBU por catálogo y trabajo, tendencia | `system.billing` |

### Señales que no son "falló un trabajo"

Un pipeline que termina en verde puede estar produciendo basura. Las señales que
detectan eso antes que el usuario:

**Volumetría fuera de rango.** Una tabla que carga 2 millones de registros diarios
y hoy cargó 200.000 no falló: cargó. Es una alerta, y de las importantes.

**Duración anómala.** Un proceso que tarda el triple suele estar leyendo más de lo
que debería, y es el aviso temprano de una marca de agua mal calculada.

**Tasa de descartes en aumento.** Una regla que pasa de 0,1% a 3% de filas
descartadas no falla, pero está diciendo que algo cambió en el origen.

**Última carga exitosa.** La señal más simple y la más olvidada. Si una tabla lleva
tres días sin cargar porque su trabajo está deshabilitado, nada falla nunca: no hay
error que reportar. Se detecta preguntando por la ausencia, no por el fallo.

---

## 3. Alertas

Tres niveles, y el criterio para asignarlos es qué se espera que haga quien la
recibe.

| Nivel | Cuándo | A quién | Canal |
|---|---|---|---|
| Crítica | La carga no terminó dentro de ventana; el cuadre con MIS falla; cierre mensual comprometido | Guardia | Teléfono + Teams |
| Alta | Un trabajo falló pero hay margen para reintentar; volumetría anómala | Equipo de datos | Teams + correo |
| Informativa | Despliegue realizado; tendencia de calidad; costo sobre umbral | Equipo de datos | Correo diario |

**Una alerta que nadie atiende hay que borrarla o arreglarla.** El mayor riesgo de
un sistema de alertas no es que falten: es que sobren. Un equipo que recibe treinta
avisos diarios deja de leerlos, y el día que llega el importante pasa igual de
desapercibido. Revisar trimestralmente qué alertas se dispararon y qué acción
provocó cada una es parte del mantenimiento, no un lujo.

---

## 4. Runbooks

Cada alerta crítica tiene un procedimiento escrito, en `docs/runbooks/`. Quien está
de guardia a las 3 de la mañana no debe improvisar ni despertar a nadie para las
primeras tres preguntas.

Estructura de cada runbook:

```markdown
# RUNBOOK · La carga diaria no terminó en ventana

## Cómo se ve
Alerta "carga_diaria_fuera_de_ventana". Tablero operativo en rojo.

## Primera pregunta: ¿el origen respondió?
    SELECT * FROM ctl.ctl_lote WHERE fecha_proceso = current_date();
Si la ingesta de Bantotal está en ERROR con timeout → el core no respondió.
No es un problema del Data Hub. Ir al paso "Escalar al área del core".

## Segunda pregunta: ¿falló una regla de calidad?
    SELECT * FROM ctl.ctl_calidad WHERE fecha = current_date() AND estado = 'FAIL';
Si hay filas → la carga se detuvo a propósito. NO forzar la publicación.
Ir al paso "Evaluar con Finanzas".

## Tercera pregunta: ¿es un fallo transitorio?
Revisar el log de la tarea. Si es timeout o error de red, reintentar UNA vez.
Si vuelve a fallar, escalar.

## Qué NO hacer
- No editar datos directamente en producción. Nunca. Por ninguna razón.
- No deshabilitar una regla de calidad para que la carga pase.
- No publicar a Gold manualmente saltando el pipeline.

## Escalamiento
15 min sin diagnóstico → responsable de Ingeniería de Datos
1 h sin solución y en cierre mensual → gerencia
```

El apartado "Qué NO hacer" es el más valioso y el que suele faltar. A las 3 de la
mañana, con presión de cierre, deshabilitar la regla que está bloqueando la carga
parece la solución obvia. Escrito de antemano y en frío, se ve lo que es.

Runbooks mínimos para arrancar: carga fuera de ventana, cuadre con MIS fallido,
refresco de Power BI fallido, volumetría anómala, y despliegue a producción fallido.

---

## 5. FinOps

El serverless cambia la naturaleza del costo: deja de ser una factura fija y pasa a
depender del comportamiento. Eso es bueno —se paga lo que se usa— y exige control,
porque un error de configuración se traduce en dinero de inmediato.

### Etiquetado

Todo recurso lleva etiquetas desde su creación. Sin etiquetas, `system.billing`
muestra un número total que no se puede atribuir ni discutir con nadie.

```yaml
tags:
  proyecto: datahub-finanzas
  entorno: prod              # dev | qa | prod | sandbox
  capa: gold                 # bronze | silver | gold | ingesta
  centro_costo: finanzas
  responsable: ingenieria-datos
```

### Controles

| Control | Qué hace |
|---|---|
| Budget policies por entorno | Límite de gasto con alerta al 80% |
| Auto-stop del warehouse | 5 minutos; el warehouse no queda encendido |
| Talla mínima del warehouse | Se sube solo con evidencia de que hace falta |
| Cuota del `sandbox` | Presupuesto propio y acotado |
| Sin DirectQuery | El costo de consumo no depende de cuánta gente mire tableros |
| Revisión mensual | Tendencia por capa y por trabajo |

### Las tres palancas que mueven la aguja

Por orden de impacto, para cuando haya que reducir costo:

**El warehouse encendido sin necesidad.** Es la primera causa de sobrecosto en
plataformas serverless. El auto-stop de cinco minutos y la ausencia de DirectQuery
lo controlan. Si alguien deja una sesión abierta con una consulta que refresca
sola, el auto-stop nunca se dispara: hay que vigilarlo.

**Reprocesos completos innecesarios.** Una tabla en modo completo que podría ser
incremental multiplica el costo por el número de días de historia. Es el principal
motivo para que `catalogo_tablas.yml` sea revisado en pull request.

**El `sandbox` sin cuota.** Un experimento de ML sobre datos completos puede costar
más en una tarde que el pipeline productivo en un mes. Por eso el sandbox tiene
presupuesto propio.

### Tablero de costos

Sobre `system.billing`, con cuatro vistas: gasto diario por entorno con su
tendencia, los diez trabajos más caros del mes, DBU por capa, y gasto contra
presupuesto con proyección de fin de mes.

Esa última vista es la que evita la conversación incómoda a fin de mes, que es
precisamente cuando ya no se puede hacer nada.

---

## 6. Mantenimiento periódico

| Cadencia | Actividad |
|---|---|
| Diaria | Revisar el tablero operativo y las alertas de la noche |
| Semanal | Revisar tendencia de calidad y volumetrías anómalas |
| Mensual | Revisar costos contra presupuesto; revisar tablas sin reglas de calidad |
| Trimestral | Revisar alertas que nadie atendió; revisar permisos otorgados; revisar ADR vigentes |
| Semestral | Prueba de recuperación: revertir una tabla Gold en QA y medir cuánto tardó |

La última es la que casi nadie hace y la que decide cómo sale el día del incidente
real. Un procedimiento de reversión escrito y nunca ejecutado es una hipótesis, no
un procedimiento.

---

## Documentos relacionados

- [03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md) — reversión y métricas del proceso
- [06 · Calidad y pruebas de datos](06_CALIDAD_Y_PRUEBAS_DE_DATOS.md) — qué detiene la publicación
- [09 · Roles y modelo operativo](09_ROLES_Y_MODELO_OPERATIVO.md) — quién atiende qué
