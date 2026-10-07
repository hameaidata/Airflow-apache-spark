# ODS y BDS: qué pasa cuando una tabla falla

`BDS_DATAHUB_PROCESOS` (`dag_bds_datahub_procesos.py`) toma lo que dejó la capa
STG y lo sube a ODS y luego a BDS. Este documento es sobre una sola cosa: qué
ocurre cuando una tabla de esas no carga.

## Las dos reglas

**1. La capa ODS tiene que terminar.** Una tabla ODS que falla no detiene a las
demás, ni al nivel de `ORDEN` siguiente, ni a la capa BDS.

**2. BDS continúa solo con lo que pasó.** Una tabla BDS corre si y solo si las
tablas ODS de las que depende terminaron en éxito.

Y una tercera, que es la contrapartida de la primera: **que un fallo no trunque
el proceso no quiere decir que se oculte.** La corrida acaba en rojo.

## Lo que había antes

No había **un solo `trigger_rule`** en el archivo, así que todo era
`all_success`. Una tabla ODS caída bloqueaba el resto de su cadena, bloqueaba el
nivel de `ORDEN` siguiente, bloqueaba `ods_completo` y con eso la capa BDS
entera. Un incidente en una tabla se llevaba por delante la carga del día.

## Regla 1: ODS no se detiene

Toda tarea de ODS, y `ods_completo`, llevan `trigger_rule="all_done"`.

Es correcto hacerlo precisamente en ODS porque **ODS no declara dependencias de
datos**: `DEPENDE_DE` está prohibido ahí, y el DAG lo valida. El encadenamiento
por `ORDEN` y por `PARALELO: false` es orden de ejecución, no de datos — existe
para no abrir veinte conexiones al core a la vez, no porque una tabla necesite
a la anterior. Si no hay dependencia de datos, un fallo en una tabla no dice
nada sobre las demás, y detenerlas es pura pérdida.

En BDS **no** se puede hacer lo mismo en bloque, porque ahí sí hay dependencias
declaradas.

## Regla 2: la dependencia entre capas se declara

Hasta ahora un proceso BDS colgaba de `ods_completo` en bloque y no sabía de qué
tabla ODS dependía: no había dónde declararlo. Ahora sí, con `DEPENDE_DE_ODS`:

```json
{
  "ID_PROCESO": 103,
  "STORED_PROCEDURE": "SP_BDS_TIPO_CAMBIO",
  "DEPENDE_DE": "",
  "DEPENDE_DE_ODS": [104]
}
```

No se reutiliza `DEPENDE_DE` porque ahí los ids son de la capa BDS, y mezclar
las dos numeraciones en un campo obliga a adivinar a qué capa pertenece cada
número. Hoy no colisionan por casualidad; mañana sí.

### La dependencia es una arista real del grafo

`DEPENDE_DE_ODS` crea una arista **desde la tarea de ODS** hasta la de BDS, con
`all_success`. Si la tarea de ODS falla, Airflow marca la de BDS
`upstream_failed` y **no la programa**: no arranca, no ocupa un slot del pool y
no abre un log. «Ni siquiera intentarlo» es literal.

Se puede hacer así porque una dependencia de ODS y una de BDS son la **misma
clase de cosa** —datos— y las dos quieren la misma regla. Lo que no se podía
mezclar era una dependencia de datos con `ods_completo`, que es orden.

Una tabla BDS cuelga, entonces, de una de dos cosas, nunca de las dos:

| Declara | Cuelga de | Regla |
|---|---|---|
| `DEPENDE_DE_ODS` y/o `DEPENDE_DE` | esas tablas, directamente | `all_success` |
| nada | `ods_completo` o el nivel de `ORDEN` anterior | `all_done` |

### El escenario, con nombres

```
ods_modulos  ──►  bds_modulos
ods_procesos ──►  bds_procesos ──►  bds_operaciones
ods_operaciones ─────────────────►  bds_operaciones
```

```json
{"ID_PROCESO": 20, "TABLA_DESTINO": "BDS_MODULOS",
 "DEPENDE_DE": "",   "DEPENDE_DE_ODS": [10]},
{"ID_PROCESO": 21, "TABLA_DESTINO": "BDS_PROCESOS",
 "DEPENDE_DE": "",   "DEPENDE_DE_ODS": [11]},
{"ID_PROCESO": 22, "TABLA_DESTINO": "BDS_OPERACIONES",
 "DEPENDE_DE": [21], "DEPENDE_DE_ODS": [12]}
```

`bds_operaciones` declara las dos cosas, y las dos aristas existen: con una sola
o correría sin su insumo de ODS, o correría antes que `bds_procesos`.

Con `ods_modulos` caído, esto es lo que ocurre — es la salida de una corrida de
verdad, no un diagrama:

```
  FALLO     ods_carga.p010      (ods_modulos)
  OK        ods_carga.p011      (ods_procesos)
  OK        ods_carga.p012      (ods_operaciones)
  OK        ods_completo
  NO CORRE  bds_carga.p020      (bds_modulos)      upstream_failed
  OK        bds_carga.p021      (bds_procesos)
  OK        bds_carga.p022      (bds_operaciones)   despues de p021
  FALLO     fin

  ESTADO DE LA CORRIDA: failed
```

`bds_modulos` no llegó a ejecutarse. Las otras cinco tablas sí. Y la corrida
acaba en rojo.

### La red de seguridad

`ejecutar_bds()` vuelve a comprobar el estado de sus tablas de ODS antes de
llamar al stored procedure. Es redundante con el grafo **a propósito**, para el
único caso que se le escapa: que alguien haga Clear sobre esa tarea, a mano,
mientras su insumo sigue en rojo. Ahí Airflow sí la ejecutaría, y sin esa
comprobación el procedimiento correría sobre una tabla ODS que no se cargó.
Cuando salta, lo hace con un `AirflowSkipException` y el motivo en el log.

## Regla 3: la corrida acaba en rojo

Con `all_done` repartido por el grafo, la última tarea terminaría siempre en
éxito y Airflow marcaría la corrida entera en verde aunque dentro hubiera tablas
en rojo. **Nadie mira el detalle de una corrida verde.**

Por eso `fin` dejó de ser un `EmptyOperator`: ahora recorre el estado real de
cada tabla, deja un resumen legible en el log y falla si hubo algún fallo.

```
RESUMEN DE LA CORRIDA  (2 tabla(s))
  failed         1
      ods_datahub_orden_1.p104_carga_stg_ods_tipo_cambio
  skipped        1
      bds_datahub_maestros.p103_carga_ods_bds_tipo_cambio
WARNING  1 tabla(s) no se ejecutaron porque su insumo no estaba: ...

AirflowException: La corrida termino con 1 tabla(s) en error: ...
Las demas SI se cargaron -por eso el proceso no se trunco- y 1 se saltaron por
depender de estas. Reintente solo las fallidas y despues sus dependientes.
```

## Resumen de las reglas de disparo

| Tarea | Regla | Por qué |
|---|---|---|
| Toda tarea de ODS | `all_done` | ODS no tiene dependencias de datos |
| `ods_completo` | `all_done` | o un fallo en una tabla deja sin ejecutar toda la capa BDS |
| BDS que declara `DEPENDE_DE` o `DEPENDE_DE_ODS` | `all_success` | sus padres son dependencias de datos |
| BDS que no declara ninguna | `all_done` | sus padres solo marcan orden |
| `bds_completo` | `all_done` | tiene que alcanzarse siempre |
| `fin` | `all_done` + falla si algo falló | reportar, no ocultar |

La dependencia entre capas va por `DEPENDE_DE_ODS` y es una arista del grafo,
así que entra en la primera fila.

## Qué hacer cuando una tabla falla

1. En el log de `fin`, ver qué falló y qué se saltó por ello.
2. Arreglar la causa de la tabla en rojo.
3. Reintentar **solo esa tarea** (Clear en la interfaz).
4. Reintentar después las que se saltaron por depender de ella.

No hace falta relanzar el DAG entero: las demás tablas ya se cargaron, que es
justamente el objetivo de todo esto.

## Tests

`airflow/tests/unit/test_datahub_ods_bds.py`, 29 tests, incluido el escenario de arriba con sus nombres reales.

```bash
docker compose -f docker-compose.windows.yml exec airflow-scheduler \
  pytest /opt/airflow/tests/unit/test_datahub_ods_bds.py -v
```

Se comprobó que tienen dientes rompiendo el código a propósito cinco veces
—devolver ODS a `all_success`, hacer que `ods_completo` exija éxito, quitar la
compuerta de `ejecutar_bds`, que el resumen no falle nunca, y no pasarle a la
tarea sus dependencias de ODS—: cada una tumba exactamente los tests que le
corresponden.
