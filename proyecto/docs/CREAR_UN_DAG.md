# Crear un DAG desde cero

Tutorial progresivo. Cada paso añade una pieza y se puede verificar antes de
seguir. Al final tendrá un proceso completo y sabrá qué hace cada línea.

**Regla de oro:** no escriba el DAG completo y después pruebe. Escriba el paso 1,
compruebe que aparece, y recién entonces siga. Si algo falla, sabrá exactamente
qué línea lo causó.

---

## Antes de empezar: dos comandos que le van a ahorrar horas

Estos ejecutan su DAG **sin pasar por la interfaz ni por el planificador**. Son
la forma rápida de programar: escribe, ejecuta, ve el error, corrige.

**Probar una sola tarea:**

```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow tasks test mi_primer_dag saludar 2026-08-22
```

**Probar el DAG completo:**

```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow dags test mi_primer_dag 2026-08-22
```

Ambos imprimen todo en la terminal. No crean ejecuciones reales ni ensucian el
historial.

**Ver si su archivo tiene errores:**

```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow dags list-import-errors
```

Si un DAG no aparece en la interfaz, este comando dice por qué. Airflow descarta
en silencio los archivos que fallan al importarse.

---

## Paso 1 — El DAG más pequeño que existe

Cree `airflow/dags/production/mi_primer_dag.py`:

```python
from datetime import datetime
from airflow.models.dag import DAG

dag = DAG(
    dag_id="mi_primer_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
```

Guarde y espere hasta 30 segundos.

**Verifique:** abra http://localhost:8080. Debe aparecer `mi_primer_dag` en la
lista, **pausado** (el interruptor a la izquierda, en gris).

### Qué hace cada línea

| Línea | Para qué |
|---|---|
| `dag_id` | El nombre. **Debe ser único** en toda la instalación |
| `start_date` | Desde cuándo tiene sentido este proceso. Una fecha en el pasado |
| `schedule=None` | No se ejecuta solo. Solo cuando usted lo dispare |
| `catchup=False` | **No ejecutar retroactivamente** todas las fechas desde `start_date` |

> **`catchup=False` no es opcional.** Si lo omite, al despausar un DAG con
> `start_date` de hace dos años y frecuencia diaria, Airflow encola **730
> ejecuciones** de golpe. Es el error más caro que se comete al empezar.

> **La variable se llama `dag`, pero podría llamarse como sea.** Airflow busca
> objetos de tipo `DAG` en el ámbito global del archivo. Lo que importa es que
> exista como variable de módulo, no cómo se llame.

---

## Paso 2 — Añadir una tarea

Un DAG sin tareas no hace nada. Reemplace el archivo:

```python
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def saludar():
    print("Hola desde Airflow")


dag = DAG(
    dag_id="mi_primer_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)

tarea_1 = PythonOperator(
    task_id="saludar",
    python_callable=saludar,
    dag=dag,
)
```

**Verifique sin la interfaz:**

```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow tasks test mi_primer_dag saludar 2026-08-22
```

Entre las líneas de registro debe salir `Hola desde Airflow`.

> `python_callable=saludar` va **sin paréntesis**. Le está pasando la función a
> Airflow para que él la llame después. Con `saludar()` la ejecutaría ahora, al
> importar el archivo, que es cada 30 segundos.

---

## Paso 3 — Dos tareas en orden

```python
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def extraer():
    print("Extrayendo datos...")


def cargar():
    print("Cargando datos...")


dag = DAG(
    dag_id="mi_primer_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)

t_extraer = PythonOperator(task_id="extraer", python_callable=extraer, dag=dag)
t_cargar  = PythonOperator(task_id="cargar",  python_callable=cargar,  dag=dag)

t_extraer >> t_cargar
```

La última línea es la dependencia: `cargar` no arranca hasta que `extraer`
termine bien.

| Se escribe | Significa |
|---|---|
| `a >> b` | `b` después de `a` |
| `a >> [b, c]` | `b` y `c` en paralelo, después de `a` |
| `[a, b] >> c` | `c` espera a que terminen `a` y `b` |

**Verifique:** en la interfaz, vista **Graph**. Debe ver dos cuadros unidos por
una flecha.

---

## Paso 4 — Pasar datos entre tareas

Lo que una tarea **devuelve** queda disponible para las siguientes.

```python
def extraer():
    filas = 1500
    print(f"Extraidas {filas} filas")
    return {"filas": filas, "origen": "ventas"}


def cargar(**context):
    datos = context["ti"].xcom_pull(task_ids="extraer")
    print(f"Cargando {datos['filas']} filas de {datos['origen']}")
```

Dos cambios: `cargar` recibe `**context`, y usa `xcom_pull` indicando de qué
tarea quiere el resultado.

> ### Lo que NO debe pasar por aquí
>
> Este mecanismo se llama XCom y **guarda los datos en la base de metadatos de
> Airflow**. Sirve para pasar cifras, rutas, banderas, identificadores.
>
> **Nunca pase un conjunto de datos.** Un DataFrame de un millón de filas por
> XCom infla la base y degrada el planificador para todos los procesos.
>
> Lo correcto: la tarea escribe el archivo a disco y devuelve **la ruta**. La
> siguiente lee esa ruta.

---

## Paso 5 — Usar la fecha del proceso

Un proceso diario necesita saber qué día está procesando. **No use
`datetime.now()`**: si el proceso del lunes se reintenta el martes, procesaría
el día equivocado.

```python
def extraer(**context):
    fecha = context["ds"]        # '2026-08-22'
    print(f"Extrayendo datos de {fecha}")
    return {"fecha": fecha}
```

Lo más usado del contexto:

| Variable | Qué es |
|---|---|
| `context["ds"]` | Fecha lógica, `'2026-08-22'` |
| `context["ds_nodash"]` | Igual, sin guiones: `'20260822'` |
| `context["ti"]` | La tarea actual — para XCom, reintentos |
| `context["dag_run"].run_id` | Identificador único de esta ejecución |
| `context["params"]` | Parámetros que se pasan al disparar |

> **`ds` es la fecha lógica, no la de hoy.** Si reprocesa el 15 de marzo dentro
> de seis meses, `ds` vale `2026-03-15`. Por eso los reprocesos dan el mismo
> resultado. Esta es la diferencia entre un proceso reproducible y uno que no.

---

## Paso 6 — Reintentos y tiempos límite

Hasta aquí, si una tarea falla, se queda fallada. En producción eso no sirve:
una caída momentánea de red no debería requerir intervención humana.

```python
from datetime import datetime, timedelta

default_args = {
    "owner": "hjara",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    dag_id="mi_primer_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
)
```

| Parámetro | Qué hace | Por qué importa |
|---|---|---|
| `owner` | Quién es responsable | Sale en la interfaz. Que no diga "airflow" |
| `retries` | Cuántas veces reintentar | Absorbe fallos momentáneos |
| `retry_delay` | Espera entre intentos | Reintentar al instante suele fallar igual |
| `execution_timeout` | Corte por tiempo | **Sin esto, una tarea colgada bloquea un worker para siempre** |
| `max_active_runs=1` | Una ejecución a la vez | Evita que dos ejecuciones escriban lo mismo |

> `execution_timeout` es el que más se olvida y el que más problemas causa. Una
> consulta que se cuelga sin él deja un worker ocupado indefinidamente, y nadie
> se entera hasta que se acaban los workers.

---

## Paso 7 — Programar la ejecución

```python
dag = DAG(
    dag_id="mi_primer_dag",
    schedule="0 6 * * *",     # todos los dias a las 06:00
    ...
)
```

| Se escribe | Cuándo corre |
|---|---|
| `None` | Solo manual |
| `"@daily"` | Todos los días a medianoche |
| `"@hourly"` | Cada hora en punto |
| `"0 6 * * *"` | Todos los días a las 06:00 |
| `"0 6 * * 1-5"` | Lunes a viernes a las 06:00 |
| `"*/15 * * * *"` | Cada 15 minutos |

Los cinco campos del cron: `minuto hora día-del-mes mes día-de-semana`.

> **El horario es UTC**, no la hora de Perú. `"0 6 * * *"` corre a la **01:00**
> hora local (UTC-5). Si quiere las 06:00 de Lima, escriba `"0 11 * * *"`.
>
> Se puede cambiar la zona horaria del DAG con `pendulum`, pero mientras haya un
> solo huso, calcular el desfase es más simple y menos propenso a errores.

**Segundo detalle:** una ejecución con `schedule="@daily"` y fecha lógica
`2026-08-22` **arranca al terminar ese día**, no al empezarlo. Airflow espera a
que el intervalo se cierre. Es correcto —procesa un día completo— pero sorprende
la primera vez.

---

## Paso 8 — Conectar a una base de datos

Nunca ponga credenciales en el archivo. Van en **Admin → Connections**.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extraer(**context):
    hook = PostgresHook(postgres_conn_id="mi_base")
    filas = hook.get_records(
        "SELECT id, monto FROM ventas WHERE fecha = %s",
        parameters=(context["ds"],),
    )
    print(f"Obtenidas {len(filas)} filas")
    return {"filas": len(filas)}
```

> **Los parámetros van con `%s`, nunca concatenados.**
>
> ```python
> # MAL — permite inyección SQL
> f"SELECT * FROM ventas WHERE fecha = '{fecha}'"
>
> # BIEN
> "SELECT * FROM ventas WHERE fecha = %s", parameters=(fecha,)
> ```

Ejemplos completos para SQL Server, DB2 y procedimientos almacenados:
`docs/GUIA_CONEXIONES_DAGS.md`.

---

## Paso 9 — Enterarse cuando falla

```python
import logging

log = logging.getLogger(__name__)


def avisar_fallo(context):
    ti = context["task_instance"]
    log.error(
        "FALLO dag=%s tarea=%s intento=%s fecha=%s",
        ti.dag_id, ti.task_id, ti.try_number, context["ds"],
    )
    # Aqui iria el envio de correo o la llamada al sistema de alertas


dag = DAG(
    dag_id="mi_primer_dag",
    on_failure_callback=avisar_fallo,
    ...
)
```

Puesto en el DAG, aplica a todas sus tareas. Se dispara **después del último
reintento**, no en cada intento.

---

## El DAG completo

```python
"""
Proceso diario de ventas.
Lee de la base de origen, transforma y deja el resultado listo para carga.

Responsable: hjara
"""

from datetime import datetime, timedelta
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


def avisar_fallo(context):
    ti = context["task_instance"]
    log.error("FALLO tarea=%s intento=%s fecha=%s",
              ti.task_id, ti.try_number, context["ds"])


def extraer(**context):
    fecha = context["ds"]
    log.info("Extrayendo datos de %s", fecha)
    filas = 1500
    return {"filas": filas, "fecha": fecha}


def transformar(**context):
    datos = context["ti"].xcom_pull(task_ids="extraer")
    log.info("Transformando %s filas", datos["filas"])
    return {"filas_validas": datos["filas"] - 12, "fecha": datos["fecha"]}


def cargar(**context):
    datos = context["ti"].xcom_pull(task_ids="transformar")
    log.info("Cargando %s filas de %s", datos["filas_validas"], datos["fecha"])
    return {"estado": "OK"}


default_args = {
    "owner": "hjara",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="ventas_diario",
    description="Proceso diario de ventas",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 11 * * *",          # 06:00 hora de Lima
    catchup=False,
    max_active_runs=1,
    tags=["produccion", "ventas"],
    on_failure_callback=avisar_fallo,
) as dag:

    t_extraer     = PythonOperator(task_id="extraer",     python_callable=extraer)
    t_transformar = PythonOperator(task_id="transformar", python_callable=transformar)
    t_cargar      = PythonOperator(task_id="cargar",      python_callable=cargar)

    t_extraer >> t_transformar >> t_cargar
```

> Con `with DAG(...) as dag:` no hace falta escribir `dag=dag` en cada tarea:
> las que se creen dentro del bloque se asocian solas. Es la forma preferida.

---

## Reglas que evitan los problemas típicos

**No ponga código pesado fuera de las funciones.** Todo lo que esté en el nivel
superior del archivo se ejecuta **cada 30 segundos**, en cada análisis.

```python
# MAL — se ejecuta cada 30 segundos, para siempre
datos = pd.read_csv("archivo_de_2GB.csv")
config = Variable.get("mi_config")

# BIEN — solo cuando la tarea corre
def procesar():
    datos = pd.read_csv("archivo_de_2GB.csv")
    config = Variable.get("mi_config")
```

**Haga las tareas idempotentes.** Ejecutar dos veces con la misma fecha debe dar
el mismo resultado. En la práctica: borre antes de insertar.

```python
hook.run("DELETE FROM destino WHERE fecha = %s", parameters=(fecha,))
hook.run("INSERT INTO destino SELECT ... WHERE fecha = %s", parameters=(fecha,))
```

Sin esto, un reintento duplica los datos.

**Una tarea, una responsabilidad.** Si una tarea extrae, transforma y carga, un
fallo en la carga obliga a repetir la extracción completa.

**Nombres descriptivos.** `extraer_ventas_sqlserver` dice qué pasó; `tarea_1`
no dice nada cuando lo vea en rojo a las tres de la mañana.

---

## La forma moderna: TaskFlow

Airflow ofrece una sintaxis más corta. El XCom es automático.

```python
from datetime import datetime
from airflow.decorators import dag, task


@dag(
    dag_id="ventas_taskflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ejemplo"],
)
def proceso_ventas():

    @task
    def extraer() -> dict:
        return {"filas": 1500}

    @task
    def transformar(datos: dict) -> dict:
        return {"filas_validas": datos["filas"] - 12}

    @task
    def cargar(datos: dict):
        print(f"Cargando {datos['filas_validas']} filas")

    cargar(transformar(extraer()))


proceso_ventas()
```

Las dependencias salen de cómo se pasan los resultados: no hay que escribir `>>`.

**Cuál usar.** Los cinco ejemplos de este proyecto usan la forma clásica, así que
por consistencia conviene empezar por ahí. TaskFlow es más limpio para procesos
nuevos que pasan bastantes datos entre tareas. Ambas conviven sin problema.

---

## Cuando algo no funciona

**El DAG no aparece.**
```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow dags list-import-errors
```
Casi siempre es un error de sintaxis o un `import` que falla.

**Aparece pero no corre.** ¿Está despausado? ¿La fecha de inicio ya pasó? ¿Hay
un worker vivo? (`docker compose ps`)

**Una tarea falla.** En la interfaz: clic en el cuadro rojo → **Logs**. Para
depurar sin la interfaz:
```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow tasks test ventas_diario extraer 2026-08-22
```

**Cambié el archivo y no pasa nada.** Espere 30 segundos. Si sigue igual, hay un
error de importación — vea el primer comando.

---

## Su primer DAG real: lista de verificación

```
[ ] dag_id unico y descriptivo
[ ] owner con su usuario, no "airflow"
[ ] catchup=False
[ ] execution_timeout definido
[ ] retries y retry_delay
[ ] max_active_runs=1 si escribe en una base
[ ] Horario en UTC, con el desfase calculado
[ ] Sin credenciales en el archivo — todo en Connections
[ ] Sin código pesado en el nivel superior
[ ] Tareas idempotentes (borrar antes de insertar)
[ ] Probado con 'airflow dags test' antes de despausarlo
[ ] tags puestos, para poder filtrarlo
```

---

## Para seguir

- `airflow/dags/templates/plantilla_dag.py` — plantilla comentada para copiar
- `airflow/dags/examples/` — cinco ejemplos con SQL Server, DB2, Spark y auditoría
- `docs/GUIA_CONEXIONES_DAGS.md` — credenciales y bases de datos a fondo
