# Conexión a Bantotal sobre IBM i (AS/400) — JDBC y ODBC

Cómo conectarse al core, con los dos clientes, y las propiedades que evitan
bloquear el sistema productivo.

> **Release confirmada: IBM i 7.5.** En IBM i la base de datos no tiene
> numeración propia: Db2 for i sigue la del sistema operativo, así que "Db2 7.5"
> significa IBM i 7.5. Con esa release todo lo actual funciona y no hace falta
> bajar a versiones antiguas de ningún driver.

## Cuál de los dos usar

| | JDBC (JTOpen) | ODBC (IBM i Access) |
|---|---|---|
| Para qué | **El pipeline de Airflow** | Herramientas de escritorio, análisis puntual, Excel, Power BI local |
| Licencia | Open source, IBM Public License. Sin dudas | El paquete se instala sin entitlement, pero el uso podría requerir 5770-XW1 (ver abajo) |
| Instalación | Un `.jar`, ya está en la imagen | Paquete del sistema, desde repositorio público de IBM |
| Plataforma | Cualquiera con JVM | Linux, Windows, macOS |

**Para el pipeline, use JDBC.** No es una preferencia estética: JTOpen es open
source y no depende de ninguna licencia de cliente, así que la pregunta de
entitlements no existe. ODBC tiene sentido para quien necesite conectar una
herramienta de escritorio.

---

## 1. Qué driver, según su versión de IBM i

El driver es **JTOpen** (IBM Toolbox for Java), el `jt400.jar`. Es el driver
oficial para Db2 for i y es open source, mantenido por IBM en GitHub.

> **Cuidado con una confusión frecuente.** `com.ibm.db2:jcc` —que ya estaba en el
> Dockerfile del proyecto— es el driver de Db2 para **LUW y z/OS**. No es el de
> IBM i. Para Bantotal hace falta `jt400`.

La versión depende de su release del AS/400. Esta es la matriz del proyecto, y la
columna que importa es **"conecta a"**, porque usted conecta *desde* el contenedor
*hacia* el AS/400; la columna "instala en" solo aplicaría si el código corriera
dentro del propio IBM i:

| JTOpen | Java mínimo | Conecta a IBM i | Última publicación |
|---|---|---|---|
| **21.x** | 8 | **7.3 o superior** | 21.0.7 · julio 2026 |
| 20.x | 7 | 7.3 o superior | 20.0.8 · diciembre 2024 |
| 11.x | 1.1 | 7.3 o superior | 11.2 · marzo 2023 |
| 10.x | 1.1 | **7.2 o superior** | 10.7 · septiembre 2021 |

**Cómo elegir:**

| Su AS/400 | Use |
|---|---|
| 7.4 | JTOpen 21.0.7 |
| 7.3 | JTOpen 21.0.7 |
| 7.2 | JTOpen 10.7 |
| 7.1 o anterior | JTOpen 9.x o anterior, desde SourceForge (no está en Maven Central) |

Un detalle que simplifica la vida respecto del driver de SQL Server: **el jar por
defecto de JTOpen está compilado para Java 8**, así que corre tanto con la JVM 17
de la imagen como con `$JAVA_HOME_8`. Un solo jar cubre las dos, no hay que elegir
variante ni preocuparse por `UnsupportedClassVersionError`.

---

## 2. Cómo averiguar su release exacta

Desde una sesión SQL en el AS/400:

```sql
SELECT OS_VERSION, OS_RELEASE FROM SYSIBMADM.ENV_SYS_INFO;
```

O desde línea de comandos del IBM i:

```
DSPPTF
```

Arriba a la derecha sale la release en formato `V7R4M0`, que se lee como 7.4.

---

## 3. Instalación

Ya está en el `Dockerfile` del proyecto. Se descarga junto con los demás drivers
a `/opt/airflow/jars/`:

```
net.sf.jt400:jt400:21.0.7:jt400.jar
```

Si su AS/400 es 7.2, cambie esa línea por `net.sf.jt400:jt400:10.7:jt400.jar` y
reconstruya la imagen.

Para verificar que quedó, sin instalar nada:

```powershell
docker compose -f docker-compose.windows.yml exec airflow-worker ls -la /opt/airflow/jars/jt400.jar
```

---

## 4. La conexión

**Clase del driver:**

```
com.ibm.as400.access.AS400JDBCDriver
```

**URL:**

```
jdbc:as400://<host>/<biblioteca_por_defecto>;<propiedad=valor>;<propiedad=valor>
```

### Propiedades que importan de verdad

Estas no son opcionales cuando se conecta a un core bancario productivo desde un
proceso automático. Cada una evita un problema concreto.

| Propiedad | Valor | Por qué |
|---|---|---|
| `prompt` | `false` | **La más importante.** Por defecto, si algo falta en la autenticación, el driver intenta abrir un diálogo gráfico. En un contenedor sin escritorio, eso se traduce en una tarea colgada hasta que la mata el timeout, sin mensaje útil |
| `transaction isolation` | `none` | No toma bloqueos de lectura sobre las tablas del core. Sin esto, una extracción larga puede bloquear operaciones de negocio |
| `access` | `read only` | El driver rechaza cualquier intento de escritura. Es una red de seguridad barata: un error de código no puede tocar el core |
| `errors` | `full` | Los mensajes por defecto son códigos crípticos. Con `full` viene el texto del mensaje del sistema, que suele decir exactamente qué pasó |
| `naming` | `sql` o `system` | `sql` usa `BIBLIOTECA.TABLA`; `system` usa `BIBLIOTECA/TABLA`. Bantotal normalmente trabaja con `system`, pero confirme con el equipo del core |
| `libraries` | lista de bibliotecas | La lista de bibliotecas de Bantotal, separadas por comas. Sin ella, las tablas no se resuelven sin calificar |
| `date format` | `iso` | Fuerza `yyyy-mm-dd`. Sin esto, el formato depende del job del sistema y cambia entre entornos |
| `time format` | `iso` | Lo mismo con las horas |
| `block size` | `512` | KB por lectura en bloque. Sube mucho el rendimiento en extracciones grandes |
| `blocking enabled` | `true` | Habilita la lectura en bloque |
| `translate binary` | `true` | Convierte campos binarios a texto según CCSID. Necesario si hay campos con tildes o ñ mal declarados |
| `query timeout mechanism` | `qqrytimlmt` | Permite que un `setQueryTimeout` realmente corte la consulta en el AS/400 |

### URL completa de ejemplo

```
jdbc:as400://as400.banco.local/BTDATA
  ;prompt=false
  ;transaction isolation=none
  ;access=read only
  ;errors=full
  ;naming=system
  ;libraries=BTDATA,BTPROD,BTCOM
  ;date format=iso
  ;time format=iso
  ;block size=512
  ;blocking enabled=true
  ;translate binary=true
```

(En la práctica va en una sola línea, sin saltos.)

---

## 5. Cómo se configura en Airflow

Se crea una Connection de tipo **Generic** o **JDBC**, y las credenciales quedan
cifradas con el Fernet key. En el campo Extra:

```json
{
  "jdbc_url": "jdbc:as400://as400.banco.local/BTDATA;prompt=false;transaction isolation=none;access=read only;errors=full;naming=system;libraries=BTDATA,BTPROD;date format=iso;time format=iso;block size=512;blocking enabled=true",
  "driver_class": "com.ibm.as400.access.AS400JDBCDriver",
  "driver_path": "/opt/airflow/jars/jt400.jar"
}
```

> **Aviso sobre el provider JDBC de Airflow.** Desde la versión 4 del
> `apache-airflow-providers-jdbc`, por seguridad **no se admiten `driver_path` ni
> `driver_class` dentro del Extra** salvo que se habiliten explícitamente:
>
> ```
> AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA=true
> AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA=true
> ```
>
> Si prefiere no habilitar eso —y es razonable no hacerlo—, use `jaydebeapi`
> directamente, como hace el pipeline S2SQL: la ruta del jar y la clase salen de
> la configuración del proyecto y solo las credenciales salen de la Connection.

### Si la Connection es de tipo Generic, declare el motor

`Generic` es el tipo que se elige cuando no hay un proveedor instalado para ese
sistema, y **no dice nada del motor**. Eso importa en cuanto hay más de una
Connection `Generic` en el proyecto —aquí hay dos, `CONEXION_BANTOTAL` al core y
`CONEXION_SQLSERVER` al destino—: cualquier regla que traduzca `generic` a un
motor fijo acierta en una y se equivoca en la otra **en silencio**. Arma una URL
`jdbc:as400://` apuntando a SQL Server y carga el driver de IBM i para escribir
en una tabla de SQL Server. El error que sale de ahí no menciona el tipo de
conexión por ningún lado, así que se buscan horas en el sitio equivocado.

Por eso cada Connection `Generic` declara su motor en el campo **Extra**:

| Connection | Extra mínimo |
|---|---|
| `CONEXION_BANTOTAL` | `{"motor": "as400", "libraries": "GPPPBTDB", "naming": "system"}` |
| `CONEXION_SQLSERVER` | `{"motor": "mssql"}` |

Si falta, `motor_de_conexion()` en `airflow/plugins/utils/spark_config.py` lo
deduce del **puerto**, pero solo cuando el puerto pertenece a un motor y nada
más: 1433 → `mssql`, 8471/9471/446 → `as400`, 5432 → `postgres`, 1521 →
`oracle`, 50000 → `db2`. El **3306 queda fuera a propósito**: lo usan MySQL y
SingleStore por igual, y adivinar el motor por un puerto compartido es el error
que esta función existe para evitar. Como último recurso, la presencia de
`libraries` o `naming` en el Extra se toma como declaración de IBM i, porque
esas propiedades solo las entiende ese driver.

Sin ninguna pista, la tarea falla **al arrancar** —no a mitad de la escritura—
con el mensaje que dice qué agregar y dónde.

### Con jaydebeapi, como en el resto del proyecto

```python
def conexion_bantotal(config):
    """Conexion de solo lectura al core, por JDBC.

    Las credenciales salen de una Connection de Airflow, cifradas. La URL y el
    jar salen de la configuracion del proyecto: asi un cambio de biblioteca no
    obliga a tocar una Connection desde la interfaz.
    """
    import jaydebeapi
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(config["conexiones"]["bantotal"])

    propiedades = ";".join(f"{k}={v}" for k, v in config["jdbc_as400"]["propiedades"].items())
    url = f"jdbc:as400://{conn.host}/{conn.schema};{propiedades}"

    conexion = jaydebeapi.connect(
        "com.ibm.as400.access.AS400JDBCDriver",
        url,
        [conn.login, conn.password],
        config["jdbc_as400"]["jar"],
    )
    # Solo lectura tambien del lado del cliente, no solo por la propiedad de la URL.
    conexion.jconn.setReadOnly(True)
    return conexion
```

---

## 6. La otra vía: ODBC

El driver se llama **IBM i Access ODBC Driver** y viene en el paquete
`ibm-iaccess`, que IBM publica en un repositorio APT/YUM **público**: no hace
falta usuario de IBM ni pasar por Entitled Systems Support para instalarlo.

### Instalación

Ya está en el `Dockerfile` del proyecto. Para hacerlo a mano en Debian o Ubuntu:

```bash
curl -fsSL https://public.dhe.ibm.com/software/ibmi/products/odbc/debs/dists/1.1.0/ibmi-acs-1.1.0.list \
  -o /etc/apt/sources.list.d/ibmi-acs.list
apt-get update && apt-get install -y ibm-iaccess
```

En Red Hat o SUSE, el repositorio es:

```
https://public.dhe.ibm.com/software/ibmi/products/odbc/rpms/ibmi-acs.repo
```

El paquete arrastra unixODBC como dependencia. Para comprobar que quedó
registrado:

```bash
odbcinst -q -d          # lista los drivers instalados
odbcinst -j             # dice dónde están odbcinst.ini y odbc.ini
```

Debe aparecer `IBM i Access ODBC Driver 64-bit`, apuntando a
`/opt/ibm/iaccess/lib64/libcwbodbc.so`.

### Configuración de un DSN

En `odbc.ini`:

```ini
[BANTOTAL]
Driver           = IBM i Access ODBC Driver 64-bit
System           = as400.banco.local
UserID           =
Password         =
DefaultLibraries = BTDATA,BTPROD
Naming           = 1
CommitMode       = 0
BlockFetch       = 1
```

O sin DSN, con cadena de conexión directa desde Python:

```python
import pyodbc

cn = pyodbc.connect(
    "DRIVER={IBM i Access ODBC Driver 64-bit};"
    "SYSTEM=as400.banco.local;"
    "UID=usr_datahub;PWD=****;"
    "DefaultLibraries=BTDATA,BTPROD;"
    "Naming=1;"          # 1 = sistema (BIBLIOTECA/TABLA); 0 = SQL (BIBLIOTECA.TABLA)
    "CommitMode=0;"      # sin commit: no toma bloqueos sobre el core
    "BlockFetch=1;",     # lectura en bloque: sube mucho el rendimiento
    readonly=True,
)
```

### Equivalencias con las propiedades de JDBC

| Objetivo | JDBC (JTOpen) | ODBC (IBM i Access) |
|---|---|---|
| No tomar bloqueos | `transaction isolation=none` | `CommitMode=0` |
| Solo lectura | `access=read only` | `readonly=True` en la conexión |
| Lista de bibliotecas | `libraries=` | `DefaultLibraries=` |
| Convención de nombres | `naming=system` / `sql` | `Naming=1` / `0` |
| Lectura en bloque | `blocking enabled=true` | `BlockFetch=1` |
| Sin diálogo gráfico | `prompt=false` | No aplica: ODBC no abre diálogos desde un proceso |

> **Cuidado con `CommitMode`.** Es el equivalente al aislamiento y es la
> propiedad que decide si su extracción bloquea o no operaciones del core.
> Confirme el valor exacto en la documentación del driver antes de usarlo contra
> producción: un número equivocado aquí se traduce en bloqueos sobre tablas que
> la operación está usando.

### Sobre la licencia de ODBC

El paquete se descarga e instala sin entitlement, eso está verificado. Cosa
distinta es si el **uso** de ODBC contra su sistema requiere la licencia *IBM i
Access Family* (5770-XW1).

IBM documenta que la emulación 5250 y Data Transfer dejaron de necesitarla a
partir de ACS 1.1.9.1, pero no encontré una declaración igual de explícita para
ODBC. Antes de usarlo en producción, confírmelo con su representante de IBM.

Es exactamente la duda que JDBC no tiene: JTOpen es open source bajo IBM Public
License y no depende de ninguna licencia de cliente. Por eso el pipeline va por
JDBC.

---

## 7. Antes de conectarse a producción

La arquitectura del Data Hub ya dice *"consultas sobre réplica o ventana acordada
para no afectar el core"*, y conviene tomárselo literalmente.

**Acuerde la ventana con el área que opera el AS/400.** Por escrito, y que quede
en el runbook. Una extracción de 38 tablas en horario de operación no es una
decisión de Ingeniería de Datos.

**Pregunte si existe una réplica.** Si la hay, se usa esa y el problema
desaparece. Es la respuesta correcta cuando está disponible.

**Pida un perfil de usuario propio, solo de lectura**, con límites de recursos. No
reutilice un perfil de aplicación ni uno de persona.

**Limite lo que se trae.** La extracción incremental por marca de agua ya está en
el diseño; el punto es que el `WHERE` viaje al AS/400 y no se traiga la tabla
entera para filtrar en el contenedor. Con JDBC eso significa que el filtro va en
el `SELECT`, no en pandas.

**Acuerde un `setQueryTimeout`.** Una consulta sin tope que se descontrola en el
core es la forma más rápida de que le retiren el acceso.

---

## 8. Problemas frecuentes y qué significan

| Síntoma | Causa habitual |
|---|---|
| La tarea se queda colgada sin log | Falta `prompt=false`: el driver está esperando un diálogo gráfico que nunca va a aparecer |
| `UnsupportedClassVersionError` | El jar no corresponde a la JVM. Con JTOpen es raro, porque el jar por defecto es Java 8 y corre en todas |
| `The application requester cannot establish the connection` | Puerto bloqueado o el servidor host de base de datos del IBM i no está arrancado. JTOpen usa varios puertos, no solo el 8471 |
| `Table not found` con el nombre correcto | Falta `libraries`, o `naming` está en `sql` cuando debía estar en `system` |
| Tildes y ñ salen mal | CCSID. Pruebe `translate binary=true` y confirme el CCSID del job con el equipo del core |
| Fechas que cambian de formato entre entornos | Falta `date format=iso`: sin eso depende del job del sistema |
| Errores que solo dicen un código | Falta `errors=full` |
| La extracción bloquea operaciones del core | Falta `transaction isolation=none` |

---

## Fuentes

- [IBM/JTOpen en GitHub](https://github.com/IBM/JTOpen) — matriz de compatibilidad por versión
- [net.sf.jt400:jt400 en Maven Central](https://central.sonatype.com/artifact/net.sf.jt400/jt400)
- [Historial de versiones de jt400](https://mvnrepository.com/artifact/net.sf.jt400/jt400)
- [Javadoc de AS400JDBCDriver](https://javadoc.midrange.com/jtopen/com/ibm/as400/access/AS400JDBCDriver.html) — lista completa de propiedades
- [JTOpen en SourceForge](https://sourceforge.net/projects/jt400/) — versiones antiguas para IBM i 7.1 y anteriores
