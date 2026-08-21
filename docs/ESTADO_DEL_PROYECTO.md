# Estado del proyecto

Fecha del corte: 21 de agosto de 2026

---

## Resumen en una línea

La infraestructura funciona y los DAGs se descubren solos. **La conexión a tus
fuentes reales todavía no funciona**, y por un motivo concreto: tu SQL Server usa
autenticación integrada de Active Directory, que la librería sobre la que
construí los ejemplos no soporta.

---

## Lo que está verificado que funciona

Verificado significa que hay evidencia, no que "debería".

| Componente | Evidencia |
|---|---|
| Compose (Windows y Ubuntu) | `docker compose config` pasa en ambos |
| El stack arranca | Lo levantaste; los contenedores están arriba |
| Auto-discovery de DAGs | El scheduler generó `__pycache__` de los 7 DAGs hoy a las 11:04 |
| Escalado de workers | `deploy.replicas` + `--scale`, sin `container_name` que lo bloquee |
| Spark master/worker | Corregido para la imagen oficial de Apache |
| Secretos fuera de git | Confirmado: `.env` no está rastreado ni aparece en el historial |
| Sintaxis de todos los DAGs | `py_compile` limpio en los 7 |

Sobre lo último: revisé el repositorio y el `.env` nunca entró en un commit. En
un proyecto bancario con remoto en GitHub eso era lo primero que había que
comprobar, y está bien.

---

## Lo que NO está verificado

| Cosa | Por qué |
|---|---|
| `docker build` del Dockerfile | Nunca se ejecutó. No tengo daemon de Docker donde trabajo |
| URLs de los drivers JDBC en Maven | No tengo salida a Maven Central desde aquí |
| Conexión real a SQL Server o DB2 | No tengo acceso a tus servidores |
| Que los ejemplos 21–24 importen | Depende de que construyas la imagen (ver abajo) |

**Comprueba esto ahora mismo**, porque si sigues con la imagen oficial los
ejemplos 21, 22 y 23 aparecerán rotos en la UI:

```powershell
docker compose -f docker-compose.windows.yml exec airflow-scheduler airflow dags list-import-errors
```

Si salen `ModuleNotFoundError` sobre `airflow.providers.microsoft.mssql` o
`...jdbc`, es que falta construir la imagen personalizada:

```powershell
docker build -t airflow-bsg:2.11.2 .
# .env:  AIRFLOW_IMAGE=airflow-bsg:2.11.2
docker compose -f docker-compose.windows.yml up -d
```

---

## SQL Server: el bloqueante real

Me confirmaste que autentica por **Windows / Active Directory integrada**. Eso
invalida parte de lo que te entregué, y prefiero decirlo claro:

> **El ejemplo 21 no va a conectar tal como está.** Usa `MsSqlHook`, que por
> debajo es `pymssql`, y **pymssql no soporta autenticación integrada**. Solo
> hace usuario y contraseña de SQL.

No es un bug que se arregle con un parámetro. Es una limitación de la librería.

Tienes dos caminos, y la diferencia de esfuerzo entre ellos es grande.

### Camino A — Cuenta de servicio con autenticación SQL

Pides al DBA una cuenta de servicio (`svc_etl_airflow`) con autenticación SQL y
permisos mínimos: `SELECT` sobre las tablas de origen y `EXECUTE` sobre los SPs
que necesites.

- **Esfuerzo técnico: cero.** Los ejemplos funcionan sin tocar una línea.
- **Coste: una conversación con el DBA y con seguridad.**

Muchas áreas de seguridad bancaria aceptan esto precisamente para integraciones
de servicio, porque la cuenta queda acotada, es auditable y no arrastra los
privilegios de una persona. Si en tu banco es viable, es la opción sensata.

### Camino B — Kerberos con ODBC

Si la política obliga a AD, hay que montar Kerberos dentro de los contenedores:

1. **Un keytab** para el principal del servicio, emitido por tu equipo de AD
   (`airflow/host@TUREALM.COM`). Esto no lo genera uno solo: lo emite AD.
2. **`krb5.conf`** con tu realm y KDCs, montado en los contenedores.
3. **ODBC Driver 18 + `krb5-user`** en la imagen — ya los añadí al `Dockerfile`.
4. **Un servicio `airflow kerberos`** que renueve el ticket desde el keytab y
   comparta el *credential cache* con los workers por un volumen.
5. **`OdbcHook`** en vez de `MsSqlHook`, con
   `Trusted_Connection=yes` en la cadena de conexión.

Cadena de conexión resultante:

```
DRIVER={ODBC Driver 18 for SQL Server};
SERVER=servidor.dominio.com,1433;
DATABASE=BD_NEGOCIO;
Trusted_Connection=yes;
Encrypt=yes;
TrustServerCertificate=no
```

Configuración de Airflow para el ticket:

```ini
AIRFLOW__CORE__SECURITY=kerberos
AIRFLOW__KERBEROS__PRINCIPAL=airflow
AIRFLOW__KERBEROS__KEYTAB=/etc/security/keytabs/airflow.keytab
AIRFLOW__KERBEROS__CCACHE=/var/kerberos/krb5cc
AIRFLOW__KERBEROS__REINIT_FREQUENCY=3600
```

**Esfuerzo: días, no horas**, y la mayor parte no depende de mí: depende de que
tu equipo de AD emita el keytab y te dé los datos del realm.

Dos avisos sobre este camino:

- El `Dockerfile` descarga el driver ODBC de `packages.microsoft.com`. En redes
  corporativas ese host suele estar bloqueado y el build falla ahí. Lo dejé
  detrás de `--build-arg INSTALAR_ODBC=false` por si necesitas saltarlo.
- `TrustServerCertificate=no` exige que el certificado del SQL Server sea válido
  y que su CA esté en el almacén del contenedor. Si pones `yes` para salir del
  paso, estás desactivando la validación del certificado — y eso es exactamente
  lo que un auditor marca. Que funcione no significa que esté bien.

### Y algo que afecta a la arquitectura de Spark

Esto es importante y no es obvio: **Spark en modo standalone no propaga
credenciales Kerberos a los executors.** En YARN o Kubernetes, Spark distribuye
*delegation tokens*; en standalone ese mecanismo no existe. Cada executor
necesitaría su propio acceso al keytab.

Consecuencia práctica para el ejemplo 23: si vas por el camino B, **Spark no
puede leer SQL Server directamente**. La arquitectura pasa a ser:

```
Airflow (tiene el ticket)  ->  extrae a Parquet  ->  Spark lee el Parquet
```

Spark deja de tocar la base de datos, y el paralelismo lo consigues particionando
el Parquet. Es un cambio de diseño, no un ajuste. Con el camino A no aparece este
problema.

---

## DB2: limitaciones y cómo se conectará

Me dijiste que aún no sabes qué variante es. Esa respuesta cambia el driver por
completo, así que aquí van las tres:

| Variante | Driver | Estado en tu proyecto |
|---|---|---|
| **Db2 LUW** (Linux/UNIX/Windows) | `jcc.jar` | Ya está en el `Dockerfile`. Funcionaría tal cual |
| **Db2 for i** (AS/400 / iSeries) | **JTOpen `jt400.jar`** | **No está.** Driver distinto, hay que cambiar Dockerfile y Connection |
| **Db2 for z/OS** (mainframe) | `jcc.jar` | El jar sirve, pero hace falta *entitlement* de **DB2 Connect** |

**Pregúntale al DBA:** «¿Db2 corre sobre Linux, sobre AS/400 o sobre el
mainframe?» Con esa frase basta.

Por qué importa cada una:

**Db2 for i (AS/400).** Muy común en banca peruana para los core antiguos. El
driver `jcc` **no** habla con él; se usa JTOpen (`jt400.jar`), que es un proyecto
distinto, con otra URL JDBC (`jdbc:as400://...`) y otra clase de driver
(`com.ibm.as400.access.AS400JDBCDriver`). Si es tu caso, el ejemplo 22 hay que
reescribirlo.

**Db2 for z/OS.** El jar funciona, pero conectarse desde un cliente externo
consume licencia de **DB2 Connect**. Es una cuestión contractual, no técnica: si
tu banco no la tiene para este uso, el proyecto se para ahí por mucho que el
driver conecte. Conviene resolverlo antes de escribir código.

### Limitaciones de DB2 que aplican en cualquier variante

1. **`ibm-db` (cliente nativo) puede no instalarse.** Descarga el driver de IBM
   al instalarse por pip, y esa descarga suele estar bloqueada en redes
   corporativas. El `Dockerfile` tolera ese fallo a propósito. No es grave: el
   camino JDBC no lo necesita.

2. **El jar `jcc` de Maven Central va bajo licencia de IBM.** Está publicado por
   IBM, pero sujeto a sus términos. En banca conviene que legal o el DBA
   confirmen que estás cubierto, en vez de asumirlo.

3. **El flag del provider JDBC.** Desde la versión 4.0.0 hay que activar
   explícitamente `ALLOW_DRIVER_PATH_IN_EXTRA` y `ALLOW_DRIVER_CLASS_IN_EXTRA`.
   Ya están en el compose. Sin ellos el hook ignora el driver en silencio.

4. **En una Connection JDBC, el campo *host* es la URL completa**
   (`jdbc:db2://servidor:50000/BD`), no el nombre del servidor. Es el error más
   frecuente con este hook.

5. **La sintaxis de SP es `CALL`, no `EXEC`.** `EXEC` es de SQL Server.

6. **Autenticación.** Db2 LUW normalmente usa usuario/contraseña, así que aquí no
   tendrías el problema de SQL Server. Db2 for i puede ir con perfil de usuario
   del sistema. z/OS depende de RACF. Otra pregunta para el DBA.

---

## Prioridades

Ordenadas por lo que desbloquea más:

**1. Confirmar con el DBA** (bloquea todo lo demás)
- ¿Cuenta de servicio SQL, o Kerberos obligatorio?
- ¿Db2 LUW, for i, o z/OS?
- ¿Hay conectividad de red desde tu equipo a esos servidores?

**2. Construir la imagen y comprobar los import errors**
Cinco minutos, y te dice si los ejemplos son utilizables.

**3. Probar una conexión mínima antes de escribir DAGs**
```powershell
docker compose exec airflow-scheduler airflow connections test <conn_id>
```

**4. Remote logging**
Tus workers de Celery son efímeros: al escalar a la baja, el contenedor
desaparece y sus logs con él. La UI dirá "log file not found" para tareas que sí
corrieron. En un entorno regulado eso es un hallazgo de auditoría.

**5. TLS**
Ahora mismo la UI, Postgres y el RPC de Spark van en claro.

---

## Deuda pendiente que sigue ahí

- **Los módulos de `airflow/plugins/`** están vacíos: solo `__init__.py` y un
  `auth_manager.py` de 1 KB cuyo `authenticate_user()` hace `return True` sin
  comprobar nada. Convendría borrarlo antes de que alguien lo tome por bueno.
- **Carpetas con nombres rotos** de un script que usó llaves sin expandir:
  `infrastructure/monitoring/{grafana/...`, `infrastructure/terraform/{environments}`.
  Están vacías. No puedo borrarlas desde aquí.
- **El proyecto vive en OneDrive.** El `.env` con las contraseñas se sincroniza a
  la nube, y OneDrive bloquea archivos mientras Docker escribe.
- **`README.md` tiene 50 bytes.**

---

## Lo que puedo hacer sin esperar al DBA

- Escribir el ejemplo con `OdbcHook` + Kerberos (camino B), listo para cuando
  tengas el keytab
- Añadir el servicio `airflow-kerberos` al compose
- Configurar remote logging
- Reconstruir los módulos de plugins
- Reescribir el ejemplo 22 para JTOpen, si resulta ser AS/400

Dime cuál y voy con eso.
