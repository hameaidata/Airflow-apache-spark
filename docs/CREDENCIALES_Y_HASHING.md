# Credenciales: qué se puede hashear y qué no

Documento para el área de seguridad del banco.

---

## La respuesta corta

Las contraseñas de conexión a SQL Server, DB2 o cualquier otra base de datos
**no se pueden hashear**. No es una limitación de Airflow ni una decisión de
diseño que se pueda revisar: es una consecuencia de cómo funciona un hash.

Pero eso no significa que tengan que estar desprotegidas. Hay cuatro niveles de
protección posibles, y este documento explica cuál conviene y por qué.

---

## Por qué no se pueden hashear

Un hash es **irreversible por definición**. Ese es exactamente su valor: si
alguien se lleva la base de datos, no puede recuperar las contraseñas.

Eso funciona cuando el sistema solo necesita **comprobar** una contraseña:

```
Usuario escribe:  "MiClave123"
Sistema calcula:  hash("MiClave123")
Sistema compara:  ¿coincide con el hash guardado?  ->  sí / no
```

El sistema nunca necesita saber cuál era la contraseña. Solo si coincide.

Pero para **conectarse** a SQL Server, Airflow tiene que hacer otra cosa:

```
Airflow necesita:  "MiClave123"     <- el valor original
Y enviárselo a:    SQL Server
SQL Server:        lo verifica contra SU propio hash  ->  concede o deniega
```

Airflow está del lado de **quien presenta** la credencial, no de quien la
verifica. Si guardara solo el hash, no tendría nada que enviar. El hash de la
contraseña no sirve para autenticarse: SQL Server esperaría la contraseña.

En una frase: **hashear sirve para verificar, no para presentar.**

### Lo que sí se hashea, y ya está hecho

Conviene señalarlo porque a veces la pregunta viene de haber visto una parte del
sistema y no la otra:

| Credencial | ¿Hasheada? | Cómo |
|---|---|---|
| Contraseñas de usuarios de la interfaz de Airflow | **Sí** | `scrypt` N=32768, r=8, p=1, salt aleatorio de 16 bytes |
| Contraseñas de conexión a SQL Server / DB2 | **No, imposible** | Cifradas (ver abajo) |

Las de la interfaz las hashea Airflow solo, con Flask-AppBuilder. No hay nada
que configurar y no se puede desactivar. Verificable en la tabla `ab_user`.

---

## Lo que sí se puede hacer, de mejor a peor

### Nivel 1 — Que no exista ninguna contraseña

La protección definitiva de un secreto es no tenerlo.

- **Kerberos / autenticación integrada de AD.** Airflow presenta un ticket
  emitido por el directorio, no una contraseña. Sigue habiendo un secreto (el
  *keytab*), pero lo gestiona, rota y revoca el equipo de AD de forma central.
- **Identidad administrada**, si alguna base está en Azure.

Es la opción que tu área de seguridad probablemente prefiera, y es hacia donde
apunta la respuesta que ya nos dieron sobre SQL Server.

### Nivel 2 — Credenciales dinámicas y de vida corta

Esta es la respuesta que suele convencer a un área de seguridad hermética, y
merece la pena ponerla sobre la mesa.

Con el *database secrets engine* de HashiCorp Vault:

1. Airflow le pide credenciales a Vault en el momento de ejecutar la tarea
2. Vault **crea un usuario temporal** en SQL Server con un TTL (por ejemplo 1 h)
3. La tarea se ejecuta con ese usuario
4. Vault **lo revoca automáticamente** al vencer

Lo que consigue:

- La contraseña **nunca se almacena** en ningún sitio, ni cifrada
- Aunque alguien la interceptara, caduca sola en una hora
- Cada ejecución queda trazada con un usuario distinto y auditable
- Rotación automática, sin intervención de nadie

Es más trabajo de montar, pero responde a la preocupación de fondo mucho mejor
que cualquier esquema de almacenamiento. Si la pregunta que te hicieron nace de
"no queremos contraseñas guardadas", **esta es la respuesta**, no el hashing.

### Nivel 3 — Gestor de secretos externo

Vault KV, Azure Key Vault o AWS Secrets Manager como *secrets backend* de
Airflow. La credencial es estática, pero:

- No vive en la base de datos de Airflow
- Se lee en tiempo de ejecución y no se persiste
- El acceso queda auditado en el gestor
- La rotación es centralizada

### Nivel 4 — Cifrado en la base de datos de Airflow (lo que hay ahora)

Airflow cifra las contraseñas de las conexiones con **Fernet**, que
concretamente es:

- **AES-128 en modo CBC** con relleno PKCS7
- **HMAC-SHA256** para autenticar el mensaje (detecta manipulación)
- IV aleatorio en cada operación

Esto es cifrado **reversible**, no un hash — y tiene que serlo, por lo explicado
arriba. Es un esquema estándar y correcto, con una condición: **la seguridad
pasa a depender enteramente de la clave Fernet**.

Quien tenga la clave y una copia de la base de datos, tiene las contraseñas.
Así que la clave hay que tratarla como el activo más sensible del sistema:

- No debe estar en el mismo sitio que la base de datos
- No debe ir a git (verificado: nuestro `.env` no está rastreado)
- Debe rotarse periódicamente

Airflow soporta rotación sin pérdida de datos: acepta varias claves separadas
por coma (la primera cifra, todas descifran) y trae el comando
`airflow rotate-fernet-key` para volver a cifrar todo con la nueva.

---

## Situación actual del proyecto

| Aspecto | Estado |
|---|---|
| Contraseñas de la UI | Hasheadas con scrypt — correcto, sin acción |
| Contraseñas de conexión | Cifradas con Fernet (AES-128-CBC + HMAC-SHA256) |
| Clave Fernet | En el `.env` local, fuera de git — verificado |
| Enmascarado en logs | Activo por defecto |
| Gestor de secretos externo | **No configurado** |
| Credenciales dinámicas | **No configurado** |
| TLS | **No configurado** — ver abajo |

### El punto débil real, y no es el hashing

Si el área de seguridad va a revisar esto, esto es lo que encontrará antes que
nada:

> **Ahora mismo todo el tráfico va sin cifrar.** La UI por HTTP plano, la
> conexión a PostgreSQL sin SSL, el RPC de Spark sin TLS.

Una contraseña perfectamente cifrada en reposo que después viaja en claro por la
red no está protegida. Discutir el almacenamiento antes que el transporte es
optimizar la parte equivocada.

Si hay que elegir un solo cambio, es TLS, no el esquema de almacenamiento.

---

## Qué responderles

Una redacción que pueden usar tal cual:

> Las credenciales de conexión a bases de datos no admiten hashing, porque el
> sistema debe presentarlas al servidor destino para autenticarse; un hash es
> irreversible y no serviría para ese fin. Las contraseñas de los usuarios de la
> interfaz sí se almacenan hasheadas (scrypt con salt aleatorio).
>
> Las credenciales de conexión se protegen con cifrado autenticado AES-128-CBC
> más HMAC-SHA256 (esquema Fernet) en la base de datos de metadatos.
>
> Proponemos elevar el control por dos vías: (a) autenticación integrada
> mediante Kerberos, que elimina la contraseña almacenada y traslada la gestión
> del secreto al Active Directory; y (b) credenciales dinámicas de vida corta
> emitidas por un gestor de secretos, que se crean al inicio de cada ejecución y
> se revocan automáticamente al finalizar, de modo que no exista ninguna
> credencial persistente.
>
> Señalamos además que el control de mayor impacto pendiente es el cifrado en
> tránsito (TLS), actualmente no implementado en el entorno de desarrollo.

Lo último conviene decirlo tú antes de que lo encuentren ellos.

---

## Verificaciones que pueden pedirte

**Que las contraseñas de la UI están hasheadas:**

```bash
docker compose exec airflow-postgres \
  psql -U airflow -d airflow -c "SELECT username, LEFT(password, 30) FROM ab_user;"
```

Debe verse `scrypt:32768:8:1$...`, nunca texto legible.

**Que las contraseñas de conexión están cifradas:**

```bash
docker compose exec airflow-postgres \
  psql -U airflow -d airflow -c "SELECT conn_id, LEFT(password, 40) FROM connection;"
```

Debe verse un token Fernet (empieza por `gAAAAA`), no la contraseña.

**Que no aparecen en los logs:**
Ejecuta un DAG que use una conexión y busca la contraseña en su log. Debe salir
`***`.

---

## Referencias

- Fernet: cifrado autenticado AES-128-CBC + HMAC-SHA256
- scrypt: función de derivación de clave, RFC 7914
- Rotación: `airflow rotate-fernet-key`
- Enmascarado: `AIRFLOW__CORE__HIDE_SENSITIVE_VAR_CONN_FIELDS` (activo por defecto)
