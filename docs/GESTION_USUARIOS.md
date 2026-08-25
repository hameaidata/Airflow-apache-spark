# Registrar y administrar usuarios

Procedimiento completo: alta, asignación de rol, contraseñas, verificación y
baja.

Todos los comandos están **verificados en esta instalación**. Donde algo no
funciona como dice la documentación oficial, se indica y se da la vuelta.

> **Antes de empezar:** los comandos son largos. Van en **una sola línea**.
> Si los parte con Enter, PowerShell los interpreta mal.
>
> Para acortar, defina esto una vez por sesión de terminal:
> ```powershell
> function af { docker compose -f docker-compose.windows.yml exec airflow-webserver airflow @args }
> ```
> A partir de ahí, `af users list` equivale al comando completo. En Linux:
> ```bash
> alias af='docker compose -f docker-compose.ubuntu.yml exec airflow-webserver airflow'
> ```

---

## Paso 0 — Verificar que los roles existen

Sin roles no se puede crear un usuario: el CLI rechaza un rol inexistente.

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow roles list
```

Deben aparecer 12: los 5 de fábrica (`Admin`, `Op`, `Public`, `User`, `Viewer`)
más los 7 de la matriz (`BSG_*`).

Si solo salen 5, aplique la matriz primero:

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver python /opt/airflow/config/aplicar_roles.py --aplicar
```

---

## Paso 1 — Decidir el rol

Antes de crear la cuenta, defina qué rol le corresponde. Detalle completo en
`docs/DISENO_ROLES.md`.

| Rol | Para quién | Ve credenciales | Ve bitácoras | Ejecuta |
|---|---|---|---|---|
| `BSG_Administrador` | Administración de la plataforma | Sí | Sí | Sí |
| `BSG_CustodioCredenciales` | Seguridad / DBA | **Sí** | No | No |
| `BSG_IngenieroDatos` | Equipo de datos | No | Sí | Sí |
| `BSG_Operador` | Mesa de operaciones | No | Sí | Sí |
| `BSG_Analista` | Usuarios de negocio | No | Sí | No |
| `BSG_Visualizador` | Consulta general | No | **No** | No |
| `BSG_Auditor` | Auditoría interna | Solo lectura | Sí | No |

**Verifique la segregación de funciones** antes de asignar. Combinaciones
prohibidas:

- `BSG_Administrador` + `BSG_IngenieroDatos`
- `BSG_CustodioCredenciales` + `BSG_IngenieroDatos`
- `BSG_Auditor` + cualquier otro

El DAG `auditoria_roles` marca estas combinaciones como incumplimiento cada
lunes.

---

## Paso 2 — Crear la cuenta

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users create -u hjara -f Hamer -l Jara -e hamer.jara.o@uni.pe -r BSG_IngenieroDatos -p 'ClaveInicial-2026'
```

| Opción | Significa | ¿Obligatoria? |
|---|---|---|
| `-u` | Usuario para iniciar sesión | Sí |
| `-f` | Nombre | Sí |
| `-l` | Apellido | Sí |
| `-e` | Correo | Sí |
| `-r` | Rol — debe existir | Sí |
| `-p` | Contraseña | Sí (ver aviso) |

Salida esperada:

```
Added user hjara
User "hjara" created with role "BSG_IngenieroDatos"
```

> ### AVISO: no use `--use-random-password`
>
> Genera una contraseña aleatoria y **nunca la muestra**. Verificado en el
> código fuente del provider:
>
> ```python
> if args.use_random_password:
>     password = "".join(random.choices(string.printable, k=16))
> ```
>
> No se imprime, no se guarda en claro, no se puede recuperar. La cuenta queda
> inservible, y el comando que la arreglaría (`reset-password`) está roto —
> ver el paso 4.
>
> Sirve solo para cuentas que jamás iniciarán sesión por la interfaz.
> **Use siempre `-p`.**

**Sobre la contraseña en el historial.** Con `-p`, la contraseña queda en el
historial de PowerShell. Después de crear la cuenta:

```powershell
Clear-History
```

Airflow **no valida complejidad**: acepta `123`. La disciplina es de quien crea
la cuenta. Cuando se integre Active Directory, la política la aplica el
directorio.

---

## Paso 3 — Verificar

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users list
```

Confirme que la cuenta aparece con el rol correcto.

### Prueba de aceptación: que vea lo que debe y nada más

Esto no es opcional en un entorno regulado. Es la evidencia de que el control
funciona.

1. Abra una **ventana de incógnito** (para no cerrar su sesión de `admin`)
2. Entre a http://localhost:8080 con la cuenta nueva
3. Compruebe contra esta tabla:

| Rol asignado | Debe ver | NO debe ver |
|---|---|---|
| `BSG_IngenieroDatos` | Lista de DAGs, código, bitácoras, menú Browse | Menú **Admin**, menú Security |
| `BSG_CustodioCredenciales` | Menú **Admin → Connections y Variables** | La lista de DAGs sale vacía. Sin menú Browse |
| `BSG_Visualizador` | Lista de DAGs y su estado | Al abrir una bitácora: **"Access is Denied"**. Sin menús Admin ni Browse |
| `BSG_Auditor` | Todo en modo lectura, incluido **Browse → Audit Logs** | Ningún botón de Trigger, Pause ni Clear |

Si ve algo que no debería, revise los permisos:

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver python /opt/airflow/config/aplicar_roles.py --verificar
```

---

## Paso 4 — Cambiar una contraseña

> ### AVISO: `airflow users reset-password` está roto desde el CLI
>
> Falla con `AssertionError: The sqlalchemy extension was not registered`.
>
> Es un fallo de Airflow, no de esta instalación. El código intenta invalidar
> las sesiones abiertas del usuario consultando la tabla de sesiones, pero desde
> el CLI la extensión de SQLAlchemy no está registrada en la aplicación Flask.
>
> **Lo importante: la contraseña NO se cambia.** El orden del código es:
>
> ```python
> user.password = generate_password_hash(password)   # solo en memoria
> self.reset_user_sessions(user)                     # <-- revienta aqui
> return self.update_user(user)                      # nunca se ejecuta
> ```
>
> El comando falla con un error largo, pero deja todo como estaba.

### Opción A — borrar y recrear (garantizada)

`users create` no pasa por el código roto.

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users delete -u hjara
```

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users create -u hjara -f Hamer -l Jara -e hamer.jara.o@uni.pe -r BSG_IngenieroDatos -p 'ClaveNueva-2026'
```

### Opción B — desde la interfaz

En la interfaz la extensión sí está registrada, así que el mismo código debería
funcionar por esa vía:

1. Entre como `admin`
2. **Security → List Users**
3. Editar la fila del usuario → **Reset Password**

Si también falla ahí, use la opción A.

---

## Paso 5 — Cambiar el rol de alguien

Añadir un rol a una cuenta **que ya existe** — no pide contraseña porque no crea
nada:

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users add-role -u hjara -r BSG_Operador
```

Quitar un rol:

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users remove-role -u hjara -r BSG_IngenieroDatos
```

> Un usuario puede tener varios roles: sus permisos se **suman**. Por eso las
> combinaciones prohibidas del paso 1 importan — dos roles compatibles por
> separado pueden anular una separación al juntarse.

**Después de cambiar roles, el usuario debe cerrar sesión y volver a entrar.**
Los permisos se leen al iniciar sesión.

---

## Paso 6 — Dar de baja a una persona

**Prefiera desactivar antes que borrar.**

Borrar elimina la cuenta del sistema. Desactivar la deja registrada pero sin
poder entrar, y conserva la trazabilidad de quién hizo qué. Para una auditoría,
un usuario borrado es un nombre que ya no se puede explicar.

### Desactivar — recomendado

Solo desde la interfaz; el CLI no tiene este comando:

1. **Security → List Users**
2. Editar el usuario
3. Desmarcar **Is Active**
4. Guardar

### Borrar — solo si la política lo exige

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users delete -u hjara
```

---

## Rastro de auditoría

Cada alta, baja y cambio de rol queda registrado. Es lo que se le entrega a un
auditor:

**En la interfaz:** `Browse → Audit Logs`, filtrando por evento.

**Exportar la lista de usuarios y roles:**

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow users export /tmp/usuarios.json
```

```powershell
docker compose -f docker-compose.windows.yml exec airflow-webserver airflow roles export /tmp/roles.json --pretty
```

Los archivos quedan dentro del contenedor. Para traerlos al equipo:

```powershell
docker compose -f docker-compose.windows.yml cp airflow-webserver:/tmp/usuarios.json .\usuarios.json
```

**Verificación automática:** el DAG `auditoria_roles` corre los lunes a las 7:00
y falla si detecta permisos no autorizados o combinaciones de roles prohibidas.
Ese historial de ejecuciones es la evidencia de que el control se revisa
periódicamente, no solo que se configuró una vez.

---

## Alta de una persona — lista de verificación

```
[ ] Rol definido y aprobado por su jefatura
[ ] Verificada la segregación de funciones (paso 1)
[ ] Cuenta creada con -p, nunca con --use-random-password
[ ] Clear-History ejecutado
[ ] Contraseña entregada por un canal seguro, no por correo
[ ] Prueba de aceptación hecha en ventana de incógnito (paso 3)
[ ] Confirmado que NO ve lo que no debe
[ ] Registrado en el control de accesos del área
```

## Baja de una persona

```
[ ] Cuenta desactivada (no borrada)
[ ] Verificado que ya no puede entrar
[ ] Revisado si tenía credenciales personales en Connections
[ ] Registrado en el control de accesos del área
```

---

## Cuando llegue Active Directory

Todo este procedimiento desaparece. Las cuentas y contraseñas las administra el
directorio, y Airflow asigna roles según los grupos de AD:

```python
AUTH_ROLES_MAPPING = {
    "CN=DATOS-Ingenieros,OU=Grupos,DC=banco,DC=com": ["BSG_IngenieroDatos"],
}
AUTH_ROLES_SYNC_AT_LOGIN = True
```

El bloque está preparado y comentado en `airflow/config/webserver_config.py`.

Con eso, quitarle el grupo a alguien en AD le retira el acceso a Airflow en su
siguiente inicio de sesión, sin que nadie tenga que acordarse de darlo de baja.
Es lo que un auditor quiere ver, y elimina de golpe los dos problemas de este
documento: contraseñas gestionadas a mano y bajas que dependen de que alguien
las ejecute.

---

## Problemas conocidos

| Síntoma | Causa | Salida |
|---|---|---|
| `--use-random-password` no muestra nada | Airflow no la imprime | Use `-p`. Si ya pasó, borre y recree |
| `reset-password` falla con `AssertionError` | Bug de Airflow en el CLI | Borre y recree, o use la interfaz |
| `X is not a valid role` | El rol no existe | Ejecute `aplicar_roles.py --aplicar` |
| El usuario entra pero no ve nada | Le falta `can_read` sobre `Website` | `aplicar_roles.py --verificar` |
| Cambió el rol y no surte efecto | Los permisos se leen al iniciar sesión | Cerrar sesión y volver a entrar |
| Avisos `RequestsDependencyWarning` | Versiones de la imagen oficial | Cosméticos, ignórelos |
