# Diseño de roles para Airflow

**Estado: propuesta. Nada implementado todavía.**

Este documento define los roles antes de tocar la instancia. La implementación
va después, cuando usted valide el diseño.

---

## 1. Cómo funciona el control de acceso en Airflow

Un permiso es siempre un par **(acción, recurso)**. Airflow 2.11 tiene
exactamente **cinco acciones** — verificadas en el paquete instalado:

| Acción | Significa |
|---|---|
| `can_read` | Ver |
| `can_create` | Crear |
| `can_edit` | Modificar |
| `can_delete` | Eliminar |
| `menu_access` | Ver la entrada de menú |

Y unos cuarenta recursos. Los que importan para este diseño:

| Recurso | Qué controla |
|---|---|
| `DAGs` | Los procesos, en conjunto |
| `DAG:<id>` | **Un DAG específico** — permite acotar por proceso |
| `DAG Code` | Ver el código fuente del DAG |
| `DAG Runs` | Las ejecuciones. `can_create` = **disparar** |
| `Task Instances` | Las tareas. `can_edit` = reintentar, limpiar |
| `Task Logs` | **Las bitácoras de ejecución** |
| `Connections` | **Las credenciales de bases de datos** |
| `Variables` | Los parámetros de configuración |
| `Audit Logs` | Quién hizo qué |
| `Configurations` | El `airflow.cfg` |
| `Pools` | Límites de concurrencia |
| `XComs` | Datos que pasan entre tareas |
| `ImportError` | DAGs que no cargan |
| `Users`, `Roles`, `Permissions` | Administración de accesos |
| `Admin`, `Browse`, `Docs` | Los menús |

> **Detalle que rompe roles y cuesta encontrar:** para que alguien vea un menú,
> no basta con darle permiso sobre el recurso. Hace falta también `menu_access`
> sobre el menú que lo contiene. Sin eso el rol tiene el permiso pero no
> encuentra dónde ejercerlo, y parece que no funciona.

---

## 2. Los roles que ya vienen, y por qué no bastan

Airflow trae cinco: `Admin`, `Op`, `User`, `Viewer`, `Public`.

El problema para un banco es que **`Op` mezcla dos funciones que deben estar
separadas**: administra credenciales *y* opera los procesos. Es exactamente la
separación que usted planteó, y los roles de fábrica no la ofrecen.

Tampoco hay un rol de auditoría —lectura total sin capacidad de ejecutar— que es
lo primero que pide un revisor de SOX.

**Recomendación: no modificar los roles de fábrica.** Crear roles nuevos con
prefijo propio. Si se tocan los originales, la próxima actualización de Airflow
puede revertir los cambios sin aviso.

---

## 3. Los siete roles propuestos

| Rol | Para quién | Idea en una línea |
|---|---|---|
| `BSG_Administrador` | 2 personas máximo | Todo, incluida la gestión de accesos |
| `BSG_CustodioCredenciales` | Seguridad / DBA | **Solo credenciales.** No ve procesos ni bitácoras |
| `BSG_IngenieroDatos` | Equipo de datos | Construye y ejecuta procesos. No administra accesos |
| `BSG_Operador` | Mesa de operaciones | Ejecuta y reintenta. No modifica código |
| `BSG_Analista` | Usuarios de negocio | Ve procesos y bitácoras. No ejecuta nada |
| `BSG_Visualizador` | Consulta general | **Solo ve el estado.** Sin bitácoras |
| `BSG_Auditor` | Auditoría interna | Lectura total, incluida la bitácora de auditoría. Cero escritura |

Los dos que responden directamente a su pregunta son el **Custodio de
credenciales** y el **Visualizador**.

---

## 4. Matriz de permisos

`R` = leer · `E` = editar · `C` = crear · `D` = eliminar · `—` = sin acceso

| Recurso | Admin | Custodio | Ingeniero | Operador | Analista | Visualiz. | Auditor |
|---|---|---|---|---|---|---|---|
| **Connections** | R E C D | **R E C D** | — | — | — | — | **R** |
| **Variables** | R E C D | **R E C D** | R | — | — | — | R |
| DAGs | R E C D | — | R E | R E | R | **R** | R |
| DAG Code | R | — | R | R | R | — | R |
| DAG Runs | R E C D | — | R C E | **R C E** | R | R | R |
| Task Instances | R E C D | — | R E C | R E | R | R | R |
| **Task Logs** | R | — | R | R | R | **—** | R |
| XComs | R D | — | R | R | — | — | R |
| ImportError | R | — | R | R | — | — | R |
| Pools | R E C D | — | R | R | — | — | R |
| **Audit Logs** | R | — | — | — | — | — | **R** |
| Configurations | R E | — | — | — | — | — | R |
| Users / Roles / Permissions | R E C D | — | — | — | — | — | R |
| Menú Admin | Sí | Sí | — | — | — | — | Sí |
| Menú Browse | Sí | — | Sí | Sí | Sí | — | Sí |

### Las tres decisiones que hay dentro de esa matriz

**El Custodio no ve procesos.** Solo `Connections` y `Variables`, más el menú
Admin para llegar a ellos. Entra, gestiona credenciales, y no ve nada más. Es lo
que hace defendible la separación ante un auditor.

**El Ingeniero no ve `Connections` en absoluto.** Es la decisión más discutible
del diseño y merece que la revise (sección 5). La alternativa sería darle
`can_read`, que le permitiría ver servidor, usuario y esquema —útil para
diagnosticar— pero también el campo `extra`, donde la gente suele poner tokens.

**El Visualizador no ve `Task Logs`.** Es deliberado y es la diferencia entre
Visualizador y Analista. Las bitácoras contienen lo que los procesos imprimen:
conteos, identificadores, a veces muestras de datos. Dar acceso a bitácoras es
dar acceso a datos, aunque no lo parezca.

---

## 5. El punto incómodo: la separación de credenciales es parcial

Esto hay que decirlo antes de implementar, porque cambia lo que se le puede
prometer a Seguridad.

> **Cualquiera que pueda escribir o modificar un DAG puede leer todas las
> credenciales**, aunque no tenga ningún permiso sobre `Connections`.

Le basta con desplegar un archivo con esto:

```python
from airflow.hooks.base import BaseHook
print(BaseHook.get_connection("sqlserver_core").password)
```

El código corre dentro del entorno de Airflow, que tiene la clave de cifrado.
Ningún permiso de la interfaz lo impide.

**Lo que esto implica:**

- `BSG_IngenieroDatos` tiene, en la práctica, acceso a todas las credenciales
- La separación con `BSG_CustodioCredenciales` **solo se sostiene si el
  despliegue de DAGs está controlado**: repositorio con revisión obligatoria,
  tubería que valide, y nadie con permiso de escritura directa en la carpeta
  `dags/`

Hoy no existe ese control. El descubrimiento automático toma cualquier archivo
que aparezca en la carpeta. Es el mismo hallazgo de la revisión de arquitectura,
visto desde otro ángulo.

**Cómo plantearlo ante Seguridad, sin exagerar en ninguna dirección:**

> El control de acceso de la interfaz separa correctamente la gestión de
> credenciales de la operación de procesos. Esa separación es efectiva para el
> acceso interactivo. Para que sea efectiva también frente al código, requiere
> control de cambios sobre el repositorio de procesos, que está previsto y
> pendiente de implementar.

### Dos precisiones sobre qué se ve realmente

**La interfaz no muestra la contraseña almacenada.** Al editar una conexión, el
campo aparece vacío y hay que reescribirla. Ni siquiera un administrador la lee
desde ahí.

**El campo `extra` sí se muestra completo.** Y es donde suelen terminar tokens,
claves de API y cadenas de conexión con secretos embebidos. Conviene una regla
explícita: **nada sensible en `extra`**.

---

## 6. Segregación de funciones

Quién no puede acumular qué, y por qué.

| Combinación | ¿Permitida? | Motivo |
|---|---|---|
| Administrador + Ingeniero | **No** | Quien construye procesos no debe concederse permisos a sí mismo |
| Custodio + Ingeniero | **No** | Anula por completo la separación de la sección 5 |
| Auditor + cualquier otro | **No** | La auditoría pierde independencia |
| Operador + Analista | Sí | Ambos de solo ejecución/lectura, sin conflicto |
| Administrador + Custodio | Aceptable | El administrador ya tiene ese acceso de hecho |

**Cantidades sugeridas:** máximo 2 administradores, máximo 2 custodios. Ambos
roles nominales, nunca compartidos, y con revisión trimestral de quién los tiene.

---

## 7. Acotar por DAG o por ambiente

El recurso `DAG:<id>` permite dar permisos sobre procesos concretos. Sirve para
que, por ejemplo, un analista de riesgos vea solo los procesos de riesgos.

Hay dos formas de hacerlo, y **no son equivalentes en términos de control**:

**a) Desde el archivo del DAG** — el parámetro `access_control`

```python
dag = DAG(
    dag_id="carga_cartera",
    access_control={
        "BSG_Analista": {"can_read"},
    },
)
```

> **Cuidado con los nombres.** Airflow 2.x acepta únicamente `can_read`,
> `can_edit` y `can_delete`. Los nombres `can_dag_read`, `can_dag_edit` y
> `can_dag_trigger` que circulan en tutoriales antiguos son de Airflow 1 y
> **provocan un error**. Verificado contra el paquete 2.11.2.

**El problema de esta vía en un banco:** el autor del DAG decide quién lo ve. Un
ingeniero puede concederse acceso a sí mismo editando una línea. Para un auditor,
eso no es control de acceso.

**b) Desde la administración de roles** — un administrador asigna permisos sobre
`DAG:<id>` al rol. Más trabajoso, pero el control queda donde debe.

**Recomendación:** empezar sin acotar por DAG. Es complejidad que solo se
justifica cuando hay varias áreas de negocio con datos que no deben cruzarse.
Cuando llegue ese momento, hacerlo por la vía (b).

**Alternativa más simple y probablemente suficiente:** separar por ambiente. Un
Airflow de desarrollo y uno de producción, con listas de usuarios distintas. Los
ingenieros tienen permisos amplios en desarrollo y solo lectura en producción.

---

## 8. Cuando llegue el Directorio Activo

Los roles no se asignan uno por uno: se mapean desde grupos de AD. El archivo
`airflow/config/webserver_config.py` ya tiene el bloque preparado y comentado:

```python
AUTH_ROLES_MAPPING = {
    "CN=DATOS-Admins,OU=Grupos,DC=banco,DC=com":      ["BSG_Administrador"],
    "CN=DATOS-Custodios,OU=Grupos,DC=banco,DC=com":   ["BSG_CustodioCredenciales"],
    "CN=DATOS-Ingenieros,OU=Grupos,DC=banco,DC=com":  ["BSG_IngenieroDatos"],
    "CN=OPERACIONES,OU=Grupos,DC=banco,DC=com":       ["BSG_Operador"],
    "CN=AUDITORIA,OU=Grupos,DC=banco,DC=com":         ["BSG_Auditor"],
}
AUTH_ROLES_SYNC_AT_LOGIN = True
```

Con `AUTH_ROLES_SYNC_AT_LOGIN` activo, quitarle el grupo a alguien en AD le
retira el acceso en Airflow en su siguiente inicio de sesión. Nadie tiene que
acordarse de darlo de baja a mano. Eso es lo que un auditor quiere ver.

**Solicitar al equipo de AD:** los cinco grupos de la tabla, o los nombres que
ya existan y sirvan.

---

## 9. Qué no puede hacer este esquema

Para que no se prometa de más:

| Limitación | Detalle |
|---|---|
| **Sin bloqueo por intentos fallidos** | Airflow no lo trae. Con AD lo aplica el directorio |
| **Sin doble factor** | Requiere un proveedor de identidad delante (Entra ID, Okta, Keycloak) |
| **Sin caducidad de contraseñas** | La aplica el directorio, no Airflow |
| **Sin permisos a nivel de fila o columna** | Airflow controla procesos, no datos. Eso se controla en la base de datos |
| **La API tiene su propia autenticación** | Los roles aplican, pero el acceso por API se configura aparte |

---

## 10. Cómo se implementaría

Esbozo, **no ejecutado**. Se hace después de que valide el diseño.

```bash
# 1. Crear el rol vacío
airflow roles create BSG_CustodioCredenciales

# 2. Añadirle permisos, uno por uno
airflow roles add-perms BSG_CustodioCredenciales \
    -a can_read can_edit can_create can_delete -r Connections
airflow roles add-perms BSG_CustodioCredenciales \
    -a can_read can_edit can_create can_delete -r Variables
airflow roles add-perms BSG_CustodioCredenciales \
    -a menu_access -r Admin

# 3. Verificar lo que quedó
airflow roles list --permissions

# 4. Asignar a una persona
airflow users add-role -u hjara -r BSG_CustodioCredenciales
```

Lo entregaría como un script idempotente —que se pueda volver a ejecutar sin
duplicar— más un DAG opcional que verifique semanalmente que los roles no se
han desviado de lo definido aquí. Ese DAG es, en la práctica, la evidencia que
se le entrega al auditor.

---

## 11. Antes de implementar, hay que decidir

1. **¿Los siete roles, o un subconjunto?** Con un equipo pequeño quizá sobren el
   Visualizador y el Analista; se pueden fusionar.

2. **¿El Ingeniero ve `Connections` en solo lectura?** La matriz dice que no.
   Facilita el diagnóstico si sí, pero expone el campo `extra`.

3. **¿Nombres de los roles?** Usé el prefijo `BSG_`. Ajústelo a la nomenclatura
   del banco.

4. **¿Quién ocupa cada rol?** Con nombre y apellido, respetando la
   segregación de la sección 6.

5. **¿Se acota por DAG desde ahora, o se deja para cuando haya varias áreas?**
   La recomendación es dejarlo.
