# 07 · Gobierno, seguridad y redes

---

## 1. Principio rector

**Nadie tiene acceso a producción de forma permanente. Los procesos sí.**

Suena drástico y es lo contrario de como suelen empezar estas plataformas, donde el
equipo que la construye conserva acceso de administrador "por si acaso". Ese "por si
acaso" es el que aparece en el informe de auditoría.

La consecuencia práctica es que **todo lo que hay que hacer en producción tiene que
ser posible sin entrar a producción**: desplegar, diagnosticar, revertir. Si algo
solo se puede resolver entrando, ese algo es un defecto de diseño del pipeline, no
una razón para dar el acceso.

---

## 2. Identidades

### Personas

Microsoft Entra ID, con grupos sincronizados a Databricks por SCIM. Los grupos se
gestionan en Entra ID; Databricks no es fuente de verdad de quién es quién.

| Grupo | Alcance | Qué puede |
|---|---|---|
| `datahub-ingenieros` | Workspace no productivo | Crear y ejecutar en DEV; leer QA |
| `datahub-arquitectos` | Ambos workspaces | Lo anterior + leer producción (sin escribir) |
| `datahub-operadores` | Workspace productivo | Ver ejecuciones y logs; reejecutar trabajos; sin acceso a datos |
| `finanzas-lectura` | Producción | Leer Gold Analytics |
| `finanzas-contabilidad` | Producción | Leer Gold Analytics + Gold Enterprise |
| `datahub-auditoria` | Ambos | Leer `system.*` y linaje; sin acceso a datos de negocio |

Dos grupos merecen explicación.

**`datahub-operadores` sin acceso a datos.** Quien está de guardia a las 3 de la
mañana necesita ver por qué falló un trabajo y volver a lanzarlo. No necesita leer
saldos de clientes. Separar esas dos cosas hace que la guardia pueda cubrirse con
un perfil de operación y no obligue a tener a un ingeniero de datos despierto.

**`datahub-arquitectos` lee producción pero no escribe.** Diagnosticar exige ver;
corregir debe pasar por el pipeline. Es la separación que mantiene honesto el
proceso: si arreglar en caliente fuera posible, sería lo que ocurriría siempre bajo
presión de cierre.

### Procesos

Un service principal por entorno, y ninguno con permisos sobre un entorno superior:

| Identidad | Escribe en | Lee de |
|---|---|---|
| `spn-datahub-dev` | `datahub_dev` | `datahub_dev` |
| `spn-datahub-qa` | `datahub_qa` | `datahub_qa`, `datahub_dev` |
| `spn-datahub-prod` | `datahub_prod` | `datahub_prod`, orígenes |

Lo importante es lo que **no** aparece en la tabla: ninguna identidad de DEV o QA
tiene permiso de lectura sobre los orígenes productivos. Eso impide por construcción
—no por norma— que alguien copie datos del core a un entorno bajo. La norma se
puede olvidar; el permiso ausente, no.

---

## 3. Permisos sobre datos

Unity Catalog, con permisos por grupo y nunca por persona. Un permiso individual es
un permiso que nadie revoca cuando esa persona cambia de área.

```sql
GRANT USE CATALOG ON CATALOG datahub_prod TO `finanzas-lectura`;
GRANT USE SCHEMA  ON SCHEMA  datahub_prod.gold_analytics TO `finanzas-lectura`;
GRANT SELECT      ON SCHEMA  datahub_prod.gold_analytics TO `finanzas-lectura`;

-- Bronze y Silver NO se exponen a usuarios de negocio.
-- Son capas técnicas: su esquema cambia cuando cambia el origen, y un usuario
-- que construya sobre ellas queda atado a decisiones que no controla.
```

Los permisos se declaran en el bundle y se despliegan con el código. Un permiso
otorgado a mano en la interfaz desaparece en el siguiente despliegue, lo cual es
exactamente lo que debe pasar.

### Etiquetas y ABAC

En lugar de enumerar columnas sensibles una por una, se etiquetan y la política se
aplica sobre la etiqueta:

```sql
ALTER TABLE datahub_prod.silver.cliente
  ALTER COLUMN documento SET TAGS ('clasificacion' = 'pii');

CREATE OR REPLACE FUNCTION datahub_prod.ctl.enmascarar_pii(valor STRING)
RETURN CASE
  WHEN is_account_group_member('finanzas-contabilidad') THEN valor
  ELSE CONCAT('***', RIGHT(valor, 3))
END;
```

Con eso, una columna nueva marcada como `pii` queda protegida desde el momento en
que se etiqueta, sin tocar la política. Enumerar columnas obliga a acordarse de
actualizar la lista cada vez, y es cuestión de tiempo que alguien no se acuerde.

---

## 4. Secretos

Azure Key Vault, expuesto a Databricks como *secret scope respaldado por Key Vault*.

```python
usuario = dbutils.secrets.get(scope="datahub-prod", key="bantotal-usuario")
clave   = dbutils.secrets.get(scope="datahub-prod", key="bantotal-clave")
```

El valor nunca está en el repositorio, ni en el bundle, ni en una variable de
entorno del workspace. La rotación se hace en Key Vault y no requiere desplegar
nada.

El CI ejecuta `gitleaks` sobre cada diff. Un secreto que llega a un repositorio
—aunque sea privado, aunque se borre en el commit siguiente— queda en el historial
y hay que considerarlo comprometido y rotarlo. Es más barato impedirlo que
gestionarlo.

---

## 5. Red

La arquitectura ya define el esquema:

| Elemento | Propósito |
|---|---|
| ExpressRoute o VPN site-to-site | Conectividad con el data center del banco |
| VNet injection (NPIP) | Los nodos no tienen IP pública |
| Private Link (back-end) | El tráfico a los servicios de Azure no sale a Internet |
| Secure Cluster Connectivity | Sin puertos entrantes abiertos hacia los nodos |
| Key Vault | Secretos |

Tres puntos que el diagrama no dice y conviene fijar por escrito.

**El acceso a Bantotal es sobre réplica o ventana acordada.** Consultar el core en
horario productivo es una conversación con el área que lo opera, no una decisión de
Ingeniería de Datos. Debe quedar acordado formalmente y documentado en el runbook.

**La salida a Internet se restringe por lista blanca.** Un clúster que puede
resolver cualquier dominio es un vector de exfiltración. Lo que se necesita es
acotado: los repositorios de paquetes y los servicios de Azure. Todo lo demás se
bloquea.

**Los endpoints privados se declaran en Terraform.** Un endpoint creado a mano no
existe cuando haya que recrear el entorno, y el descubrimiento ocurre en el peor
momento posible.

---

## 6. Auditoría

Unity Catalog escribe en `system tables`. Lo relevante para un banco:

| Pregunta | Dónde se responde |
|---|---|
| Quién consultó qué tabla y cuándo | `system.access.audit` |
| Quién cambió un permiso | `system.access.audit` |
| De dónde sale el dato de una celda de un tablero | `system.access.table_lineage` y `column_lineage` |
| Qué se desplegó, cuándo y quién lo aprobó | Historial de Azure DevOps + tags de Git |
| Qué versión de una tabla existía en una fecha | `DESCRIBE HISTORY` de Delta |

Las tres primeras las da la plataforma sola. Las dos últimas dependen de que el
flujo de CI/CD esté implementado como se describe en
[03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md): si alguien despliega a mano, esa
evidencia no existe.

**El linaje de columna es la capacidad más valiosa y la menos usada.** Poder
contestar "qué campos de Bantotal alimentan esta celda del tablero de Contabilidad"
sin reconstruirlo a mano cambia por completo dos conversaciones: la de impacto
antes de un cambio, y la de auditoría después de un hallazgo.

---

## 7. Lo que hay que acordar con Seguridad antes de construir

| Tema | Decisión pendiente | Documento |
|---|---|---|
| Datos en entornos bajos | Enmascaramiento de QA, con importes conservados | [ADR-004](10_DECISIONES_ADR.md#adr-004) |
| Acceso de emergencia a producción | ¿Existe un mecanismo de acceso temporal con aprobación y expiración, o no existe acceso? | — |
| Retención de logs de auditoría | Cuánto tiempo, según la normativa aplicable | — |
| Ventana de acceso a Bantotal | Horario acordado con el área del core | — |
| Salida a Internet | Lista blanca de dominios permitidos | — |

La segunda es la que conviene resolver antes y no durante el primer incidente. Si
no existe un mecanismo de acceso de emergencia definido, lo que va a ocurrir bajo
presión de cierre es que alguien pida credenciales de administrador por chat, y esa
excepción se vuelve permanente.

---

## Documentos relacionados

- [04 · Entornos y promoción](04_ENTORNOS_Y_PROMOCION.md)
- [03 · CI/CD y DataOps](03_CICD_Y_DATAOPS.md) — identidades y secretos en el pipeline
- [10 · Decisiones de arquitectura](10_DECISIONES_ADR.md)
