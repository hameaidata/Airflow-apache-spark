# 03 · CI/CD y DataOps

> Documento central de la propuesta. Los demás describen qué se construye; este
> describe cómo se cambia lo construido sin romperlo.

---

## 1. Por qué el CI/CD de datos no es el CI/CD de aplicaciones

El banco ya tiene una práctica de CI/CD para software. Reutilizarla tal cual sobre
una plataforma de datos falla, y conviene entender por qué antes de diseñar nada.

Una aplicación es **sin estado** respecto de su despliegue. Si la versión 5 sale
mal, se despliega la versión 4 y el sistema vuelve a como estaba. El artefacto y el
sistema son la misma cosa.

Una plataforma de datos no. El despliegue tiene dos mitades que se comportan
distinto:

| | Código | Datos |
|---|---|---|
| Qué es | Pipelines, trabajos, reglas, configuración | Tablas Delta con historia |
| Se versiona en | Git | El propio Delta (time travel) |
| Revertir significa | Volver al commit anterior | `RESTORE` a una versión anterior de la tabla |
| ¿Basta con revertir el código? | Sí | **No.** El código malo ya escribió filas |

El caso concreto: un cambio en la regla de saldo ajustado se despliega el lunes, el
proceso corre de madrugada y sobrescribe la tabla Gold. El martes se descubre el
error. Revertir el commit arregla el código, pero la tabla Gold sigue con los datos
malos hasta que vuelva a correr, y el tablero que el Directorio miró el martes ya
mostró la cifra equivocada.

De ahí las tres reglas que gobiernan todo este documento:

**Regla 1 — Los procesos son idempotentes.** Volver a correr el proceso del día D
produce exactamente el mismo resultado, sin duplicar ni acumular. Sin esto, la
recuperación ante un error es una intervención manual y el pipeline no se puede
reintentar solo.

**Regla 2 — La calidad se valida antes de publicar, no después.** Es más barato
detener una carga que explicarle a Finanzas por qué el tablero estuvo mal medio día.

**Regla 3 — Toda tabla de negocio es reversible.** Retención de time travel
suficiente para volver atrás el cierre anterior, y un procedimiento escrito para
hacerlo.

---

## 2. La unidad de despliegue: Databricks Asset Bundles

Todo lo que compone la plataforma se declara en un único paquete versionado:
**Databricks Asset Bundle** (DAB), definido en `databricks.yml`.

Un bundle agrupa en un solo artefacto los trabajos de Lakeflow, los pipelines
declarativos, los notebooks y el código Python, los permisos sobre esos objetos, y
los parámetros que cambian entre entornos. Se despliega con un comando y
`databricks bundle validate` verifica la definición antes de tocar nada.

La consecuencia práctica es la que importa: **lo que se probó en certificación es
literalmente el mismo artefacto que entra a producción.** Solo cambian los valores
del target: el catálogo, el warehouse, las credenciales y la programación.

Ver [`plantillas/databricks.yml`](plantillas/databricks.yml) para la definición
completa de los tres entornos.

```yaml
# El núcleo de la idea, resumido
bundle:
  name: datahub-finanzas

targets:
  dev:
    mode: development          # prefija objetos con el usuario, pausa schedules
    default: true
  qa:
    mode: production
    variables: { catalogo: datahub_qa }
  prod:
    mode: production           # falla si hay algo sin declarar
    variables: { catalogo: datahub_prod }
    permissions:
      - level: CAN_VIEW
        group_name: finanzas_lectura
```

`mode: development` no es cosmético: prefija cada objeto con el nombre del usuario,
de forma que dos ingenieros trabajando a la vez no se pisan los trabajos, y deja
las programaciones en pausa para que nadie dispare una carga sin querer desde su
rama.

### Lo que NO va en el bundle

| Fuera del bundle | Dónde vive | Por qué |
|---|---|---|
| Secretos y contraseñas | Azure Key Vault, vía secret scope | Un secreto en Git es un secreto comprometido, aunque el repositorio sea privado |
| Datos | Los propios catálogos | Los datos no se despliegan, se cargan |
| El metastore de Unity Catalog | Terraform, capa de landing zone | Es infraestructura compartida; su ciclo de vida es más lento que el de los pipelines |
| Redes, ADLS, workspaces | Terraform | Igual: cambian trimestralmente, no en cada sprint |

Esa separación entre **infraestructura** (Terraform, cambia poco, la toca
Plataforma) y **carga de trabajo** (Bundles, cambia a diario, la toca Ingeniería de
Datos) evita que un cambio de una regla de negocio tenga que pasar por el mismo
comité que un cambio de red.

---

## 3. Estructura del repositorio

```
datahub-finanzas/
├── databricks.yml                 definición del bundle y sus tres targets
├── resources/                     un archivo por grupo de recursos
│   ├── jobs_ingesta.yml
│   ├── jobs_transformacion.yml
│   ├── pipelines_bronze.yml
│   ├── pipelines_silver.yml
│   └── pipelines_gold.yml
├── src/
│   ├── ingesta/                   Bantotal, MIS, archivos
│   ├── transformacion/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── calidad/                   las 30 reglas, como código
│   └── comun/                     utilidades compartidas
├── tests/
│   ├── unitarias/                 lógica pura, sin cluster
│   ├── integracion/               contra datos de prueba, con cluster
│   └── datos/                     contratos y reconciliación
├── conf/
│   ├── dev.yml  qa.yml  prod.yml  parámetros por entorno
│   └── catalogo_tablas.yml        las 38 tablas y su configuración
├── infra/                         Terraform: workspaces, redes, UC, ADLS
├── .azure-pipelines/
│   ├── ci.yml
│   └── cd.yml
└── docs/                          esta documentación
```

Dos decisiones de esta estructura merecen explicación.

**`conf/catalogo_tablas.yml` separa la configuración del código.** Las 38 tablas se
declaran como datos: origen, destino, modo de carga, clave, columna de corte. Añadir
la tabla 39 es una línea de configuración revisada en un pull request, no un
notebook nuevo. Esto es lo que hace que el framework de ingesta por metadatos que
menciona la tabla de alcance sea real y no una promesa.

**`infra/` está en el mismo repositorio pero tiene su propio pipeline.** Vivir junto
facilita ver el sistema completo; tener pipelines separados evita que un cambio de
red espere por las pruebas de un pipeline de datos, y al revés.

---

## 4. Estrategia de ramas

**Trunk-based con ramas de vida corta.** `main` siempre está en estado desplegable.

```
main ──────●───────●───────●──────●─────────────►  siempre desplegable
            \     /         \    /
             ●───●           ●──●                   ramas de 1 a 3 días
          feature/           fix/
```

| Rama | Origen | Vida | Destino |
|---|---|---|---|
| `feature/DH-123-saldo-ajustado` | `main` | 1 a 3 días | PR a `main` |
| `fix/DH-456-timeout-bantotal` | `main` | horas | PR a `main` |
| `hotfix/DH-789-cierre-roto` | tag de producción | horas | PR a `main` + despliegue directo |

Se descarta GitFlow a propósito. Con ramas de release de larga duración, el equipo
termina manteniendo dos versiones de las reglas de negocio a la vez, y en un
dominio contable eso significa dos versiones del mismo saldo. La rama corta obliga
a integrar seguido, que es incómodo la primera semana y barato para siempre.

La liberación a producción se marca con un **tag**: `v2026.09.24-1`. Ese tag es la
única referencia válida para desplegar a producción, y es lo que se revierte si hay
que volver atrás.

---

## 5. Integración continua

Se dispara en cada pull request contra `main`. Ninguna etapa toca datos reales.

| # | Etapa | Qué hace | Corta el PR |
|---|---|---|---|
| 1 | Formato y linting | `ruff` y `black` sobre `src/` y `tests/` | Sí |
| 2 | Validación del bundle | `databricks bundle validate` para los tres targets | Sí |
| 3 | Validación de configuración | `conf/catalogo_tablas.yml` contra su esquema: modos válidos, claves presentes, sin tablas duplicadas | Sí |
| 4 | Pruebas unitarias | Lógica de transformación con datos en memoria, sin cluster | Sí |
| 5 | Cobertura | Umbral mínimo sobre `src/transformacion/` | Sí |
| 6 | Análisis de secretos | `gitleaks`: ninguna credencial en el diff | Sí |
| 7 | Pruebas de integración | Sobre el workspace de DEV, con datos sintéticos | Sí |
| 8 | Publicación del artefacto | El bundle empaquetado queda como artefacto de la ejecución | — |

La etapa 3 vale la pena explicarla porque atrapa la clase de error más cara: una
tabla declarada en modo incremental sin columna de corte, o una clave de merge que
no está entre las columnas que se extraen. Sin esa validación, el error aparece de
madrugada, en producción, cuando no hay nadie mirando.

Las etapas 1 a 6 corren en minutos y sin cluster. La 7 necesita cómputo y es la
más lenta, por eso va al final: si algo básico está mal, el PR se corta antes de
gastar cómputo.

Ver [`plantillas/azure-pipelines-ci.yml`](plantillas/azure-pipelines-ci.yml).

---

## 6. Despliegue continuo

```
PR aprobado y fusionado a main
        │
        ▼
  Despliegue automático a DEV ──► pruebas de integración
        │
        ▼
  Despliegue automático a QA ───► pruebas funcionales · validación de BI · UAT
        │
        ▼
  [ APROBACIÓN MANUAL ]  ◄── responsable de Ingeniería de Datos + Finanzas
        │
        ▼
  Despliegue a PROD desde el tag ──► verificación posterior
```

**A DEV** se despliega en cada fusión a `main`, sin aprobación. Es el entorno donde
se descubre lo que las pruebas unitarias no ven.

**A QA** también automático. Aquí corre la validación funcional: reglas de negocio,
tableros y aceptación del usuario de Finanzas. Es el entorno que debe parecerse a
producción en estructura, aunque no en volumen ni en contenido de los datos (ver
[04 · Entornos](04_ENTORNOS_Y_PROMOCION.md)).

**A PROD** requiere aprobación manual registrada, y se despliega **desde el tag**,
no desde `main`. La diferencia importa: entre la aprobación y el despliegue pueden
haber entrado commits nuevos a `main`, y lo aprobado tiene que ser exactamente lo
que se instala.

La ventana de despliegue es fuera del horario de carga. Desplegar mientras el
proceso diario corre deja trabajos a medio camino con definiciones mezcladas.

### Verificación posterior al despliegue

Un despliegue que termina sin error no significa que el sistema funcione. Después
de cada pase a producción, el pipeline ejecuta automáticamente:

que todos los trabajos existan y estén habilitados; que la programación sea la
esperada y no la de desarrollo en pausa; que los permisos de Unity Catalog sean los
declarados; y una consulta de humo contra cada tabla Gold que confirme que responde
y trae filas.

Si algo de eso falla, el pipeline marca el despliegue como fallido y notifica,
aunque la instalación en sí haya terminado bien.

---

## 7. Reversión

Aquí es donde el CI/CD de datos se separa del de aplicaciones, y donde conviene
tener el procedimiento escrito **antes** de necesitarlo.

### Revertir el código

Inmediato: se despliega el tag anterior.

```bash
databricks bundle deploy -t prod --var="version=v2026.09.23-1"
```

Esto arregla el comportamiento futuro. No arregla los datos que ya se escribieron.

### Revertir los datos

Delta Lake guarda historia. Una tabla se devuelve a como estaba antes de la carga
defectuosa:

```sql
-- Qué versiones hay y qué operación las generó
DESCRIBE HISTORY datahub_prod.gold.cu_contabilidad;

-- Volver a la versión anterior al despliegue malo
RESTORE TABLE datahub_prod.gold.cu_contabilidad TO VERSION AS OF 142;
```

Tres condiciones para que esto funcione el día que haga falta, y todas hay que
prepararlas antes:

**Retención suficiente.** `delta.deletedFileRetentionDuration` debe cubrir al menos
dos cierres. El valor por defecto de siete días deja sin margen un cierre mensual
que se detecta tarde.

**`VACUUM` controlado.** Un `VACUUM` agresivo borra los archivos que el time travel
necesita. La Predictive Optimization de Databricks gestiona esto, pero el parámetro
de retención se declara explícitamente en el bundle y no se deja al defecto.

**Orden de reversión.** Se revierte **desde Gold hacia atrás**, no al revés: Gold es
lo que ve el usuario y es lo que hay que estabilizar primero. Después se corrige
Silver, se vuelve a correr y se compara.

### Cuándo no se revierte

Si el error está en los datos de origen y no en el código, revertir es lo
equivocado: devuelve la tabla a un estado que tampoco era correcto. En ese caso se
detiene la publicación, se avisa a Finanzas y se espera la corrección del origen.
La distinción entre "nuestro código está mal" y "el dato de origen está mal" es la
primera pregunta del runbook de incidentes.

---

## 8. Identidades y secretos

Cada entorno tiene su **service principal** de Microsoft Entra ID. Los pipelines se
autentican con esa identidad, nunca con la cuenta de una persona.

| Entorno | Identidad | Permisos |
|---|---|---|
| DEV | `spn-datahub-dev` | Escritura en `datahub_dev` |
| QA | `spn-datahub-qa` | Escritura en `datahub_qa`, lectura en `datahub_dev` |
| PROD | `spn-datahub-prod` | Escritura en `datahub_prod` |

Ninguna identidad de un entorno inferior tiene permiso sobre uno superior. Y el
service principal de producción **no tiene permiso de lectura sobre los orígenes
desde DEV o QA**, lo que impide por construcción que alguien copie datos
productivos hacia abajo.

Los secretos viven en Azure Key Vault y se exponen a Databricks como *secret scope
respaldado por Key Vault*. El código referencia `dbutils.secrets.get(...)`; el valor
nunca está en el repositorio ni en la configuración del bundle. La rotación se hace
en Key Vault y no requiere desplegar nada.

---

## 9. Qué se despliega y qué no

Una fuente de confusión recurrente en plataformas de datos, que conviene dejar
escrita:

| Objeto | ¿Se despliega? | Mecanismo |
|---|---|---|
| Trabajos y pipelines | Sí | Bundle |
| Código de transformación | Sí | Bundle |
| Reglas de calidad | Sí | Bundle (son código) |
| Esquemas y volúmenes de UC | Sí | Bundle o Terraform |
| Permisos sobre objetos de UC | Sí | Bundle |
| Datos | **No** | Se cargan, no se despliegan |
| Grupos de Entra ID y sus miembros | No | SCIM desde Entra ID |
| Warehouses SQL | Sí | Terraform (son infraestructura compartida) |
| Modelos semánticos de Power BI | Sí | Pipeline propio de Power BI |

La última fila es la que más se olvida. Los tableros son código y deben viajar por
su propio flujo de despliegue con sus propios entornos, apuntando al warehouse del
entorno correspondiente. Un tablero de QA que apunta al warehouse de producción es
un incidente de seguridad, no un descuido.

---

## 10. Métricas del propio proceso

DataOps se mide. Sin números, la discusión sobre si el proceso ayuda o estorba se
vuelve una cuestión de opiniones.

| Métrica | Qué indica | Objetivo inicial |
|---|---|---|
| Frecuencia de despliegue | Si el equipo puede entregar seguido | Al menos semanal |
| Tiempo de ciclo (commit a producción) | Cuánta fricción tiene el flujo | Menos de 5 días |
| Tasa de fallo en despliegue | Si las pruebas atrapan lo que deben | Menos del 15% |
| Tiempo de recuperación | Cuánto dura un incidente | Menos de 4 horas |
| Cobertura de reglas de calidad | Cuántas tablas Gold tienen control | 100% de Gold |
| Puntualidad de la carga diaria | Si el dato está cuando Finanzas lo necesita | 99% dentro de ventana |

Las cuatro primeras son las métricas DORA, que el banco probablemente ya mide para
software. Usar el mismo lenguaje evita tener que explicar desde cero por qué esto
importa.

---

## Documentos relacionados

- [04 · Entornos y promoción](04_ENTORNOS_Y_PROMOCION.md) — qué aísla cada entorno y qué datos viven en cada uno
- [05 · Estándares y convenciones](05_ESTANDARES_Y_CONVENCIONES.md) — nombres, estilo y parametrización
- [06 · Calidad y pruebas de datos](06_CALIDAD_Y_PRUEBAS_DE_DATOS.md) — qué prueba cada etapa del CI
- [07 · Gobierno, seguridad y redes](07_GOBIERNO_SEGURIDAD_Y_REDES.md) — permisos e identidades en detalle
