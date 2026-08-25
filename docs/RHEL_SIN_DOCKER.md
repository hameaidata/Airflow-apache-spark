# RHEL 9.4 sin Docker: la solución es Podman

## La respuesta corta

**No hace falta Docker.** RHEL 9.4 incluye **Podman**, que:

- Está soportado por Red Hat, incluido en la suscripción que el banco ya paga
- **Construye el mismo `Dockerfile` sin cambiarle una línea**
- Ejecuta las mismas imágenes, del mismo registro
- Puede exponer un socket compatible con la API de Docker, contra el cual
  `docker compose` funciona

Es decir: **todo lo construido en este proyecto sirve tal cual.** No hay que
rehacer nada.

> **Aclaración de algo que dije antes.** Nunca fue que Docker no se *pueda*
> instalar en RHEL: se instala desde el repositorio de Docker sin problema
> técnico. Lo que ocurre es que **Red Hat no da soporte a un RHEL con Docker
> instalado**, y en un banco eso pesa más que la comodidad. Podman evita esa
> conversación por completo.

---

## Comprobar qué hay ya en el servidor

Estos tres comandos responden si hace falta instalar algo. Puede pedírselos a
TI antes de cualquier trámite:

```bash
podman --version              # RHEL 9.4 trae 4.9.x o superior
cat /etc/redhat-release       # confirmar la version exacta
dnf list installed | grep -E "podman|buildah|skopeo|container-tools"
```

Si Podman no estuviera:

```bash
sudo dnf install -y container-tools
```

`container-tools` es un metapaquete de RHEL: trae Podman, Buildah y Skopeo. Sin
repositorios externos, sin excepciones de política.

---

## Tres formas de ejecutarlo, en orden de menor a mayor esfuerzo

### Opción A — `podman-docker`: el camino de un comando

RHEL trae un paquete que instala un ejecutable `docker` que redirige a Podman:

```bash
sudo dnf install -y podman-docker
```

A partir de ahí, **todos los comandos de este proyecto funcionan escritos igual**:

```bash
docker build -t airflow-bsg:2.11.2 .
docker images
docker ps
```

Es Podman por debajo. RHEL lo incluye precisamente para esto.

> Lo único que `podman-docker` **no** trae es `docker compose`, que es un
> binario aparte. Para eso está la opción B.

### Opción B — Socket compatible + `docker compose` *(recomendada para empezar)*

Podman expone un socket que habla la API de Docker. `docker compose` v2 se
conecta a él y funciona con los archivos que ya tenemos.

```bash
# Activar el socket para el usuario de servicio
systemctl --user enable --now podman.socket

# Apuntar las herramientas a ese socket
export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock

# Y ya
docker compose -f docker-compose.ubuntu.yml up -d
```

Para que `DOCKER_HOST` quede fijo:

```bash
echo 'export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock' \
    >> ~/.bashrc
```

**A favor:** los `docker-compose.*.yml` ya escritos y validados funcionan sin
tocarlos. Puesta en marcha inmediata.

**En contra:** el binario `docker compose` no forma parte de RHEL, así que hay
que instalarlo aparte (es un solo archivo, sin dependencias). Para desarrollo
es perfectamente razonable; para producción preferimos la opción C.

### Opción C — Quadlet: contenedores como servicios de systemd *(producción)*

Es la forma nativa de RHEL 9.3 en adelante. Cada contenedor se declara en un
archivo `.container` y systemd lo gestiona igual que cualquier otro servicio
del banco:

```bash
systemctl --user status airflow-webserver
systemctl --user restart airflow-scheduler
journalctl --user -u airflow-worker -f
```

Ejemplo de cómo se ve, para PostgreSQL:

```ini
# ~/.config/containers/systemd/airflow-postgres.container

[Unit]
Description=PostgreSQL - metadatos de Airflow

[Container]
Image=docker.io/library/postgres:16-alpine
ContainerName=airflow-postgres
EnvironmentFile=/opt/airflow-spark/.env
Volume=airflow-postgres.volume:/var/lib/postgresql/data:Z
Network=airflow.network
PublishPort=127.0.0.1:5432:5432
HealthCmd=pg_isready -U airflow
HealthInterval=10s
HealthRetries=10

[Service]
Restart=always

[Install]
WantedBy=default.target
```

**A favor:** totalmente soportado, sin binarios externos, y el área de
operaciones lo gestiona con las herramientas que ya usa. Arranque automático,
reinicio ante fallo, bitácoras en journald — todo lo que ya saben hacer.

**En contra:** hay que traducir los `docker-compose.yml` a unidades Quadlet. Son
unas dieciséis (diez servicios, la red y los volúmenes). **Es trabajo nuestro,
no de TI**, y puedo generarlas cuando decidan por esta vía.

> **Por qué no las genero ya:** son dieciséis archivos que no puedo probar desde
> aquí. Entregar dieciséis archivos sin ejecutar da una falsa sensación de que
> está resuelto. Prefiero generarlas cuando haya un servidor donde validarlas de
> inmediato, o entregarlas de dos en dos, comprobando cada par.

---

## Recomendación

| Ambiente | Opción | Por qué |
|---|---|---|
| **Desarrollo** | B — socket + `docker compose` | Todo funciona hoy, sin adaptar nada |
| **Producción** | C — Quadlet | Es lo que operaciones puede gestionar y auditar |

Empezar por B no cierra la puerta a C: los mismos contenedores, las mismas
imágenes, distinta forma de arrancarlos.

---

## Lo que hay que configurar en RHEL, una sola vez

Podman sin privilegios necesita tres cosas que en Docker no existen. Ninguna es
complicada, pero si faltan los síntomas son confusos.

### 1. Rangos de subuid y subgid

Podman sin root asigna a cada contenedor un rango de identificadores de usuario
dentro del rango del anfitrión. Sin eso, los contenedores no arrancan.

```bash
sudo usermod --add-subuids 100000-165535 \
             --add-subgids 100000-165535 svc_airflow
podman system migrate     # aplicar el cambio
```

Comprobar:

```bash
grep svc_airflow /etc/subuid /etc/subgid
```

### 2. Lingering: que los servicios sobrevivan al cierre de sesión

Sin esto, los contenedores del usuario **se detienen cuando cierra sesión**, y
no vuelven a arrancar tras reiniciar el servidor.

```bash
sudo loginctl enable-linger svc_airflow
```

Es el error más frecuente al empezar con Podman sin privilegios, y el síntoma
—"se apagó solo por la noche"— no apunta hacia ahí.

### 3. Puertos por debajo de 1024

Un usuario sin privilegios no puede publicar el puerto 443. Dos salidas:

```bash
# Permitir puertos bajos a usuarios sin privilegios
echo 'net.ipv4.ip_unprivileged_port_start=80' \
    | sudo tee /etc/sysctl.d/99-podman.conf
sudo sysctl --system
```

O, mejor para un banco: **publicar en 8080 y poner nginx delante** en el 443,
gestionado como servicio del sistema. Así la terminación TLS queda donde
operaciones espera encontrarla.

### 4. SELinux y los volúmenes

RHEL trae SELinux en modo activo, y no proponemos desactivarlo. Los montajes de
volumen necesitan una etiqueta:

```
:Z    etiqueta privada — el volumen es de un solo contenedor
:z    etiqueta compartida — varios contenedores lo usan
```

En nuestros compose, `spark_data` lo comparten Airflow y Spark, así que va con
`:z`. Los demás con `:Z`.

Es un ajuste que hay que hacer al pasar a RHEL. Sin él, el síntoma es
`Permission denied` sobre un volumen cuyos permisos se ven perfectos — porque el
problema no está en los permisos sino en la etiqueta.

---

## Diferencias reales que se van a notar

Ninguna es bloqueante, pero conviene saberlas antes que descubrirlas.

| Aspecto | Docker | Podman |
|---|---|---|
| Demonio | Uno central, como root | **Ninguno.** Cada contenedor es un proceso del usuario |
| Seguridad | El grupo `docker` equivale a root | Sin privilegios por defecto |
| `docker compose` | Nativo | Por socket compatible, u opción C |
| `deploy: replicas` | Lo respeta | Lo respeta vía socket; `podman-compose` puede que no |
| Nombres de imagen | `postgres:16` implica Docker Hub | Pide el registro completo: `docker.io/library/postgres:16` |
| Registro por defecto | Docker Hub | Se configura en `/etc/containers/registries.conf` |

**Sobre los nombres de imagen:** es la diferencia que más aparece al principio.
Podman no asume Docker Hub. En un banco con registro interno eso juega a favor
—se configura el registro corporativo y las imágenes se resuelven ahí— pero
mientras tanto puede dar `short-name resolution failed`. Se arregla poniendo el
nombre completo, o configurando `unqualified-search-registries`.

---

## Qué pedirle a TI ahora

Con esto, la sección 3 de `README-TI.md` se simplifica. Ya no hay que negociar
Docker:

```
[ ] Confirmar version de Podman instalada (podman --version)
[ ] Instalar container-tools si faltara
[ ] Rangos subuid/subgid para la cuenta de servicio
[ ] loginctl enable-linger para esa cuenta
[ ] Decidir: puertos bajos por sysctl, o nginx delante
[ ] Confirmar el registro interno de imagenes y su ruta
```

Ninguno de esos puntos requiere software fuera de la suscripción de Red Hat, ni
una excepción de política. Es la diferencia entre una solicitud que se aprueba
en una reunión y una que se discute durante semanas.

---

## Lo que sigue igual

Para que quede claro qué **no** cambia al pasar de Docker a Podman:

- El `Dockerfile` — se construye con `podman build`, sin editarlo
- Los drivers JDBC y los providers — se instalan igual
- Los DAGs, los plugins, el manifiesto JSON — nada que ver con el motor
- La imagen resultante — es la misma, cumple el estándar OCI
- Los archivos `docker-compose.*.yml` — con la opción B, sin cambios

Lo único que cambia es **cómo se arrancan los contenedores**. Todo lo demás es
idéntico.
