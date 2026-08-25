# Quadlet: contenedores como servicios de systemd (RHEL)

Es la **opción C** de `docs/RHEL_SIN_DOCKER.md`: la forma nativa de RHEL 9.3 en
adelante de correr contenedores, y la que el área de operaciones del banco
puede gestionar con las herramientas que ya usa.

```bash
systemctl --user status airflow-webserver
systemctl --user restart airflow-scheduler
journalctl --user -u airflow-worker -f
```

---

## Cuándo usar esto y cuándo no

| | Compose sobre el socket de Podman | Quadlet |
|---|---|---|
| Puesta en marcha | Inmediata | Hay que instalar 16 unidades |
| `docker compose` | Hace falta el binario, ajeno a RHEL | No hace falta nada externo |
| Gestión | Comandos de compose | `systemctl`, `journalctl` |
| Arranque al reiniciar | `restart: always` | systemd, con `linger` |
| Escalar workers | `--scale airflow-worker=5` | Copiar la unidad N veces |
| **Recomendado para** | **Desarrollo y pruebas** | **Producción** |

Empezar por compose no cierra la puerta a Quadlet: los mismos contenedores,
las mismas imágenes, distinta forma de arrancarlos.

---

## Las unidades no se versionan: se generan

**No hay 16 archivos guardados en el repositorio, y es a propósito.**

```bash
python3 scripts/generar_quadlet.py
```

Dos razones:

**1. Contienen secretos en claro.** systemd no expande variables en
`Environment=`, así que la contraseña de PostgreSQL termina escrita dentro de
la cadena de conexión. El generador las escribe con permisos `0600` y
`rhel/quadlet/` está en `.gitignore`. **Genérelas en el servidor, con el
`.env` de ese servidor.**

**2. Se desincronizarían.** Mantener 16 unidades a mano en paralelo con el
compose garantiza que en tres meses digan cosas distintas y nadie sepa cuál es
la verdad. Generándolas, el compose sigue siendo la única fuente.

> **Si cambia el `.env`, vuelva a generar.** Los valores quedaron resueltos
> dentro de las unidades; editar el `.env` después no las afecta.

---

## Instalación

```bash
# 1. Generar con el .env real del servidor
cd ~/airflow-spark
python3 scripts/generar_quadlet.py

# 2. Instalar
mkdir -p ~/.config/containers/systemd
cp rhel/quadlet/* ~/.config/containers/systemd/

# 3. Que systemd las lea
systemctl --user daemon-reload

# 4. Arrancar. Al pedir el webserver arrancan sus dependencias.
systemctl --user start airflow-webserver

# 5. Que sobrevivan al cierre de sesión y al reinicio
sudo loginctl enable-linger $USER
```

El paso 5 no es opcional. Sin `linger`, **los contenedores se detienen cuando
usted cierra sesión** y no vuelven tras reiniciar el servidor. Es el error más
frecuente al empezar con Podman sin privilegios, y el síntoma —"se apagó solo
por la noche"— no apunta hacia ahí.

---

## Lo que Quadlet NO hace, y hay que saber

**systemd espera a que la unidad anterior *arranque*, no a que esté *sana*.**

En el compose, `depends_on: condition: service_healthy` hace que el webserver
espere a que PostgreSQL responda de verdad. En Quadlet sobre Podman 4.x —el de
RHEL 9.4— esa ordenación por salud no existe: `After=` solo garantiza el
orden de inicio.

**Consecuencia práctica:** en el primer arranque, el webserver puede intentar
conectarse antes de que PostgreSQL acepte conexiones, fallar, y ser
reiniciado. Las unidades llevan `Restart=always` y `RestartSec=15`
precisamente por eso. **Converge sola en menos de un minuto**, pero verá
errores en el journal durante ese rato y no son un problema.

Si prefiere un arranque limpio, arranque en orden a mano la primera vez:

```bash
systemctl --user start postgres redis
sleep 20
systemctl --user start airflow-init
systemctl --user start airflow-webserver airflow-scheduler airflow-worker
```

---

## Escalar workers

Compose tiene `--scale`; systemd no. Se copia la unidad:

```bash
cd ~/.config/containers/systemd
for i in 2 3; do
    sed "s/ContainerName=airflow-worker/ContainerName=airflow-worker-$i/" \
        airflow-worker.container > airflow-worker-$i.container
    chmod 600 airflow-worker-$i.container
done
systemctl --user daemon-reload
systemctl --user start airflow-worker-2 airflow-worker-3
```

Celery los descubre solos: se registran contra Redis al arrancar. No hay que
tocar ninguna configuración.

---

## Diagnóstico

| Síntoma | Comando |
|---|---|
| Una unidad no arranca | `systemctl --user status <nombre>` |
| Ver qué pasó | `journalctl --user -u <nombre> -n 100 --no-pager` |
| Quadlet no genera el servicio | `/usr/libexec/podman/quadlet -dryrun -user` |
| Ver los servicios generados | `systemctl --user list-unit-files "*airflow*"` |
| Un contenedor no está sano | `podman healthcheck run <nombre>` |

El tercero es el más útil cuando **la unidad ni siquiera aparece**: Quadlet
traduce los `.container` a servicios al hacer `daemon-reload`, y si un archivo
tiene un error de sintaxis lo descarta en silencio. `-dryrun` muestra lo que
generaría y dónde se atasca.

---

## Nota de confianza

Estas unidades se generaron a partir del compose y se revisaron línea por
línea, pero **no se han ejecutado en un RHEL real** — aquí no hay Podman con
el que probarlas.

Lo primero que conviene hacer en el servidor, antes de instalar nada:

```bash
python3 scripts/generar_quadlet.py
cp rhel/quadlet/* ~/.config/containers/systemd/
/usr/libexec/podman/quadlet -dryrun -user
```

Si `-dryrun` no se queja, la sintaxis es correcta. Lo que quede después será
de configuración, no de forma, y el journal lo dirá.
