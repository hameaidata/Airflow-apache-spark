# Nginx: proxy inverso con TLS

Cómo funciona la capa de nginx, cómo levantarla, cómo verificarla y cómo
quitarla si hace falta.

> **Este documento es el operativo.** El trámite del certificado con la CA del
> banco —generar el CSR, enviarlo, instalar lo que devuelvan— está en
> [`docs/TLS_CERTIFICADOS.md`](docs/TLS_CERTIFICADOS.md). Aquí se asume que ya
> tiene el certificado.

---

## 1. Qué resuelve

| | Sin nginx | Con nginx |
|---|---|---|
| Airflow | `http://servidor:8080` | `https://airflow.<dominio>` |
| Spark | `http://servidor:8082` | `https://spark.<dominio>` |
| Flower | `http://servidor:5555` | `https://flower.<dominio>` |
| Contraseñas | **en claro por la red** | cifradas |
| Puertos abiertos a la red | 8080, 5555, 8082 | solo 443 |
| Certificado | ninguno | firmado por la CA del banco |

El problema concreto: hoy, cualquiera con acceso a la red del banco y un
analizador de tráfico ve su contraseña de Airflow al iniciar sesión. Es el
hallazgo más inmediato de una auditoría.

---

## 2. Cómo viaja una petición

```
  Navegador del usuario
        |
        |  https://airflow.bsg.banco.local
        v
  +-------------------------------------------+
  |  nginx-tls          puertos 443 y 80      |
  |                                           |
  |  - termina el TLS con el certificado      |
  |    firmado por el banco                   |
  |  - decide por el nombre de host           |
  |    (server_name) a quien se lo pasa       |
  |  - anade las cabeceras X-Forwarded-*      |
  +-------------------------------------------+
        |                |                |
        v                v                v
  airflow-webserver  spark-master   airflow-flower
       :8080            :8080           :5555
       (HTTP interno, red del compose)
```

**Los tres nombres llegan al mismo puerto 443.** Nginx los separa por la
cabecera `Host`: es lo que hace `server_name` en cada bloque. Por eso hace
falta un certificado con los tres nombres como SAN, y por eso hacen falta tres
entradas de DNS apuntando al mismo servidor.

El tráfico entre nginx y los contenedores va en HTTP plano, pero **nunca sale
del servidor**: viaja por la red interna del compose. El cifrado se termina en
la única frontera que cruza la red del banco.

---

## 3. Los archivos y qué hace cada uno

```
docker-compose.tls.yml                    la superposicion que anade nginx
nginx/
  templates/
    plataforma.conf.template              los tres sitios y los ajustes de TLS
  docker-entrypoint.d/
    10-resolver.sh                        escribe la directiva resolver al arrancar
tls/
  plataforma.key                          LA CLAVE PRIVADA. No sale de aqui
  plataforma.crt                          certificado firmado + cadena de la CA
  plataforma.csr                          lo que se envio a la CA
  plataforma.cnf                          config del CSR, para renovar igual
spark/conf/
  spark-defaults.conf                     Spark detras del proxy
scripts/
  generar_csr.sh / .ps1                   genera clave y CSR
  instalar_certificado.sh / .ps1          instala lo que devuelve la CA
```

### `docker-compose.tls.yml` es una superposición

No sustituye a ningún compose: **se suma** al que corresponda a su sistema.
Los tres archivos base siguen intactos y probados, y quitar TLS es simplemente
no incluirlo.

Además de añadir el servicio `nginx`, hace dos cosas más:

- Le dice a **Airflow** que está detrás de un proxy (`ENABLE_PROXY_FIX`, `BASE_URL`)
- Monta el `spark-defaults.conf` en el master y en los workers de **Spark**

### `plataforma.conf.template` no es un `.conf`

Es una **plantilla**. La imagen oficial de nginx ejecuta `envsubst` sobre
`/etc/nginx/templates/*.template` al arrancar y deja el resultado en
`/etc/nginx/conf.d/`. Así el dominio sale del `.env` y no está escrito dentro
del archivo.

---

## 4. Antes de levantarlo

```
[ ] Certificado instalado en tls/plataforma.crt
    (ver docs/TLS_CERTIFICADOS.md)
[ ] Clave en tls/plataforma.key
[ ] Tres entradas de DNS apuntando a este servidor
[ ] Las variables DOMINIO_* en el .env
```

**Variables que hay que añadir al `.env`:**

```bash
# Los tres nombres de host. Deben coincidir con los SAN del certificado
# y con las entradas de DNS.
DOMINIO_AIRFLOW=airflow.bsg.banco.local
DOMINIO_SPARK=spark.bsg.banco.local
DOMINIO_FLOWER=flower.bsg.banco.local

# Cerrar las puertas traseras. Poner nginx delante no sirve de nada si los
# puertos siguen abiertos a toda la red: cualquiera los usa y se salta el TLS.
# Con la direccion delante quedan accesibles SOLO desde el propio servidor.
AIRFLOW_WEB_PORT=127.0.0.1:8080
FLOWER_PORT=127.0.0.1:5555
SPARK_MASTER_UI_PORT=127.0.0.1:8082

# Opcionales
HTTPS_PORT=443
HTTP_PORT=80
NGINX_IMAGE=nginx:1.27-alpine
```

> No hace falta editar ningún compose para cerrar los puertos: la variable
> admite una dirección delante, y `127.0.0.1:8080:8080` es una publicación
> válida.

---

## 5. Levantarlo

**Windows**

```powershell
docker compose -f docker-compose.windows.yml -f docker-compose.tls.yml up -d
```

**Ubuntu**

```bash
docker compose -f docker-compose.ubuntu.yml -f docker-compose.tls.yml up -d
```

**RHEL / Podman**

```bash
docker compose -f docker-compose.rhel.yml -f docker-compose.tls.yml up -d
```

El orden importa: el archivo base primero, la superposición después.

> **Escríbalo siempre con los dos `-f`.** Un `docker compose -f
> docker-compose.ubuntu.yml down` a secas no ve el servicio de nginx y lo deja
> corriendo huérfano. Conviene guardarse el comando completo en un alias.

---

## 6. Verificar que quedó bien

### La configuración es válida

```bash
docker compose -f docker-compose.tls.yml exec nginx nginx -t
```

Debe decir `syntax is ok` y `test is successful`.

### La sustitución de variables funcionó

```bash
docker compose -f docker-compose.tls.yml exec nginx \
    grep server_name /etc/nginx/conf.d/plataforma.conf
```

Debe mostrar sus nombres reales. Si sale `${DOMINIO_AIRFLOW}` sin sustituir,
falta la variable en el `.env`.

### El resolver se aplicó

```bash
docker compose -f docker-compose.tls.yml exec nginx \
    cat /etc/nginx/conf.d/00-resolver.conf
```

Debe tener una línea `resolver <ip> valid=10s ipv6=off;`.

### El certificado que se está sirviendo de verdad

```bash
openssl s_client -connect airflow.bsg.banco.local:443 \
        -servername airflow.bsg.banco.local </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -dates -ext subjectAltName
```

Confirme tres cosas: que el emisor es la CA del banco, que la fecha de fin es
la esperada, y que los **tres nombres** están en el SAN.

### Airflow sabe que está detrás del proxy

```bash
docker compose exec airflow-webserver airflow config get-value webserver base_url
```

Debe devolver `https://airflow.<dominio>`, **sin barra al final**.

### La redirección de HTTP funciona

```bash
curl -s -o /dev/null -w "%{http_code} -> %{redirect_url}\n" \
     http://airflow.bsg.banco.local/
```

Debe dar `301 -> https://airflow.bsg.banco.local/`.

---

## 7. Operación diaria

### Recargar tras cambiar la configuración

```bash
# Comprobar primero, recargar despues. Nunca al reves.
docker compose -f docker-compose.tls.yml exec nginx nginx -t
docker compose -f docker-compose.tls.yml exec nginx nginx -s reload
```

`reload` no corta las conexiones en curso. Si la configuración tuviera un
error, `nginx -t` lo dice **antes** de que el `reload` deje el servicio caído.

> Un cambio en la **plantilla** (`*.template`) no se aplica con `reload`: la
> plantilla solo se procesa al arrancar el contenedor. Ahí hace falta
> `restart nginx`.

### Ver las bitácoras

```bash
# Todo junto
docker compose -f docker-compose.tls.yml logs -f nginx

# Por sitio, dentro del contenedor
docker compose -f docker-compose.tls.yml exec nginx tail -f /var/log/nginx/airflow.access.log
docker compose -f docker-compose.tls.yml exec nginx tail -f /var/log/nginx/airflow.error.log
```

Cada sitio tiene su par de archivos: `airflow`, `spark` y `flower`. Separarlos
es lo que permite responder "¿quién entró a Flower el martes?" sin filtrar
entre todo el tráfico.

### Reiniciar solo nginx

```bash
docker compose -f docker-compose.windows.yml -f docker-compose.tls.yml restart nginx
```

### Tras renovar el certificado

```bash
./scripts/instalar_certificado.sh --cert renovado.cer --cadena ca.pem
docker compose -f docker-compose.ubuntu.yml -f docker-compose.tls.yml restart nginx
```

El certificado se lee al arrancar, así que un `reload` **no basta**: hace falta
`restart`.

---

## 8. Quitarlo o volver atrás

La superposición se quita simplemente no incluyéndola:

```bash
# 1. Parar nginx
docker compose -f docker-compose.ubuntu.yml -f docker-compose.tls.yml stop nginx
docker compose -f docker-compose.ubuntu.yml -f docker-compose.tls.yml rm -f nginx

# 2. Devolver los puertos a toda la red, en el .env
#    AIRFLOW_WEB_PORT=8080
#    FLOWER_PORT=5555
#    SPARK_MASTER_UI_PORT=8082

# 3. Levantar sin la superposicion
docker compose -f docker-compose.ubuntu.yml up -d
```

Airflow vuelve a `http://servidor:8080` sin más cambios: las variables de
proxy vivían solo en la superposición.

---

## 9. Diagnóstico

| Síntoma | Causa real | Solución |
|---|---|---|
| **La interfaz de Airflow carga sin CSS ni JavaScript** | Falta `ENABLE_PROXY_FIX`; Airflow genera enlaces `http://` que el navegador bloquea | Compruebe que incluyó `-f docker-compose.tls.yml` |
| El webserver no arranca, `base_url conf cannot have a trailing slash` | `DOMINIO_AIRFLOW` acabó generando una URL con `/` final | Quite la barra |
| `nginx: [emerg] host not found in upstream` | El resolver no se aplicó | Compruebe que `10-resolver.sh` está montado y es ejecutable |
| `nginx: [emerg] unknown directive "http2"` | Imagen anterior a nginx 1.25.1 | Use `nginx:1.27-alpine` |
| `[emerg] socket() [::]:80 failed` | IPv6 deshabilitado en el servidor | No debería ocurrir: la config es IPv4. Si aparece, alguien añadió un `listen [::]` |
| `ERR_CERT_COMMON_NAME_INVALID` | El certificado no tiene SAN | La CA descartó las extensiones. Repetir con otra plantilla |
| `ERR_CERT_AUTHORITY_INVALID` | Falta la cadena de la CA | Reinstalar con `--cadena`, o publicar la raíz por GPO |
| 502 Bad Gateway | El backend está caído | `docker compose ps`; mire el servicio concreto |
| 504 Gateway Timeout en un DAG grande | La respuesta tarda más que el tiempo de espera | Ya está en 300 s; súbalo en la plantilla si su caso lo necesita |
| Flower carga pero los números no cambian | WebSocket sin cabecera `Upgrade` | Ya está en la plantilla; compruebe que nginx la cargó |
| Enlaces de Spark a nombres internos | Falta `spark.ui.reverseProxyUrl` | Regenere con `generar_csr` y reinicie master **y** workers |
| `${DOMINIO_AIRFLOW}` literal en la config | La variable no está en el `.env` | Añádala y `restart nginx` |

---

## 10. Detalles de diseño

Cuatro decisiones que no son obvias, y que se tomaron después de comprobar el
comportamiento real.

### `envsubst` se come las variables de nginx

La imagen oficial procesa las plantillas con `envsubst`, que por defecto
sustituye **toda** variable con forma `$NOMBRE`. Eso incluye `$host`,
`$remote_addr` y `$scheme`, que son de nginx y deben llegar intactas. Si se
sustituyen, quedan vacías y el proxy falla de formas muy difíciles de
diagnosticar. Por eso:

```yaml
NGINX_ENVSUBST_FILTER: "^DOMINIO_"
```

Solo se tocan las variables que empiezan por `DOMINIO_`.

### Nginx resuelve los nombres al arrancar

Con `proxy_pass http://airflow-webserver:8080`, si el contenedor de Airflow no
existe todavía, nginx **no arranca degradado: no arranca**.

```
[emerg] host not found in upstream "airflow-webserver"
```

En un arranque en frío eso es una carrera que se pierde a menudo. La solución
son upstreams por **variable** más un `resolver`, que hace que la resolución
ocurra en cada petición:

```nginx
set $destino airflow-webserver;
proxy_pass http://$destino:8080;
```

El `resolver` lo escribe `10-resolver.sh` leyendo el DNS del propio
contenedor. Se lee de `/etc/resolv.conf` y no se fija `127.0.0.11` porque esa
es la dirección de Docker: **Podman usa otra**, y fijarla haría que esto
funcionara en Windows y Ubuntu y fallara justo en RHEL.

Beneficio adicional: si un backend se recrea y cambia de IP, nginx lo nota en
10 segundos sin que nadie reinicie nada.

### Solo IPv4

Un `listen [::]:80` en un servidor con IPv6 deshabilitado —habitual en un
hardening bancario— impide que nginx arranque:

```
[emerg] socket() [::]:80 failed (97: Address family not supported by protocol)
```

La red interna del banco es IPv4. Añadir IPv6 solo aportaba ese riesgo.

### Spark reescribe sus propios enlaces

```
spark.ui.reverseProxy      true
spark.ui.reverseProxyUrl   https://spark.<dominio>
```

Estas dos opciones **deben estar puestas igual en el master y en todos los
workers**. Si faltan, la interfaz carga pero cada enlace a un worker apunta a
un nombre interno del contenedor que el navegador no resuelve.

Van en un archivo montado (`spark/conf/spark-defaults.conf`) y no en variables
de entorno, porque ese archivo lo leen siempre el master y los workers,
mientras que el paso de opciones por entorno depende de cómo arranque cada
demonio.

---

## 11. Qué cubre y qué no

**Cubre:**

- Cifrado de todo el tráfico entre el navegador y el servidor
- Certificado emitido por la CA del banco, con los tres nombres
- Una sola puerta de entrada por red: el 443
- TLS 1.2 y 1.3 únicamente — 1.0 y 1.1 están retirados (RFC 8996) y son un
  hallazgo automático en auditoría
- HSTS, `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`
- Cookie de sesión de Airflow marcada como `Secure`
- Bitácoras de acceso separadas por interfaz

**No cubre, y conviene saberlo:**

| Qué falta | Comentario |
|---|---|
| **Autenticación centralizada** | Los usuarios siguen viviendo solo en Airflow. Un empleado que se va del banco conserva su acceso. Es lo siguiente que yo haría |
| Cifrado entre nginx y los contenedores | Va en HTTP plano, pero no sale del servidor |
| Cifrado en reposo | Es cosa del disco, la pone TI |
| Limitación de peticiones | Nginx puede hacerlo (`limit_req`); no está configurado porque hoy no hay un problema que resolver |
| Grapado OCSP | Desactivado a propósito: necesita salir a Internet, y la red es aislada |

---

## 12. Sin Internet

`nginx:1.27-alpine` es **una imagen más** que tiene que viajar en el paquete.
Ya está incluida en `scripts/preparar-bundle-offline.sh` y `.ps1`, junto con
`docker-compose.tls.yml` y la carpeta `nginx/`.

**La carpeta `tls/` no viaja, y es correcto.** La clave privada no debe salir
del servidor donde se generó. En el servidor aislado se genera su propia clave
y su propio CSR, y se tramita su propio certificado.

Esto significa que **el trámite con la CA hay que hacerlo dos veces**: una para
desarrollo y otra para producción. Vale la pena preverlo en los plazos: si el
banco tarda dos semanas en firmar, son dos semanas por entorno y no se pueden
solapar del todo.
