# TLS con certificado firmado por el banco

Cómo pasar de las tres interfaces en HTTP claro a HTTPS con un certificado
emitido por la CA interna del banco.

---

## Qué cambia

| | Antes | Después |
|---|---|---|
| Airflow | `http://servidor:8080` | `https://airflow.<dominio>` |
| Spark | `http://servidor:8082` | `https://spark.<dominio>` |
| Flower | `http://servidor:5555` | `https://flower.<dominio>` |
| Contraseñas | **viajan en claro** | cifradas |
| Puertos abiertos | 8080, 5555, 8082 | solo 443 |

Hoy, cualquiera con acceso a la red del banco y un analizador de tráfico ve la
contraseña de Airflow al iniciar sesión. Es el hallazgo más inmediato de una
auditoría, y es el problema que esto cierra.

---

## Un certificado, tres nombres

No hacen falta tres certificados. **Uno solo con tres SAN** (Subject
Alternative Name) cubre los tres nombres:

```
airflow.<dominio>    spark.<dominio>    flower.<dominio>
```

Un trámite con la CA en vez de tres, y una sola fecha de caducidad que vigilar.

> **El CN ya no sirve para validar nombres.** Chrome ignora el Common Name
> desde la versión 58; Firefox y Edge igual. Un certificado con CN correcto
> pero **sin** la extensión SAN da `ERR_CERT_COMMON_NAME_INVALID` aunque el
> nombre coincida exactamente. Por eso el script siempre escribe el SAN, y por
> eso hay que insistirle a la CA en que lo conserve.

---

## El procedimiento, de principio a fin

### Paso 1 — Pedir el dominio y el DNS a TI

Antes de generar nada, necesita de TI:

```
[ ] Un dominio interno bajo el que colgar las interfaces, p.ej. bsg.banco.local
[ ] Tres entradas de DNS A apuntando a este servidor:
        airflow.bsg.banco.local
        spark.bsg.banco.local
        flower.bsg.banco.local
[ ] El procedimiento para solicitar un certificado a la CA interna
[ ] La cadena de la CA (raiz e intermedias) en formato PEM
```

### Paso 2 — Generar la clave y el CSR

```powershell
.\scripts\generar_csr.ps1 -Dominio bsg.banco.local -Org "Banco XYZ"
```
```bash
./scripts/generar_csr.sh --dominio bsg.banco.local --org "Banco XYZ"
```

Produce tres archivos en `tls/`:

| Archivo | Qué es |
|---|---|
| `plataforma.key` | **La clave privada. Nunca sale de este servidor.** |
| `plataforma.csr` | Lo que se envía al banco |
| `plataforma.cnf` | La configuración usada, para repetirla al renovar |

Y además `spark/conf/spark-defaults.conf`, con la configuración de proxy
inverso que Spark necesita.

> El script comprueba que el CSR lleva el SAN antes de darlo por bueno. Un CSR
> sin SAN se ve perfectamente normal y produce un certificado inservible —
> mejor descubrirlo en el momento que tras dos semanas de trámite.

### Paso 3 — Enviar el CSR

Envíe **únicamente** `tls/plataforma.csr`.

> **Nunca envíe `plataforma.key`.** Una CA firma un CSR sin ver jamás la clave
> privada: es el diseño del sistema. Si alguien se la pide, está pidiendo algo
> que no necesita, y a partir de ese momento esa persona puede suplantar sus
> tres interfaces.

En el correo, pida expresamente dos cosas:

1. **Que el certificado conserve los tres SAN.** Algunas plantillas de
   Microsoft AD CS descartan las extensiones del CSR y generan las suyas. El
   certificado vuelve sin SAN, se ve correcto, y el navegador lo rechaza.
2. **La cadena de la CA en PEM.** Sin ella los navegadores desconfían aunque
   el certificado sea válido.

### Paso 4 — Instalar lo que devuelvan

```powershell
.\scripts\instalar_certificado.ps1 -Cert firmado.cer -Cadena ca-cadena.pem
```
```bash
./scripts/instalar_certificado.sh --cert firmado.cer --cadena ca-cadena.pem
```

Hace cuatro comprobaciones antes de tocar nada:

| # | Comprueba | Por qué importa |
|---|---|---|
| 1 | Formato (PEM / DER / PKCS#7) | Nginx solo entiende PEM. Los otros se convierten solos |
| 2 | **Que corresponda a la clave** | El fallo más caro: si no, hay que repetir el trámite entero |
| 3 | Que conserve los tres SAN | La plantilla de la CA pudo descartarlos |
| 4 | Que no esté caducado | Una CA interna puede firmar con fechas de una plantilla vieja |

Acepta `.p7b` de AD CS directamente y extrae la cadena que trae dentro.

### Paso 5 — Configurar el `.env`

```bash
DOMINIO_AIRFLOW=airflow.bsg.banco.local
DOMINIO_SPARK=spark.bsg.banco.local
DOMINIO_FLOWER=flower.bsg.banco.local
```

Y **cierre las puertas traseras**. Poner nginx delante no sirve de nada si los
puertos siguen abiertos a toda la red:

```bash
AIRFLOW_WEB_PORT=127.0.0.1:8080
FLOWER_PORT=127.0.0.1:5555
SPARK_MASTER_UI_PORT=127.0.0.1:8082
```

No hace falta editar ningún compose: la variable admite una dirección delante.
Quedan accesibles desde el propio servidor —útil para diagnosticar— y cerrados
desde fuera.

### Paso 6 — Levantar

```powershell
docker compose -f docker-compose.windows.yml -f docker-compose.tls.yml up -d
```
```bash
docker compose -f docker-compose.ubuntu.yml -f docker-compose.tls.yml up -d
docker compose -f docker-compose.rhel.yml   -f docker-compose.tls.yml up -d
```

`docker-compose.tls.yml` es una **superposición**: no sustituye a su compose,
se suma a él. Los tres archivos base siguen intactos, y quitar TLS es
simplemente no incluirlo.

---

## Lo que se configuró por dentro, y por qué

### Airflow tiene que saber que está detrás de un proxy

```yaml
AIRFLOW__WEBSERVER__BASE_URL: "https://airflow.<dominio>"
AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX: "True"
```

Sin `ENABLE_PROXY_FIX`, Airflow ignora las cabeceras `X-Forwarded-*` y cree que
la petición llegó por HTTP al 8080. Genera entonces todos los enlaces con
`http://`, el navegador los bloquea como contenido mixto, y **la interfaz carga
a medias**: se ve el HTML pero no el CSS ni el JavaScript.

> `BASE_URL` **no puede terminar en barra**. Airflow lanza
> `AirflowConfigException: webserver.base_url conf cannot have a trailing
> slash` y el webserver no arranca. Está comprobado en el código de 2.11.2.

### Spark reescribe sus propios enlaces

```
spark.ui.reverseProxy      true
spark.ui.reverseProxyUrl   https://spark.<dominio>
```

Estas dos opciones **deben estar puestas igual en el master y en todos los
workers**. Si faltan, la interfaz carga pero cada enlace a un worker apunta a
un nombre interno del contenedor que el navegador no resuelve.

Van en `spark/conf/spark-defaults.conf` —un archivo montado— y no en variables
de entorno, porque ese archivo lo leen siempre el master y los workers,
mientras que el paso de opciones por entorno depende de cómo arranque cada
demonio.

### Tres detalles de nginx que costaron encontrar

**1. envsubst se come las variables de nginx.** La imagen oficial procesa las
plantillas con `envsubst`, que por defecto sustituye *toda* variable con forma
`$NOMBRE` — incluidas `$host`, `$remote_addr` y `$scheme`, que son de nginx y
deben llegar intactas. Por eso el compose define:

```yaml
NGINX_ENVSUBST_FILTER: "^DOMINIO_"
```

**2. Nginx resuelve los nombres al arrancar.** Con `proxy_pass
http://airflow-webserver:8080`, si el contenedor de Airflow no existe todavía,
nginx **no arranca**:

```
[emerg] host not found in upstream "airflow-webserver"
```

En un arranque en frío eso es una carrera que se pierde a menudo. La solución
son upstreams por variable más un `resolver`, que un script de arranque escribe
leyendo el DNS del propio contenedor — así funciona igual en Docker y en
Podman, que usan servidores DNS distintos.

**3. IPv6 puede impedir el arranque.** Un `listen [::]:80` en un servidor con
IPv6 deshabilitado —habitual en hardening bancario— da
`[emerg] socket() [::]:80 failed (97: Address family not supported)` y nginx no
levanta. La configuración es IPv4 solamente, a propósito.

---

## Renovación

Un certificado interno suele durar uno o dos años. Antes de que caduque:

```bash
# La MISMA clave y el MISMO cnf: no hay que regenerar nada
openssl req -new -key tls/plataforma.key -out tls/plataforma-renovacion.csr \
        -config tls/plataforma.cnf

# Se envia, y cuando vuelva:
./scripts/instalar_certificado.sh --cert renovado.cer --cadena ca.pem
docker compose -f docker-compose.ubuntu.yml -f docker-compose.tls.yml restart nginx
```

Por eso `plataforma.cnf` se guarda en disco: al renovar se repite exactamente
el mismo CSR sin tener que recordar qué parámetros se usaron.

> **No regenere la clave al renovar** salvo que haya motivo. Reutilizarla hace
> que el trámite sea solo firmar, y evita el riesgo de que el certificado
> nuevo no corresponda a la clave instalada.

---

## Diagnóstico

| Síntoma | Causa | Solución |
|---|---|---|
| `ERR_CERT_COMMON_NAME_INVALID` | El certificado no tiene SAN | La CA descartó las extensiones. Repetir con otra plantilla |
| `ERR_CERT_AUTHORITY_INVALID` | Falta la cadena de la CA | Reinstalar con `--cadena`, o publicar la raíz por GPO |
| Interfaz sin CSS ni JavaScript | Falta `ENABLE_PROXY_FIX` | Está en `docker-compose.tls.yml`; compruebe que lo incluyó |
| Webserver no arranca | `BASE_URL` termina en barra | Quítela |
| Nginx no arranca, `host not found in upstream` | El resolver no se aplicó | Compruebe que `10-resolver.sh` está montado |
| Nginx no arranca, `unknown directive "http2"` | Imagen anterior a 1.25.1 | Use `nginx:1.27-alpine` |
| Enlaces de Spark a nombres internos | Falta `spark.ui.reverseProxyUrl` | Regenere con `generar_csr` y reinicie Spark |
| Flower con números congelados | WebSocket sin `Upgrade` | Ya está en la plantilla; compruebe que nginx la cargó |

Comprobaciones útiles:

```bash
# La configuracion de nginx es valida?
docker compose -f docker-compose.tls.yml exec nginx nginx -t

# Que certificado esta sirviendo de verdad?
openssl s_client -connect airflow.bsg.banco.local:443 -servername airflow.bsg.banco.local </dev/null 2>/dev/null | openssl x509 -noout -subject -dates -ext subjectAltName

# Airflow ve el HTTPS?
docker compose exec airflow-webserver airflow config get-value webserver base_url
```

---

## Sin Internet

`nginx:1.27-alpine` es **una imagen más** que tiene que viajar en el paquete.
Ya está incluida en `scripts/preparar-bundle-offline.sh` y `.ps1`, junto con
`docker-compose.tls.yml` y la carpeta `nginx/`.

**La carpeta `tls/` no viaja**, y es correcto: la clave privada no debe salir
del servidor donde se generó. En el servidor aislado se genera su propia clave
y su propio CSR.

El grapado OCSP está desactivado a propósito: requiere que nginx consulte al
respondedor de la CA, y esa consulta no sale de una red aislada. Dejarlo activo
solo añadiría un tiempo de espera en cada arranque.
