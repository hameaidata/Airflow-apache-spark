"""
webserver_config.py — configuracion de autenticacion de la interfaz de Airflow.

Se monta en /opt/airflow/webserver_config.py (ver docker-compose).
Aqui es donde se configura el login de verdad. NO en variables de entorno:
AIRFLOW__WEBSERVER__AUTHENTICATE y AIRFLOW__WEBSERVER__AUTH_BACKEND no existen
en Airflow 2.x — si las viste en algun tutorial, son de la version 1.

Las contrasenas de estos usuarios SI se hashean, y lo hace Airflow solo:
Flask-AppBuilder las guarda en la tabla ab_user con scrypt:32768:8:1 y salt
aleatorio de 16 bytes. No hay nada que configurar para eso.
"""

import os

# Las constantes de FAB han cambiado de ruta entre versiones de Airflow.
# Se intentan las rutas conocidas y, si ninguna vale, se usan los valores
# numericos, que llevan estables desde siempre.
try:
    from flask_appbuilder.const import (
        AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER,
    )
except ImportError:  # pragma: no cover
    try:
        from airflow.www.fab_security.manager import (
            AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER,
        )
    except ImportError:
        AUTH_OID, AUTH_DB, AUTH_LDAP, AUTH_REMOTE_USER, AUTH_OAUTH = 0, 1, 2, 3, 4

basedir = os.path.abspath(os.path.dirname(__file__))


# =============================================================================
# TIPO DE AUTENTICACION
# =============================================================================

# AUTH_DB = usuarios en la base de datos de Airflow, con contrasena hasheada.
# Es lo que esta activo ahora. Para produccion bancaria lo normal es AUTH_LDAP
# contra el Active Directory: ver el bloque comentado mas abajo.
AUTH_TYPE = AUTH_DB

# Que NO se creen cuentas solas. Con LDAP u OAuth, si esto queda en True
# cualquiera que autentique contra el directorio entra a Airflow, aunque no
# tenga nada que ver con el equipo de datos.
AUTH_USER_REGISTRATION = False

# Rol de quien entre sin rol asignado. "Public" no ve nada: es el valor seguro.
AUTH_USER_REGISTRATION_ROLE = "Public"

# Resincroniza los roles desde el directorio en cada login. Asi, quitarle el
# grupo a alguien en AD le retira el acceso en Airflow al siguiente inicio de
# sesion, sin que nadie tenga que acordarse de hacerlo a mano.
AUTH_ROLES_SYNC_AT_LOGIN = True


# =============================================================================
# ENDURECIMIENTO DE LA SESION
# =============================================================================

# Solo enviar la cookie por HTTPS.
# OJO: mientras el stack corra en HTTP plano, esto TE DEJARA FUERA de la UI.
# Actívalo el mismo dia que pongas TLS delante, ni antes ni despues.
SESSION_COOKIE_SECURE = os.environ.get("AIRFLOW_HTTPS", "false").lower() == "true"

# JavaScript no puede leer la cookie: mitiga el robo de sesion por XSS.
SESSION_COOKIE_HTTPONLY = True

# No se envia la cookie en peticiones desde otros sitios: mitiga CSRF.
SESSION_COOKIE_SAMESITE = "Lax"

# Cierre de sesion por inactividad. 8 horas es una jornada; para banca suele
# pedirse bastante menos. Ajusta a tu politica.
from datetime import timedelta  # noqa: E402
PERMANENT_SESSION_LIFETIME = timedelta(
    minutes=int(os.environ.get("AIRFLOW_SESSION_MINUTOS", "480"))
)
SESSION_REFRESH_EACH_REQUEST = True

# Token CSRF con caducidad limitada.
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = 3600


# =============================================================================
# LDAP / ACTIVE DIRECTORY  (para cuando lo habilites)
# =============================================================================
#
# Descomenta este bloque y cambia AUTH_TYPE = AUTH_LDAP arriba.
#
# Los valores sensibles se leen del entorno, nunca se escriben aqui: este
# archivo va a git.
#
# AUTH_TYPE = AUTH_LDAP
#
# AUTH_LDAP_SERVER = os.environ["LDAP_SERVER"]           # ldaps://dc.banco.com:636
# AUTH_LDAP_SEARCH = os.environ["LDAP_SEARCH_BASE"]      # DC=banco,DC=com
# AUTH_LDAP_BIND_USER = os.environ["LDAP_BIND_USER"]
# AUTH_LDAP_BIND_PASSWORD = os.environ["LDAP_BIND_PASSWORD"]
#
# AUTH_LDAP_UID_FIELD = "sAMAccountName"                 # AD usa este, no "uid"
# AUTH_LDAP_FIRSTNAME_FIELD = "givenName"
# AUTH_LDAP_LASTNAME_FIELD = "sn"
# AUTH_LDAP_EMAIL_FIELD = "mail"
#
# # TLS obligatorio. Sin esto las credenciales viajan en claro por la red.
# AUTH_LDAP_USE_TLS = True
# AUTH_LDAP_TLS_CACERTFILE = "/etc/ssl/certs/ca-banco.crt"
# AUTH_LDAP_ALLOW_SELF_SIGNED = False
#
# # Mapeo de grupos de AD a roles de Airflow.
# # Es el control de acceso de verdad: quien puede editar DAGs y quien solo mira.
# AUTH_ROLES_MAPPING = {
#     "CN=DATOS-Admins,OU=Grupos,DC=banco,DC=com":     ["Admin"],
#     "CN=DATOS-Ingenieros,OU=Grupos,DC=banco,DC=com": ["Op"],
#     "CN=DATOS-Analistas,OU=Grupos,DC=banco,DC=com":  ["User"],
#     "CN=AUDITORIA,OU=Grupos,DC=banco,DC=com":        ["Viewer"],
# }
# AUTH_LDAP_GROUP_FIELD = "memberOf"
#
# # Solo estos grupos pueden entrar, aunque autentiquen contra el directorio.
# AUTH_LDAP_SEARCH_FILTER = "(memberOf=CN=DATOS-Todos,OU=Grupos,DC=banco,DC=com)"


# =============================================================================
# LO QUE ESTE ARCHIVO NO PUEDE HACER
# =============================================================================
#
# - Bloqueo de cuenta tras N intentos fallidos: Airflow no lo trae. Con LDAP lo
#   aplica el directorio, que es donde debe estar. Con AUTH_DB no hay bloqueo:
#   una razon mas para no quedarse en AUTH_DB en produccion.
#
# - MFA: no es nativo. Se resuelve poniendo un proveedor de identidad delante
#   (Entra ID, Okta, Keycloak) con AUTH_OAUTH.
#
# - Limite de peticiones: se hace en el proxy inverso (nginx, Traefik), no aqui.
#
# - Caducidad de contrasenas: la aplica el directorio, no Airflow.
