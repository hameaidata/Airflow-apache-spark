from airflow.models import Variable


# ============================================================
# LEER VARIABLE JSON DESDE AIRFLOW
# ============================================================

CONFIG = Variable.get(
    "config_bases_datos",
    deserialize_json=True
)


# ============================================================
# DESMENUZAR JSON
# ============================================================

SQLSERVER_CONFIG = CONFIG["sqlserver"]

SINGLESTORE_CONFIG = CONFIG["singlestore"]

BANCO_TOTAL_CONFIG = CONFIG["banco_total"]


# ============================================================
# OBTENER VALORES INDIVIDUALES
# ============================================================

SQLSERVER_CONN_ID = SQLSERVER_CONFIG["conn_id"]

SINGLESTORE_CONN_ID = SINGLESTORE_CONFIG["conn_id"]

BT_CONN_ID = BANCO_TOTAL_CONFIG["conn_id"]

{
    "sqlserver": {
        "conn_id": "sqlserver_connection"
    },
    "singlestore": {
        "conn_id": "singlestore_connection"
    },
    "banco_total": {
        "conn_id": "banco_total_connection"
    }
}

SQLSERVER_CONFIG ={
    "conn_id": "sqlserver_connection"
}