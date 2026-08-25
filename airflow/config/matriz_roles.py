"""
Matriz de roles — origen unico de verdad.

Este archivo lo leen dos cosas:
  1. aplicar_roles.py           — crea y actualiza los roles
  2. dags/production/auditoria_roles.py — verifica semanalmente que no cambiaron

Vive en $AIRFLOW_HOME/config, que Airflow anade al sys.path al arrancar, por lo
que se puede importar desde un DAG con  `from matriz_roles import ROLES`.

Diseno completo y justificacion: docs/DISENO_ROLES.md

-----------------------------------------------------------------------------
ACCIONES VALIDAS EN AIRFLOW 2.11 (verificadas contra el paquete):
    can_read, can_create, can_edit, can_delete, menu_access

NO existen can_dag_read, can_dag_edit ni can_dag_trigger. Son de Airflow 1 y
provocan error. Aparecen en muchos tutoriales antiguos.
-----------------------------------------------------------------------------
"""

from __future__ import annotations

PREFIJO = "BSG_"

# Lo minimo para que una persona pueda entrar a la interfaz y ver su perfil.
# Se anade a TODOS los roles. Sin esto el usuario autentica pero no ve nada,
# y el sintoma es confuso: la sesion inicia y la pagina queda en blanco.
PERMISOS_BASE: dict[str, list[str]] = {
    "Website": ["can_read"],
    "My Profile": ["can_read", "can_edit"],
    "My Password": ["can_read", "can_edit"],
}


ROLES: dict[str, dict] = {

    # =========================================================================
    "BSG_Administrador": {
        "descripcion": "Administracion completa de la plataforma, incluida la "
                       "gestion de usuarios y permisos. Maximo 2 personas.",
        "permisos": {
            # Credenciales y configuracion
            "Connections":       ["can_read", "can_create", "can_edit", "can_delete"],
            "Variables":         ["can_read", "can_create", "can_edit", "can_delete"],
            "Pools":             ["can_read", "can_create", "can_edit", "can_delete"],
            "Configurations":    ["can_read", "can_edit"],
            # Procesos
            "DAGs":              ["can_read", "can_edit", "can_delete"],
            "DAG Code":          ["can_read"],
            "DAG Runs":          ["can_read", "can_create", "can_edit", "can_delete"],
            "DAG Dependencies":  ["can_read"],
            "DAG Warnings":      ["can_read"],
            "Task Instances":    ["can_read", "can_create", "can_edit", "can_delete"],
            "Task Logs":         ["can_read"],
            "Task Reschedules":  ["can_read"],
            "XComs":             ["can_read", "can_delete"],
            "ImportError":       ["can_read", "can_delete"],
            "SLA Misses":        ["can_read"],
            "Datasets":          ["can_read", "can_create", "can_edit", "can_delete"],
            # Plataforma
            "Jobs":              ["can_read"],
            "Plugins":           ["can_read"],
            "Providers":         ["can_read"],
            "Triggers":          ["can_read"],
            "Cluster Activity":  ["can_read"],
            "Audit Logs":        ["can_read"],
            # Gestion de accesos
            "Users":             ["can_read", "can_create", "can_edit", "can_delete"],
            "Roles":             ["can_read", "can_create", "can_edit", "can_delete"],
            "Permissions":       ["can_read"],
            "Permission Views":  ["can_read"],
            "View Menus":        ["can_read"],
            "Passwords":         ["can_read", "can_edit"],
            # Menus
            "Admin":             ["menu_access"],
            "Browse":            ["menu_access"],
            "Docs":              ["menu_access"],
            "Documentation":     ["menu_access"],
        },
    },

    # =========================================================================
    "BSG_CustodioCredenciales": {
        "descripcion": "Gestiona unicamente credenciales y parametros. No ve "
                       "procesos ni bitacoras. Separacion de funciones frente "
                       "al equipo de datos.",
        "permisos": {
            "Connections":  ["can_read", "can_create", "can_edit", "can_delete"],
            "Variables":    ["can_read", "can_create", "can_edit", "can_delete"],
            "Admin":        ["menu_access"],
            # Deliberadamente SIN: DAGs, Task Logs, DAG Runs.
            # Entra, gestiona credenciales, y no ve nada mas.
        },
    },

    # =========================================================================
    "BSG_IngenieroDatos": {
        "descripcion": "Construye, ejecuta y depura procesos. No administra "
                       "accesos ni credenciales.",
        "permisos": {
            "DAGs":             ["can_read", "can_edit"],
            "DAG Code":         ["can_read"],
            "DAG Runs":         ["can_read", "can_create", "can_edit"],
            "DAG Dependencies": ["can_read"],
            "DAG Warnings":     ["can_read"],
            "Task Instances":   ["can_read", "can_create", "can_edit"],
            "Task Logs":        ["can_read"],
            "Task Reschedules": ["can_read"],
            "XComs":            ["can_read"],
            "ImportError":      ["can_read"],
            "SLA Misses":       ["can_read"],
            "Datasets":         ["can_read"],
            "Pools":            ["can_read"],
            "Jobs":             ["can_read"],
            "Providers":        ["can_read"],
            "Cluster Activity": ["can_read"],
            "Browse":           ["menu_access"],
            "Docs":             ["menu_access"],
            "Documentation":    ["menu_access"],
            # SIN Connections por diseno. Si decide darle lectura, ponga
            # PERMITIR_CONEXIONES_A_INGENIERO = True mas abajo.
        },
    },

    # =========================================================================
    "BSG_Operador": {
        "descripcion": "Ejecuta, pausa y reintenta procesos. No modifica "
                       "codigo ni ve credenciales.",
        "permisos": {
            "DAGs":             ["can_read", "can_edit"],   # can_edit = pausar
            "DAG Code":         ["can_read"],
            "DAG Runs":         ["can_read", "can_create"],  # can_create = disparar
            "Task Instances":   ["can_read", "can_edit"],    # can_edit = limpiar
            "Task Logs":        ["can_read"],
            "ImportError":      ["can_read"],
            "SLA Misses":       ["can_read"],
            "Jobs":             ["can_read"],
            "Cluster Activity": ["can_read"],
            "Browse":           ["menu_access"],
        },
    },

    # =========================================================================
    "BSG_Analista": {
        "descripcion": "Consulta el estado de los procesos y sus bitacoras. "
                       "No ejecuta nada.",
        "permisos": {
            "DAGs":           ["can_read"],
            "DAG Code":       ["can_read"],
            "DAG Runs":       ["can_read"],
            "Task Instances": ["can_read"],
            "Task Logs":      ["can_read"],
            "SLA Misses":     ["can_read"],
            "Browse":         ["menu_access"],
        },
    },

    # =========================================================================
    "BSG_Visualizador": {
        "descripcion": "Ve el estado de los procesos. SIN acceso a bitacoras: "
                       "las bitacoras contienen datos de negocio.",
        "permisos": {
            "DAGs":     ["can_read"],
            "DAG Runs": ["can_read"],
            # Deliberadamente SIN Task Logs ni DAG Code.
            # Esta es la unica diferencia real con BSG_Analista.
        },
    },

    # =========================================================================
    "BSG_Auditor": {
        "descripcion": "Lectura total, incluida la bitacora de auditoria. "
                       "Cero capacidad de escritura o ejecucion.",
        "permisos": {
            "Audit Logs":       ["can_read"],
            "Connections":      ["can_read"],
            "Variables":        ["can_read"],
            "Configurations":   ["can_read"],
            "DAGs":             ["can_read"],
            "DAG Code":         ["can_read"],
            "DAG Runs":         ["can_read"],
            "DAG Dependencies": ["can_read"],
            "Task Instances":   ["can_read"],
            "Task Logs":        ["can_read"],
            "XComs":            ["can_read"],
            "ImportError":      ["can_read"],
            "SLA Misses":       ["can_read"],
            "Pools":            ["can_read"],
            "Jobs":             ["can_read"],
            "Plugins":          ["can_read"],
            "Providers":        ["can_read"],
            "Datasets":         ["can_read"],
            "Cluster Activity": ["can_read"],
            "Users":            ["can_read"],
            "Roles":            ["can_read"],
            "Admin":            ["menu_access"],
            "Browse":           ["menu_access"],
            # NINGUN can_create, can_edit ni can_delete. Es lo que hace que la
            # auditoria sea independiente.
        },
    },
}


# =============================================================================
# INTERRUPTOR DE LA DECISION PENDIENTE
# =============================================================================
# El diseno deja al ingeniero SIN acceso a Connections. Ponerlo en True le da
# lectura: podra ver servidor, usuario y esquema para diagnosticar.
#
# El costo: tambien vera el campo "extra" completo, donde suelen terminar
# tokens y claves. Si lo activa, establezca la regla de que nada sensible va
# en "extra".
PERMITIR_CONEXIONES_A_INGENIERO = False

if PERMITIR_CONEXIONES_A_INGENIERO:
    ROLES["BSG_IngenieroDatos"]["permisos"]["Connections"] = ["can_read"]
    ROLES["BSG_IngenieroDatos"]["permisos"]["Admin"] = ["menu_access"]


# =============================================================================
# SEGREGACION DE FUNCIONES
# =============================================================================
# Combinaciones que el DAG de auditoria marcara como incumplimiento.
COMBINACIONES_PROHIBIDAS: list[tuple[str, str, str]] = [
    ("BSG_Administrador", "BSG_IngenieroDatos",
     "Quien construye procesos no debe concederse permisos a si mismo"),
    ("BSG_CustodioCredenciales", "BSG_IngenieroDatos",
     "Anula la separacion entre gestion de credenciales y desarrollo"),
    ("BSG_Auditor", "BSG_Administrador",
     "La auditoria pierde independencia"),
    ("BSG_Auditor", "BSG_IngenieroDatos",
     "La auditoria pierde independencia"),
    ("BSG_Auditor", "BSG_Operador",
     "La auditoria pierde independencia"),
    ("BSG_Auditor", "BSG_CustodioCredenciales",
     "La auditoria pierde independencia"),
]

# Limite de personas por rol. 0 = sin limite.
LIMITES_POR_ROL: dict[str, int] = {
    "BSG_Administrador": 2,
    "BSG_CustodioCredenciales": 2,
}


def resumen() -> str:
    """Texto de una linea por rol, para bitacoras e informes."""
    lineas = []
    for nombre, cfg in ROLES.items():
        n_recursos = len(cfg["permisos"])
        n_permisos = sum(len(a) for a in cfg["permisos"].values())
        lineas.append(f"{nombre:28} {n_recursos:2} recursos, {n_permisos:3} permisos")
    return "\n".join(lineas)


if __name__ == "__main__":
    print("MATRIZ DE ROLES")
    print("=" * 60)
    print(resumen())
    print()
    print(f"Conexiones visibles para el ingeniero: {PERMITIR_CONEXIONES_A_INGENIERO}")
    print(f"Combinaciones prohibidas definidas:    {len(COMBINACIONES_PROHIBIDAS)}")
