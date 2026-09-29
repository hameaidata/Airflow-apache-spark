"""Pipeline BT2SQL: Bantotal (IBM i) -> Parquet -> STG en SQL Server.

Todo lo de este pipeline lleva el prefijo bt2sql. Ver bt2sql_comun.py para el
mapa completo de nombres (DAG, Variables, tablas de control y rutas).

Este paquete NO debe definir DAGs ni ejecutar nada al importarse:
airflow/dags/.airflowignore lo excluye del DagBag justamente para que el
scheduler no lo parsee cada 30 segundos. Quien define el DAG es
dag_bt2sql_stg.py, que importa estos modulos DENTRO de sus funciones.
"""
