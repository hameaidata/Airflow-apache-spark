"""Pipeline S2SQL: SingleStore -> Parquet -> SQL Server 2022.

Todo lo de este pipeline lleva el prefijo s2sql. Ver s2sql_comun.py para el
mapa completo de nombres (DAG, Variable, tablas de control y rutas).

Este paquete NO debe definir DAGs ni ejecutar nada al importarse: airflow/dags/
.airflowignore lo excluye del DagBag justamente para que el scheduler no lo
parsee cada 30 segundos. Quien define el DAG es dag_s2sql_export.py.
"""
