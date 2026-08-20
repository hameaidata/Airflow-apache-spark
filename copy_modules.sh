#!/bin/bash
# Este script recibe los archivos desde Claude y los copia al lugar correcto

# Crear estructura de carpetas completa
cd "$HOME/mnt/airflow-spark"

# Crear __init__.py en todos los módulos
find airflow/plugins -type d | while read dir; do
  touch "$dir/__init__.py"
done

echo "✅ Estructura de carpetas lista"
