# airflow/jars/ — drivers JDBC propios

Todo `.jar` que se deje en esta carpeta se copia a `/opt/airflow/jars/` de la
imagen de Airflow **y** a `$SPARK_HOME/jars/` de la imagen de Spark, al
construirlas.

## Para qué es

Para los drivers que **no se pueden descargar** en el build:

- No están en Maven Central (drivers propietarios, entregados por el proveedor).
- La red corporativa bloquea `repo1.maven.org`.
- Hace falta fijar una versión distinta de la que descarga el Dockerfile.

Si el driver sí está en Maven Central y la red lo permite, es mejor agregarlo a
`JDBC_DRIVERS` en los dos Dockerfile: así queda versionado como texto y no como
un binario de varios MB dentro del repositorio.

## Precedencia: el jar local GANA

Los jars locales se copian **después** de la descarga. Si un archivo de aquí
tiene el mismo nombre que uno descargado, el local lo reemplaza.

Eso permite fijar una versión concreta sin editar el Dockerfile: se deja el
archivo con el nombre de destino que usa el catálogo y listo.

    /opt/airflow/jars/mssql-jdbc.jar    <- descargado 12.8.1.jre11
    airflow/jars/mssql-jdbc.jar         <- si existe, este es el que queda

## Cuidado con las versiones duplicadas

Dejar `jt400-11.2.jar` cuando el build ya descarga `jt400.jar` pone **dos
versiones del mismo driver** en el classpath. Cuál gana depende del orden del
classloader, que no está definido, así que el comportamiento puede cambiar
entre el driver y los executors sin motivo aparente.

El build lo detecta y avisa, pero no lo corrige: la decisión es suya. Si quiere
una versión concreta, use el mismo nombre de archivo que el descargado para
reemplazarlo, en vez de añadir un segundo archivo.

## Después de dejar un jar aquí

Hay que **reconstruir las dos imágenes**. No es un montaje: se copia dentro.

    docker build -t airflow-bsg:2.11.2 .
    docker build -t spark-bsg:3.5.3 -f spark/Dockerfile .

Y comprobar que llegó a las dos:

    docker compose -f docker-compose.windows.yml exec airflow-worker ls -la /opt/airflow/jars/
    docker compose -f docker-compose.windows.yml exec spark-worker   ls -la /opt/spark/jars/

## Nota sobre git

`.gitignore` no excluye `*.jar`, así que lo que deje aquí **se versiona**. Es lo
que se quiere para un driver propietario que hay que conservar, pero tenga
presente que un jar de 6 MB queda en el historial del repositorio para siempre.
