# Image officielle Apache Spark 3.5.9 (Java 17, Python 3.10) + connecteur Cassandra 3.5.1 (supporte Cassandra 5.0).
# Le JAR est copié dans /opt/spark/jars : l'utilisateur « spark » de l'image n'a pas de répertoire personnel,
# donc --packages (cache Ivy dans ~/.ivy2) échouerait.
FROM spark:3.5.9-scala2.12-java17-python3-ubuntu
USER root
RUN pip3 install --no-cache-dir numpy==1.26.4 \
 && wget -q -O /opt/spark/jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
 && chmod 644 /opt/spark/jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
 && mkdir -p /opt/spark/conf /opt/app/spark /opt/app/results
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PATH="/opt/spark/bin:${PATH}" PYTHONUNBUFFERED=1
WORKDIR /opt/app/spark
