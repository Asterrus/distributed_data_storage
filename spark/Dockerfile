FROM apache/spark:4.0.1-python3

USER root
WORKDIR /opt/spark

RUN rm -rf \
    /opt/spark/examples \
    /opt/spark/data \
    /opt/spark/R \
    /opt/spark/python/docs \
    /opt/spark/python/test_support

RUN find /opt/spark/jars -name "*hive*" -delete

RUN find /opt/spark/jars -name "*-tests.jar" -delete && \
    find /opt/spark/jars -name "*-sources.jar" -delete && \
    find /opt/spark/jars -name "*-javadoc.jar" -delete && \
    find /opt/spark/jars -name "*-hadoop-yarn*.jar" -delete && \
    find /opt/spark/jars -name "*-hadoop-mapreduce*.jar" -delete && \
    find /opt/spark/jars -name "*-hadoop-hdfs*.jar" -delete && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ARG POSTGRES_JDBC_VERSION=42.7.7

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/spark/jars/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    psycopg2-binary==2.9.11 \
    pyarrow==22.0.0 \
    pandas==2.3.3 \
    python-dotenv>=1.2.1 && \
    rm -rf /root/.cache

RUN chown -R spark:spark /opt/spark/jars
RUN mkdir -p /opt/spark/checkpoints && \
    chown -R spark:spark /opt/spark/checkpoints
USER spark
WORKDIR /opt/spark
