# FROM apache/spark:4.0.1-python3

# USER root

# WORKDIR /opt/spark

# ARG POSTGRES_JDBC_VERSION
# ARG AWS_SDK_V2_VERSION=2.24.6

# # PostgreSQL JDBC
# ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar jars/

# # Hadoop S3A + AWS SDK
# ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar jars/
# ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar jars/
# ADD https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWS_SDK_V2_VERSION}/url-connection-client-${AWS_SDK_V2_VERSION}.jar jars/
# # Для потокового чтения
# ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar jars/
# ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar jars/
# ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar jars/
# ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar jars/

# RUN chown -R spark:spark jars

# RUN pip install --no-cache-dir \
#     psycopg2-binary==2.9.11 \
#     pyarrow==22.0.0 \
#     pandas==2.3.3 \
#     python-dotenv>=1.2.1 \
#     pyspark>=4.1.1

# USER spark
FROM apache/spark:4.0.1-python3

USER root
WORKDIR /opt/spark

# === 1. Удаляем ненужные директории ===
RUN rm -rf \
    /opt/spark/examples \
    /opt/spark/data \
    /opt/spark/R \
    /opt/spark/python/docs \
    /opt/spark/python/test_support

# === 2. КРИТИЧЕСКИ ВАЖНО: Удаляем ВСЕ Hive-зависимости ===
RUN find /opt/spark/jars -name "*hive*" -delete

# === 3. Удаляем другие ненужные JAR-файлы ===
RUN find /opt/spark/jars -name "*-tests.jar" -delete && \
    find /opt/spark/jars -name "*-sources.jar" -delete && \
    find /opt/spark/jars -name "*-javadoc.jar" -delete && \
    find /opt/spark/jars -name "*-hadoop-yarn*.jar" -delete && \
    find /opt/spark/jars -name "*-hadoop-mapreduce*.jar" -delete && \
    find /opt/spark/jars -name "*-hadoop-hdfs*.jar" -delete && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# === 4. Добавляем ТОЛЬКО нужные зависимости ===
ARG POSTGRES_JDBC_VERSION=42.7.7

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/spark/jars/

# === 5. Python зависимости ===
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    psycopg2-binary==2.9.11 \
    pyarrow==22.0.0 \
    pandas==2.3.3 \
    python-dotenv>=1.2.1 && \
    rm -rf /root/.cache

# === 6. Права доступа ===
RUN chown -R spark:spark /opt/spark/jars

USER spark
WORKDIR /opt/spark