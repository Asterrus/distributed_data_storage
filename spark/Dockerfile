FROM apache/spark:4.0.1-python3

USER root

WORKDIR /opt/spark

ARG POSTGRES_JDBC_VERSION
ARG AWS_SDK_V2_VERSION=2.24.6

# PostgreSQL JDBC
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar jars/

# Hadoop S3A + AWS SDK
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar jars/
ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar jars/
ADD https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWS_SDK_V2_VERSION}/url-connection-client-${AWS_SDK_V2_VERSION}.jar jars/

RUN chown -R spark:spark jars

RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.11 \
    pyarrow==22.0.0 \
    pandas==2.3.3 \
    python-dotenv>=1.2.1 \
    pyspark>=4.1.1

USER spark
