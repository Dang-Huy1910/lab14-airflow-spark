# Custom Airflow Image with Spark 3.4.1 Client & Java 17 for SparkSubmitOperator
FROM apache/airflow:2.7.3-python3.10

USER root

# Copy Java 17, Spark 3.4.1 binaries, and ps utility from Bitnami Spark image
COPY --from=bitnamilegacy/spark:3.4.1 /opt/bitnami/java /opt/java
COPY --from=bitnamilegacy/spark:3.4.1 /opt/bitnami/spark /opt/spark
COPY --from=bitnamilegacy/spark:3.4.1 /bin/ps /bin/ps
COPY --from=bitnamilegacy/spark:3.4.1 /lib/x86_64-linux-gnu/libprocps.so.8* /lib/x86_64-linux-gnu/

# Configure Environment Variables
ENV JAVA_HOME=/opt/java
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

# Setup directory for JDBC drivers and permissions
RUN mkdir -p /opt/spark/jars /opt/airflow/jars /opt/airflow/data /opt/airflow/spark_jobs && \
    chown -R airflow:root /opt/spark /opt/airflow /opt/java

COPY --chown=airflow:root jars/postgresql-42.6.0.jar /opt/spark/jars/postgresql-42.6.0.jar
COPY --chown=airflow:root jars/postgresql-42.6.0.jar /opt/airflow/jars/postgresql-42.6.0.jar

USER airflow

# Install Spark Provider for Airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r /requirements.txt
