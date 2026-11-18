FROM eclipse-temurin:11-jre-jammy

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.3
ENV HADOOP_HOME=/opt/hadoop SPARK_HOME=/opt/spark \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
    PYSPARK_PYTHON=python3
ENV PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin"

RUN apt-get update && apt-get install -y --no-install-recommends curl python3 python3-pip procps \
    && rm -rf /var/lib/apt/lists/* && pip3 install --no-cache-dir psutil==5.9.8

RUN set -eux; cd /tmp; \
    curl -fsSLO "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"; \
    curl -fsSLO "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512"; \
    sha512sum -c "hadoop-${HADOOP_VERSION}.tar.gz.sha512"; \
    tar -xzf "hadoop-${HADOOP_VERSION}.tar.gz" -C /opt && mv "/opt/hadoop-${HADOOP_VERSION}" "$HADOOP_HOME"; \
    curl -fsSLO "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"; \
    curl -fsSLO "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512"; \
    sha512sum -c "spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512"; \
    tar -xzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" -C /opt && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "$SPARK_HOME"; \
    rm -f /tmp/*.tgz /tmp/*.tar.gz /tmp/*.sha512

COPY conf/*.xml "$HADOOP_HOME/etc/hadoop/"
COPY spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
