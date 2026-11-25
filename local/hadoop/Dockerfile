FROM eclipse-temurin:11-jre-jammy

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.3

ENV HADOOP_HOME=/opt/hadoop \
    SPARK_HOME=/opt/spark \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl python3 python3-pip procps \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir psutil==5.9.8

# Apache's checksum files have used more than one text format. Extract the
# published SHA-512 digest, then verify the archive with sha512sum itself.
RUN set -eux; \
    cd /tmp; \
    curl --retry 3 --retry-all-errors -fsSLo hadoop.tar.gz \
      "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"; \
    curl --retry 3 --retry-all-errors -fsSLo hadoop.tar.gz.sha512 \
      "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz.sha512"; \
    digest="$(sed -E -n 's/.*([0-9a-fA-F]{128}).*/\1/p' hadoop.tar.gz.sha512 | head -n 1)"; \
    test "${#digest}" -eq 128; \
    printf '%s  %s\n' "$digest" hadoop.tar.gz | sha512sum --check --status; \
    tar -xzf hadoop.tar.gz -C /opt; \
    mv "/opt/hadoop-${HADOOP_VERSION}" "$HADOOP_HOME"; \
    curl --retry 3 --retry-all-errors -fsSLo spark.tgz \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"; \
    curl --retry 3 --retry-all-errors -fsSLo spark.tgz.sha512 \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512"; \
    digest="$(sed -E -n 's/.*([0-9a-fA-F]{128}).*/\1/p' spark.tgz.sha512 | head -n 1)"; \
    test "${#digest}" -eq 128; \
    printf '%s  %s\n' "$digest" spark.tgz | sha512sum --check --status; \
    tar -xzf spark.tgz -C /opt; \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" "$SPARK_HOME"; \
    rm -f /tmp/hadoop.tar.gz* /tmp/spark.tgz*

COPY conf/*.xml /opt/hadoop/etc/hadoop/
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod 0755 /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
