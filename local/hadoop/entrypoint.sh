#!/usr/bin/env bash
set -euo pipefail

case "${1:-client}" in
  namenode)
    if [ ! -d /hadoop/nn/current ]; then
      hdfs namenode -format -nonInteractive -clusterId instacart
    fi
    exec hdfs namenode ;;
  datanode) exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager) exec yarn nodemanager ;;
  historyserver) exec mapred historyserver ;;
  sparkhistory) exec spark-class org.apache.spark.deploy.history.HistoryServer ;;
  client) exec sleep infinity ;;
  *) exec "$@" ;;
esac
