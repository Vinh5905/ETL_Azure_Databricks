#!/usr/bin/env bash
set -euo pipefail

role="${1:-client}"
case "$role" in
  namenode)
    mkdir -p /hadoop/nn
    if [[ ! -d /hadoop/nn/current ]]; then
      hdfs namenode -format -nonInteractive -clusterId instacart
    fi
    exec hdfs namenode
    ;;
  datanode)
    mkdir -p /hadoop/dn
    exec hdfs datanode
    ;;
  resourcemanager)
    exec yarn resourcemanager
    ;;
  nodemanager)
    mkdir -p /hadoop/yarn/local /hadoop/yarn/logs
    exec yarn nodemanager
    ;;
  historyserver)
    exec mapred historyserver
    ;;
  sparkhistory)
    exec spark-class org.apache.spark.deploy.history.HistoryServer
    ;;
  client)
    exec sleep infinity
    ;;
  *)
    exec "$@"
    ;;
esac
