#!/usr/bin/env bash
set -euo pipefail
for scale in 10 25 50 100; do
  hdfs dfs -mkdir -p "/ds/v1/product/scale=$scale"
  hdfs dfs -D dfs.blocksize=8m -put -f "/data/ds/v1/product/scale=$scale/"*.txt "/ds/v1/product/scale=$scale/"
done
hdfs dfs -mkdir -p /ds/v1/product/scale=200
for file in $(hdfs dfs -ls -C /ds/v1/product/scale=100/); do
  base="$(basename "$file")"
  hdfs dfs -D dfs.blocksize=8m -cp -f "$file" "/ds/v1/product/scale=200/a_$base"
  hdfs dfs -D dfs.blocksize=8m -cp -f "$file" "/ds/v1/product/scale=200/b_$base"
done
hdfs dfs -count -h /ds/v1/product/*
hdfs fsck /ds/v1/product/scale=100 -files -blocks -locations | tail -20
