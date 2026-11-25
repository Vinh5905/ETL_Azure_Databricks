# Local Hadoop, YARN and Spark

This Compose project provides one NameNode, two DataNodes, a YARN ResourceManager and NodeManager, the Hadoop JobHistory server, a Spark History server, and an idle client container. Every role uses the same Java 11, Hadoop 3.3.6, Spark 3.5.3, and Python 3 image.

## Host requirements

Use Docker Desktop with the Linux container backend and allocate at least **8 CPUs and 16 GB of memory**. The NodeManager reserves 12 GB for YARN and its Docker container is limited to 14 GB, leaving memory for the other services and the host. Reduce the Compose CPU limit and the YARN vcore setting together if the machine has fewer than 8 CPUs; benchmark results then must use the same core ceiling.

## Start and smoke test

From the repository root:

```powershell
docker compose -f local/hadoop/docker-compose.yml up -d --build
docker compose -f local/hadoop/docker-compose.yml exec client hdfs dfs -mkdir -p /spark-logs /ds
docker compose -f local/hadoop/docker-compose.yml exec client hdfs dfsadmin -report
docker compose -f local/hadoop/docker-compose.yml exec client yarn node -list
docker compose -f local/hadoop/docker-compose.yml exec client hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar pi 4 1000
```

The HDFS report should list two live DataNodes. YARN should list one NodeManager with 8 vcores. The final command runs Hadoop's example `pi` MapReduce job. Spark on YARN can then be submitted from `client`; Spark event logs are written to `/spark-logs` for the history server.

| Service | Local URL |
|---|---|
| HDFS NameNode UI | <http://localhost:9870> |
| YARN ResourceManager UI | <http://localhost:8088> |
| Hadoop JobHistory UI | <http://localhost:19888> |
| Spark History UI | <http://localhost:18080> |

The host data mount is read-only at `/data/ds`; HDFS named volumes keep NameNode and DataNode state across container restarts. The image verifies the Apache archive SHA-512 checksums before extraction.
