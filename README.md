# Docker Hadoop Workbench

A Hadoop cluster based on Docker, including Hive and Spark.

## Introduction
This repository uses [Docker Compose](https://docs.docker.com/compose/) to initialize a Hadoop cluster including the following:

- Hadoop
- Hive
- Spark

Please note that this project is built on top of [Big Data Europe](https://github.com/big-data-europe) works. Please check their [Docker Hub](https://hub.docker.com/u/bde2020/) for latest images.

This project is based on the following Docker versions:
```
Client:
 Version:           20.10.2
Server: Docker Engine - Community
 Engine:
  Version:          20.10.6
docker-compose version 1.29.1, build c34c88b2
```

## Quick Start

To start the cluster simply run:

```
./start_demo.sh
```

Alternatively, you can use `v2`, which is built on top of my own `spark-master` and `spark-history-server`:

```
./start_demo_v2.sh
```

You can stop the cluster using `./stop_demo.sh` or `./stop_demo_v2.sh`. Also you can modify `DOCKER_COMPOSE_FILE` in `start_demo.sh` and `stop_demo.sh` to use other YAML files.

## Interfaces

- Namenode: http://localhost:9870/dfshealth.html#tab-overview
- Datanode: http://localhost:9864/
- ResourceManager: http://localhost:8088/cluster
- NodeManager: http://localhost:8042/node
- HistoryServer: http://localhost:8188/applicationhistory
- HiveServer2: http://localhost:10002/
- Spark Master: http://localhost:8080/
- Spark Worker: http://localhost:8081/
- Spark Job WebUI: http://localhost:4040/ (only works when Spark job is running on `spark-master`)
- Presto WebUI: http://localhost:8090/
- Spark History Server：http://
localhost:18080/

## Connections

Use `hdfs dfs` to connect to `hdfs://localhost:9000/` (Please make sure you have [Hadoop](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) installed first):

```
hdfs dfs -ls hdfs://localhost:9000/
```

Use Beeline to connect to HiveServer2 (Please make sure you have [Hive](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Installation) installed first):

```
beeline -u jdbc:hive2://localhost:10000/gold -n hive -p hive
```

- To submit spark job you can use:
```
spark-submit --jars /Drivers/SQL_Sever/jdbc/sqljdbc42.jar spark_job.py
```

Use `spark-shell` to connect to Hive Metastore via thrift protocol (Please make sure you have [Spark](https://spark.apache.org/downloads.html) installed first):

```
$ spark-shell

      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.1.2
      /_/

Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 11.0.11)

scala> :paste
// Entering paste mode (ctrl-D to finish)

import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.master("local").config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport.appName("thrift-test").getOrCreate

spark.sql("show databases").show


// Exiting paste mode, now interpreting.

+---------+
|namespace|
+---------+
|  default|
+---------+

import org.apache.spark.sql.SparkSession
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@1223467f
```

Use [Presto CLI](https://prestodb.io/docs/current/installation/cli.html) to connect to Presto and query Hive data:

```bash
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.255/presto-cli-0.255-executable.jar
mv presto-cli-0.255-executable.jar presto
chmod +x presto
./presto --server localhost:8090 --catalog hive --schema default
```

## Run MapReduce Job `WordCount`

This part is based on [Big Data Europe's Hadoop Docker](https://github.com/big-data-europe/docker-hadoop) project.

First run `hadoop-base` as a helper container:
```bash
docker run -d --network hadoop --env-file hadoop.env --name hadoop-base bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8 tail -f /dev/null
```

Then run the following:
```bash
docker exec -it hadoop-base hdfs dfs -mkdir -p /input/
docker exec -it hadoop-base hdfs dfs -copyFromLocal -f /opt/hadoop-3.2.1/README.txt /input/
docker exec -it hadoop-base mkdir jars
docker cp jars/WordCount.jar hadoop-base:jars/WordCount.jar
docker exec -it hadoop-base /bin/bash 
hadoop jar jars/WordCount.jar WordCount /input /output
```

You should be able to see the job in http://localhost:8088/cluster/apps and http://localhost:8188/applicationhistory (when finished).

After the job is finished, check the result:
```bash
hdfs dfs -cat /output/*
```

Then type `exit` to exit the container.

## Run Hive Job

Make sure `hadoop-base` is running. Then prepare the data:

```bash
docker exec -it hadoop-base hdfs dfs -mkdir -p /test/
docker exec -it hadoop-base mkdir test
docker cp data hadoop-base:test/data
docker exec -it hadoop-base /bin/bash
hdfs dfs -put test/data/* /test/
hdfs dfs -ls /test
exit
```

Then create the table:
```bash
docker cp scripts/hive-beers.q hive-server:hive-beers.q
docker exec -it hive-server /bin/bash
cd /
hive -f hive-beers.q
exit
```

Then play with data using Beeline:
```
beeline -u jdbc:hive2://localhost:10000/test -n hive -p hive

0: jdbc:hive2://localhost:10000/test> select count(*) from beers;
```

You should be able to see the job in http://localhost:8088/cluster/apps and http://localhost:8188/applicationhistory (when finished).

## Run Spark Shell

Make sure you have prepared the data and created the table in the previous step.

```
docker exec -it spark-master spark/bin/spark-shell

scala> spark.sql("show databases").show
+---------+
|namespace|
+---------+
|  default|
|     test|
+---------+

scala> val df = spark.sql("select * from test.beers")
df: org.apache.spark.sql.DataFrame = [id: int, brewery_id: int ... 11 more fields]

scala> df.count
res0: Long = 7822
```

You should be able to see the Spark Shell session in http://localhost:8080/ and your job in http://localhost:4040/jobs/.

If you encounter the following warning when running `spark-shell`:
```
WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
```
Please check the logs of `spark-master` using `docker logs -f spark-master`. If the following exists, please restart your `spark-worker` using `docker-compose restart spark-worker`:
```
WARN Master: Got heartbeat from unregistered worker worker-20210622022950-xxx.xx.xx.xx-xxxxx. This worker was never registered, so ignoring the heartbeat.
```

Similarly, to run `spark-sql`, use `docker exec -it spark-master spark/bin/spark-sql`.

## Run Spark Submit

```bash
docker exec -it spark-master /spark/bin/spark-submit --class org.apache.spark.examples.SparkPi /spark/examples/jars/spark-examples_2.12-3.1.1.jar 100
```

You should be able to see Spark Pi in http://localhost:8080/ and your job in http://localhost:4040/jobs/.

## Configuration Files

Some configuration file locations are listed below. The non-empty configuration files are also copied to `conf` folder for future reference.

- `namenode`:
  - `/etc/hadoop/core-site.xml` CORE_CONF
  - `/etc/hadoop/hdfs-site.xml` HDFS_CONF
  - `/etc/hadoop/yarn-site.xml` YARN_CONF
  - `/etc/hadoop/httpfs-site.xml` HTTPFS_CONF
  - `/etc/hadoop/kms-site.xml` KMS_CONF
  - `/etc/hadoop/mapred-site.xml` MAPRED_CONF
- `hive-server`:
  - `/opt/hive/hive-site.xml` HIVE_CONF
