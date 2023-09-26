# Data Engineering Project
![Comp 1 (1)](https://github.com/Mouhamed-Jinja/Hadoop-Docker-Spark-Sql-Hive-Data-Integration-and-Warehousing-Project/assets/132110499/28f49870-fa31-4872-9fe5-7fe48de4fdcf)

A Hadoop cluster based on Docker, including SQL-Server, Jupyter, Hive, and Spark.

## Introduction
This repository uses [Docker Compose](https://docs.docker.com/compose/) to initialize a Hadoop cluster including the following:

- Hadoop
- SQL Server
- Jupyter
- Hive
- Spark

Please note that this project's Hadoop part is built on top of [Big Data Europe](https://github.com/big-data-europe) works. Please check their [Docker Hub](https://hub.docker.com/u/bde2020/) for the latest images.


## Quick Start

If you using Linux run below bash script,
And If you Windows user you can download git and use git bash in the build context of the project.
To start the cluster simply run:
```
./start_demo.sh
```
- Please make sure that you run this bash script because it copies the configuration file to the containers not just up the docker-compose.


You can stop the cluster using:
 `./stop_demo.sh` 

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
- Spark History Server：http://
localhost:18080/

## Connections

Use Beeline to connect to HiveServer2 (Please make sure you have [Hive](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Installation) installed first):

```
beeline -u jdbc:hive2://localhost:10000/default -n hive -p hive
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
- If you got an error that spak-shell not difined, you can use this code
```
docker exec -it spark-master spark/bin/spark-shell
```
if you got an error becuse can't find this dir :hdfs://namenode:9000/user/spark/applicationHistory
you can create it by using :
```
hdfs dfs -mkdir -p hdfs://namenode:9000/user/spark/applicationHistory
``` 


# Run Jobs
Open Jupyter logs and use Jupyter-pyspark to run the jobs.
- DimEmployee: create employee dimensions in side Hive DWH salesSchema,
  by ingesting from SQL Server
- DimCustomer
- DimProduct
- DimDate
- FactSales
You can add your databases in the build context file "DBs" and read it in the SQL server
to apply your transformation

note: please while you read the tables from the database, make sure that you use the right
SQL Server IP, you can inspect your sql server container and get the 




Then play with data using Beeline:
```
beeline -u jdbc:hive2://localhost:10000/default -n hive -p hive

0: jdbc:hive2://localhost:10000/default> show databases;
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

scala> spark.sql("USE brozeSalesSchema;")

scala> spark.sql("Show tables;").show()
```

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

## there isupdates?
yes, this project will be updated to add new data layers 
- SilverSalesScheam : by filtering and applying business rules on the data
- goldSalesSchema: to serve business logic and needs
# Hadoop-Docker-Spark-Sql-Hive-Data-Integration-and-Warehousing-Project
# Hadoop-Docker-Spark-Sql-Hive-Data-Integration-and-Warehousing-Project
# Hadoop-Docker-Spark-Sql-Hive-Data-Integration-and-Warehousing-Project
