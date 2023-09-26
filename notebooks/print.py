from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sql("use gold")
spark.sql("show tables;").show()
spark.sql("drop table profit1;")
spark.sql("show tables;").show()