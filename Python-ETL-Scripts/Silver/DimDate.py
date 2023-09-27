from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Employee") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("use bronze")
print("we use Bronze Schema now")

bronze_Date = spark.sql("select * from bronze.dimdate")

spark.sql("use silver;")
print("using silver schema to wrote dimDate")
bronze_Date.write.format("hive").mode("overwrite").saveAsTable("silver.dimdate")

print("___________________dim date has been written.______________________")