from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Employee") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("use bronze")
print("________________________Bronze Schema is used___________________________")

bronze_Product= spark.sql("select * from bronze.dimproduct")
bronze_Product =bronze_Product.drop(col("DiscontinuedDate"))

bronze_Product.write.format("hive").mode("overwrite").saveAsTable("silver.DimProduct")
print("________________________DimProduct has been written___________________________")
