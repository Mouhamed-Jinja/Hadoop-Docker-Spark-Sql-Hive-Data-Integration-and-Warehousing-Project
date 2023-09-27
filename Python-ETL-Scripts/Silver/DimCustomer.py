from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Employee") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("use bronze")
print("_________________________Bronze Schema is Used____________________________")

bronze_customer = spark.sql("select * from bronze.dimcustomer")

bronze_customer= bronze_customer\
            .withColumn("Address", array(col("AddressLine1"), col("AddressLine2")))\
            .drop(col("AdditionalContactInfo"), col("Suffix"), col("Title"), col("AddressLine1"), col("AddressLine2"))


bronze_customer.write.format("hive").mode("overwrite").saveAsTable("silver.DimCustomer")
print("_________________________DimCustomer Has Been Written____________________________")
