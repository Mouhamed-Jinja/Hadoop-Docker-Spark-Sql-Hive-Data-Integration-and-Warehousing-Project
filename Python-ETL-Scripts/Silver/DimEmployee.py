from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Employee") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("use bronze")
print("_______Bronze Schema is used________")

bronze_Employee = spark.sql("select * from bronze.dimemployee")

bronze_Employee= bronze_Employee\
            .withColumn("Address", array(col("AddressLine1"), col("AddressLine2")))\
            .drop(col("AdditionalContactInfo"), col("Suffix"), col("Title"), col("AddressLine1"), col("AddressLine2"))

bronze_Employee.createOrReplaceTempView("sEmp")
bronze_Employee = spark.sql("""
    select *
    from semp
    where OrganizationNode is not null or OrganizationLevel is not null
""")

print("________Has Been finished from Cleaning_______")


bronze_Employee.write.format("hive").mode("overwrite").saveAsTable("silver.DimEmployee")
print("____________written_____________")

