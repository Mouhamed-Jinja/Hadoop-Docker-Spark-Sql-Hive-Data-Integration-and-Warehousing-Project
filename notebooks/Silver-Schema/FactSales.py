from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Employee") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()


spark.sql("use bronze;")

bronze_Fact= spark.sql("select * from bronze.FactSales")

bronze_Fact =bronze_Fact.drop(col("Comment"),col("CreditCardApprovalCode"))\
    .where("EmployeeKey is not null")\
    .where("PurchaseOrderNumber is not null")\
    .where("CarrierTrackingNumber is not null")

bronze_Fact.write.format("hive").mode("overwrite").saveAsTable("silver.FactSales")
print("___________written____________")