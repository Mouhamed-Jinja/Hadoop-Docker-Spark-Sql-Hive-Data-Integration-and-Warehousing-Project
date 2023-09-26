from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("SampleDataToHive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()


orderDate =spark.read.format("jdbc")\
        .option("url", "jdbc:sqlserver://172.18.0.7:1433;databaseName=AdventureWorks2017")\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("dbtable", "[Sales].[SalesOrderHeader]")\
        .option("user","sa")\
        .option("password", "Mo*012105")\
        .load()


order_dates = orderDate.select("OrderDate").distinct()

DimDate = order_dates.select(
    col("OrderDate").alias("DateKey"),
    date_format("OrderDate", "yyyy-MM-dd").alias("Date"),
    date_format("OrderDate", "yyyy").alias("Year"),
    date_format("OrderDate", "MM").alias("Month"),
    date_format("OrderDate", "dd").alias("Day"),
    quarter("OrderDate").alias("Quarter"))\
    .repartition(4, "DateKey")


DimDate.write.format("hive").mode("overwrite").saveAsTable("bronze.DimDate")
print("*****************writing Have Done ***************")
DimDate.show(5)