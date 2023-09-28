from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport()\
    .getOrCreate()


spark.sql("use silver")
spark.sql("show tables").show()

Customer_Lifetime_Value=spark.sql("""
  SELECT
    fs.CustomerKey,
    SUM(fs.TotalDue) AS TotalRevenue,
    COUNT(DISTINCT fs.SalesOrderID) AS NumberOfOrders,
    cast((SUM(fs.TotalDue) / COUNT(DISTINCT fs.SalesOrderID)) as decimal(10,4)) as CLV
  FROM
    FactSales fs
  GROUP BY
    fs.CustomerKey
""")

Customer_Lifetime_Value.write.format("hive").mode("overwrite").saveAsTable("gold.Customer_Lifetime_Value")
print("-------------------------> Writing Done")
Customer_Lifetime_Value.orderBy("CLV", ascending=False).show(truncate=False)