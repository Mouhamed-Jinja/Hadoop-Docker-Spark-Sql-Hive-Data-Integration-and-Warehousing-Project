from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport()\
    .getOrCreate()
    
spark.sql("use silver")

profit2 =spark.sql("""
  SELECT
    fs.*,
    (fs.SubTotal - fs.TaxAmt - fs.Freight) AS Profit
  FROM
    FactSales fs
""")
profit2.select("SalesOrderID","CustomerKey", "EmployeeKey", "ProductKey","profit").show(5)
profit2.write.format("hive").mode("overwrite").saveAsTable("gold.profit")
