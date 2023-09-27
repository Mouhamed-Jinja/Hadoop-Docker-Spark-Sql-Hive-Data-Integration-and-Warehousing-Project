from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport()\
    .getOrCreate()


spark.sql("use silver")
spark.sql("show tables").show()

profit =spark.sql("""
  SELECT
    fs.*,
    (fs.SubTotal - fs.TaxAmt - fs.Freight) AS Profit
  FROM
    FactSales fs
""")
profit =profit.select("SalesOrderID","CustomerKey", "EmployeeKey", "ProductKey","profit")

profit.write.format("hive").mode("overwrite").saveAsTable("gold.profit")

spark.sql("USE gold;")
spark.sql("select * from profit limit 5;").show()
print("Done!!")
