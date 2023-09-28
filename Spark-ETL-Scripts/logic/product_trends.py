from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport()\
    .getOrCreate()


spark.sql("use silver;")
spark.sql("show tables").show()

product_trends =spark.sql("""
  SELECT
    p.ProductID,
    p.productName,
    d.Year,
    d.Month,
    SUM(fs.TotalDue) AS MonthlySales
  FROM FactSales fs

  inner join dimproduct p
  ON fs.ProductKey = p.ProductID
  
  inner join dimdate d
  ON fs.OrderDate = d.DateKey
  
  GROUP BY
    p.ProductID, p.productName, d.Year, d.Month
    
  ORDER BY
    p.ProductID, d.Year, d.Month
""")


product_trends.write.format("hive").mode("overwrite").saveAsTable("gold.product_trends")
print("-------------------------------> Writing have done!")
spark.sql("select * from gold.product_trends limit 5;").show()
