from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport()\
    .getOrCreate()


spark.sql("use silver")
spark.sql("show tables").show()

Emp_Sales_performace =spark.sql("""
  SELECT
    e.EmployeeID,
    e.EmployeeName,
    SUM(fs.TotalDue) AS TotalSales,
    cast(AVG(fs.TotalDue) AS DECIMAL(10,4)) as AvgOrderValue,
    COUNT(DISTINCT fs.SalesOrderID) AS TotalOrders 
    FROM FactSales fs
    
    inner join dimemployee e
    ON fs.EmployeeKey = e.EmployeeID
    
    GROUP BY e.EmployeeID, e.EmployeeName
""")

Emp_Sales_performace.write.format("hive").mode("overwrite").saveAsTable("gold.Emp_Sales_performace")
print("-------------------------------> Writing have done!")
Emp_Sales_performace.orderBy("TotalOrders",ascending =False).show()