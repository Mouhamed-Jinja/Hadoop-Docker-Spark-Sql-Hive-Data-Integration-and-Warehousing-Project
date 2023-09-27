import sys

# Add the path to the parent directory to sys.path
sys.path.append('../')

# Now you can import the module from the Config.sparkConfig package
from Config.sparkConfig import get_spark_session

# Remove the added path if needed (optional)
sys.path.remove('../')
from pyspark.sql.functions import *
from Config.sparkConfig import get_spark_session
from Config.sqlServerConnetor import sql_connector
spark = get_spark_session("DimDate")

Tables = ["[Sales].[SalesOrderHeader]"]

dataFrames =sql_connector(Tables)
orderDate = dataFrames["[Sales].[SalesOrderHeader]"]

order_dates = orderDate.select("OrderDate").distinct()
DimDate = order_dates.select(
    col("OrderDate").alias("DateKey"),
    date_format("OrderDate", "yyyy-MM-dd").alias("Date"),
    date_format("OrderDate", "yyyy").alias("Year"),
    date_format("OrderDate", "MM").alias("Month"),
    date_format("OrderDate", "dd").alias("Day"),
    quarter("OrderDate").alias("Quarter"))\
    .repartition(4, "DateKey")

#spark.sql("drop table bronze.dimdate;")
DimDate.write.format("hive").mode("overwrite").saveAsTable("bronze.DimDate")
print("*****************writing Have Done ***************")
DimDate.show(5)