from pyspark.sql.functions import *
from ConfigAndConnectors.sparkConfig import get_spark_session
from ConfigAndConnectors.hiveConnector import Write_In_Hive_Schema
import SQL_QueriesAndAttributes.queries as query 

spark= get_spark_session("silver-Stage")
spark.sql("USE bronze")

#create Customer dimension by joining transcational tables, and selected the desired columns.
DimCustomer =spark.sql(query.join_customer_tables)
DimCustomer =DimCustomer.withColumn("CustomerName", DimCustomer["EmployeeName"])\
    .select(query.customer_columns)\
    .repartition(4, "CustomerID")
DimCustomer.createOrReplaceTempView("DimCustomer")
    
    
DimEmployee =spark.sql(query.join_employee_tables)
DimEmployee =DimEmployee.withColumn('EmployeeID', DimEmployee['e.BusinessEntityID'])\
    .select(query.employee_attributes)\
    .repartition(4, "EmployeeID")
DimEmployee.createOrReplaceTempView("DimEmployee")


DimProduct =spark.sql(query.join_product_tables)\
    .select(query.product_attributes)\
    .repartition(2, "ProductID")
DimProduct.createOrReplaceTempView("DimProduct")


FactSales =spark.sql(query.join_fact_tables)
FactSales =FactSales.withColumn("CustomerKey", FactSales["CustomerID"]) \
    .withColumn("EmployeeKey", FactSales["SalesPersonID"])\
    .withColumn("ProductKey", FactSales["ProductID"])\
    .select(query.fact_attributes)\
    .repartition(4,'SalesOrderID', 'CustomerKey', 'EmployeeKey', 'ProductKey')
FactSales.createOrReplaceTempView("FactSales")


DimDate = spark.sql(query.Date_table)
DimDate = DimDate.repartition(4, "dateKey")
DimDate.createOrReplaceTempView("DimDate")

sliver_stage_tables =["DimCustomer", "DimEmployee", "DimProduct", "FactSales", "DimDate"]
schemaName= "silver"

#Writing Silver DWH
Write_In_Hive_Schema(schema="silver", TablesNamesList=sliver_stage_tables)
