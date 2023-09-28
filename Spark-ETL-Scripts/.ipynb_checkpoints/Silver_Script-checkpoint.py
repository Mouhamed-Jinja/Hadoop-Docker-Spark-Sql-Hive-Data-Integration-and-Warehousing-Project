from pyspark.sql.functions import *
from ConfigAndConnectors.sparkConfig import get_spark_session
from ConfigAndConnectors.hiveConnector import hive_connector
import SQL_QueriesAndAttributes.queries as query 

spark= get_spark_session("silver-Stage")
spark.sql("USE bronze")

#create Customer dimension by joining transcational tables, and selected the desired columns.
DimCustomer = spark.sql(query.join_customer_tables)
DimCustomer = DimCustomer.withColumn("CustomerName", DimCustomer["EmployeeName"])\
    .select(query.customer_columns)\
    .repartition(4, "CustomerID")

    
DimEmployee = spark.sql(query.join_employee_tables)
DimEmployee = DimEmployee.withColumn('EmployeeID', DimEmployee['e.BusinessEntityID'])\
    .select(query.employee_attributes)\
    .repartition(4, "EmployeeID")


DimProduct = spark.sql(query.join_product_tables)\
            .select(query.product_attributes)\
            .repartition(2, "ProductID")


FactSales= spark.sql(query.join_fact_tables)
FactSales =FactSales.withColumn("CustomerKey", FactSales["CustomerID"]) \
            .withColumn("EmployeeKey", FactSales["SalesPersonID"])\
            .withColumn("ProductKey", FactSales["ProductID"])\
            .select(query.fact_attributes)\
            .repartition(4,'SalesOrderID', 'CustomerKey', 'EmployeeKey', 'ProductKey')



DimDate = spark.sql(query.Date_table)
DimDate = DimDate.repartition(4, "dateKey")
DimDate.show(5)
sliver_stage_tables =[
    "DimCustomer", "DimEmployee", "DimProduct", "FactSales", "DimDate"
]
for table in sliver_stage_tables:
    print(table)
    df= spark.table(table)
    
"""
#Writing Silver DWH
for table_name in sliver_stage_tables:
    df = spark.table(table_name)
    hive_connector(dataFrame=df, tableName=table_name)
    print("___________writing_______________")

"""