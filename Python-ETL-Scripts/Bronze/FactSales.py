from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("FactSales") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()


Tables = ["[Sales].[SalesOrderHeader]", "[Sales].[SalesOrderDetail]", "[Sales].[Customer]", "[Production].[Product]", "[HumanResources].[Employee]"] 
print(len(Tables), " tables")

dataFrames= {}
for table in Tables:
    query = f"select * from {table}"
    df =spark.read.format("jdbc")\
        .option("url", "jdbc:sqlserver://172.18.0.7:1433;databaseName=AdventureWorks2017")\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("dbtable", f"({query}) as temp")\
        .option("user","sa")\
        .option("password", "Mo*012105")\
        .load()
    dataFrames[table] = df
print(dataFrames.keys())


SalesOrderHeader = dataFrames["[Sales].[SalesOrderHeader]"]\
    .select(
        'SalesOrderID',
         'RevisionNumber',
         'OrderDate',
         'DueDate',
         'ShipDate',
         'Status',
         'OnlineOrderFlag',
         'SalesOrderNumber',
         'PurchaseOrderNumber',
         'AccountNumber',
         'CustomerID',
         'SalesPersonID',
         'TerritoryID',
         'CreditCardApprovalCode',
         'SubTotal',
         'TaxAmt',
         'Freight',
         'TotalDue',
         'Comment')\
    .repartition(4, 'SalesOrderID', 'CustomerID', 'SalesPersonID')\
    .cache()
SalesOrderHeader.createOrReplaceTempView("SalesOrderHeader")


SalesOrderDetail = dataFrames["[Sales].[SalesOrderDetail]"]\
    .select(
        'SalesOrderID',
         'SalesOrderDetailID',
         'CarrierTrackingNumber',
         'OrderQty',
         'ProductID',
         'UnitPrice',
         'UnitPriceDiscount',
         'LineTotal')\
    .repartition(4,'SalesOrderID', 'SalesOrderDetailID','ProductID')\
    .cache()
SalesOrderDetail.createOrReplaceTempView("SalesOrderDetail")

customer = dataFrames['[Sales].[Customer]']\
    .select('CustomerID',
             'PersonID',
             'StoreID',
             'TerritoryID',
             'AccountNumber')\
    .repartition(4, "CustomerID")\
    .cache()
customer.createOrReplaceTempView("customer")


emp = dataFrames["[HumanResources].[Employee]"]\
    .select('BusinessEntityID')\
    .repartition(4, "BusinessEntityID").cache()
emp.createOrReplaceTempView("emp")


product = dataFrames["[Production].[Product]"]\
    .select('ProductID',
         expr('Name').alias("productName")
           )\
    .repartition(4,'ProductID')\
    .cache()
product.createOrReplaceTempView("product")


print("_____________ Start Joining ____________________")

FactSales= spark.sql("""
    select soh.*, sod.*
    from SalesOrderHeader soh inner join SalesOrderDetail sod
    on soh.SalesOrderID = sod.SalesOrderID
    
    inner join product p
    on sod.ProductID = p.ProductID
    
    inner join customer c
    on soh.CustomerID = c.CustomerID
    
    left outer join emp 
   on soh.SalesPersonID = emp.BusinessEntityID

""")
FactSales = FactSales.select(
             'soh.SalesOrderID',
             expr('CustomerID').alias("CustomerKey"),
             expr('SalesPersonID').alias("EmployeeKey"),
             expr('ProductID').alias("ProductKey"),
             'RevisionNumber',
             'OrderQty',
             'UnitPrice',
             'UnitPriceDiscount',
             'SubTotal',
             'TaxAmt',
             'Freight',
             'TotalDue',
             'OrderDate',
             'DueDate',
             'ShipDate',
             'Status',
             'OnlineOrderFlag',
             'SalesOrderNumber',
             'PurchaseOrderNumber',
             'AccountNumber',
             'CreditCardApprovalCode',
             'Comment',
             'CarrierTrackingNumber',
             'LineTotal')\
            .repartition(4,'SalesOrderID', 'CustomerKey', 'EmployeeKey', 'ProductKey')\
            .cache()

FactSales.write.format("hive").mode("overwrite").saveAsTable("bronze.FactSales")

print("**************** Done ******************")
spark.sql("use bronze;")
spark.sql("show tables").show()