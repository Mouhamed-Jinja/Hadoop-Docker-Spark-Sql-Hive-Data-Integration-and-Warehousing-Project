from pyspark.sql.functions import *
from connectors.spark_session import get_spark_session
from connectors.sql_server_reader import sql_connector
from sql_server_selected_tables.sql_tables import Bronze_Stage_Tables
from connectors.hive_writer import Write_In_Hive_Schema

spark= get_spark_session("Bronze-Stage")
Tables = Bronze_Stage_Tables
dataFrames= sql_connector(Tables)

# _________Tables Needed for Customer Dimension__________
#columns which are selected that are needed only in the Bronze layer, because transactional tables have musch columns and foreign keys

customer = dataFrames['[Sales].[Customer]']\
    .select('CustomerID',
             'PersonID',
             'StoreID',
             'TerritoryID',
             'AccountNumber')\
    .repartition(4, "CustomerID")\
    .cache()
customer.createOrReplaceTempView("customer")

depthist = dataFrames["[HumanResources].[EmployeeDepartmentHistory]"]
depthist.createOrReplaceTempView("depthist")

#select the last department for each employee
depthist = spark.sql("""
select * from depthist
where EndDate is null
""")
depthist=depthist.select("BusinessEntityID", "DepartmentID")\
    .repartition("BusinessEntityID", "DepartmentID")\
    .cache()
depthist.createOrReplaceTempView("depthist")


dept = dataFrames["[HumanResources].[Department]"]\
    .select('DepartmentID', 'Name', 'GroupName')\
    .repartition(4,"DepartmentID")\
    .cache()
dept.createOrReplaceTempView("dept")


person= dataFrames['[Person].[Person]'].withColumn("EmployeeName", concat_ws(" ", "FirstName","LastName"))
person= person.select('BusinessEntityID',
     'EmployeeName',        
     'PersonType',
     'NameStyle',
     'Title',
     'Suffix',
     'EmailPromotion',
     'AdditionalContactInfo',
     'Demographics')\
    .repartition(4,"BusinessEntityID")\
    .cache()

person.createOrReplaceTempView("person")


address = dataFrames["[Person].[Address]"]\
    .select('AddressID',
         'AddressLine1',
         'AddressLine2',
         'City',
         'StateProvinceID',
         'PostalCode',
         'SpatialLocation')\
    .repartition(4,"AddressID")\
    .cache()
address.createOrReplaceTempView("address")
address.columns


entityAdd= dataFrames["[Person].[BusinessEntityAddress]"]\
    .select('BusinessEntityID', 'AddressID')\
    .repartition(4,'BusinessEntityID', 'AddressID')\
    .cache()
entityAdd.createOrReplaceTempView("entityAdd")
entityAdd.columns

state = dataFrames["[Person].[StateProvince]"]\
    .select('StateProvinceID',
     'StateProvinceCode',
     'CountryRegionCode',
     'IsOnlyStateProvinceFlag',
     expr('Name').alias("stateName"))\
    .repartition(4,'StateProvinceID')\
    .cache()
state.createOrReplaceTempView("state")


# _________Tables Needed for Employee Dimension_________
employee = dataFrames["[HumanResources].[Employee]"]\
    .select('BusinessEntityID',
             'NationalIDNumber',
             'LoginID',
             'OrganizationNode',
             'OrganizationLevel',
             'JobTitle',
             'BirthDate',
             'MaritalStatus',
             'Gender',
             'HireDate',
             'SalariedFlag',
             'VacationHours',
             'SickLeaveHours',
             'CurrentFlag')\
            .repartition(4, "BusinessEntityID").cache()
employee.createOrReplaceTempView("employee")


# ___________Tables Needed for Product Dimension_____________
# select just neeeded columns.
product = dataFrames["[Production].[Product]"]\
    .select('ProductID',
         expr('Name').alias("productName"),
         'ProductNumber',
         'MakeFlag',
         'FinishedGoodsFlag',
         'Color',
         'SafetyStockLevel',
         'ReorderPoint',
         'StandardCost',
         'ListPrice',
         'Size',
         'SizeUnitMeasureCode',
         'WeightUnitMeasureCode',
         'Weight',
         'DaysToManufacture',
         'ProductLine',
         'Class',
         'Style',
         'ProductSubcategoryID',
         'ProductModelID',
         'SellStartDate',
         'SellEndDate',
         'DiscontinuedDate')\
    .repartition(4,'ProductID')\
    .cache()
product.createOrReplaceTempView("product")



category = dataFrames['[Production].[ProductCategory]']\
    .select('ProductCategoryID', 'Name')\
    .repartition(4,'ProductCategoryID')\
    .cache()
category.createOrReplaceTempView("category")


subCat = dataFrames['[Production].[ProductSubcategory]']\
    .select('ProductSubcategoryID',
             'ProductCategoryID',
             expr('Name').alias("subCategoryName"))\
    .repartition(4,'ProductSubcategoryID', 'ProductCategoryID')\
    .cache()
subCat.createOrReplaceTempView("subCat")


productHist = dataFrames['[Production].[ProductCostHistory]']\
    .where(col("EndDate").isNull())\
    .select("ProductID", "StandardCost")\
    .repartition(4,'ProductID')\
    .cache()
productHist.createOrReplaceTempView("productHist")



culture = dataFrames['[Production].[ProductModelProductDescriptionCulture]']\
    .where(col("CultureID") =='en')\
    .select('ProductModelID', 'ProductDescriptionID', 'CultureID')\
    .repartition(4,'ProductModelID', 'ProductDescriptionID')\
    .cache()
culture.createOrReplaceTempView("culture")

description = dataFrames['[Production].[ProductDescription]']\
    .select('ProductDescriptionID', 'Description')\
    .repartition(4,'ProductDescriptionID')\
    .cache()
description.createOrReplaceTempView("description")




# ___________Tables Needed for Fact Table_________________
# columns which are selected is the desired only
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

# ________________Date Dimension______________________
orderDate = dataFrames["[Sales].[SalesOrderHeader]"]
order_dates = orderDate.select("OrderDate").distinct()
DateTable = order_dates.select(
    col("OrderDate").alias("DateKey"),
    date_format("OrderDate", "yyyy-MM-dd").alias("Date"),
    date_format("OrderDate", "yyyy").alias("Year"),
    date_format("OrderDate", "MM").alias("Month"),
    date_format("OrderDate", "dd").alias("Day"),
    quarter("OrderDate").alias("Quarter"))\
    .repartition(4, "DateKey")
DateTable.createOrReplaceTempView("DateTable")


# ____________________write into Hive.____________________

bronze_data_layer_tables = [
    "address", "category", "culture", "customer", "datetable",
    "dept", "deptHist", "description", "employee", "entityAdd",
    "person", "product", "productHist", "SalesOrderdetail",
    "Salesorderheader", "state", "subcat"
]
schemaName = "bronze"
Write_In_Hive_Schema(schema=schemaName, TablesNamesList=bronze_data_layer_tables)
spark.sql(f"USE {schemaName};")
spark.sql("show tables;").show()