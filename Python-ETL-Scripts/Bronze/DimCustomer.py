from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Employee") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()

Tables= ["[Sales].[Customer]", "[Person].[StateProvince]", "[Person].[BusinessEntityAddress]", "[Person].[Address]", "[Person].[Person]", "[HumanResources].[Department]", "[HumanResources].[EmployeeDepartmentHistory]", "[HumanResources].[Employee]"]
print(len(Tables), " tables")

dataFrames= {}
for table in Tables:
    query = f"select * from {table}"
    df =spark.read.format("jdbc") \
        .option("url", "jdbc:sqlserver://172.18.0.7:1433;databaseName=AdventureWorks2017") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", f"({query}) as temp") \
        .option("user","sa") \
        .option("password", "Mo*012105")\
        .load()
    dataFrames[table] = df
print(dataFrames.keys())

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


print("_______________# join the all dataFrames to get DimCustomer DF______________")
DimCustomer = spark.sql("""

    select *
    from customer c inner join person p
    on c.PersonID = p.BusinessEntityID
    
    inner join entityadd ea
    on p.BusinessEntityID = ea.BusinessEntityID

    inner join address add
    on ea.AddressID = add.AddressID

    inner join state s
    on add.StateProvinceID = s.StateProvinceID
    
""")

DimCustomer= DimCustomer.select(
             'CustomerID',
             'AccountNumber',
             expr('EmployeeName').alias("CustomerName"),
             'PersonType',
             'NameStyle',
             'Title',
             'Suffix',
             'EmailPromotion',
             'AdditionalContactInfo',
             'Demographics',
             'AddressLine1',
             'AddressLine2',
             'City',
             'PostalCode',
             'SpatialLocation',
             'StateProvinceCode',
             'CountryRegionCode',
             'IsOnlyStateProvinceFlag',
             'stateName')\
            .repartition(4,"CustomerID")
DimCustomer.createOrReplaceTempView("DimCustomer")


DimCustomer.write.mode("overwrite").format("hive").saveAsTable("bronze.DimCustomer")
print("**************************** Write Have Done *****************************")
spark.sql("use bronze")
spark.sql("show tables;").show()
