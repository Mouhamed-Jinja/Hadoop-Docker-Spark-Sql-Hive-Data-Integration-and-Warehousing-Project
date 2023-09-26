from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Customer") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()

Tables= ["[Person].[StateProvince]", "[Person].[BusinessEntityAddress]", "[Person].[Address]", "[Person].[Person]", "[HumanResources].[Department]", "[HumanResources].[EmployeeDepartmentHistory]", "[HumanResources].[Employee]"]
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


emp = dataFrames["[HumanResources].[Employee]"]\
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
emp.createOrReplaceTempView("emp")

#there is dublicated values ?
spark.sql("""
select * from(
    select BusinessEntityID, count(BusinessEntityID) as count
    from emp
    group by BusinessEntityID) as nt
    order by count asc
""").show(3)


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



entityAdd= dataFrames["[Person].[BusinessEntityAddress]"]\
    .select('BusinessEntityID', 'AddressID')\
    .repartition(4,'BusinessEntityID', 'AddressID')\
    .cache()
entityAdd.createOrReplaceTempView("entityAdd")



state = dataFrames["[Person].[StateProvince]"]\
    .select('StateProvinceID',
     'StateProvinceCode',
     'CountryRegionCode',
     'IsOnlyStateProvinceFlag',
     expr('Name').alias("stateName"))\
    .repartition(4,'StateProvinceID')\
    .cache()
state.createOrReplaceTempView("state")



print("__________________Start Joinging The Tables_________________")
DimEmployee = spark.sql("""
    select *
    from emp e inner join depthist dh
    on e.BusinessEntityID = dh.BusinessEntityID

    inner join dept d
    on dh.DepartmentID = d.DepartmentID

    inner join person p
    on e.BusinessEntityID =p.BusinessEntityID

    inner join entityadd ea
    on e.BusinessEntityID = ea.BusinessEntityID

    inner join address add
    on ea.AddressID = add.AddressID

    inner join state s
    on add.StateProvinceID = s.StateProvinceID
""")
print("__________Finished___________")


DimEmployee= DimEmployee.select(
                 expr('e.BusinessEntityID').alias("EmployeeID"),
                 'EmployeeName',
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
                 'CurrentFlag',
                 'GroupName',
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
           .repartition(2,"EmployeeID")

DimEmployee.write.mode("overwrite").saveAsTable("bronze.DimEmployee")
print("****************************** Finished Writing (Dobe!) *******************************")

spark.sql("use bronze")
spark.sql("show tables;").show()


