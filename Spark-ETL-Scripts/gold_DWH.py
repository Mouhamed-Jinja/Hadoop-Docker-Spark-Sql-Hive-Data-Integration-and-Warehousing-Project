from pyspark.sql.functions import *
from ConfigAndConnectors.sparkConfig import get_spark_session
from ConfigAndConnectors.hiveConnector import Write_In_Hive_Schema
import SQL_QueriesAndAttributes.queries as query 
from isnull import haveNulls
spark = get_spark_session("Gold_DWH")

#check if customer dimension have nulls
DimCustomer =spark.table("silver.dimcustomer")
customerColumns= query.customer_columns

#we will build gold data layer based on silver layer.
SchemaName= "silver"

#df =haveNulls(columnsList=customerColumns,schema=SchemaName, dataFrameName="dimcustomer")
#df.show()
"""
+--------------------+--------------+
|          CustomerID|             0|
|        CustomerName|             0|
|       AccountNumber|             0|
|          PersonType|             0|
|           NameStyle|             0|
|               Title|         18407|
|              Suffix|         18505|
|      EmailPromotion|             0|
|AdditionalContact...|         18508|
|        Demographics|             0|
|        AddressLine1|             0|
|        AddressLine2|         18194|
|                City|             0|
|          PostalCode|             0|
|     SpatialLocation|             0|
|   StateProvinceCode|             0|
|   CountryRegionCode|             0|
|IsOnlyStateProvin...|             0|
|           stateName|             0|
+--------------------+--------------+
"""
DimCustomer= DimCustomer\
            .withColumn("Address", array(col("AddressLine1"), col("AddressLine2")))\
            .drop(col("AdditionalContactInfo"), col("Suffix"), col("Title"), col("AddressLine1"), col("AddressLine2"))

DimCustomer.createOrReplaceTempView("DimCustomer")
cols= DimCustomer.columns
#print(cols)


#employee dimension
DimEmployee =spark.table("silver.dimemployee")
employeeColumns= query.employee_attributes
#df =haveNulls(columnsList=employeeColumns,schema=SchemaName, dataFrameName="dimemployee")
#df.show()
"""
+-----------------+--------------+
|      column name|count of nulls|
+-----------------+--------------+
|       EmployeeID|             0|
|     EmployeeName|             0|
| NationalIDNumber|             0|
|          LoginID|             0|
| OrganizationNode|             1|
|OrganizationLevel|             1|
|         JobTitle|             0|
|        BirthDate|             0|
|    MaritalStatus|             0|
|           Gender|             0|
|         HireDate|             0|
|     SalariedFlag|             0|
|    VacationHours|             0|
|   SickLeaveHours|             0|
|      CurrentFlag|             0|
|        GroupName|             0|
|       PersonType|             0|
|        NameStyle|             0|
|            Title|           282|
|           Suffix|           288|
+-----------------+--------------+
"""
DimEmployee= DimEmployee\
            .where(col("OrganizationNode").isNotNull())\
            .where(col("OrganizationLevel").isNotNull())\
            .drop(col("AdditionalContactInfo"), col("Suffix"), col("Title"))

DimEmployee.createOrReplaceTempView("DimEmployee")
cols =DimEmployee.columns
#print(cols)


# Fact Sales: granularity is transactional level.
FactSales=spark.table("silver.FactSales")
factColumns= query.silver_fact_attributes
#df =haveNulls(columnsList=factColumns,schema=SchemaName, dataFrameName="factsales")
#df.show()
FactSales = FactSales.drop(col("PurchaseOrderNumber"))
FactSales.createOrReplaceTempView("FactSales")


#_________Product Dimension____________
DimProduct =spark.table("silver.dimproduct")
DimProduct.createOrReplaceTempView("DimProduct")
productColumns= query.silver_product_attributes
#df =haveNulls(columnsList=productColumns,schema=SchemaName, dataFrameName="dimproduct")
#df.show()
"""
+--------------------+--------------+
|         column name|count of nulls|
+--------------------+--------------+
|           ProductID|             0|
|         productName|             0|
|       ProductNumber|             0|
|            MakeFlag|             0|
|   FinishedGoodsFlag|             0|
|               Color|           248|
|    SafetyStockLevel|             0|
|        ReorderPoint|             0|
|        StandardCost|             0|
|           ListPrice|             0|
|                Size|           293|
| SizeUnitMeasureCode|           328|
|WeightUnitMeasure...|           299|
|              Weight|           299|
|   DaysToManufacture|             0|
|         ProductLine|           226|
|               Class|           257|
|               Style|           293|
|       SellStartDate|             0|
|         SellEndDate|           406|
+--------------------+--------------+
"""
#here is hard to make a decision about null values, we will discuss it with the business team

# Date Dimension won't change from silver data layer to gold
DimDate = spark.table("silver.dimdate")
DimDate.createOrReplaceTempView("DimDate")


# Write in apache hive 
goldDFList =["DimCustomer", "DimEmployee", "DimProduct", "FactSales", "DimDate"]
Write_In_Hive_Schema(schema="gold", TablesNamesList=goldDFList)

"""
ls=[]
for tablename in goldDFList:
    df = spark.table(tablename)
    cnt =df.count()
    ls.append(cnt)
print("->"*15,ls)
"""

























