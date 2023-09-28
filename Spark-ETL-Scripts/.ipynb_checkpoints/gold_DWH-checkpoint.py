from pyspark.sql.functions import *
from ConfigAndConnectors.sparkConfig import get_spark_session
from ConfigAndConnectors.hiveConnector import hive_connector
import SQL_QueriesAndAttributes.queries as query 
from isnull import haveNulls
spark = get_spark_session("Gold_DWH")

#check if customer dimension have nulls
goldDimCustomer =spark.table("silver.dimcustomer")
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
goldDimCustomer= goldDimCustomer\
            .withColumn("Address", array(col("AddressLine1"), col("AddressLine2")))\
            .drop(col("AdditionalContactInfo"), col("Suffix"), col("Title"), col("AddressLine1"), col("AddressLine2"))

cols= goldDimCustomer.columns
#print(cols)


#employee dimension
goldDimEmployee =spark.table("silver.dimemployee")
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
goldDimEmployee= goldDimEmployee\
            .where(col("OrganizationNode").isNotNull())\
            .where(col("OrganizationLevel").isNotNull())\
            .drop(col("AdditionalContactInfo"), col("Suffix"), col("Title"))

cols =goldDimEmployee.columns
print(cols)

# Fact Sales: granularity is transactional level.
goldFactSales=spark.table("silver.FactSales")
employeeColumns= query.employee_attributes
#df =haveNulls(columnsList=employeeColumns,schema=SchemaName, dataFrameName="dimemployee")
#df.show()























