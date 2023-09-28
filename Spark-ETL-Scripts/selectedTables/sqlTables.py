#These tables needed to create Customer dimension
Customer_Tables= ["[Sales].[Customer]", "[Person].[StateProvince]", "[Person].[BusinessEntityAddress]", "[Person].[Address]", "[Person].[Person]", "[HumanResources].[Department]", "[HumanResources].[EmployeeDepartmentHistory]"]

#These tables needed to create Product dimension.
Product_Tables= ["[Production].[Product]", "[Production].[ProductDescription]", "[Production].[ProductModelProductDescriptionCulture]", "[Production].[ProductSubcategory]", "[Production].[ProductCategory]", "[Production].[ProductCostHistory]"]

Employee_Tables= ["[Person].[StateProvince]", "[Person].[BusinessEntityAddress]", "[Person].[Address]", "[Person].[Person]", "[HumanResources].[Department]", "[HumanResources].[EmployeeDepartmentHistory]", "[HumanResources].[Employee]"]

Fact_Tables = ["[Sales].[SalesOrderHeader]", "[Sales].[SalesOrderDetail]", "[Sales].[Customer]", "[Production].[Product]", "[HumanResources].[Employee]"] 

Date_Tables = ["[Sales].[SalesOrderHeader]"]

Bronze_Stage_Tables = list(set(Customer_Tables +Employee_Tables +Product_Tables +Fact_Tables))

#print(type(Bronze_Stage_Tables), "\n", Bronze_Stage_Tables)
#print(len(Bronze_Stage_Tables))

               