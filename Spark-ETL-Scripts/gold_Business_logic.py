from pyspark.sql.functions import *
from connectors.spark_session import get_spark_session
from connectors.hive_writer import Write_In_Hive_Schema

spark =get_spark_session("Business-Logic")
spark.sql("USE gold")

customer_churn90 =spark.sql("""
    WITH ReferenceDate AS (
  SELECT DATE_SUB(MAX(OrderDate), 90) AS reference_date
  FROM FactSales
  ),

-- Retrieve customer keys who made purchases after the reference date
    purches_after_90 as(
    SELECT DISTINCT CustomerKey
    FROM FactSales
    WHERE OrderDate > (SELECT reference_date FROM ReferenceDate)
    ),
    
    customer_IDs_not_purches_last90 as (
    select CustomerKey from factsales
    where factsales.CustomerKey not in (select CustomerKey from  purches_after_90)
    ),
    
    fact as (
    select * from factsales c
    where c.customerkey in (select CustomerKey from customer_IDs_not_purches_last90)
    )
    
    SELECT
    dimcustomer.CustomerID,
    dimcustomer.CustomerName,
    MAX(fact.OrderDate) AS LastPurchaseDate
    from fact inner join dimcustomer
    on fact.CustomerKey = dimcustomer.customerid
    group by dimcustomer.CustomerID, dimcustomer.CustomerName
    
""")
customer_churn90.createOrReplaceTempView("customer_churn90")


Customer_Lifetime_Value=spark.sql("""
  SELECT
    fs.CustomerKey,
    SUM(fs.TotalDue) AS TotalRevenue,
    COUNT(DISTINCT fs.SalesOrderID) AS NumberOfOrders,
    cast((SUM(fs.TotalDue) / COUNT(DISTINCT fs.SalesOrderID)) as decimal(10,4)) as CLV
  FROM
    FactSales fs
  GROUP BY
    fs.CustomerKey
""")
Customer_Lifetime_Value.createOrReplaceTempView("Customer_Lifetime_Value")


Emp_Sales_performace =spark.sql("""
  SELECT
    e.EmployeeID,
    e.EmployeeName,
    SUM(fs.TotalDue) AS TotalSales,
    cast(AVG(fs.TotalDue) AS DECIMAL(10,4)) as AvgOrderValue,
    COUNT(DISTINCT fs.SalesOrderID) AS TotalOrders 
    FROM FactSales fs
    
    inner join dimemployee e
    ON fs.EmployeeKey = e.EmployeeID
    
    GROUP BY e.EmployeeID, e.EmployeeName
""")
Emp_Sales_performace.createOrReplaceTempView("Emp_Sales_performace")


product_trends =spark.sql("""
  SELECT
    p.ProductID,
    p.productName,
    d.Year,
    d.Month,
    SUM(fs.TotalDue) AS MonthlySales
  FROM FactSales fs

  inner join dimproduct p
  ON fs.ProductKey = p.ProductID
  
  inner join dimdate d
  ON fs.OrderDate = d.DateKey
  
  GROUP BY
    p.ProductID, p.productName, d.Year, d.Month
    
  ORDER BY
    p.ProductID, d.Year, d.Month
""")
product_trends.createOrReplaceTempView("product_trends")

profit =spark.sql("""
  SELECT
    fs.*,
    (fs.SubTotal - fs.TaxAmt - fs.Freight) AS Profit
  FROM
    FactSales fs
""")
profit.createOrReplaceTempView("profit")

tablesList = ["profit", "product_trends", "Emp_Sales_performace", 
              "Customer_Lifetime_Value","customer_churn90"]

Write_In_Hive_Schema(schema="gold", TablesNamesList=tablesList)
