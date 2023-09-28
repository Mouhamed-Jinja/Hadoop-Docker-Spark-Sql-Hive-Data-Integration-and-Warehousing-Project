from pyspark.sql.functions import *
from ConfigAndConnectors.sparkConfig import get_spark_session
from ConfigAndConnectors.hiveConnector import Write_In_Hive_Schema

spark = get_spark_session("parqut")
spark.sql("USE gold;")
spark.sql("show tables;").show()

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


customer_churn90.write.format("hive").partitionBy("CustomerID").mode("overwrite").saveAsTable("gold.customer_churn90")
spark.sql("select * from gold.customer_churn90").show()