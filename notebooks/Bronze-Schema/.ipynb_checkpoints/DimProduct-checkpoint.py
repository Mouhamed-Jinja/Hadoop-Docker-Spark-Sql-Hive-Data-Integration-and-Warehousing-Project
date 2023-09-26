from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("product") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
    .enableHiveSupport() \
    .getOrCreate()


Tables= ["[Production].[Product]", "[Production].[ProductDescription]", "[Production].[ProductModelProductDescriptionCulture]", "[Production].[ProductSubcategory]", "[Production].[ProductCategory]", "[Production].[ProductCostHistory]"]
print(len(Tables), " tables")

dataFrames = {}
for table in Tables:
    query = f"select * from {table}"
    df =spark.read.format("jdbc")\
        .option("url","jdbc:sqlserver://172.18.0.7:1433;databaseName=AdventureWorks2017")\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .option("dbtable", f"({query}) AS temp")\
        .option("user", "sa")\
        .option("password", "Mo*012105")\
        .load()
    dataFrames[table]= df
    
print(dataFrames.keys())


# Prepare the core table product, select desired columns, repartition and cache it
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
productHist.show(3)


culture = dataFrames['[Production].[ProductModelProductDescriptionCulture]']\
    .where(col("CultureID") =='en')\
    .select('ProductModelID', 'ProductDescriptionID', 'CultureID')\
    .repartition(4,'ProductModelID', 'ProductDescriptionID')\
    .cache()
culture.createOrReplaceTempView("culture")
culture.columns
subCat.show(3)

description = dataFrames['[Production].[ProductDescription]']\
    .select('ProductDescriptionID', 'Description')\
    .repartition(4,'ProductDescriptionID')\
    .cache()
description.createOrReplaceTempView("description")
description.columns

print("______________Start joining__________________")

DimProduct = spark.sql("""
    select *
    from product p left outer join subCat sc
    on p.ProductSubcategoryID = sc.ProductSubcategoryID

    left outer join category c
    on sc.ProductCategoryID = c.ProductCategoryID

    left outer join productHist ph
    on p.productID = ph.productID

    left outer join culture
    on p.ProductModelID = culture.ProductModelID

    left outer join description desc
    on culture.ProductDescriptionID = desc.ProductDescriptionID
    
""")

DimProduct= DimProduct.select(
        'p.ProductID',
         'productName',
         'ProductNumber',
         'MakeFlag',
         'FinishedGoodsFlag',
         'Color',
         'SafetyStockLevel',
         'ReorderPoint',
         'p.StandardCost',
         'ListPrice',
         'Size',
         'SizeUnitMeasureCode',
         'WeightUnitMeasureCode',
         'Weight',
         'DaysToManufacture',
         'ProductLine',
         'Class',
         'Style',
         'SellStartDate',
         'SellEndDate',
         'DiscontinuedDate',
         'subCategoryName',
         'Description')\
    .repartition(2,"ProductID")\
    .cache()

DimProduct.write.format("hive").mode("overwrite").saveAsTable("bronze.DimProduct")
print("**************** Job Done!! *********************")

spark.sql("USE bronze;")
spark.sql("show tables;").show()

