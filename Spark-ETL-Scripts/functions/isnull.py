from pyspark.sql.functions import *
from ConfigAndConnectors.sparkConfig import get_spark_session

spark = get_spark_session("isNull")

result = []
def haveNulls(columnsList, schema, dataFrameName):
    df = spark.table(f"{schema}.{dataFrameName}")
    
    for columnName in columnsList: 
        count_nulls = df.where(col(columnName).isNull()).count()
        result.append((columnName, count_nulls))
        
    result_df = spark.createDataFrame(result, ["column name", "count of nulls"])
    return result_df
