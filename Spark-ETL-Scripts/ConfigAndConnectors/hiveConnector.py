from .sparkConfig import get_spark_session
spark =get_spark_session("hiveConnector")

def Write_In_Hive_Schema(schema,TablesNamesList):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for tableName in TablesNamesList:
        df= spark.table(tableName)
        df.write.format("hive").mode("overwrite").saveAsTable(f"{schema}.{tableName}")
        print("___________writing_______________")