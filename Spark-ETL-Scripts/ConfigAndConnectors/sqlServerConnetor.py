from .sparkConfig import get_spark_session
spark = get_spark_session("sqlConnector")
dataFrames= {}
def sql_connector(Tables):
    for table in Tables:
        query = f"select * from {table}"
        df =spark.read.format("jdbc") \
            .option("url", "jdbc:sqlserver://172.18.0.5:1433;databaseName=AdventureWorks2017") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("dbtable", f"({query}) as temp") \
            .option("user","sa") \
            .option("password", "Mo*012105")\
            .load()
        dataFrames[table] = df
    return dataFrames