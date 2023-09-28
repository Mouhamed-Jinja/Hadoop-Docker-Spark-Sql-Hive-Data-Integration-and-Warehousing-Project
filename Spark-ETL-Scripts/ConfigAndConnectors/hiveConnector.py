def hive_connector(schema,dataFrame, tableName):
    dataFrame.write.format("hive").mode("overwrite").saveAsTable(f"{schema}.{tableName}")