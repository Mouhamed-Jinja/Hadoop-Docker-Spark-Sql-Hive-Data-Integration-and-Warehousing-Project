def hive_connector(dataFrame):
    dataFrame.write.format("hive").mode("overwrite").saveAsTable(f"bronze.{dataFrame}")
    
