def hive_connector(dataFrame, DFname):
    dataFrame.write.format("hive").mode("overwrite").saveAsTable(f"bronze.{DFname}")
    
