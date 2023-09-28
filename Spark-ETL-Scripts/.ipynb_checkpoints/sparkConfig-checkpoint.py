from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.jars", "/Drivers/SQL_Sever/jdbc/sqljdbc42.jar")\
        .enableHiveSupport() \
        .getOrCreate()
    return spark