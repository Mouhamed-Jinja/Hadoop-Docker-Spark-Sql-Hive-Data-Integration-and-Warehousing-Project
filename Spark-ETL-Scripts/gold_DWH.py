from pyspark.sql.functions import *
from ConfigAndConnectors.sparkConfig import get_spark_session
from ConfigAndConnectors.hiveConnector import hive_connector
import SQL_QueriesAndAttributes.queries as query 

spark =get_spark_session("Gold_DWH")
