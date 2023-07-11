from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
  "sfURL" : "vqkwubf-es59758.snowflakecomputing.com",
  "sfUser" : "ARJUN1234",
  "sfPassword" : "Etl@1234",
  "sfDatabase" : "avddb",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH"
}
df = spark.read.format(SNOWFLAKE_SOURCE_NAME)\
    .options(**sfOptions)\
    .option("dbtable", "asl")\
    .option("autopushdown", "off")\
    .load()
# df.show()
data =r'C:\bigdata\project\data\bank-full.csv'
df =spark.read.format("csv")\
    .option("sep",",")\
    .option("header","true")\
    .option("inferSchema", "true")\
    .load(data)

df.write.mode("append")\
    .format(SNOWFLAKE_SOURCE_NAME)\
    .options(**sfOptions)\
    .option("dbtable","bank")\
    .save()
