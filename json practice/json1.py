from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


DATA =r'C:\bigdata\project\data\zips.json'
df=spark.read.format("json").option("mode","MALFORMEDDATA").load(DATA)

df.show()
df =df.withColumn("loc",explode(col("loc")))

df.printSchema()
df.show()