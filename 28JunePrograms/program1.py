from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = r'C:\bigdata\project\drivers\zips.json'
df=spark.read.format("json").load(data)

df =df.withColumn("loc",explode(col("loc"))).withColumnRenamed("_id","id")

op =r'C:\bigdata\project\drivers\zips'
df.coalesce(1).write.mode()
df.printSchema()
df.show()