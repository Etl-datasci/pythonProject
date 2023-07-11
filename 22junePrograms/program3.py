from sympy import false

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

DATA =r'C:\bigdata\project\drivers\us-500.csv'
df =spark.read.format("csv").option("header","true").option("inferSchema","true").load(DATA)
# df=df.columns
# print(df)

# ndf = df.withColumn("age",lit("testing"))
ndf = df.withColumn("age",lit("18")).withColumn("full_name",concat(col("first_name"),col("last_name")))\
    .withColumn("phone1",regexp_replace(col("phone1"),"-",""))\
    .withColumn("phone2",regexp_replace(col("phone2"),"-",""))\
    .withColumn("email1",regexp_replace(col("email"),"[@,.]",""))
ndf.show(5,truncate=False)
# df.show(5,truncated=True)
# cols = df.columns
#
# print(cols)
# df.show(2)