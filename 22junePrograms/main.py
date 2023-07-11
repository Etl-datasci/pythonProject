from pyspark.sql import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = r'C:\bigdata\project\drivers\donations.csv'
df = spark.read.format("csv").load(data
df.show()



