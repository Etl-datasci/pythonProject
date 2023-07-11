from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.master("local").appName("test").getOrCreate()
data = r'C:\bigdata\pythonProject\drivers\asl.csv'
df =spark.read.format("csv").option("inferSchema","true").option("header","true").load(data)

df.show()