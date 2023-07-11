from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
DATA = r'C:\bigdata\project\drivers\us-500.csv'
df = spark.read.format("csv") \
    .option("header", "True") \
    .option("inferSchema", "True") \
    .load(DATA).withColumnRenamed("zip", "sal")

win = Window.orderBy(col("sal").desc())
ndf = df.withColumn("rno", row_number().over(win).where(col("rno")).between(90, 100))
ndf.show()
