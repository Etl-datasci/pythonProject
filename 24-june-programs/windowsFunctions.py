from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

DATA = r'C:\bigdata\project\drivers\empmysql.csv'

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true").load(DATA)

# df.printSchema()
win = Window.partitionBy("job").orderBy(col("sal").desc())
# ndf = df.orderBy(col("sal").desc())

ndf = df.withColumn("rnk", rank().over(win))\
    .withColumn("drank", dense_rank().over(win))\
    .withColumn("rn", row_number().over(win))\
    .withColumn("per", percent_rank().over(win))\
    .withColumn("ntil",ntile(3).over(win))

"""if u get any duplicates values at that time ranking create problems thats y """
# rank give raniking based on data but bot give rani in sequence
ndf.show(truncate=False)

