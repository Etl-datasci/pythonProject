from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

DATA =r'C:\bigdata\project\drivers\empmysql.csv'
df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load(DATA)

# win = Window.orderBy(col("sal").desc())
win = Window.partitionBy(col("ename")\
                         .orderBy(col("sal")).desc())
mdf = df.withColumn("rno", row_number().over(win))
mdf.show()

