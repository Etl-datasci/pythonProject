from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data=r"C:\bigdata\project\drivers\empmysql.csv"
spark.conf.set("spark.sql.session.timeZone", "IST")

df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
from pyspark.sql.window import *
#df=df1.orderBy(col("sal").desc())

#win=Window.orderBy(col("sal").desc())
win=Window.partitionBy("job").orderBy(col("sal").desc())
df=df1.withColumn("rnk", rank().over(win)).withColumn("drank",dense_rank().over(win))\
    .withColumn("rownum",row_number().over(win)).where(col("drank")<=2)

df.show()
#row_number ... no duplicative rankings 100% ull get unique ranking. it's not consider values.

#rank function .... it give ranks, but not in seq especially if u have duplicate ranks ... rank is not in seq.
#dense rank .... it give ranks , in seq especilly if u have duplicate values.
#windows functions separated .... ranking functions and analysis functions
#ranking funcions: rank, dense_rank(), row_number(),ntile(),percent_rank()
#https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
