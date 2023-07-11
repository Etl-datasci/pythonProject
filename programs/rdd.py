from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext

data=r"C:\bigdata\pythonProject\drivers\asl_skip4lines.txt"
#external data convert to rdd use sc.textFile
drdd = sc.textFile(data)
res=drdd.zipWithIndex().filter(lambda x:x[1]>4).map(lambda x:x[0])

res.collect()
#rdd convert to dataframe
df = res.map(lambda x:x.split(",")).toDF(['name','age','city'])
# df.show()
for x in res.take(10):
    print(x)


