from pyspark.shell import sc
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
lst =[1,2,3,5,4,5,6,7,8,9,10,11,12]

lrdd =sc.parallelize(lst)

process = lrdd.map(lambda x:x*x)

for x in process.collect():
    print(x)
