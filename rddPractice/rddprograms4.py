from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data=r"C:\bigdata\pythonProject\csv_files\asl.csv"
ardd = sc.textFile(data)
#after read, process
#insql ...select * from emp.. group by tab ... join table...
#rdd pure programming model api.
proc = ardd.filter(lambda x:"age" not in x)\
    .map(lambda x:x.split(",")).toDF(["name","age","city"])

#proc.show()
res=proc.groupBy(col("city")).count()
res.show()
#dataframe convert to rdd..
result = res.rdd
#rdd convert to dataframe use toDF(cols name)

#in scala/python its called methods but in spark it's called transformation and actions.
#map: apply a logic on top of multiple elements use map. how many input elements u have same number of output elements u ll get.


for x in result.take(11):
    print(x)
#take(11) take first 11 rows to process.

#uwant to carry 20kg.. 1 person ..very big
#2 people .... 10+10kg = 20kg u can easily carry.
#4 people ....,5,5,5,5kgs = 20kg ...easy to carry
#pycharm third party tool so u have to configure everything by default no sparkSession or any spark config

#rdd same like coins ..
#now 99% use google pay or notes..dataframe like notes/wallet




