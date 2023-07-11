from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
sc=spark.sparkContext
data=r"C:\bigdata\pythonProject\csv_files\wcdata.txt"
ardd = sc.textFile(data)