

from pyspark.sql import *
from pyspark.sql.functions import *


spark=SparkSession.builder.master("local").appName("test").getOrCreate()
data=r"C:\bigdata\drivers\us-500.csv"
#data="C:\\bigdata\\drivers\\us-500.csv"
#data="C:/bigdata/drivers/us-500.csv"
df=spark.read.format("csv").option("header","true").load(data)
#df.show()
display(df)
#processing
ndf=df.withColumn("age",lit("testing")).withColumn("fullname",concat(col("first_name"),lit(" "),col("last_name"), lit(" "),col("state")))\
    .withColumn("phone1", regexp_replace(col("phone1"),"-",""))\
    .withColumn("email1",regexp_replace(col("email"),"[@,.]",""))\
    .withColumn("phone2",regexp_replace(col("phone2"),"_",""))
#withcolumn accepting only functions or columns as second
ndf.show()
