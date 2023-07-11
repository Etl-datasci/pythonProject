from pyspark.shell import sc
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import *

# spark session object
spark = SparkSession.builder.master("local[4]").appName("test").getOrCreate()

#
jsonString = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df = spark.createDataFrame(data=[(1, jsonString)],schema=["id", "value"])
df.show(truncate=False)

df2 = df.withColumn("value", from_json(df.value, MapType(StringType(), StringType())))
df2.show(truncate=False)

df3=df2.rdd.map(lambda x:(x.id , x.value['Zipcode'],x.value['City'],x.value['State']))\
    .toDF(['id','zipcode','city','state'])
df3.printSchema()
df3.show(truncate=False)
