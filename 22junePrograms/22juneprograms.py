# from pyspark.sql import *
# from pyspark.sql.functions import *
#
# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#
# data ="C:\\bigdata\\project\\drivers\\asl.csv"
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# #df.show()
# #res=df.withColumn("grade",when(col("age")<=12,"kid").when((col("age")>12) & (col("age")<30), "youth").when((col("age")>=30) & (col("age")<60),"professional").otherwise("oldaged"))
# #res=df.withColumn("offers",when(col("city").isin("hyd","blr","nyc"),"30% off").when(col("city")=="mas", "20% off").when(col("city").like("%l"),"5% off").otherwise("no offer"))
# def offer(c):
#     if(c=="hyd"):
#         return "90% off"
#     elif(c=="blr"):
#         return "40% off"
#     elif(c=="mas"):
#         return "20% off"
#     else:
#         return "no offer"
#
# #if no suitable function avail, create ur won python function .. convert to udf (spark support only udf )
# #python function convert to udf
# uoff = udf(offer)
# res=df.withColumn("weekendoffer",uoff(col("city")))
# #use this udf in spark.sql ... udf register as sql functions
# spark.udf.register("ofr",uoff)
#
# df.createOrReplaceTempView("test")
# res1=spark.sql("select *, ofr(city) todayoffers from test")
#
# res1.show()
#
#
"""Program 2==========="""
from pyspark.sql import *
from pyspark.sql.functions import *

import pandas as pd
spark=SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timeZone", "IST").getOrCreate()
#spark.conf.set("spark.sql.session.timeZone", "IST")


data ="C:\\bigdata\\project\\drivers\\donations.csv"
df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df=df1.withColumn("dt",to_date(col("dt"),"d-M-yyyy")).withColumn("today",current_date())\
    .withColumn("datediff",datediff(col("today"),col("dt")))\
    .withColumn("datediffyymmdd",col("today")-col("dt"))\
    .withColumn("ts",current_timestamp()).withColumn("lastdt",last_day(col("dt")))\
    .withColumn("add",date_add(col("dt"),100)).withColumn("sub",date_add(col("dt"),-100))\
    .withColumn("addmon",add_months(col("today"),10))\
    .withColumn("dtformat",date_format(col("dt"),"yyyy/MMMM/dd/EEE/z/D"))\
    .withColumn("nextday",next_day(col("dt"),"Friday"))\
    .withColumn("lastfri",next_day(date_add(last_day(col("dt")),-7),"Friday"))\
    .withColumn("lastsun",date_add(last_day(col("dt")),-7))




df.show(truncate=False)

#https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
#892
#by default spark understand 'yyyy-MM-dd' format only ... thsts y based on input data  u have to mention within to_date( format)