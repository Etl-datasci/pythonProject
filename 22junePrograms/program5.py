import re

from pyspark.sql import *
from pyspark.sql.functions import *

# File uploaded to /FileStore/tables/flipkartdelivary-2.csv
# File uploaded to /FileStore/tables/uber-2.csv
# File uploaded to /FileStore/tables/uber_driversinfo-2.csv


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
driverinfo_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(r'C:\bigdata\project\dataset_task\uber_driversinfo.csv')
uber_ride_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(r'C:\bigdata\project\dataset_task\uber.csv')
driverinfo_df.show(5, truncate=False)

allcols = [re.sub("[^a-zA-Z]", "", n) for n in driverinfo_df.columns]

driver_df = driverinfo_df.toDF(*allcols)
driver_df.show(5, truncate=False)

trip_df = uber_ride_df.toDF(*allcols)
trip_df.show(5, truncate=False)


joined_df = driver_df.join(trip_df,"Date")
joined_df.distinct().show(50, truncate=False)
