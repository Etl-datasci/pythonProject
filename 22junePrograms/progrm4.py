from numba.core.typing.builtins import Int
from numpy.array_api._array_object import Array
import numpy as np
from pyspark.shell import sc
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()



#
# # Define the data as a list of tuples
# data =r'C:\bigdata\project\dataset fro task\uber.csv'
#
#
# # Create a DataFrame from the data
# df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#
# # Fill null values in the 'Date' column with the first non-null value
# # df = df.withColumn('Date', last('Date', ignorenulls=True).over(Window.orderBy('Time (Local)')))
#
# # Fill null values in other columns with 0
# # df = df.fillna(0)
# df_filled = df.fillna()
# # Show the modified DataFrame
# df_filled.show(10000,truncate=False)




