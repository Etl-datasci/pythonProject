import re

from IPython.core.display_functions import display
from sqlalchemy import true

from pyspark.cloudpickle import load
from pyspark.sql import *
from pyspark.sql.functions import *

import pandas as pd

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
DATA = r'C:\bigdata\project\drivers\10000Records.csv'
df = spark.read.format("csv").option("header" , "True").option("inferSchema","True").load(DATA)
# df.show()
df.show(5,truncate=False)



cols =['Emp ID', 'Name Prefix', 'First Name', 'Middle Initial', 'Last Name', 'Gender', 'E Mail', "Father's Name", "Mother's Name", "Mother's Maiden Name", 'Date of Birth', 'Time of Birth', 'Age in Yrs.', 'Weight in Kgs.', 'Date of Joining', 'Quarter of Joining', 'Half of Joining', 'Year of Joining', 'Month of Joining', 'Month Name of Joining', 'Short Month', 'Day of Joining', 'DOW of Joining', 'Short DOW', 'Age in Company (Years)', 'Salary', 'Last % Hike', 'SSN', 'Phone No. ', 'Place Name', 'County', 'City', 'State', 'Zip', 'Region', 'User Name', 'Password']

all =[re.sub("[^a-zA-Z0-9]", "",n) for n in cols]

ndf =df.toDF("all")

ndf.show(5,truncate=true)

# ndf=df.toDF("empid","name","fname","mname","lname","gender","email","father's_name","mother's_name","mother_maiden_name","dob","tob")
# df.printSchema()
#
# print(cols)
#
# dt  =df.dtypes
# print(dt)
#
# # processing data using dataframe api
# pdf = df.where(col("age") >= 50).show()
# print(pdf)
#
#
# df.createOrReplaceTempView("tab")
# Process = spark.sql("select * from tab where age >=50")
# Process.show()



















# col1 = df.
# print(cols)
# print(col1)

""" api in spark
RDD 
IMP ---- dataframe 
dataset sqlapi 
"""


