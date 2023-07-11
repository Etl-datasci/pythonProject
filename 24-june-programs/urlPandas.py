from pyspark.sql import *
from pyspark.sql.functions import *
import pandas as pd

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

#
# DATA = r"C:\bigdata\project\drivers\empmysql.csv"
# df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(DATA)
#
# win=Window.partitionBy("job").orderBy(col("sal").desc())
#
# #ndf=df.orderBy(col("sal").desc())
# ndf=df.withColumn("rnk",rank().over(win))\
#     .withColumn("drank",dense_rank().over(win))\
#     .withColumn("rno",row_number().over(win))\
#     .withColumn("per",percent_rank().over(win))\
#     .withColumn("ntile",ntile(3).over(win))
#
# ndf.show(10,truncate=True)
"""----------------------------------------------------------------
#percent_rank ... give rank between 0 and 1 .. 0.1, 0,3 0,4 ...
#ntile rank ... based on ur input (3) ...
#if u get any duplicate value at that time ranking create problem.. thats y
#rank give rankings based on data, but not give rank in seq
#dense_rank() ... give rank based on value, rank always in seq manner.
#rowno not depends on value or duplicate value .. always based on row give unique rank.
ndf.show()
#2 purpose ... ranking, ..2) analyitic purpose
----------------------------------------------------------------"""

"""
----------------------------------------------------------------

second program for dataframe windows functions and date fucnctions

----------------------------------------------------------------
"""
#
# data=r"C:\bigdata\project\drivers\us-500.csv"
# df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
# df=df.withColumnRenamed("zip","sal").withColumnRenamed("first_name","fname")
#
# win=Window.orderBy(col("sal").desc())
# noneed=["rno","last_name","county","phone1"]
# ndf=df.withColumn("rno",row_number().over(win)).where(col("rno").between(30,40)).drop(*noneed)
#
# ndf.show(500)
#withColumnRenamed used to rename only one column at a time.
#drop(col) ..let eg: 1000 columns ... if u select 980 cols .. select(col1, col2, col3, col4
#if u use drop (col,col2,col3) .... u have ll columns but pls ignore these specified 3 columns ..

'''
----------------------------------------------------------------
3rd programs for windows fucntions
----------------------------------------------------------------
'''


data1=r"C:\bigdata\project\drivers\file_example_XLSX_5000.xlsx"
pdf=pd.read_excel(data1)
#print(pdf)
# pandas dataframe convert to spark dataframe
df=spark.createDataFrame(pdf)
ndf=df.withColumn("Date",to_date(col("Date"),"dd/MM/yyyy")).withColumnRenamed("Unnamed: 0","rno")
ndf.show(truncate=False)



url="https://vincentarelbundock.github.io/Rdatasets/csv/AER/BankWages.csv"
c=pd.read_csv(url)
df=spark.createDataFrame(c).withColumnRenamed("Unnamed: 0","rno")
print("Df :::::")
df.show()

joindf = ndf.join(df,ndf.rno==df.rno,"left")
print("join of 2 DF")
joindf.show()
#spark dataframe convert to python dataframe use toPandas()
op=r"C:\bigdata\project\Output_to_excelurl.csv"
print("Output_to_excel_and_url")
join_df = joindf.toPandas().to_csv(op)
print("Join DF outpot ot excel and url")
