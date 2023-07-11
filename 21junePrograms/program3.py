from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
data=r"E:\bigdata\drivers\cloth_reviews.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#df.show()
#rename columns
#Rename Column
#you can rename columns two ways 1) withColumnRenamed(old, new) used to rename only 1 column at a time
# 2) toDF methods to rename all columns
#rename column name using alias or as also another way (3rd way)
df.withColumnRenamed('Department Name', 'Dept_Name').show(2)
#rename two columns
df.withColumnRenamed('Department Name', 'Dept_Name').withColumnRenamed('Class Name', 'Cls_Nm').show(2)

#rename columns using alias
df.select(col('Clothing ID').alias('cloth_id')).show(2)

###Rename all columns once
#lower_cols = list(map(str.lower,df.columns))
#lower_cols = [col(col).alias(col.lower()) for col in df.columns]

# Replace special characters in column names
cleaned_cols = [regexp_replace(col, '[^a-zA-Z0-9_]', '').alias(col) for col in df.columns]
cdf=df.select(*cleaned_cols)

cdf.show()

#task 0: remove special characters from dataframe alternative way
import re
all= [re.sub("[^a-zA-Z]","",n) for n in df.columns]
df=df.toDF(*all)
#updating existing dataframe
#task1: senect multiple columns
cols_lst = df.columns
t1=df.select(cols_lst[2:6])
t1.show()
#except one or two,3 columns remaining all columns i want to select in this scenario use drop
df.drop(*['Age','Rating']).show(2)


#2) apply different filter logics
#remove nulls
t2 = df.na.drop()

t3=df.filter(df.Age.between(20,30))
#u can write like this also
t3=df.filter(col("Age").between(20,30)).show()
#get any value
t4=df.filter(col('ClothingID')==1080).show()
#u can write like this also
#t4=df.filter(df.ClothingID==1080)
#in clothid get any two digits id
t5=df.filter(col('ClothingID').isin([1080,829]))
t5.show(5)

t6=df.filter((col('Rating') >= 3) & (col('Age') <=30))
t6.show()
###To cxclude records use "~" before condition. Excluding records where age is not in 20,22,18
###task: get who is not in 18,20,22 this age, get distinct values and get age based on desc order.
t7=df.filter(~(col('Age').isin([20,22,18]))).select(col('Age')).distinct().sort(col('Age'),ascending=False)
t7.show()
####As we do not have Null in our data set adding them first to demo.
t8=df.withColumn('New_Rating', when(df.Rating == 5, None).otherwise(df.Rating)).dropna()
t8.show()

##############
df.select('DepartmentName').distinct().show()
#in this scenario only distinct records u ll get
df.distinct()
#if u enter this entire data is duplicate, ignore those let eg: in this dataset, u have two rows same, so ull get one record.
'''
,Clothing ID,Age,Title,Review Text,Rating,Recommended IND,Positive Feedback Count,Division Name,Department Name,Class Name
0,767,33,,Absolutely wonderful - silky and sexy and comfortable,4,1,0,Initmates,Intimate,Intimates
0,767,33,,Absolutely wonderful - silky and sexy and comfortable,4,1,0,Initmates,Intimate,Intimates
'''

#////////////////////////////////////Task
# Add one column , within how many years they ll take regire? usually retirement age 60 years of age.

###find Min & Max
df.agg({'Age':'max'}).show()

print(df.agg({'Age':'min'}).collect(), end='\n\n')

df.agg(max(col('ClothingID'))).show()

df.agg({'Rating':'Avg'}).show()

df.groupBy('DivisionName').agg({'Rating':'Avg'}).show()

df.groupBy('DivisionName').agg({'Rating':'max'}).show()

df.groupBy('DivisionName').agg({'Rating':'min'}).show()
