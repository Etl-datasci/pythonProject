from select import select

from pyspark.sql import *
from pyspark.sql.functions import *
import re
DATA = r'C:\bigdata\project\drivers\Credit_score.csv'
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
df =spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true").load(DATA)

# cols =df.columns
all_cols =['ID', 'Customer_ID', 'Month', 'Name', 'Age', 'SSN', 'Occupation', 'Annual_Income', 'Monthly_Inhand_Salary', 'Num_Bank_Accounts', 'Num_Credit_Card', 'Interest_Rate', 'Num_of_Loan', 'Type_of_Loan', 'Delay_from_due_date', 'Num_of_Delayed_Payment', 'Changed_Credit_Limit', 'Num_Credit_Inquiries', 'Credit_Mix', 'Outstanding_Debt', 'Credit_Utilization_Ratio', 'Credit_History_Age', 'Payment_of_Min_Amount', 'Total_EMI_per_month', 'Amount_invested_monthly', 'Payment_Behaviour', 'Monthly_Balance']
# print(cols)

# remove the underscore (_) from the header names
new_columns = [col(column).alias(column.replace('_', '')) for column in df.columns]
df = df.select(new_columns)

# remove the underscore (_) from the table data
df = df.select([regexp_replace(col(column), '[_#%$*&@]', '').alias(column) for column in df.columns])

# remove the  - from NumOfLoan column
df = df.withColumn('numOfLoan', regexp_replace(col('NumofLoan'), '[^a-zA-Z0-9\s]', '')).fillna('')
df.show()

'''partition the data by months'''






# windowSpec = Window.partitionBy("Month").orderBy("Customer_ID")
#
# df = df.select([col("ID").fillna(lag(col("Name")).over(windowSpec)).fillna(lead(col("Name"))\
#                               .over(windowSpec)) for column in df.columns])
# df.show()


# data = 'ID', 'Customer_ID', 'Month', 'Name', 'Age', 'SSN', 'Occupation', 'Annual_Income', 'Monthly_Inhand_Salary', 'Num_Bank_Accounts', 'Num_Credit_Card', 'Interest_Rate', 'Num_of_Loan', 'Type_of_Loan', 'Delay_from_due_date', 'Num_of_Delayed_Payment', 'Changed_Credit_Limit', 'Num_Credit_Inquiries', 'Credit_Mix', 'Outstanding_Debt', 'Credit_Utilization_Ratio', 'Credit_History_Age', 'Payment_of_Min_Amount', 'Total_EMI_per_month', 'Amount_invested_monthly', 'Payment_Behaviour', 'Monthly_Balance'



# ndf.show()



# datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
#                      ("2016-02-29", "2016-02-29 08:08:08.999"),
#                      ("2017-10-31", "2017-12-31 11:59:59.123"),
#                      ("2019-11-30", "2019-08-31 00:00:00.000")
#                 ]
# cols =["date", "date_time"]
# dtdf = spark\
#     .createDataFrame(datetimes)\
#     .toDF(*cols)\
#
#
# dtdf.show(truncate=True)

# emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
#     (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
#     (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
#     (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
#     (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
#     (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
#     (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
#     (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
#     (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
#     (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]
# empdf = spark.createDataFrame(emp, ["id", "name", "dept", "salary", "date"])
#
# empdf.printSchema()
# empdf.show()
#
#
# df= empdf.withColumn("next_month",add_months("date",1))\
#     .withColumn("add_day",date_add("date",10))
#
# df.show()
#
# DATA = r'C:\bigdata\project\ex.csv'
# df = spark.read.format("csv").option("header", "true") \
#     .option("inferSchema", "true").load(DATA)
# df.show()
# print(cols)
# # df.show()
#
# df = df.withColumn("Temp_day", dayofmonth("date"))
# df_lt28 = df.filter(df["temp_day"] < 28).select('id', 'name', 'amnt', 'addrs', 'date')
# # df_lt28.show()
# df_gt28 = df.filter(df["temp_day"] >= 28).select('id', 'name', 'amnt', 'addrs', 'date')
# # df_gt28.show()
#
# dt_diff = expr("date_add(date,28-temp_day)")
# df_gt28 = df_gt28.withColumn("date",dt_diff)
#
# df =df_lt28.union(df_gt28).drop(col("temp_day"))
# df.show()
