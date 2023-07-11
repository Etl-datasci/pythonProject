# from pyspark.sql import *
# from pyspark.sql.functions import *
#
# DATA = r'C:\bigdata\project\drivers\datemultiformat.csv'
# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#
# df1 = spark.read.format("csv").option("header", "True").option("inferSchema", "True").load(DATA)
# df = df1.withColumn("dt", to_date(col("hiredate"), ))
#
possible_formats =['yyyy-MM-dd','MM/dd/yyyy','dd-MM-yyyy','yyyy/MM/dd','dd/MM/yyyy','MMM dd, yyyy',
    'MMMM dd, yyyy','dd MMM yyyy','dd MMMM yyyy','yyyy MMM dd','yyyy-MM-dd HH:mm:ss','MM/dd/yyyy HH:mm:ss',
    'dd-MM-yyyy HH:mm:ss','yyyy/MM/dd HH:mm:ss','dd/MM/yyyy HH:mm:ss','MMM dd, yyyy HH:mm:ss','MMMM dd, yyyy HH:mm:ss',
    'dd MMM yyyy HH:mm:ss','dd MMMM yyyy HH:mm:ss','yyyy MMM dd HH:mm:ss',"%M/%d/%Y", "%M-%d-%Y", "%Y-%M-%d", "%Y/%M/%d",
    "%d-%M-%Y", "%d/%M/%Y",'dd-MM-yyyy', "dd-MMM-yyyy", "dd/MMM/yyyy", "d-MMM-yyyy", "dd/mm/yyyy", "yyyy-MMM-d", "yyyy-MM-dd"]
#
#
# def dateto(col, formats=possible_formats):
#     # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
#     return coalesce(*[to_date(col, f) for f in formats])
#
#
# df = df1.withColumn("dtFormat", dateto(col("hiredate"))).withColumn("day_of_month", dayofweek(col("dt")))\
#     .withColumn("day_of_month", dayofmonth(col("dt"))).withColumn("day_of_year", dayofyear(col("dt")))
#     # .withColumn("dtrunc", date_trunc("minute", current_timestamp())) \
#     # .withColumn("dtformat", date_format(col("dt"), "dd-MMM-yyyy-EEEE")) \
#     # .withColumn("qtr", quarter(col("dt")))
# # .withColumn("renaining_days_in_month", datediff(last_day(col("dt")),col("dt")))
# # .withColumn("date_trunc",date_trunc("year",col("dt")))
#
# df.show()
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = r"C:\bigdata\project\drivers\datemultiformat.csv"
spark.conf.set("spark.sql.session.timeZone", "IST")

df1 = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(data)

# possible_formats = ["%M/%d/%Y", "%M-%d-%Y", "%Y-%M-%d", "%Y/%M/%d", "%d-%M-%Y", "%d/%M/%Y"]
# possible_formats = ['dd-MM-yyyy', "dd-MMM-yyyy", "dd/MMM/yyyy", "d-MMM-yyyy", "dd/mm/yyyy", "yyyy-MMM-d", "yyyy-MM-dd"]


from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data=r"C:\Bigdata\drivers\datemultiformat.csv"
spark.conf.set("spark.sql.session.timeZone", "IST")

df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)


#possible_formats = ["%M/%d/%Y", "%M-%d-%Y", "%Y-%M-%d", "%Y/%M/%d", "%d-%M-%Y", "%d/%M/%Y"]
possible_formats = ['dd-MM-yyyy', "dd-MMM-yyyy", "dd/MMM/yyyy","d-MMM-yyyy", "dd/mm/yyyy", "yyyy-MMM-d", "yyyy-MM-dd"]

def dateto(col, formats=possible_formats):
    # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
    return coalesce(*[to_date(col, f) for f in formats])

df =df1.withColumn("dt",dateto(col("hiredate"))).withColumn("dayofweek", dayofweek(col("dt")))\
    .withColumn("dyofmon",dayofmonth(col("dt"))).withColumn("dayofyr",dayofyear(col("dt")))\
    .withColumn("remainingdays",datediff(last_day(col("dt")),col("dt")))\
    .withColumn("dttrunc",date_trunc("minute",current_timestamp()))\
    .withColumn("dtformat",date_format(col("dt"),"EEEE"))\
    .withColumn("qtr",quarter(col("dt"))).withColumn("weekofyr",weekofyear(col("dt")))


#quarter ... 4 acceptable values jan,feb,mar ..1, apr, may, june 2 .. jul,aug,sep,3 ..oct, nov,dec ..4
#dayofweek means 1 for a Sunday through to 7 for a Saturday
#date_trunc if u mention year ... within year everything truncated u ll get first day of year ..
#if u mention month, within month all days truncated u ll get first day of month
#if u metntion day, within day everything truncated u ll get fist day hour ...
#similarly accept day', 'dd' to truncate by day, Other options are: 'second', 'minute', 'hour', 'week', 'month', 'quarte

#dayof year: from jan 1 to specified date how many days completed u ll get in the form of int
#dayofmon ... in specied date what is the month in that month how many days completed
#above dateto function no need to convert to UDf ... ufunc = udf(tdateto)
#ithe main reason in above function ur always using spark functions not python functions so no need to convert to udf.
#df=df1.withColumn("dt",to_date(col("hiredate"),"dd-MM-yyyy"))
df.show()
