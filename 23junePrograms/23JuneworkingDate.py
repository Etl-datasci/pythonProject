from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data=r"C:\bigdata\project\drivers\datemultiformat.csv"
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


