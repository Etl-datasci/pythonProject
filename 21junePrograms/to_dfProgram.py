from pyspark.sql import *
from pyspark.sql.functions import *
import re

spark=SparkSession.builder.master("local").appName("test").getOrCreate()
data=r"C:\bigdata\project\drivers\us-500.csv"

df=spark.read.format("csv").option("header","true").load(data)
#df.show()

#processing
ndf=df.withColumn("age",lit("testing")).withColumn("fullname",concat(col("first_name"),lit(" "),col("last_name"), lit(" "),col("state")))\
    .withColumn("phone1", regexp_replace(col("phone1"),"-",""))\
    .withColumn("email1",regexp_replace(col("email"),"[@,.]",""))\
    .withColumn("phone2",regexp_replace(col("phone2"),"_",""))
#withcolumn accepting only functions or columns as second
ndf.show()


spark = SparkSession.builder.master("local").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
data=r""
df=spark.read.format("csv").option("header","true").load(data)
all= [re.sub("[^a-zA-Z]","",n) for n in df.columns]
#ndf=df.toDF("empid","pref","fname","lname") #its failed
#ndf=df.toDF('empid', 'pref', 'fname', 'mname', 'lname', 'Gender', 'EMail', 'FathersName', 'MothersName', 'MothersMaidenName', 'DateofBirth', 'TimeofBirth', 'AgeinYrs', 'WeightinKgs', 'DateofJoining', 'QuarterofJoining', 'HalfofJoining', 'YearofJoining', 'MonthofJoining', 'MonthNameofJoining', 'ShortMonth', 'DayofJoining', 'DOWofJoining', 'ShortDOW', 'AgeinCompanyYears', 'Salary', 'LastHike', 'SSN', 'PhoneNo', 'PlaceName', 'County', 'City', 'State', 'Zip', 'Region', 'UserName', 'Password')
allcols=['EmpID', 'NamePrefix', 'FirstName', 'MiddleInitial', 'LastName', 'Gender', 'EMail', 'FathersName', 'MothersName', 'MothersMaidenName', 'DateofBirth', 'TimeofBirth', 'AgeinYrs', 'WeightinKgs', 'DateofJoining', 'QuarterofJoining', 'HalfofJoining', 'YearofJoining', 'MonthofJoining', 'MonthNameofJoining', 'ShortMonth', 'DayofJoining', 'DOWofJoining', 'ShortDOW', 'AgeinCompanyYears', 'Salary', 'LastHike', 'SSN', 'PhoneNo', 'PlaceName', 'County', 'City', 'State', 'Zip', 'Region', 'UserName', 'Password']
ndf=df.toDF(*allcols)
#ndf.show()

#toDF() its case sensitive. it's used only 2 purpose 1) rdd convert to dataframe .. 2) rename all columns
#let eg: in above dataframe u have 37 columns i want to rename all 37 columns at that time use todf.


#df.show()
ndf.createOrReplaceTempView("tab")
#res=spark.sql("select * from tab")
#res=ndf.where(col("EMail").like("%hotmail%"))
#res=ndf.groupBy(col("EMail")).agg(count(col("EMail")))
#res=ndf.withColumn("isVacinated",lit("True")).withColumn("Gender",lit("Male"))
res=ndf.withColumn("mail",split(col("Email"),"@")[1])
#gbk=res.groupBy(col("mail")).agg(count(col("mail")).alias("cnt"))
gbk=res.groupBy(col("mail")).count().orderBy(col("count").desc())
#split function split based on pattern (@)  and output results u ll get in list format
# if u mention split(col("Email"),"@")[1]) like [0] u ll get first value, if u get [1] second value ull get
gbk.show(15,False)
#withColumn ... used 2 purpose 1) add new column with value if this column not exists... 2) update existing column if that column already exists



#show display top 20 rows and first 20 chars only

'''
if u type df.columns u ll get list of dataframe columns u ll get in list.like this
 df.columns
['Emp ID', 'Name Prefix', 'First Name', 'Middle Initial', 'Last Name', 'Gender', 'E Mail', "Father's Name", "Mother's Name", "Mother's Maiden Name", 'Date of Birth', 'Time of Birth', 'Age in Yrs.', 'Weight in Kgs.', 'Date of Joining', 'Quarter of Joining', 'Half of Joining', 'Year of Joining', 'Month of Joining', 'Month Name of Joining', 'Short Month', 'Day of Joining', 'DOW of Joining', 'Short DOW', 'Age in Company (Years)', 'Salary', 'Last % Hike', 'SSN', 'Phone No. ', 'Place Name', 'County', 'City', 'State', 'Zip', 'Region', 'User Name', 'Password']

next with the help of re.sub remove special characters..'''