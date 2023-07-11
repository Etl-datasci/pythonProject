from pyspark.sql import *
from pyspark.sql.functions import *

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "IST")
#host="jdbc:mysql://mysqldb.cqzgycybrmlz.ap-south-1.rds.amazonaws.com:3306/my?user=myuser&password=mypassword&useSSL=false"
host="jdbc:mysql://mysqldb.cqzgycybrmlz.ap-south-1.rds.amazonaws.com:3306/myConn?useSSL=false"
df1 = spark.read.format("jdbc")\
    .option("url",host)\
    .option("user","myuser")\
    .option("password","mypassword")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable","Persons").load()


df1.show()
df1.show()
# processign that data
df=df1.withColumn("ts",current_timestamp())\
    .withColumn("newyorktime",to_utc_timestamp(from_utc_timestamp(current_timestamp(),"America/Chicago"),"IST"))\
    .withColumn("AustraliaMelbourne",to_utc_timestamp(from_utc_timestamp(current_timestamp(),"Australia/Melbourne"),"IST"))
    # .withColumn("Tokyo",to_utc_timestamp(from_utc_timestamp(current_timestamp(),"Tokyo"),"IST"))
df.show(truncate=False)
#
df.write.mode("overwrite").format("csv")\
    .option("url",host).option("user","myuser")\
    .option("password","mypassword")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable","Persons").save()



