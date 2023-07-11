from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)
lines = ssc.socketTextStream("ec2-43-204-236-27.ap-south-1.compute.amazonaws.com", 1234)
myconf = {
    "url":"jdbc:mysql://db.cqzgycybrmlz.ap-south-1.rds.amazonaws.com:3306/mydb?useSSL=false",
    "user":"myuser",
    "password":"12345678",
    "driver":"com.mysql.cj.jdbc.Driver"
}
#lines.pprint()
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())
        # Convert RDD[String] to DataFrame
        df = rdd.map(lambda x:x.split(",")).toDF(["name","age","city"])
        df.show()
        #process data
        hyddf=df.where(col("city")=="hyd")
        deldf=df.where(col("city")=="del")
        #processed data store in oracle or snowflake..
        hyddf.write.mode("append").format("jdbc").options(**myconf).option("dbtable","live_hyd_jul10").save()
        deldf.write.mode("append").format("jdbc").options(**myconf).option("dbtable", "live_del_jul10").save()

    except:
        pass

lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()