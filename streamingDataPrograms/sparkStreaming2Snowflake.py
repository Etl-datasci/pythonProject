from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)
lines = ssc.socketTextStream("ec2-43-204-236-27.ap-south-1.compute.amazonaws.com", 1234)

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
sfOptions = {
    "sfURL": "vqkwubf-es59758.snowflakecomputing.com",
    "sfUser": "ARJUN1234",
    "sfPassword": "Etl@1234",
    "sfDatabase": "avddb",
    "sfSchema": "public",
    "sfWarehouse": "COMPUTE_WH"
}


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
        df = rdd.map(lambda x: x.split(",")).toDF(["name", "age", "city"])
        df.show()
        # process data
        hyddf = df.where(col("city") == "hyd")
        deldf = df.where(col("city") == "del")
        # processed data store in oracle or snowflake..
        hyddf.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "htb").save()
        deldf.write.mode("overwrite").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","dtb").save()
    except:
        pass


lines.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
