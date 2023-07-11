from pyspark.sql import *
from pyspark.sql.functions import *

DATA = r'C:\bigdata\project\drivers\asl.csv'
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
print(spark)
df = spark.read.format("csv").option("header", "True").option("inferSchema", "True").load(DATA)

df.show()
ndf = df.withColumn("offers", when(col("city").isin("hyd", "blr", "nyc"), "30% off") \
                    .when(col("city") == "mas", "20% off") \
                    .when(col("city").like("%l"), "5% off") \
                    .otherwise("No offer"))
ndf.show(5, truncate=True)


def offer(c):
    if (c == "hyd"): return "40% off"
    if (c == "blr"): return "30% off"
    if (c == "mas"): return "20% off"
    if (c == "lon"): return "no offer"


uodf = udf(offer)

res = df.withColumn("Weekend offer", uodf(col("city")))

spark.udf.register("ofr", uodf)
df.createOrReplaceTempView("test")
res1 = spark.sql("select * ofr(city) todayOffers from test")
res1.show()