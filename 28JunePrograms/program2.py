from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = r'C:\bigdata\project\drivers\world_bank.json'
df = spark.read.format("json").option("mode", "DROPMALFORMED").load(data)

df = df.withColumn("theme_namecode", explode(col("theme_namecode")))\
    .withColumn("sector_namecode", explode(col("sector_namecode")))\
    .withColumn("sector", explode(col("sector")))\
    .withColumn("projectdocs", explode(col("projectdocs")))\
    .withColumn("mjtheme_namecode", explode(col("mjtheme_namecode")))\
    .withColumn("mjtheme", explode(col("mjtheme")))\
    .withColumn("mjsector_namecode", explode(col("mjsector_namecode")))\
    .withColumn("theme_namecode", col("theme_namecode.name"))\
    .withColumn("theme_namecode", col("theme_namecode.code"))

df.printSchema()

df.show()

