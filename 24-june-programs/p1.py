from pyspark.sql import *
from pyspark.sql.functions import *
import re
import itertools

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

DATA = r'C:\bigdata\project\drivers\Credit_score.csv'
create_score = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(DATA)
# cols = create_score.columns
print("_---------------------------------------------")
win =Window.partitionBy("Customer_ID").orderBy(col("ID").asc())
df =create_score.withColumn("rank",rank().over(win))\
            .filter("rank > 1")\
            .drop("rank")\
            .dropDuplicates()
df.distinct().show()

# print(cols)
# create_score.groupby(cols)      .count() \
#     .where("count > 1") \
#     .drop("count") \
#     .show()



# create_score.printSchema()
# count_df = create_score.count()
# count_df.show()


# ddf = create_score.distinct()
#
# # cols=create_score.columns
# all = [re.sub("[^a-zA-Z]", "", n) for n in create_score.columns]
# allcols = create_score.toDF(*all)
# print("distinct Count:" + str(ddf.count()))
#
# ddf.show(truncate=False)
