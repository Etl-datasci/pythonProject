from Tools.scripts.dutree import display
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
DATA =r"C:\bigdata\project\drivers\parquet\data.csv"
df = spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .option("mode", "DROPMALFORMED")\
    .option("dateformat","dd.MM.yyyy")\
    .load(DATA)

df.show()
# df.dropDuplicates().show()
# df.distinct().show()


# df.dropDuplicates(["id"]).show()
df.drop

# data = r'C:\bigdata\project\drivers\parquet\sample.csv'
# df =spark.read.format("csv").option("delimeter","|").option("header","true").option("inferSchema","true").load(data)
# df.show()
# sample_df = df.sampleBy("key",fractions={2: 1.0},seed=0)
#
# sample_df.show()




