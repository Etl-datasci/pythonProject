from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import *
from pyspark.sql.functions import *

appName = "JSON Parse Example"
master = "local[2]"

source = [{"attr_1": 1, "attr_2": "[{\"a\":1,\"b\":1},{\"a\":2,\"b\":2}]"}, {
    "attr_1": 2, "attr_2": "[{\"a\":3,\"b\":3},{\"a\":4,\"b\":4}]"}]
# Read the list into data frame
spark= SparkSession.builder.master("local").appName("json").getOrCreate()
df=spark.read.format("csv").option("inferSchema","true").option("header","true").load(source)
df.show()
df.printSchema()
# Function to convert JSON array string to a list
import json


def parse_json(array_str):
    json_obj = json.loads(array_str)
    for item in json_obj:
        yield (item["a"], item["b"])


# Define the schema

json_schema = ArrayType(StructType([StructField('a', IntegerType(), nullable=False), StructField('b', IntegerType(), nullable=False)]))
# Define udf

udf_parse_json = udf(lambda str: parse_json(str), json_schema)
# Generate a new data frame with the expected schema
df_new = df.select(df.attr_1, udf_parse_json(df.attr_2).alias("attr_2"))
df_new.show()
df_new.printSchema()
