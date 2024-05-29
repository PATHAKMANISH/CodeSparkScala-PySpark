from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("word_count") \
    .getOrCreate()

sc = spark.sparkContext

data=[('ABSHFJFJ12QWERT12',1),('QWERT5674OTUT1',2),('DGDGNJDJ1234UYI',3)]
df=spark.createDataFrame(data,schema="input_string string,id int")
df.show()

df.select("*").withColumn("new_col",regexp_extract(df.input_string,"(^[a-zA-Z]*([0-9]*))",1)).show()
df.select("*").withColumn("new_col",regexp_extract(df.input_string,"(^[a-zA-Z]*[0-9]*)",1))\
           .withColumn("new_col1",regexp_extract(df.input_string,"(^[a-zA-Z]*[0-9]*$",2))\
    .drop("input_string","id")\
    .show()