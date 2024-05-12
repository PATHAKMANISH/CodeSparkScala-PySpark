from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("null_rows") \
    .getOrCreate()

sc = spark.sparkContext

null_Row_DF = spark.read\
    .option("header",True)\
    .option("nullValue","null")\
    .csv("file:///C:/Users/Manish/CodeSparkScala/src/main/resources/nullColumn.csv")

null_Row_DF.show()

df_res = null_Row_DF.select([count(i) for i in null_Row_DF.columns])
df_res.show()

df_res1 = null_Row_DF.select([count(i) for i in null_Row_DF.columns])
df_res1.show()

df_res2 = null_Row_DF.select([count(col(i).isNull()) for i in null_Row_DF.columns])
df_res2.show()

df_res3 = null_Row_DF.select([count(when(col(i).isNull(),i)).alias(i) for i in null_Row_DF.columns])
df_res3.show()





