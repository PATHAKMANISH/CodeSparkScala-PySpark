from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("null_rows") \
    .getOrCreate()

sc = spark.sparkContext

multiDelimeter_DF = spark.read\
    .option("header",True)\
    .option("sep",',')\
    .option("nullValue","null")\
    .csv("file:///C:/Users/Manish/CodeSparkScala/src/main/resources/multi_delimeter.csv")

multiDelimeter_DF.show()

multiDelimeter_DF1 = multiDelimeter_DF\
    .withColumn("Physics",split(multiDelimeter_DF.MARKS,'\\|'))

multiDelimeter_DF1.show()

multiDelimeter_DF2 = multiDelimeter_DF \
    .withColumn("Physics",split(multiDelimeter_DF.MARKS,'\\|')[0])\
    .withColumn("Chemistry",split(multiDelimeter_DF.MARKS,'\\|')[1]) \
    .withColumn("Maths",split(multiDelimeter_DF.MARKS,'\\|')[2])\
    .drop(col("MARKS"))

multiDelimeter_DF2.show()

multiDelimeter_DF3 = multiDelimeter_DF1.select("ID","NAME","AGE",explode(split(multiDelimeter_DF1.MARKS,'\\|')))
multiDelimeter_DF3.show()







