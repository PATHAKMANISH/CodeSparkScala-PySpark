from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("word_count") \
    .getOrCreate()

sc = spark.sparkContext

rdd1 = sc.textFile(r"C:\Users\Manish\CodeSparkScala\src\main\resources\wordCount.txt")


rdd2 = rdd1.flatMap(lambda x:x.split(" "))


rdd3 = rdd2.map(lambda x:(x,1))


rdd4 = rdd3.reduceByKey(lambda x,y : x+y)

rdd4.foreach(print)
rdd4.saveAsTextFile(r"C:\Users\Manish\CodeSparkScala\src\main\resources\OutwordCount.txt")