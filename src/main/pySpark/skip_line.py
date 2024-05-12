from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql import functions as f

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("skip_line") \
    .getOrCreate()

sc = spark.sparkContext

skipData = sc.textFile("file:///C:/Users/Manish/CodeSparkScala/src/main/resources/skip_line.csv") \
    .zipWithIndex()\
    .filter(lambda a:a[1] > 3)\
    .map(lambda a:a[0].split(','))

print(skipData)

head_rdd = skipData.first()

main_rdd = skipData.filter(lambda a:a!= head_rdd)
print(main_rdd)

df = main_rdd.toDF()
df.show()








