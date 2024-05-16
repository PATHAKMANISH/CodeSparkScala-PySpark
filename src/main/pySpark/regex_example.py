import dbutils as dbutils
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

from pyspark.sql import functions as f

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("regex_example") \
    .getOrCreate()

df = spark.read.text(r"C:\Users\Manish\CodeSparkScala\src\main\resources\regex.txt")
df.printSchema()
df.show()

df_1 = df.withColumn("new_value",regexp_replace("value","(.*?\\-){3}","$0,")).drop("value")

df_1.show()

df_2 = df_1.withColumn("new_value_1",explode(split(df_1.new_value,'-,'))).drop("new_value")
df_2.show()

#df_3 = df_2.select(split(df_2.new_value_1,'-'))

#df_3.printSchema()
#df_3.show()

df_4 = df_2.withColumn("ID",(split(df_2.new_value_1,'-')[0])) \
    .withColumn("NAME",(split(df_2.new_value_1,'-')[1])) \
    .withColumn("AGE",(split(df_2.new_value_1,'-')[2])) \
    .drop("new_value_1")

df_4.show()

















































