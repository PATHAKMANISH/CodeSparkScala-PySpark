import spark as spark
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("duplicate records") \
    .master("local[2]").getOrCreate()

df_null = spark.read.option('delimiter', '|') \
    .option("header", True) \
    .csv(r"C:\Users\Manish\CodeSparkScala\src\main\resources\explode")

df_null.show()
# to understand which valueis null and which value is empty
df_null_1 = spark.read.option('delimiter', '|') \
    .option("header", True) \
    .option("emptyTable", "null") \
    .option("nullValue", "null") \
    .csv(r"C:\Users\Manish\CodeSparkScala\src\main\resources\explode")

df_null_1.show()

# df_null_1.filter(col("Id isNull")).count()


df_null_1.filter("Name IS NULL").show()
df_null_1.filter(df_null_1.Name.isNull()).show()

print(df_null_1.filter("Name IS NULL").count())
print(df_null_1.filter(df_null_1.Name.isNull()).count())

col1 = df_null_1.columns
print(col1)

#now I want to iterate through each column

df_res = df_null_1.select([f.count(f.col(i)).alias(i) for i in df_null_1.columns])

df_res.show()

df_res_null = df_null_1.select([f.count(f.when(f.col(i).isNull(),i)).alias(i) for i in df_null_1.columns])

df_res_null.show()

