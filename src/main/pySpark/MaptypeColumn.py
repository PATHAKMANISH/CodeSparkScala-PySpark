# how to deal MapType column
# if you are working with bigger table which conatin many column at that time
# it's better to merge few columns in to one column ...like space also will get reduced
# example : FirstName->xyz ,Lastname->xyz ,email ->xyz ,mobile->xyz means in key value format
from pyspark.sql import *
from pyspark.sql import functions as f

# file_loc = r"C:\Users\Manish\CodeSparkScala\src\main\resources\mapTypeColumn.csv"
# file_type ="csv"
# delimiter="|"
spark = SparkSession.builder \
    .master("local[2]") \
    .appName("mapTypeColumn") \
    .getOrCreate()

df = spark.read.format("csv").option("inferSchema", "true") \
    .option("header", True) \
    .option('delimiter', '|') \
    .option("multiline", True) \
    .load(r"C:\Users\Manish\CodeSparkScala\src\main\resources\mapTypeColumn.csv")
df.show()
df.show(truncate=False)

# Create MapType column

df1 = df.withColumn("employee_details",
                    f.create_map(f.lit('FirstName'), f.col('FirstName'),
                                 f.lit('LastName'), f.col('LastName'),
                                 f.lit('EMAIL'), f.col('EMAIL'),
                                 f.lit('PHONE_NUMBER'), f.col('PHONE_NUMBER')
                                 ))
df2 = df1.drop('FirstName').drop('LastName').drop('EMAIL').drop('PHONE_NUMBER')
df2.show(truncate=False)

df3 = df2.withColumn('first_name',df2.employee_details['FirstName'])
df3.show(truncate=False)

df4 = df2.withColumn('first_name',df2.employee_details.getItem('FirstName'))
df4.show(truncate=False)

# THIS WILL CREATE A MAP WITH 4 KEYS AND 4 VALUES

