import pyspark

#import SparkSession for creating a session
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import functions as fn

#and import struct types and other data types
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,ArrayType
from pyspark.sql.functions import array_contains, to_date, current_timestamp

#create an app named linuxhint
spark_app = SparkSession.builder.appName('linuxhint').getOrCreate()

# consider an array with 5 elements
my_array_data = [(1, ['A']), (2, ['B','L','B']), (3, ['K','A','K']),(4, ['K']),
                 (3, ['B','P'])]

#define the StructType and StructFields
#for the above data
schema = StructType([StructField("Item_ID", IntegerType()),StructField("CREATED_BY", ArrayType(StringType()))])

#create the dataframe and add schema to the dataframe
df = spark_app.createDataFrame(my_array_data, schema=schema)

df.show()

df1 = df.withColumn("USER",fn.explode("CREATED_BY"))
df1.show()

df2 = df1.groupBy("USER").count()
df2.show()


print("-------------------date----------------------------")
df=spark_app.createDataFrame(
    data = [ ("1","2019-06-24 12:01:19.000")],
    schema=["id","input_timestamp"])
df.printSchema()

#Timestamp String to DateType
df.withColumn("date_type",to_date("input_timestamp")) \
    .show(truncate=False)

#Timestamp Type to DateType
df.withColumn("date_type",to_date(current_timestamp())) \
    .show(truncate=False)


to_date("")




