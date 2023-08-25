from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col
from pyspark.sql.types import IntegerType,StringType
spark = SparkSession.builder.master("local[2]").appName("udf_example").getOrCreate()

# udf can be created and applied on dataframe without making them register individually
# why to use - if some function is to be used on multiple dataframe
# how to use a) by registering them to spark   and b ) by using decorators

df1 = spark.createDataFrame([('Manish',1,1000,400),('Rahul',2,14000,500),('Amit',3,160000,800),('Amol',4,180000,890)],['Name','RollNo','Fees','Fine'])
df1.show(truncate=False)

def sum(x,y):
    return x+y;

sum_udf = udf(sum,IntegerType()) #registering
# we can also register this in spark session
df2 = df1.withColumn('Total_Amount',sum_udf('Fees','Fine')) # registering
df2.show(truncate=False)


# second approach - decorator
@udf(returnType=StringType())
def uppercase(str):    # this function is a plain python function which will take one tring at a time
                       # but since we have registered it as udf decorator we can run on each row of dataframe

 return str.upper()
df2.select('*',uppercase(col('Name')).alias('UpperCase Name')).show()

# 2 ways to add new column is using with column and second one is using select clause
# select is optimised when we have to add many columns