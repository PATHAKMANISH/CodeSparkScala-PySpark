from pyspark.sql import *
from pyspark.sql import functions as f

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("explode") \
    .getOrCreate()

df = spark.read\
    .option("multiline",True)\
    .json(r"C:\Users\Manish\CodeSparkScala\src\main\resources\nested_json.json")
df.show()
df.show(truncate=False)
# phone has array of object [{1234-4444, home}, {5555-4444, work}]

#let us explode phone
df1 = df.withColumn('phone',f.explode('phone'))
df1.show(truncate=False)
df1.printSchema()

#### FIRST WAY

df2 = df1.select("address.city","address.street","address.zipcode","name","age","email",
                 "phone.number","phone.type")
df2.printSchema()
df2.show(truncate=False)

#### SECOND WAY

df3 = df1.select("address.*","name","age","email",
                 "phone.*")
df3.printSchema()
df3.show(truncate=False)

