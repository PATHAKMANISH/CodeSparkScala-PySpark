from pyspark.sql import *
from pyspark.sql import functions as f

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("explode") \
    .getOrCreate()



simpleData = [("James","Sales","NY",90000,34,10000),
              ("Michael","Sales","NY",86000,56,20000),
              ("Robert","Sales","CA",81000,30,23000),
              ("Maria","Finance","CA",90000,24,23000),
              ("Raman","Finance","CA",99000,40,24000),
              ("Scott","Finance","NY",83000,36,19000),
              ("Jen","Finance","NY",79000,53,15000),
              ("Jeff","Marketing","CA",80000,25,18000),
              ("Kumar","Marketing","NY",91000,50,21000)
              ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)
df.groupBy("department").sum("salary").show(truncate=False)

df.groupBy("department").count()