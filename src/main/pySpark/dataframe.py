from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]") \
    .appName("SparkByExamples.com").getOrCreate()

# Create DataFrame
data = [
    ("James",None,"M"),
    ("Anna","NY","F"),
    ("Julia",None,None)
]

columns = ["name","state","gender"]
df = spark.createDataFrame(data,columns)
df.show()


