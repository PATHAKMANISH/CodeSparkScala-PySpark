from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("nullValue_EachColumn") \
    .getOrCreate()

data = [(1,'Sagar',23),(2,None,33),(None,'John',40),(5,'Alex',None),(4,'Kim',20)]

schema = StructType([ \
    StructField("ID",IntegerType(),True), \
    StructField("Name",StringType(),True), \
    StructField("Age",IntegerType(),True),
    ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

print(df.columns)
print(df.schema)
for i in df.columns:
    print(i)

df_1 = df.select([count(col(i)) for i in df.columns])
df_1.show()

df_2 = df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])

df_2.show()