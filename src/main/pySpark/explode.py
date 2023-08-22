from pyspark.sql import *
from pyspark.sql import functions as f

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("explode") \
    .getOrCreate()

df = spark.read.option('delimiter', '|').option("Header", True) \
    .csv(r"C:\Users\Manish\CodeSparkScala\src\main\resources\explode")
df.printSchema()
df.show()

df1 = df.withColumn("City",
                    f.explode(f.split(f.col('Address'), ','))
                    )

df1.show()
# if we want to take null value as well
df2 = df.withColumn("City",
                    f.explode_outer(f.split(f.col('Address'), ','))
                    )
df2.show()

df3 = df.withColumn("City",f.explode_outer(f.split(f.col('Address'), ','))).drop('Address').show()


# pos_explode - doesnot work with withColumn so try with select
df4 = df.select("*",f.posexplode_outer(f.split(f.col('Address'), ','))).show()

df5 = df.select("*",f.posexplode_outer(f.split(f.col('Address'), ','))).drop('Address')
df5.show()
df5.withColumnRenamed('col','city').show()



