from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("duplicate records")\
                     .master("local[2]").getOrCreate()

df_duplicate = spark.read.option('delimiter','|')\
               .option("header",True)\
               .csv(r"C:\Users\Manish\CodeSparkScala\src\main\resources\explode")\

df_duplicate.printSchema()
df_duplicate.show()

df_duplicate.groupby("Id", "Name", "Address").count().show()

df_duplicate.groupby("Id", "Name", "Address").count().where("count > 1").show()
df_duplicate.groupby("Id", "Name", "Address").count().where("count > 1").drop("count").show()


windowSpec = Window.partitionBy("Name").orderBy("Id")
df_duplicate.withColumn("row_num",row_number().over(windowSpec)).show()
df_duplicate.withColumn("row_num",row_number().over(windowSpec)).where("row_num > 1").show()
df_duplicate.withColumn("row_num",row_number().over(windowSpec)).where("row_num > 1")\
                           .drop("row_num").show()

# groupby and row_number approach
