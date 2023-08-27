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

df1 = df.withColumn('Company', f.lit("TCS"))
df1.show()
df1.printSchema()
df2 = df1.drop(f.col('Address'))
df2.show()
df2.printSchema()


# column exists in dataframe or not

def addIfNotExists(df, colName):
    columns = df.columns
    if colName not in columns:
        df = df.withColumn(colName, f.lit("TCS"))

    return df


addNewDF = addIfNotExists(df, "NewCompany")

addNewDF.printSchema()


def dropIfNotExists(df, colName):
    columns = df.columns
    if colName not in columns:
        df = df.drop(colName)

    return df


dropNewDF = dropIfNotExists(addNewDF, "NewCompany")

dropNewDF.printSchema()
