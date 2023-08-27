from pyspark.sql.session import SparkSession

spark = SparkSession.builder.master("local[2]").appName("columnPresentOrNot").getOrCreate()

df = spark.read.option('delimiter', '|').option("Header", True) \
    .csv(r"C:\Users\Manish\CodeSparkScala\src\main\resources\explode")
df.printSchema()
df.show()

# We want to know Name is present in this dataframe or not

# 1. First Way
columns = df.schema.fieldNames()
print(columns)
# 1. Second Way
columns_Name = df.columns
print(columns_Name)


# Column present or not
if columns.count("Name")>0:
    print("Column is present")
else:
    print("Column is not present")



if columns.count("City")>0:
    print("Column is present")
else:
    print("Column is not present")

