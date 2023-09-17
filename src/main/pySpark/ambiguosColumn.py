import spark as spark
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("duplicate records") \
    .master("local[2]").getOrCreate()

customer_df = spark.read.option('delimiter',',') \
    .option("header",True) \
    .csv(r"C:\Users\Manish\CodeSparkScala\src\main\resources\customer_data.csv")\

customer_df.printSchema()
customer_df.show(truncate=False)

order_df = spark.read.option('delimiter',',') \
    .option("header",True) \
    .csv(r"C:\Users\Manish\CodeSparkScala\src\main\resources\order_data.csv")\

order_df.printSchema()
order_df.show(truncate=False)

order_df = order_df.withColumnRenamed("customer_id","order_customer_id")
order_df.printSchema()
#join_condition = customer_df["customer_id"] == order_df["customer_id"]
join_condition = customer_df["customer_id"] == order_df["order_customer_id"]
join_type="inner"
joinedDF = order_df.join(customer_df,join_condition,join_type)

joinedDF.select("order_id","customer_id").show(truncate=False)

# change column name before joining



# secondway