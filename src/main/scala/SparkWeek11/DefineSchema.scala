package SparkWeek11

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object DefineSchema extends App {

  val spark = SparkSession.builder()
    .appName("Application 11")
    .master("local[*]")
    .getOrCreate()

  val orderSchema =
    StructType(List(

      StructField("order_id",IntegerType),
      StructField("orderdate",TimestampType),
      StructField("customerid",IntegerType),
      StructField("status",StringType)
    ))
    val ordersDff  = spark.read
        .format("csv")
        .option("header",true)
        .schema(orderSchema)
        .option("path","src/main/resources/orders.csv")
        .load()

    ordersDff.show(false)

  spark.stop()

}
