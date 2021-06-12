package SparkWeek12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object SimpleDataFrameJoin extends App
{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Application 11")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  val orderDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "src/main/resources/orders.csv")
    .load()

  val customerDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "src/main/resources/customers.csv")
    .load()

// simple join
  val joinedDf= orderDf.join(customerDf,
                 orderDf.col("order_customer_id")===customerDf.col("customer_id"),"inner")
// make it more modular

  val joinCondition = orderDf.col("order_customer_id")===customerDf.col("customer_id")

  //val joinType = "inner"
  val joinType = "right_outer"  //so that we donot miss any information and to find things more we can sort
  val joinedDf1 = orderDf.join(customerDf,joinCondition,joinType).sort("order_customer_id")
  joinedDf1.show()
  joinedDf.show()


}
