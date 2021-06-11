package SparkWeek12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object SparkAggregation extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Application 11")
    .master("local[*]")
    .getOrCreate()

  val invoiceDf =  spark.read
    .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("path","src/main/resources/order_data.csv")
      .load()

 // invoiceDf.show(false)
//Programatic Approach - Column object expression
  val res1 = invoiceDf.select(
    count("*").as("RowCount"),
    sum("Quantity").as("TotalQuantity"),
    avg("UnitPrice").as("AvgPrice"),
    countDistinct("InvoiceNo").as("CountDistinct")
  )
    res1.show(false)

//String expression method

val res2 =invoiceDf.selectExpr(

    "count(*) as RowCount",
     "count(StockCode) as StockCount" ,  // ....it will not count null values

    "sum(Quantity) as TotalQuantity",
    "avg(unitPrice) as AvgPrice",
    "count(Distinct(InvoiceNo)) as CountDistinct"
  )
  res2.show(false)
spark.stop()

  // spark Sql

  invoiceDf.createOrReplaceTempView("sales") // it is liketable distributed across various machine
  spark.sql("select count(*),sum(Quantity)avg(UnitPrice),count(distinct(InvoiceNo)) from sales")
    .show()
}
