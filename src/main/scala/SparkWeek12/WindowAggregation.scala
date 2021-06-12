package SparkWeek12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

object WindowAggregation extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Application 11")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  val invoiceDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", "src/main/resources/windowdata.csv")
    .load()

  val myWindow = Window.partitionBy("country")
    .orderBy("weeknum")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  //.rowsBetween(-1, Window.currentRow)
  //


 val myWindow1 = invoiceDf.withColumn("RunningTotal",
              sum("invoicevalue").over(myWindow))


  myWindow1.show()




  spark.stop()

}
