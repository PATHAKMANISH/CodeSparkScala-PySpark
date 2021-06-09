package SparkWeek12

import SparkWeek12.DataFrameWriter.spark
import org.apache.spark.sql.SparkSession

object DataframeWriteAsTable extends App {

  val spark = SparkSession.builder()
    .appName("Application 11")
    .master("local[*]")
    .getOrCreate()

  val ordersDf1 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "src/main/resources/orders.csv")
    .load()

  print("orderDf has " + ordersDf1.rdd.getNumPartitions)

  val orderfRep = ordersDf1.repartition(4)

  print("orderDf has " + orderfRep.rdd.getNumPartitions)


  ordersDf1.show(false)

  ordersDf1.write
    .format("csv")
    // by default format is parquet format
    .mode(saveMode = "overwrite")
    .saveAsTable("orderTable")


  scala.io.StdIn.readLine()
  spark.stop()
}
