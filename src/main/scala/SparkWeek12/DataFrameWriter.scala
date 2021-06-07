package SparkWeek12

import org.apache.spark.sql.SparkSession

object DataFrameWriter extends App {



  val spark = SparkSession.builder()
    .appName("Application 1")
    .master("local[*]")
    .getOrCreate()

  val ordersDf = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("src/main/resources/orders.csv")


/*
  ordersDf.show(30)
  ordersDf.printSchema()
  scala.io.StdIn.readLine()
  spark.stop()

  ordersDf.show(30)
  ordersDf.printSchema()

*/
  ordersDf.write
    .format("csv")
    // by default format is parquet format
    .mode(saveMode = "overwrite")
    .option("path","src/main/resources/newResult1")
    .save()
  scala.io.StdIn.readLine()
  spark.stop()

}