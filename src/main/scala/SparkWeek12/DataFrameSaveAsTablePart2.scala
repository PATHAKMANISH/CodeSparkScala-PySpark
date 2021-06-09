package SparkWeek12

import org.apache.spark.sql.SparkSession

object DataFrameSaveAsTablePart2 extends App {


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

  spark.sql("create data if not exists retail1")

 ordersDf1.write
    .format("csv")
    // by default format is parquet format
    .mode(saveMode = "overwrite")
    .bucketBy(4,"order_customer_id")
    .sortBy("order_customer_id")
    .saveAsTable("retail1.orderTable")

  spark.catalog.listTables("retail1").show()


  scala.io.StdIn.readLine()
  spark.stop()
}
