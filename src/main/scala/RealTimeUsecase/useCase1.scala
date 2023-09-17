package Usecase
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object useCase1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("Application 1")
    .master("local[*]")
    .getOrCreate()

  val ordersDf = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("src/main/resources/orders.csv")

  ordersDf.show(30)
  ordersDf.printSchema()
  scala.io.StdIn.readLine()
  spark.stop()


}
