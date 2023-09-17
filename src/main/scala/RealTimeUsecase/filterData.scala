package scala.RealTimeUsecase
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object filterData extends App{
  val spark = SparkSession.builder()
    .appName("filterValues")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val filterDF = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("path", "src/main/resources/filter.csv")
    .load()
    .toDF("bat_order","name","role","runs")
  filterDF.show(false)

  //Step-3: Create the list containing the required players list
  val reqList = List("Sachin", "Dravid", "Ganguly")

  //Step-4: Filtering using multiple conditions and displaying result
  filterDF.filter(col("name").isin(reqList: _*) || col("runs") > 20)
    .show(false)



}
