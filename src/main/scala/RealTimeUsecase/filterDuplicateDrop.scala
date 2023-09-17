package scala.RealTimeUsecase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object filterDuplicateDrop extends App {


  val spark = SparkSession.builder()
    .appName("filterValues")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val filterDropDF = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("path", "src/main/resources/filterAndDropDuplicates.csv")
    .load()
    .toDF("batOrder", "name", "role", "runs", "matchDay")

  //Step-2: Display the loaded file
  filterDropDF.show()

  //Step-3: Drop rows, columns as specified & show results
  filterDropDF.drop("matchDay")
    .filter(!col("role").isin("Batsman"))
    .dropDuplicates("name", "role")
    .na.drop(Array("runs"))
    .show(false)


}
