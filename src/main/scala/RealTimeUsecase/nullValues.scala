package scala.RealTimeUsecase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._


object nullValues extends App {

  val spark = SparkSession.builder()
    .appName("nullValues")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val nullDF = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("path", "src/main/resources/null_data.csv")
    .load()
    .toDF("player", "bday", "totalMatches", "total4s", "total6s")

  nullDF.show(false)

  //Step-2: Displaying rows of input file loaded


  //Step-3: Doing column-specific null replacements and showing result
  nullDF.na.fill("Unknown", Array("player"))
    .na.fill("2000-01-01", Array("bday"))
    .na.fill(-1, Array("totalMatches"))
    .na.fill("NA", Array("total4s", "total6s"))
    .show(false)

//  Note: In
//  case we want to replace nulls in all string columns
//  with "NA" and in numeric columns
//  with -1
//  , then below can be used:
//  nullDF.na.fill
//  ("NA")
//    .na.fill(-1)
//    .show()

  val dfDropNull =  nullDF.na.drop(Seq("player"))
  dfDropNull.show(false)
}



