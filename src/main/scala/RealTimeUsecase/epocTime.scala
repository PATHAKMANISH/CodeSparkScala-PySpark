package scala.RealTimeUsecase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object epocTime extends App{

  val spark = SparkSession.builder()
    .appName("Application 1")
    .master("local[*]")
    .getOrCreate()


  Logger.getLogger("org").setLevel(Level.ERROR)
  //Step-1: Creating a list
  val myList = List(
    ("Ram", "1972-01-18", 383944),
    ("Hari", "2000-02-15", 495995123),
    ("Rohan", "1993-11-23", 950553000)
  )

  //Step-2: Creating dataframe from the list
  val df1 = spark.createDataFrame(myList)
    .toDF("Name", "bday", "epoch_fabday")

  //Step-3:
  //a) Converting bday(date) -> epoch_bday(epoch_timestamp)
  //b) Converting epoch_fabday(epoch_timestamp) -> fabday(date)
  df1.withColumn("epoch_bday", unix_timestamp(col("bday").cast(DateType)))
    .withColumn("fabday", from_unixtime(col("epoch_fabday").cast(LongType), "yyyy-MM-dd"))
    .show()


}
