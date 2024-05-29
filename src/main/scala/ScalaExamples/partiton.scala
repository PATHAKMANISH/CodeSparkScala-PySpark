package ScalaExamples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object partiton extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Multi Delimeter")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._
  val x = (1 to 10).toList
  val numberDf = sc.parallelize(x, 2).toDF()
  numberDf.show
  print(numberDf.rdd.partitions.size)

}
