package SparkWeek11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._

object wordCount extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","Count")  //-- this is for local run

  val spark = SparkSession.builder()
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()

  val rdd1 = sc.textFile("src/main/resources/wordCount.txt")
  val rdd2 = rdd1.flatMap(x => x.split(" "))
  val rdd3 = rdd2.map(x=>(x,1))
  val rdd4 = rdd3.reduceByKey((x,y) => (x+y))
  rdd4.collect.foreach(println)

}
