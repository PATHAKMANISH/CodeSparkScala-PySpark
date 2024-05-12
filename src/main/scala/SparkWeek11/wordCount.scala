package SparkWeek11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._

object wordCount extends App{
  val sc = new SparkContext("local[*]","Count")  //-- this is for local run

  val spark = SparkSession.builder()
    .appName("filterValues")
    .master("local[*]")
    .getOrCreate()

  val rdd1 = sc.textFile("src/main/resources/wordCount.txt")
  rdd1.collect()

  val rdd2 = rdd1.flatMap(x => x.split(" "))

  rdd2.collect()

  val rdd3 = rdd2.map(x=>(x,1))

  rdd3.collect()

  val rdd4 = rdd3.reduceByKey((x,y) => (x+y))

  rdd4.collect()

}
