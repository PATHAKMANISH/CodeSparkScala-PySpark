package ScalaExamples
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object skipLine extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Skip_Line")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val skipData = sc.textFile("file:///C:/Users/Manish/CodeSparkScala/src/main/resources/skip_line.csv")




  // rddCollect.foreach(println)

}
