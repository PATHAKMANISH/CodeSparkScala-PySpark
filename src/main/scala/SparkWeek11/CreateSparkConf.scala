package SparkWeek11
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object CreateSparkConf extends App {

 val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")

  val spark =  SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  /*
  Processing
   */

  spark.stop()

}