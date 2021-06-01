package SparkWeek11

import org.apache.spark.sql.SparkSession

object CreateSparkSession extends App {

  val spark = SparkSession.builder()
              .appName("Application 1")
              .master("local[*]")
              .getOrCreate()

  /*
  Processing
   */

  spark.stop()
}
