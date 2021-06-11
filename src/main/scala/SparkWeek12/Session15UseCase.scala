package SparkWeek12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object Session15UseCase extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Application 11")
    .master("local[*]")
    .getOrCreate()

val myList = List(
  (1,"2019-05-07",11559,"CLOSED"),
  (2,"2018-05-07",256,"PENDING_PAYMENT"),
  (3,"2020-05-07",11559,"COMPLETE"),
  (4,"2021-05-07",8827,"CLOSED"))


  import spark.implicits._

   val orderDf = spark.createDataFrame(myList)
    .toDF("orderid","orderdate","customerid","status")

  val newDf = orderDf
            .withColumn("orderdate",unix_timestamp(col("orderdate")
            .cast(DateType)))
            .withColumn("newid",monotonically_increasing_id())
            .dropDuplicates("orderdate","customerid")
            .drop("orderid")
            .sort("orderdate")
  newDf.printSchema()
  newDf.show(false)



  // convert to unix timestamp

  /*
  in this spark should infer the data type  ....other things are fine but column names are not human readable again
so we need to give valid column name
   */

  //from this quicly create dataframe
  /*
  rdd approach
  val rdd = spark.sparkContext.parallelize(myList)
  rdd.toDF

  but i want to minimize the use of rdd so using dataframe
  val orderDf= spark.createDataFrame(myList).toDF("orderid","orderdate","customerid","status"
   */





}
