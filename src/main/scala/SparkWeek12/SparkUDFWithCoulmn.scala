package SparkWeek12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class Person(name:String,age:Int,city:String)

object SparkUDF extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def ageCheck(age:Int):String = {
    if (age>18) "Y" else "N"
  }

  val spark = SparkSession.builder()
    .appName("Application 11")
    .master("local[*]")
    .getOrCreate()

  val df22 = spark.read
    .format("csv")
    // .option("header", true) because here file does not have any header
    .option("inferSchema", true)
    .option("path", "src/main/resources/dataset1")
    .load()

  df22.show()
  df22.printSchema()
  import spark.implicits._

  val df33: Dataset[Row] = df22.toDF("name","age","city")

  val parseAgeFunction = udf(ageCheck(_:Int):String)
  val df44 =df33.withColumn("Adult",parseAgeFunction(col("age")))
  //df33.withColumn("Adult",parseAgeFunction(col($"age")))

  df44.show()


  df33.show()

  df33.printSchema()



  val ds1 = df33.as[Person]


  val df2 = ds1.toDF()
  val df3 = ds1.toDF().as[Person]


  spark.stop()

}
