/*
Dropping Rows & Columns from input using #spark

- We can drop columns using drop function.
- For dropping rows, we might choose one of the below:
 (1) Drop rows with specified column values meeting specified criteria.
 (2) Drop rows when specified column contains null values.
 (3) Drop rows which are duplicates decided based on values of specified columns.

Below is a small demo for the same.

Problem Statement: From the dataset containing below columns
"batOrder","name","role","runs","matchDay"
- Drop the column "matchDay".
- Drop the rows where "role" is Batsman.
- Drop the rows where "runs" is null.
- Drop the rows which are duplicates based on "name" & "role" column.

 */
package scala.RealTimeUsecase
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object dropRows extends App{

  val spark = SparkSession.builder()
    .appName("filterValues")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  //Step-1: Load the input file
  val df1 = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("path", "src/main/resources/filterAndDropDuplicates.csv")
    .load()
    .toDF("batOrder", "name", "role", "runs", "matchDay")

  //Step-2: Display the loaded file
  df1.show()

  //Step-3: Drop rows, columns as specified & show results
  df1.drop("matchDay")
    .filter(!col("role").isin("Batsman"))
    .dropDuplicates("name", "role")
    .na.drop(Array("runs"))
    .show(false)

}
