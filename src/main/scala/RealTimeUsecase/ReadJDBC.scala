package RealTimeUsecase

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object ReadJDBC extends App {

  // Create SparkSession in spark 2.x or later
  val spark = SparkSession.builder().master("local[*]")
    .appName("SparkByExamples.com")
    .getOrCreate()


  Logger.getLogger("org").setLevel(Level.ERROR)
  // Connection Properties
  val connProp = new Properties()
  connProp.setProperty("driver", "com.mysql.cj.jdbc.Driver");
  connProp.put("user", "root")
  connProp.put("password", "Rkslputra@1")

  // Read from MySQL Table
  val df = spark.read
    .jdbc("jdbc:mysql://localhost:3306/manish", "employee", connProp)

  // Show DataFrame
  df.show()

  // Using format()
  val df2 = spark.read
    .format("jdbc")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("url", "jdbc:mysql://localhost:3306/manish")
    .option("dbtable", "employee")
    .option("user", "root")
    .option("password", "Rkslputra@1")
    .load()

  df2.show()

  import spark.implicits._

  val columns = Seq("language", "users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  val rdd = spark.sparkContext.parallelize(data)


  val dfFromRDD1 = rdd.toDF("language", "users_count")
  dfFromRDD1.printSchema()

  dfFromRDD1.show()

  val actual_value = List("Java", "Python")

  dfFromRDD1.createOrReplaceTempView("step")

  val res = for (a <- actual_value) {

    val agg_df = spark.sql(s"""select users_count from step where language = '||${a}||' """)

  } 


}

