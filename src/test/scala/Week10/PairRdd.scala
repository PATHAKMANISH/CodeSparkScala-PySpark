package Week10

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object PairRdd extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","TotalSpent")  //-- this is for local run



  val input = sc.textFile("C:\\ScalaSpark\\CodeSparkScala\\src\\main\\resources\\customerorders.csv")
  val mappedInput = input.map(x =>(x.split(",")(0),x.split(",")(2).toFloat))

  val totalByCustomer = mappedInput.reduceByKey((x,y) =>x+y)

  //totalByCustomer.filter

  //val sortedTotal = totalByCustomer .sortBy(x =>x._2)

 //val result =sortedTotal.collect
 //result.foreach(println)

 // sortedTotal.saveAsTextFile("C:\\ScalaSpark\\CodeSparkScala\\src\\main\\spark_output")

}

