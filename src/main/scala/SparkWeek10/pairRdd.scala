package SparkWeek10

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object pairRdd extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","TotalSpent")  //-- this is for local run

  val input = sc.textFile("src/main/resources/customerorders.csv")

  val mappedInput = input.map(x =>(x.split(",")(0),x.split(",")(2).toFloat))

  val totalByCustomer = mappedInput.reduceByKey((x,y) =>x+y)
  val premiumCustomers = totalByCustomer.filter(x=>x._2 > 5000)

  val doubleAmount = premiumCustomers.map(x=> (x._1 ,x._2 *2))

  doubleAmount.collect.foreach(println)
  println(doubleAmount.count)

}
