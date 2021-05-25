package SparkCode
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Accumulator extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)



  val sc = new SparkContext("local[*]","accumulatorhyh")
  /*
  this is how we created spark context , using the new keyword u are creating a new spark context because
  By default spark context is not available for you and this takes 2 parameters
  local[*] means the cluster is on local only , as of we do not have any other cluster made
  * means use all cores on my machine means ok i am running it on my local but ateast all core CPU can be used
  wisely so that multi threading can be achieved in place of parallelism
   */
/*
  val accum = sc.textFile("C:\\ScalaSpark\\CodeSparkScala\\src\\main\\resources\\accumulator.txt")
  val myaccum = sc.longAccumulator("my first spark accumulator")
  accum.foreach(x=> if (x =="")myaccum.add(1))
  val res = myaccum.value
  println(res)

 */
  // -----------------------------------------------------
  //val sc = new SparkContext("local[*]","WordCount11")  //-- this is for local run
  //val sc = new SparkContext()
  val input = sc.textFile("C:\\ScalaSpark\\SparkScalaPractice\\src\\main\\resources\\search_data.txt")
  //val input = sc.textFile("s3n://trendytech-manish/book-data.txt")
  val words = input.flatMap(x=>x.split(" "))
  val wordMap = words.map(x=>(x,1))
  val finalCount = wordMap.reduceByKey((a,b)=>a+b)
  val m = finalCount.collect.foreach(println)
  println(m)

}
