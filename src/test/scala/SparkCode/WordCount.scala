package SparkCode
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)


  //val sc = new SparkContext("local[*]","WordCount")  //-- this is for local run
  val sc = new SparkContext()
  //val input = sc.textFile("C:\\ScalaSpark\\SparkScalaPractice\\src\\main\\resources\\search_data.txt")
  val input = sc.textFile("s3n://trendytech-manish/book-data.txt")
  val words = input.flatMap(x=>x.split(" "))
  val wordMap = words.map(x=>(x,1))
  val finalCount = wordMap.reduceByKey((a,b)=>a+b)
  val m = finalCount.collect.foreach(println)
  println(m)
  //scala.io.StdIn.readLine()   // this even when you get your answer the
  // program is running to hold the screen and with this you will be to see the dag in spark UI
}
