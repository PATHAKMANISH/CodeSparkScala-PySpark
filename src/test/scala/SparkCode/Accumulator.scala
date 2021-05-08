package SparkCode
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Accumulator extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext()

  val accum = sc.textFile("C:\\ScalaSpark\\CodeSparkScala\\src\\main\\resources\\accumulator.txt")
  val myaccum = sc.longAccumulator("my first spark accumulator")
  accum.foreach(x=> if (x =="")myaccum.add(1))
   val res = myaccum.value

}
