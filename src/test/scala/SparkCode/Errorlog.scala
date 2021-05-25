package SparkCode

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Errorlog  extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext ("local[*]", "wordcount2")

}
