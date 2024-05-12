package ScalaExamples
import java.io.File
import scala.io.Source

object ReadingFile extends App {

  val filePath = "src/main/resources/accumulator.txt"

  val file = new File(filePath)

  val ScalaFileContents: Iterator[String] = Source.fromFile(file).getLines()
  ScalaFileContents.foreach(println)



}