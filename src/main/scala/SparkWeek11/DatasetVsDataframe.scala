package SparkWeek11
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp


case class OrdersData (order_id: Int,order_date: Timestamp,order_customer_id: Int,order_status: String)
object DatasetVsDataframe extends App {

  val spark = SparkSession.builder()
    .appName("Application 1")
    .master("local[*]")
    .getOrCreate()

  val ordersDf  = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("src/main/resources/orders.csv")

  val groupedOrdersdf =ordersDf
    .repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id","order_customer_id")
    .groupBy("order_customer_id")
    .count()

  import spark.implicits._

  val ordersDs = ordersDf.as[OrdersData]
  ordersDs.filter(x=>x.order_id <10).show()
//  ordersDs.filter(x=>x.order_id <10).show()  ----it will give in case of dataset

  ordersDf.filter("order_id<10 ").show()

 // ordersDf.filter("order_id<10 ").show()--- it will not give error here but give surprise at run time
  groupedOrdersdf.foreach(x => {
    println
  })

  ordersDf.show(30)
  ordersDf.printSchema()
  scala.io.StdIn.readLine()
  spark.stop()

}













