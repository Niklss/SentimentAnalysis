import org.apache.spark._
import org.apache.spark.streaming._


object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(200))

    val lines = ssc.socketTextStream("10.91.66.168", 8989)
//    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val count = lines.count()
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    lines.print()
    count.print()
    words.print()

    print("GOVNO_ZALUPAAAAAAAAAAAAAAA")

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  def sentiment(value: Any): Unit ={

  }
}