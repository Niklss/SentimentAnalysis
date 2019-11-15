import org.apache.spark._
import org.apache.spark.streaming._


object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("10.91.66.168", 8989)
    print(lines)
//    lines.saveAsTextFiles("")



//    lines.map(status => status.getText).map(tweet => (tweet, sentiment(tweet)))
//      .foreachRDD(rdd => rdd.collect().foreach(tuple => println(" Sentiment => " + tuple._2 + " :-: TWEET => " + tuple._1)))

    val words = lines.flatMap(_.split(" "))


    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  def sentiment(value: Any): Unit ={

  }
}