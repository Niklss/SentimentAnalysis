import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.streaming._
import java.time.{Instant, ZoneId, ZonedDateTime}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Twitter-Sentiment")
    val ssc = new StreamingContext(conf, Seconds(20))
    val spark = SparkSession
      .builder
      .appName("SentimentClassifier")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val lines = ssc.socketTextStream("10.91.66.168", 8989)
//    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val count = lines.count()
    lines.foreachRDD(
      (rdd, time) =>
        if (!rdd.isEmpty()) {
          val df = rdd.toDF
          // do something
          df.collect.foreach(println)
          println(time)
          val df_out = df.withColumn("time", lit(time.toString())). //Time when message arrived
            withColumn("class", lit(1.toString)) //Write the class of this message

          df_out.repartition(1)
              .write.format("com.databricks.spark.csv")
              .mode("append")
              .save("output.csv")

        }
    )
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    lines.print()
//    count.print()
//    words.print()

    print("GOVNO_ZALUPAAAAAAAAAAAAAAA")

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  def sentiment(value: Any): Unit ={

  }
}