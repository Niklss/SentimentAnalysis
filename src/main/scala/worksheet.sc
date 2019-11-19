import org.apache.spark._
import org.apache.spark.streaming._



val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
val ssc = new StreamingContext(conf, Seconds(1))

val lines = ssc.socketTextStream("10.91.66.168", 8989)


ssc.start() // Start the computation
ssc.awaitTermination()