import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Normalizer, Tokenizer}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}

case class TrainingTweet(ItemId: Long, Sentiment: Integer, SentimentText: String)

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

    //Training


    var trainTweets = spark.read.format("com.databricks.spark.csv") //train tweets for model to learn
      .option("header", "true")
      .option("inferSchema", "true")
      .load("train.csv").as[TrainingTweet] //adding train.csv
      .withColumn("SentimentText", functions.lower(functions.col("SentimentText")))
      .withColumnRenamed("Sentiment","label")


    val tokenizer = new Tokenizer()
      .setInputCol("SentimentText")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(10000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val regression1 = new LinearSVC().setMaxIter(10) //
    val regression2 = new LogisticRegression()
                    .setMaxIter(10)
                    .setRegParam(0.01)

    val pipeline1 = new Pipeline()
      .setStages(
        Array(
          tokenizer,   // 1) tokenize
          hashingTF,   // 2) hashing
          regression1)) // 3) making regression

    val pipeline2 = new Pipeline()
      .setStages(
        Array(
          tokenizer,   // 1) tokenize
          hashingTF,   // 2) hashing
          regression2)) // 3) making regression

    trainTweets = trainTweets
      .withColumn("SentimentText", functions.regexp_replace(
        functions.col("SentimentText"),
        """[\p{Punct}&&[^.]]""", ""))

    val Array(trainingData, testData) = trainTweets.randomSplit(Array(0.7, 0.3))

    //fitting model with preprocessing data
    val model1 = pipeline1.fit(trainingData)
    val predictions1 = model1.transform(testData)

    val model2 = pipeline2.fit(trainingData)
    val predictions2 = model2.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy1 = evaluator.evaluate(predictions1)
    val accuracy2 = evaluator.evaluate(predictions2)
    var finalModel = model2
    if(accuracy1<accuracy2) {finalModel = model1}

    println("Test Error in SVM = " + (1.0 - accuracy1))
    println("Test Error in Logistic Regression = " + (1.0 - accuracy2))


    //Stream listening

    val lines = ssc.socketTextStream("10.91.66.168", 8989)
//    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val count = lines.count()
    lines.foreachRDD(
      (rdd, time) =>
        if (!rdd.isEmpty()) {
          val df = rdd.toDF
          // do something
          df.printSchema()

          var tweets = df
            .withColumn("ItemId", functions.monotonically_increasing_id())
            .withColumn("SentimentText", functions.col("value"))

          tweets.collect.foreach(println)
          //Classification

          tweets = tweets.withColumn("SentimentText",
            functions.regexp_replace(functions.col("SentimentText"),
              """[\p{Punct}&&[^.]]""", ""))

          val predictions = finalModel.transform(tweets)
            .select("SentimentText", "prediction", "probability")

          //printing result
          print(predictions.show() + " hello")

          val df_out = df.withColumn("time", lit(time.toString())). //Time when message arrived
            withColumn("class", lit(predictions.limit(1)
                                          .select("prediction")
                                          .collect.toList.head.toString)) //Write the class of this message

          df_out.repartition(1)
              .write.format("com.databricks.spark.csv")
              .mode("append")
              .save("output.csv")
        }
    )


    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  def sentiment(value: Any): Unit ={

  }
}