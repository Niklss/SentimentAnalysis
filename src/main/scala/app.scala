import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.streaming._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Normalizer, Tokenizer}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}

case class TrainingTweet(ItemId: Long, Sentiment: Integer, SentimentText: String) // For each tweeter in reading train data

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SentimentClassifier") //set Spark configuration
    val ssc = new StreamingContext(conf, Seconds(20)) //Set the StreamingContext
    val spark = SparkSession                           //Set the Spark Session
      .builder
      .appName("SentimentClassifier")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //Training part

    var trainTweets = spark.read.format("com.databricks.spark.csv") //read train tweets for model to learn
      .option("header", "true")
      .option("inferSchema", "true")
      .load("train.csv").as[TrainingTweet] //adding train.csv and write it to special class TrainingTweet
      .withColumn("SentimentText", functions.lower(functions.col("SentimentText"))) //convert to lower case
      .withColumnRenamed("Sentiment","label")


    val tokenizer = new Tokenizer()  // Tokenizer to tokenize each work in the tweet
      .setInputCol("SentimentText")
      .setOutputCol("words")

    val hashingTF = new HashingTF()  // Specify feature extractor
      .setNumFeatures(10000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val regression1 = new LinearSVC().setMaxIter(10) // specify regression model with some parameters
    val regression2 = new LogisticRegression()
                    .setMaxIter(10)
                    .setRegParam(0.01)

    val pipeline1 = new Pipeline()   //Create ML pipeline for every classification algorithm
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

    trainTweets = trainTweets          // Pre-processing of training data
      .withColumn("SentimentText", functions.regexp_replace(
        functions.col("SentimentText"),
        """[\p{Punct}&&[^.]]""", ""))   //delete punctuation from the tweet text

    //Split the data to training and validation data sets 70% - 30%
    val Array(trainingData, testData) = trainTweets.randomSplit(Array(0.7, 0.3))

    //fitting model with pre-processing data
    val model1 = pipeline1.fit(trainingData)
    val predictions1 = model1.transform(testData)
    val model2 = pipeline2.fit(trainingData)
    val predictions2 = model2.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()  //Compare results on training with validate data
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    // evaluate accuracy of the models
    val accuracy1 = evaluator.evaluate(predictions1)
    val accuracy2 = evaluator.evaluate(predictions2)
    var finalModel = model2
    if(accuracy1<accuracy2) {finalModel = model1}

    println("Test Error in SVM = " + (1.0 - accuracy1))
    println("Test Error in Logistic Regression = " + (1.0 - accuracy2))


    //Stream listening

    val lines = ssc.socketTextStream("10.91.66.168", 8989)
    lines.foreachRDD(    //DStream -> RDD
      (rdd, time) =>       // Take RDD and Time
        if (!rdd.isEmpty()) {   // Check if it is empty
          val df = rdd.toDF     //RDD -> Data Frame


          // Data pre-processing

          var tweets = df
            .withColumn("ItemId", functions.monotonically_increasing_id())
            .withColumn("SentimentText", functions.col("value"))


          tweets = tweets.withColumn("SentimentText",
            functions.regexp_replace(functions.col("SentimentText"),
              """[\p{Punct}&&[^.]]""", ""))


          //Classification

          val predictions = finalModel.transform(tweets)
            .select("SentimentText", "prediction", "probability")


          //Results post-processing

          val df_out = df.withColumn("time", lit(time.toString())). //Time when message arrived
            withColumn("class", lit(predictions.limit(1)
                                          .select("prediction")
                                          .collect.toList.head.toString)) //Write the class of this message

          //Writing to output (CSV)

          df_out.repartition(1)
              .write.format("com.databricks.spark.csv")
              .mode("append")
              .save("output.csv")
        }
    )


    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}