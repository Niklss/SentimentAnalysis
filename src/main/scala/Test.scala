import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Sentiment")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import org.apache.spark.ml.feature.FeatureHasher

    val dataset = spark.createDataFrame(Seq(
      (2.2, true, "1", "foo"),
      (3.3, false, "2", "bar"),
      (4.4, false, "3", "baz"),
      (5.5, false, "4", "foo")
    )).toDF("real", "bool", "stringNum", "string")

    val hasher = new FeatureHasher()
      .setInputCols("string")
      .setOutputCol("features")

    val featurized = hasher.transform(dataset)
    featurized.show(false)
  }
}


