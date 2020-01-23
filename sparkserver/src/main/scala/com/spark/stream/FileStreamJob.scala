package com.spark.stream

import com.spark.utils.SparkUtil
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object FileStreamJob {

  def main(args: Array[String]): Unit = {

    val master = "local[4]"
    val appName = "Movie Recommendation File Stream"
    val logLevel = "ERROR"

    val spark = SparkUtil.getSparkSession(master, appName, logLevel)

    // userId, movieId, rating, timestamp
    val schema = StructType(
      Array(StructField("userId", IntegerType),
        StructField("movieId", IntegerType),
        StructField("rating", DoubleType),
        StructField("timestamp", LongType))
    )

    val ratingsFilePath = "data/test-data"
    val outputPath = "data/model/alsmodel_ml"

    val savedALSModel = ALSModel.load(outputPath)

    val ratingsStreamDF = spark.readStream
      .option("header", true)
      .schema(schema).csv(ratingsFilePath)

    val predictions = savedALSModel.transform(ratingsStreamDF)

    val query = predictions.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()

    //    spark.streams.awaitAnyTermination()
  }
}
