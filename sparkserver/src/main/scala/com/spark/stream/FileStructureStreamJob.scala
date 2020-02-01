package com.spark.stream

import com.spark.utils.SparkUtil
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object FileStructureStreamJob {

  def main(args: Array[String]): Unit = {

    val master = "local[4]"
    val appName = "Movie Recommendation File Structure Stream"
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

    ratingsStreamDF.printSchema()

    val predictionsDF = savedALSModel.transform(ratingsStreamDF)
//    val recommendationsDF = savedALSModel.recommendForItemSubset(ratingsStreamDF, 5)
//    val recWithTimeDF = recommendationsDF.withColumn("timestamp", current_timestamp())

    val query = predictionsDF
//      .withWatermark("timestamp", "1 seconds")
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()

//    spark.streams.awaitAnyTermination()
  }
}
