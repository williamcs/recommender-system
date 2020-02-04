package com.spark.stream

import com.configuration.Configuration._
import com.spark.utils.SparkUtil
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object FileStructureStreamJob {

  def main(args: Array[String]): Unit = {

    val master = SPARK_MASTER
    val appName = SPARK_FILE_STRUCTURE_APP_NAME
    val logLevel = SPARK_LOG_LEVEL

    val testRatingsFilePath = TEST_RATINGS_FILE_PATH
    val outputPath = SPARK_ML_MODEL_PATH

    val spark = SparkUtil.getSparkSession(master, appName, logLevel)

    // userId, movieId, rating, timestamp
    val schema = StructType(
      Array(StructField("userId", IntegerType),
        StructField("movieId", IntegerType),
        StructField("rating", DoubleType),
        StructField("timestamp", LongType))
    )

    val savedALSModel = ALSModel.load(outputPath)

    val ratingsStreamDF = spark.readStream
      .option("header", true)
      .schema(schema).csv(testRatingsFilePath)

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
