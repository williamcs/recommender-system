package com.spark.stream

import com.configuration.Configuration.RATINGS_TOPIC
import com.spark.utils.SparkUtil
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.streaming.OutputMode

case class RawRatings(userId: Int, movieId: Int, timeStamp: Long)

object KafkaStructureStreamJob {

  def main(args: Array[String]): Unit = {

    val master = "local[4]"
    val appName = "Movie Recommendation Kafka Stream"
    val logLevel = "ERROR"
    val brokers = "localhost:9092"
    val outputPath = "data/model/alsmodel_ml"

    val spark = SparkUtil.getSparkSession(master, appName, logLevel)

    import spark.implicits._

    val inputDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", RATINGS_TOPIC)
      .load()

    val savedALSModel = ALSModel.load(outputPath)

    val ratingsStringDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

    val ratingsDF = ratingsStringDF
//      .map(row => row.getAs[String](0))
      .map(_.split(","))
      .map(r => RawRatings(r(0).toInt, r(1).toInt, r(3).toLong))

    ratingsDF.printSchema()

    val predictionsDF = savedALSModel.transform(ratingsDF)

    val query = predictionsDF.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

}
