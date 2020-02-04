package com.spark.stream

import com.configuration.Configuration._
import com.datatypes.RawRatings
import com.spark.utils.SparkUtil
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.streaming.OutputMode

object KafkaStructureStreamJob {

  def main(args: Array[String]): Unit = {

    val master = SPARK_MASTER
    val appName = SPARK_KAFKA_STRUCTURE_STREAM_APP_NAME
    val logLevel = SPARK_LOG_LEVEL
    val brokers = KAFKA_KAFKA_BROKER
    val mlModelPath = SPARK_ML_MODEL_PATH

    val spark = SparkUtil.getSparkSession(master, appName, logLevel)

    import spark.implicits._

    val inputDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", KAFKA_RATINGS_TOPIC)
      .load()

    val savedALSModel = ALSModel.load(mlModelPath)

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
