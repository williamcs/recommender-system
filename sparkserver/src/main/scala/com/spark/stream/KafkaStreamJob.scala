package com.spark.stream

import com.configuration.Configuration._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Streaming + MLlib refer:
  * https://databricks.com/blog/2015/07/30/diving-into-apache-spark-streamings-execution-model.html
  */
object KafkaStreamJob {

  def main(args: Array[String]): Unit = {

    val master = SPARK_MASTER
    val appName = SPARK_KAFKA_STREAM_APP_NAME
    val brokers = KAFKA_KAFKA_BROKER
    val mllibModelPath = SPARK_MLLIB_MODEL_PATH

    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // create direct kafka stream with brokers and topics
    val topics = Array(KAFKA_RATINGS_TOPIC)
    val group = KAFKA_RATINGS_GROUP
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> group
    )

    val ratingsStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[Array[Byte], Array[Byte]](topics, kafkaParams)
    )

    // the saved model can't be applied so far, need to find a solution.
    val savedALSModel = MatrixFactorizationModel.load(sc, mllibModelPath)

    val userIds = ratingsStream
      .map(event => new String(event.value())).filter(_ != "")
      .map(x => x.split(",")(0).toInt)

    val result = userIds.map { userId =>
      "userId: " + userId
//      val recommendResult = savedALSModel.recommendProducts(userId, 3)
//      recommendResult
    }

    result.foreachRDD { rdd =>
      rdd.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
