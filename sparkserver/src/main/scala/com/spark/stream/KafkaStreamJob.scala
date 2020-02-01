package com.spark.stream

import com.configuration.Configuration._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaStreamJob {

  def main(args: Array[String]): Unit = {

    val master = "local[4]"
    val appName = "Movie Recommendation Kafka Stream"
    val brokers = "localhost:9092"
    val modelPath = "data/model/alsmodel_mllib"

    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // create direct kafka stream with brokers and topics
    val topics = Array(RATINGS_TOPIC)
    val group = RATINGS_GROUP
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

    //    ratingsStream.foreachRDD { rdd =>
    //      rdd.foreachPartition { partition =>
    ////        val savedALSModel = MatrixFactorizationModel.load(sc, modelPath)
    //
    //        while (partition.hasNext) {
    //          val record = new String(partition.next().value())
    //
    //          if (record != "") {
    //            val userId = record.split(",")(0).toInt
    //            println("userId: " + userId)
    //          }
    //        }
    //      }
    //
    //    }

    /*
    ratingsStream.foreachRDD { rdd =>
      val savedALSModel = MatrixFactorizationModel.load(sc, modelPath)
      rdd.foreachPartition { partition =>
        val userList = partition
          .map(record => new String(record.value()))
          .filter(_ != "")
          .map(x => x.split(",")(0).toInt)

        while (userList.hasNext) {
          val userId = userList.next()
          val recommendResult = savedALSModel.recommendProducts(userId, 5)
          println("recommendResult: " + recommendResult)
        }
      }
    }*/

    /*
    ratingsStream.foreachRDD { rdd =>
      val savedALSModel = MatrixFactorizationModel.load(rdd.sparkContext, modelPath)

      rdd.foreach { record =>
        val row = new String(record.value())
        if (row != "") {
          val userId = row.split(",")(0).toInt
          println("userId: " + userId)

          val recommendResult = savedALSModel.recommendProducts(userId, 5)
          println("recommendResult: " + recommendResult)
        }
      }
    }*/

    val savedALSModel = MatrixFactorizationModel.load(sc, modelPath)

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

    /*
    ratingsStream.foreachRDD { rdd =>
      val userIds = rdd.map(record => new String(record.value()))
        .filter(_ != "")
        .map(x => x.split(",")(0).toInt)

      val result = userIds.map { userId =>
        val recommendResult = savedALSModel.recommendProducts(userId, 3)
        recommendResult
      }

      result.foreach(println)
    }*/

    ssc.start()
    ssc.awaitTermination()
  }
}
