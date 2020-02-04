package com.client.client

import com.client.{MessageListener, RecordProcessorTrait}
import com.configuration.Configuration._

object DataReader {

  def main(args: Array[String]): Unit = {
    println(s"Using kafka brokers at ${KAFKA_KAFKA_BROKER}")

    val listener = MessageListener(KAFKA_KAFKA_BROKER, KAFKA_RATINGS_TOPIC, KAFKA_RATINGS_GROUP, new RecordProcessor())
    listener.start()
  }
}
