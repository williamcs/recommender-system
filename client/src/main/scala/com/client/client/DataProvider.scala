package com.client.client

import com.client.{KafkaLocalServer, MessageSender}
import com.datatypes.Ratings

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

object DataProvider {

  import com.configuration.Configuration._

  val ratingsFilePath = "data/ml-latest-small/ratings.csv"
  val directory = "data/"
  val dataTimeInterval = 1000 * 7 // 1 sec

  def main(args: Array[String]): Unit = {

    //    val records = getListOfDataRecords(ratingsFilePath)
    //    records.reverse.take(10).map(println)

    println(s"Using kafka brokers at $KAFKA_BROKER")
    println(s"Data Message delay $dataTimeInterval")

    val kafka = KafkaLocalServer(true)
    kafka.start()
    kafka.createTopic(RATINGS_TOPIC)

    println(s"Cluster created")

    publishRatingData()

    while (true) {
      pause(600000)
    }
  }

  def publishRatingData(): Future[Unit] = Future {

    val sender = MessageSender(KAFKA_BROKER)

    val records = getListOfDataRecords(ratingsFilePath)
    var nrec = 0

    while (true) {
      records.foreach(record => {
        println(s"record: " + record)

        sender.writeValue(RATINGS_TOPIC, record.toString.getBytes())
        nrec += 1

        if (nrec % 10 == 0) {
          println(s"wrote $nrec records")
        }

        pause(dataTimeInterval)
      })
    }
  }

  private def pause(timeInterval: Long): Unit = {
    try {
      Thread.sleep(timeInterval)
    } catch {
      case _: Throwable => // Ignore
    }
  }

  def getListOfDataRecords(file: String): Seq[Ratings] = {

    var result = Seq.empty[Ratings]
    val bufferedSource = Source.fromFile(file)

    for (line <- bufferedSource.getLines) {
      if (!line.contains("userId")) {

        val record = Ratings.fromString(line)
        result = record +: result
      }
    }

    bufferedSource.close
    result
  }
}
