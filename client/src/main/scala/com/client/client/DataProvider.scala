package com.client.client

import com.client.{KafkaLocalServer, MessageSender}
import com.datatypes.{Movies, Ratings}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

object DataProvider {

  import com.configuration.Configuration._

  val ratingsFilePath = RATINGS_FILE_PATH
  val moviesFilePath = MOVIES_FILE_PATH
  val dataTimeInterval = 1000 * 7 // 7 sec

  def main(args: Array[String]): Unit = {

    println(s"Using kafka brokers at $KAFKA_KAFKA_BROKER")
    println(s"Data Message delay $dataTimeInterval")

    val kafka = KafkaLocalServer(true)
    kafka.start()
    kafka.createTopic(KAFKA_RATINGS_TOPIC)

    println(s"Cluster created")

    publishRatingsData()
    publishMoviesData()

    while (true) {
      pause(600000)
    }
  }

  def publishRatingsData(): Future[Unit] = Future {

    val sender = MessageSender(KAFKA_KAFKA_BROKER)

    val records = getListOfRatingsDataRecords(ratingsFilePath)
    var nrec = 0

    while (true) {
      records.foreach(record => {
        println(s"rating record: " + record)

        sender.writeValue(KAFKA_RATINGS_TOPIC, record.toString.getBytes())
        nrec += 1

        if (nrec % 10 == 0) {
          println(s"wrote $nrec records")
        }

        pause(dataTimeInterval)
      })
    }
  }

  def publishMoviesData(): Future[Unit] = Future {

    val sender = MessageSender(KAFKA_KAFKA_BROKER)

    val records = getListOfMoviesDataRecords(moviesFilePath)
    var nrec = 0

    while (true) {
      records.foreach(record => {
        println(s"movie record: " + record)

        sender.writeValue(KAFKA_MOVIES_TOPIC, record.toString.getBytes())
        nrec += 1

        if (nrec % 10 == 0) {
          println(s"wrote $nrec records")
        }

        pause(dataTimeInterval)
      })
    }
  }

  def getListOfRatingsDataRecords(file: String): Seq[Ratings] = {
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

  def getListOfMoviesDataRecords(file: String): Seq[Movies] = {
    var result = Seq.empty[Movies]
    val bufferedSource = Source.fromFile(file)

    for (line <- bufferedSource.getLines) {
      if (!line.contains("movieId")) {
        val record = Movies.fromString(line)
        result = record +:result
      }
    }

    bufferedSource.close()
    result
  }

  private def pause(timeInterval: Long): Unit = {
    try {
      Thread.sleep(timeInterval)
    } catch {
      case _: Throwable => // Ignore
    }
  }
}
