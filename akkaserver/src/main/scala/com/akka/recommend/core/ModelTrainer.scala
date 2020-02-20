package com.akka.recommend.core

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import com.datastax.spark.connector._

object ModelTrainer {

  case object Train

  case class TrainingResult(model: MatrixFactorizationModel)

  def props(sc: SparkContext) = Props(new ModelTrainer(sc))
}

class ModelTrainer(sc: SparkContext) extends Actor with ActorLogging {

  import ModelTrainer._

  override def receive: Receive = {
    case Train => trainModel()
  }

  private def trainModel(): Unit = {
    val keyspace = context.system.settings.config.getString("cassandra.keyspace")
    val table = context.system.settings.config.getString("cassandra.table")

    val ratings = sc.cassandraTable(keyspace, table).map(record => Rating(record.get[Int]("user_id"),
      record.get[Int]("movie_id"), record.get[Double]("rating")))

    val rank = 10
    val iterations = 10
    val lambda = 0.01

    val model = ALS.train(ratings, rank, iterations, lambda)

    sender ! TrainingResult(model)

    context.stop(self)
  }
}


