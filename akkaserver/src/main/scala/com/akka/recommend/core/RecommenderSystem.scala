package com.akka.recommend.core

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object RecommenderSystem {

  case object Train
  case class LoadModel(modelPath: String)
  case class GenerateRecommendations(userId: Int)
  case class Recommendation(contentItemId: Int, rating: Double)
  case class Recommendations(items: Seq[Recommendation])

  def props(sc: SparkContext) = Props(new RecommenderSystem(sc))

}

class RecommenderSystem(sc: SparkContext) extends Actor with ActorLogging {

  import RecommenderSystem._

  var model: Option[MatrixFactorizationModel] = None

  override def receive: Receive = {
    case "hello" => doPrint()
    case Train => trainModel()
    case LoadModel(modelPath) => loadModel(modelPath)
    case GenerateRecommendations(userId) => generateRecommendations(userId, 10)
    case ModelTrainer.TrainingResult(model) => storeModel(model)
  }

  private def trainModel(): Unit = {
    // start a separate actor to train the recommendation system
    // this enables the service to continue service requests when it learns new recommendations.
    val trainer = context.actorOf(ModelTrainer.props(sc), "model-trainer")
    trainer ! ModelTrainer.Train
  }

  private def generateRecommendations(userId: Int, count: Int): Unit = {
    log.info(s"Generating ${count} recommendations for user with ID ${userId}")

    // generate recommendations based on the machine learning model
    // when there is no trained model return an empty list instead
    val results = model match {
      case Some(m) => m.recommendProducts(userId, count)
        .map(rating => Recommendation(rating.product, rating.rating))
        .toList
      case None => Nil
    }

    println("results is: " + results.mkString(" "))

    sender ! Recommendations(results)
  }

  private def storeModel(model: MatrixFactorizationModel): Unit = {
    this.model = Some(model)
  }

  private def loadModel(mllibModelPath: String): Unit = {
    this.model = Some(MatrixFactorizationModel.load(sc, mllibModelPath))

    log.info(s"The mllib model is loaded with path $mllibModelPath")
  }

  private def doPrint(): Unit = {
    println("do print test")
    println("Hello World!")
  }
}
