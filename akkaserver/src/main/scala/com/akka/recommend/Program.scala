package com.akka.recommend

import akka.actor.ActorSystem
import akka.util.Timeout
import com.akka.recommend.core.RecommenderSystem
import com.akka.recommend.core.RecommenderSystem.{GenerateRecommendations, LoadModel}
import com.configuration.Configuration.SPARK_MLLIB_MODEL_PATH
import org.apache.spark.{SparkConf, SparkContext}

object Program extends App {

  private val timeoutSeconds = 5

  import akka.pattern._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._
  implicit val timeout = Timeout(timeoutSeconds second)

  val mllibModelPath = SPARK_MLLIB_MODEL_PATH

  val system = ActorSystem("RecommendeSystem")

  val config = new SparkConf()

  config.setMaster(system.settings.config.getString("spark.master"))
  config.setAppName("recommended-content-service")
  config.set("spark.cassandra.connection.host", system.settings.config.getString("cassandra.server"))

  val sparkContext = new SparkContext(config)

  val recommenderActor = system.actorOf(RecommenderSystem.props(sparkContext))

  val loadModel = LoadModel(mllibModelPath)

  recommenderActor ! "hello"
  recommenderActor ! loadModel
//  recommenderActor ! GenerateRecommendations(68)

  val returnResult = recommenderActor ? GenerateRecommendations(68)

  Thread.sleep(1000 * 30)

  for {
    res <- returnResult
  } yield println(s"return result is: $res")

  system.terminate()

}
