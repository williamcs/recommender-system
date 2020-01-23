package com.spark.recommend

import com.spark.utils.CommonUtil
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * https://spark.apache.org/docs/latest/ml-collaborative-filtering.html
  */
object RecommendationML {

  case class Rating(userId: Int, movieId: Int, rating: Double, timestamp: Long)

  def train(spark: SparkSession, ratingsDF: DataFrame): Unit = {
    import spark.implicits._

    val splits = ratingsDF.map(row => {
      val userId = row.get(0).toString
      val movieId = row.get(1).toString
      val rating = row.get(2).toString
      val timestamp = row.get(3).toString

      Rating(userId.toInt, movieId.toInt, rating.toDouble, timestamp.toLong)
    })
      .randomSplit(Array(0.80, 0.20), seed = 12345L)

    val (trainingDF, testingDF) = (splits(0), splits(1))

    val numTraining = trainingDF.count()
    val numTesting = testingDF.count()
    println("Training: " + numTraining + " testing: " + numTesting)

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    // save the model for future use
    val outputPath = "data/model/alsmodel_ml"
    // delete output path first
    CommonUtil.deleteDir(outputPath)

    //    val model: PipelineModel = new Pipeline().setStages(Array(als)).fit(trainingDF)
    val model = als.fit(trainingDF)

    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")

    // save the model to file
    model.save(outputPath)

    // load the saved model
    val savedALSModel = ALSModel.load(outputPath)

    val predictions = savedALSModel.transform(testingDF).select("rating", "prediction")

    predictions.show(false)

    predictions.printSchema()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    val rmse1 = computeRMSE(savedALSModel, testingDF.toDF())
    println("Test RMSE: " + rmse1)

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(5)
    userRecs.show(false)

    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(5)
    movieRecs.show(false)

  }

  /**
    * Compute the RMSE for the model.
    *
    * @param model
    * @param data
    * @return
    */
  def computeRMSE(model: ALSModel, data: DataFrame): Double = {
    val result = model.transform(data).select("rating", "prediction")

    math.sqrt(result.rdd.map(x => math.pow(x.getAs[Double](0).toFloat - x.getAs[Float](1), 2.0)).mean())
  }
}
