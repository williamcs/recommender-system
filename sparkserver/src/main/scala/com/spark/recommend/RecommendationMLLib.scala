package com.spark.recommend

import com.spark.utils.CommonUtil
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object RecommendationMLLib {

  /**
    * Train ALS model.
    *
    * @param spark
    * @param ratingsDF
    */
  def train(spark: SparkSession, ratingsDF: DataFrame): Unit = {
    val splits = ratingsDF.randomSplit(Array(0.80, 0.20), seed = 12345L)
    val (trainingDF, testingDF) = (splits(0), splits(1))

    val numTraining = trainingDF.count()
    val numTesting = testingDF.count()
    println("Training: " + numTraining + " testing: " + numTesting)

    val ratingsRDD = trainingDF.rdd.map(row => {
      val userId = row.get(0).toString
      val movieId = row.get(1).toString
      val ratings = row.get(2).toString

      Rating(userId.toInt, movieId.toInt, ratings.toDouble)
    })

    val testRDD = testingDF.rdd.map(row => {
      val userId = row.get(0).toString
      val movieId = row.get(1).toString
      val ratings = row.get(2).toString

      Rating(userId.toInt, movieId.toInt, ratings.toDouble)
    })

    val rank = 20
    val numIterations = 10
    val lambda = 0.01
    val alpha = 10

    val model = new ALS()
      .setIterations(numIterations)
      .setBlocks(-1)
      .setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank)
      .setSeed(12345L)
      .setImplicitPrefs(false)
      .run(ratingsRDD)

    // save the model for future use
    val outputPath = "data/model/alsmodel_mllib"
    // delete output path first
    CommonUtil.deleteDir(outputPath)

    // save the model to file
    model.save(spark.sparkContext, outputPath)

    // load the saved model
    val savedALSModel = MatrixFactorizationModel.load(spark.sparkContext, outputPath)

    val rmseTest = computeRMSE(model, testRDD, true)
    println("Test RMSE: " + rmseTest)

    predict(savedALSModel, 68)
  }

  /**
    * Make prediction for specified user.
    *
    * @param model
    * @param data
    */
  def predict(model: MatrixFactorizationModel, userId: Int): Unit = {
    // make predictions, get top 5 movie predictions for user 68
    println("Rating:(UserID, MovieID, Rating)")
    println("---------------------------------")

    val topRecommendForUser = model.recommendProducts(userId, 5)
    for (rating <- topRecommendForUser) {
      println(rating.toString)
    }
    println("---------------------------------")

    // movie recommendation for a specific user, get the top 5 movie predictions for user 68
    println("Recommendations: (MovieId => Rating)")
    println("----------------------------------")
    val recommendationsUser = model.recommendProducts(userId, 5)
    recommendationsUser.map(rating => (rating.product, rating.rating)).foreach(println)
    println("----------------------------------")
  }

  /**
    * Compute the RMSE for the model.
    *
    * @param model
    * @param data
    * @param implicitPrefs
    * @return
    */
  def computeRMSE(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map {
      x => ((x.user, x.product), x.rating)
    }.join(data.map(x => ((x.user, x.product), x.rating))).values

    if (implicitPrefs) {
      println("(Prediction, Rating)")
      println(predictionsAndRatings.take(5).mkString("\n"))
    }

    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

}
