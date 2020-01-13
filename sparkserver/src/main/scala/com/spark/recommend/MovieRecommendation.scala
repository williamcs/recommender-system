package com.spark.recommend

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object MovieRecommendation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Movie Recommendation")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val ratingsFile = "data/ml-latest-small/ratings.csv"
    val moviesFile = "data/ml-latest-small/movies.csv"

    val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratingsFile)
    val ratingsDF = df1.select(df1.col("userId"), df1.col("movieId"), df1.col("rating"), df1.col("timestamp"))
    ratingsDF.show(false)

    val df2 = spark.read.format("com.databricks.spark.csv").option("header", true).load(moviesFile)
    val moviesDF = df2.select(df2.col("movieId"), df2.col("title"), df2.col("genres"))
    moviesDF.show(false)

    // eda
    eda(spark, ratingsDF, moviesDF)

    // train the model
    train(spark, ratingsDF)

    spark.stop()
  }

  /**
    * Train ALS model.
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
      val userId = row.getString(0)
      val movieId = row.getString(1)
      val ratings = row.getString(2)

      Rating(userId.toInt, movieId.toInt, ratings.toDouble)
    })

    val testRDD = testingDF.rdd.map(row => {
      val userId = row.getString(0)
      val movieId = row.getString(1)
      val ratings = row.getString(2)

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
    val outputPath = "data/model/alsmodel"
    // delete output path first
    deleteDir(outputPath)

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
    * Exploratory data analysis
    *
    * @param spark
    * @param ratingsDF
    * @param moviesDF
    */
  def eda(spark: SparkSession, ratingsDF: DataFrame, moviesDF: DataFrame): Unit = {
    ratingsDF.createOrReplaceTempView("ratings")
    moviesDF.createOrReplaceTempView("movies")

    // explore and query with spark data frames
    val numRatings = ratingsDF.count()
    val numUsers = ratingsDF.select(ratingsDF.col("userId")).distinct().count()
    val numMovies = moviesDF.select(moviesDF.col("movieId")).distinct().count()
    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numMovies + " movies.")

    // get the max, min ratings along with the count of users who have rated a movie
    val sql1 =
      """
        |select movies.title, movierates.maxr, movierates.minr, movierates.cntu
        |from (SELECT ratings.movieId, max(ratings.rating) as maxr,
        |      min(ratings.rating) as minr,count(distinct userId) as cntu FROM ratings group by ratings.movieId
        |      ) movierates
        |      join movies on movierates.movieId = movies.movieId order by movierates.cntu desc
      """.stripMargin
    val result1 = spark.sql(sql1)
    result1.show(false)

    // show the top 10 most active users and how many times they rated a movie
    val mostActiveUsers = spark.sql("select ratings.userId, count(*) as cnt from ratings " +
      "group by ratings.userId order by cnt desc limit 10")
    mostActiveUsers.show(false)

    // find the movies that user 68 rated higher than 4
    val sql2 =
      """
        |select ratings.userId, ratings.movieId, ratings.rating, movies.title
        |from ratings join movies
        |on ratings.movieId = movies.movieId
        |where ratings.userId = 68 and ratings.rating > 4
      """.stripMargin
    val result2 = spark.sql(sql2)
    result2.show(false)
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

  /**
    * Delete directory.
    *
    * @param path
    */
  def deleteDir(path: String): Unit = {
    val dir = new File(path)

    if (!dir.exists()) return

    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f.getPath)
      } else {
        f.delete()
      }
    })

    dir.delete()
  }

}
