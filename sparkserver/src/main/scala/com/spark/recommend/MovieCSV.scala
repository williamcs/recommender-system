package com.spark.recommend

import com.spark.utils.SparkUtil

object MovieCSV {

  def main(args: Array[String]): Unit = {

    val master = "local[4]"
    val appName = "Movie Recommendation"
    val logLevel = "ERROR"

    val spark = SparkUtil.getSparkSession(master, appName, logLevel)

    val ratingsFile = "data/ml-latest-small/ratings.csv"
    val moviesFile = "data/ml-latest-small/movies.csv"

    val df1 = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratingsFile)
    val ratingsDF = df1.select(df1.col("userId"), df1.col("movieId"), df1.col("rating"), df1.col("timestamp"))
    ratingsDF.show(false)

    println("ratings count: " + ratingsDF.count())

    ratingsDF.printSchema()

    val df2 = spark.read.format("com.databricks.spark.csv").option("header", true).load(moviesFile)
    val moviesDF = df2.select(df2.col("movieId"), df2.col("title"), df2.col("genres"))
    moviesDF.show(false)

    println("movies count: " + moviesDF.count())

    // eda
    ExploratoryDataAnalysis.eda(spark, ratingsDF, moviesDF)

    // train the model
//    RecommendationML.train(spark, ratingsDF)
    RecommendationMLLib.train(spark, ratingsDF)

    spark.stop()
  }

}
