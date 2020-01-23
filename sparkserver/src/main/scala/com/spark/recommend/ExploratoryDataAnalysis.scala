package com.spark.recommend

import org.apache.spark.sql.{DataFrame, SparkSession}

object ExploratoryDataAnalysis {

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
}
