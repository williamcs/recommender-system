package com.spark.recommend

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.rdd.ReadConf
import com.spark.utils.SparkUtil

object MovieCassandra {

  def main(args: Array[String]): Unit = {

    val master = "local[4]"
    val appName = "Movie Recommendation"
    val configKey = "spark.cassandra.connection.host"
    val configValue = "localhost"
    val logLevel = "ERROR"

    val spark = SparkUtil.getSparkSession(master, appName, configKey, configValue, logLevel)

    val df1 = spark
      .read
      .cassandraFormat("ratings", "recommendation")
      .options(ReadConf.SplitSizeInMBParam.option(32))
      .load()

    val ratingsDF = df1
      .select(df1.col("user_id"), df1.col("movie_id"), df1.col("rating"), df1.col("timestamp"))
      .withColumnRenamed("movie_id", "movieId")
      .withColumnRenamed("user_id", "userId")

    ratingsDF.show(false)
    println("ratingsDF count: " + ratingsDF.count())

    ratingsDF.printSchema()

    val df2 = spark
      .read
      .cassandraFormat("movies", "recommendation")
      .options(ReadConf.SplitCountParam.option(32))
      .load()

    val moviesDF = df2
      .select(df2.col("movie_id"), df2.col("title"), df2.col("genres"))
      .withColumnRenamed("movie_id", "movieId")

    moviesDF.show(false)
    println("moviesDF count: " + moviesDF.count())

    ExploratoryDataAnalysis.eda(spark, ratingsDF, moviesDF)

    RecommendationMLLib.train(spark, ratingsDF)
//    RecommendationML.train(spark, ratingsDF)

    spark.stop()
  }
}
