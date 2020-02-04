package com.flink.topologies

import com.configuration.Configuration._
import com.datatypes.{Movies, Ratings}
import com.flink.sinks.{MoviesSink, RatingsSink}
import com.flink.sources.{MoviesSource, RatingsSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object FlinkFileJob {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setMaxParallelism(1)
    env.setParallelism(1)

    val ratingsFilePath = RATINGS_FILE_PATH
    val moviesFilePath = MOVIES_FILE_PATH

    val ratingsStream: DataStream[Ratings] = env.addSource(new RatingsSource(ratingsFilePath))
    val ratingsTupleStream: DataStream[(String, Int, Int, Double, Long)] = ratingsStream
      .map(r => (r.id, r.userId, r.movieId, r.rating, r.timeStamp))

    ratingsStream.print()

    val moviesStream: DataStream[Movies] = env.addSource(new MoviesSource(moviesFilePath))
    val moviesTupleStream: DataStream[(String, Int, String, String)] = moviesStream
      .map(r => (r.id, r.movieId, r.title, r.genres))

    moviesStream.print()

    // sink to cassandra
    RatingsSink.sinkToCassandra(ratingsTupleStream)
    MoviesSink.sinkToCassandra(moviesTupleStream)

    env.execute("Flink File Job")
  }

}
