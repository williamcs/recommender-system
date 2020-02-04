package com.flink.topologies

import java.util.Properties

import com.configuration.Configuration._
import com.datatypes.{Movies, Ratings}
import com.flink.sinks.{MoviesSink, RatingsSink}
import com.flink.typeschema.ByteArraySchema
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{Configuration, JobManagerOptions, TaskManagerOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration, RpcServiceSharing}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._

object FlinkKafkaJob {

  def main(args: Array[String]): Unit = {

    val port = FLINK_PORT
    val parallelism = FLINK_PARALLELISM

    val config = new Configuration()
    config.setInteger(JobManagerOptions.PORT, port)
    config.setString(JobManagerOptions.ADDRESS, "localhost")
    config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, parallelism)

    val flinkCluster = new MiniCluster(
      new MiniClusterConfiguration(config, 1, RpcServiceSharing.SHARED, null))

    try {
      // start server and create environment
      flinkCluster.start()
      val env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", port, parallelism)

      // build graph
      buildGraph(env)

      val jobGraph = env.getStreamGraph.getJobGraph()

      // submit to the server and wait for completion
      flinkCluster.executeJobBlocking(jobGraph)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * Build the execution graph.
    *
    * @param env
    */
  def buildGraph(env: StreamExecutionEnvironment): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)

    val ratingsTupleStream = createRatingsStream(env)
    ratingsTupleStream.print()

    val moviesTupleStream = createMoviesStream(env)
    moviesTupleStream.print()

    // sink to cassandra
//    RatingsSink.sinkToCassandra(ratingsTupleStream)
//    MoviesSink.sinkToCassandra(moviesTupleStream)
  }

  /**
    * Create Ratings Stream.
    *
    * @param env
    * @return
    */
  def createRatingsStream(env: StreamExecutionEnvironment): DataStream[(String, Int, Int, Double, Long)] = {
    val ratingsKafkaProps = new Properties
    ratingsKafkaProps.setProperty("bootstrap.servers", KAFKA_KAFKA_BROKER)
    ratingsKafkaProps.setProperty("group.id", KAFKA_RATINGS_GROUP)
    ratingsKafkaProps.setProperty("auto.offset.reset", "earliest")

    // create kafka consumer
    val ratingsConsumer = new FlinkKafkaConsumer[Array[Byte]](KAFKA_RATINGS_TOPIC, new ByteArraySchema, ratingsKafkaProps)

    // create input data stream
    val rawRatingsStream = env.addSource(ratingsConsumer)
    val ratingsStream = rawRatingsStream
      .map(raw => new String(raw))
      .map(line => Ratings.fromString(line))

    val ratingsTupleStream: DataStream[(String, Int, Int, Double, Long)] = ratingsStream
      .map(r => (r.id, r.userId, r.movieId, r.rating, r.timeStamp))

    ratingsTupleStream
  }

  /**
    * Create Movies Stream.
    *
    * @param env
    * @return
    */
  def createMoviesStream(env: StreamExecutionEnvironment): DataStream[(String, Int, String, String)] = {
    val topic = KAFKA_MOVIES_TOPIC
    val broker = KAFKA_KAFKA_BROKER
    val group = KAFKA_MOVIES_GROUP
    val offset = "earliest"
    val schema = new ByteArraySchema

    val consumer = new FlinkKafkaJob[Array[Byte]].getFlinkKafkaConsumer(topic, broker, group, offset, schema)
    val rawMoviesStream = env.addSource(consumer)
    val moviesStream = rawMoviesStream
      .map(raw => new String(raw))
      .map(line => Movies.fromString(line))

    val moviesTupleStream: DataStream[(String, Int, String, String)] = moviesStream
      .map(m => (m.id, m.movieId, m.title, m.genres))

    moviesTupleStream
  }

  def apply[Array[Byte]]: FlinkKafkaJob[Array[Byte]] = new FlinkKafkaJob()
}

class FlinkKafkaJob[T] {

  /**
    * Get flink kafka consumer.
    *
    * @param topic
    * @param broker
    * @param group
    * @param offset
    * @param schema
    * @return
    */
  def getFlinkKafkaConsumer(topic: String,
                            broker: String,
                            group: String,
                            offset: String,
                            schema: DeserializationSchema[T]): FlinkKafkaConsumer[T] = {
    val kafkaProps = new Properties
    kafkaProps.setProperty("bootstrap.servers", broker)
    kafkaProps.setProperty("group.id", group)
    kafkaProps.setProperty("auto.offset.reset", offset)

    // create kafka consumer
    val consumer = new FlinkKafkaConsumer[T](topic, schema, kafkaProps)

    consumer
  }
}
