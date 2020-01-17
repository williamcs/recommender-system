package com.flink.sinks

import com.datastax.driver.core.{Cluster, Session}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}

object RatingsSink {

  private val HOST = "127.0.0.1"

  private val CREATE_KEYSPACE =
    """
      |CREATE KEYSPACE IF NOT EXISTS recommendation
      |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """.stripMargin

  private val CREATE_TABLE =
    """
      |CREATE TABLE IF NOT EXISTS recommendation.ratings (
      | id text,
      | user_id int,
      | movie_id int,
      | rating double,
      | timestamp bigint,
      | PRIMARY KEY(id)
      |);
    """.stripMargin

  private val INSERT_QUERY =
    """
      |INSERT INTO recommendation.ratings(id, user_id, movie_id, rating, timestamp) values (?, ?, ?, ?, ?);
    """.stripMargin

  private val builder = new ClusterBuilder {
    override def buildCluster(builder: Cluster.Builder): Cluster = {
      builder.addContactPoint(HOST).build()
    }
  }

  /**
    * Create keyspace and table before insert the data.
    */
  def createKeySpaceAndTable(): Unit = {
    var cluster: Cluster = null
    var session: Session = null

    try {
      cluster = builder.getCluster
      session = cluster.connect

      session.execute(CREATE_KEYSPACE)
      session.execute(CREATE_TABLE)

    } catch {
      case e: Exception =>
        println("exception is: " + e.getCause)
    } finally {
      closeCassandra(cluster, session)
    }

  }

  /**
    * Close cassandra.
    *
    * @param cluster
    * @param session
    */
  def closeCassandra(cluster: Cluster, session: Session): Unit = {
    if (session != null) {
      session.close()
    }

    if (cluster != null) {
      cluster.close()
    }
  }

  /**
    * Sink to cassandra.
    *
    * @param ratingsTupleStream
    */
  def sinkToCassandra(ratingsTupleStream: DataStream[(String, Int, Int, Double, Long)]): Unit = {
    createKeySpaceAndTable()

    CassandraSink
      .addSink(ratingsTupleStream)
      .setQuery(INSERT_QUERY)
      .setHost(HOST)
      .build()
  }
}
