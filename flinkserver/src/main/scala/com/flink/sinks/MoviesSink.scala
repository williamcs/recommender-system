package com.flink.sinks

import com.configuration.Configuration._
import com.datastax.driver.core.{Cluster, Session}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.{CassandraSink, ClusterBuilder}

object MoviesSink {

  private val HOST = CASSANDRA_HOST

  private val CREATE_KEYSPACE =
    """
      |CREATE KEYSPACE IF NOT EXISTS recommendation
      |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """.stripMargin

  private val CREATE_TABLE =
    """
      |CREATE TABLE IF NOT EXISTS recommendation.movies (
      | id text,
      | movie_id int,
      | title text,
      | genres text,
      | PRIMARY KEY(id)
      |);
    """.stripMargin

  private val INSERT_QUERY =
    """
      |INSERT INTO recommendation.movies(id, movie_id, title, genres) values (?, ?, ?, ?);
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
    * @param moviesTupleStream
    */
  def sinkToCassandra(moviesTupleStream: DataStream[(String, Int, String, String)]): Unit = {
    createKeySpaceAndTable()

    CassandraSink
      .addSink(moviesTupleStream)
      .setQuery(INSERT_QUERY)
      .setHost(HOST)
      .build()
  }
}
