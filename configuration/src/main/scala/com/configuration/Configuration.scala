package com.configuration

import com.typesafe.config.ConfigFactory

object Configuration {

  val appConf = ConfigFactory.load("application.conf")
  val kafkaConf = appConf.getConfig("kafka")
  val sparkConf = appConf.getConfig("spark")
  val flinkConf = appConf.getConfig("flink")
  val cassandraConf = appConf.getConfig("cassandra")

  val MODEL_SERVING_PORT = appConf.getInt("model_serving_port")

  // file configuration
  val RATINGS_FILE_PATH = appConf.getString("ratings_file_path")
  val MOVIES_FILE_PATH = appConf.getString("movies_file_path")
  val TEST_RATINGS_FILE_PATH = appConf.getString("test_ratings_file_path")


  // kafka configuration
  val KAFKA_ZOOKEEPER_HOST = kafkaConf.getString("zookeeper_host")
  val KAFKA_KAFKA_BROKER = kafkaConf.getString("kafka_brokers")

  val KAFKA_RATINGS_TOPIC = kafkaConf.getString("ratings_topic")
  val KAFKA_MOVIES_TOPIC = kafkaConf.getString("movies_topic")

  val KAFKA_RATINGS_GROUP = kafkaConf.getString("ratings_group")
  val KAFKA_MOVIES_GROUP = kafkaConf.getString("movies_group")

  val KAFKA_CHECKPOINT_DIR = kafkaConf.getString("checkpoint_dir")

  // spark configuration
  val SPARK_MASTER = sparkConf.getString("master")

  val SPARK_FILE_STRUCTURE_APP_NAME = sparkConf.getString("file_structure_app_name")
  val SPARK_KAFKA_STREAM_APP_NAME = sparkConf.getString("kafka_stream_app_name")
  val SPARK_KAFKA_STRUCTURE_STREAM_APP_NAME = sparkConf.getString("kafka_structure_stream_app_name")

  val SPARK_LOG_LEVEL = sparkConf.getString("log_level")

  val SPARK_ML_MODEL_PATH = sparkConf.getString("ml_model_path")
  val SPARK_MLLIB_MODEL_PATH = sparkConf.getString("mllib_model_path")

  // flink configuration
  val FLINK_PORT = flinkConf.getInt("port")
  val FLINK_PARALLELISM = flinkConf.getInt("parallelism")

  // cassandra configuration
  val CASSANDRA_HOST = cassandraConf.getString("host")
}
