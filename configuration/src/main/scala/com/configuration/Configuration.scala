package com.configuration

object Configuration {

  val ZOOKEEPER_HOST = "localhost:2181"
  val KAFKA_BROKER = "localhost:9092"

  val RATINGS_TOPIC = "ratings"
  val DATA_TOPIC = "mdata"
  val MODELS_TOPIC = "models"

  val RATINGS_GROUP = "ratingsGroup"
  val DATA_GROUP = "wineRecordsGroup"
  val MODELS_GROUP = "modelRecordsGroup"

  val CHECKPOINT_DIR = "checkpoint"

  val MODELSERVING_PORT = 5500
}
