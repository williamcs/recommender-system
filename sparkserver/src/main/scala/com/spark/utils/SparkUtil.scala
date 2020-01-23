package com.spark.utils

import org.apache.spark.sql.SparkSession

object SparkUtil {

  /**
    * Get spark session.
    *
    * @param master
    * @param appName
    * @param logLevel
    * @return
    */
  def getSparkSession(master: String, appName: String, logLevel: String): SparkSession = {
    val spark = SparkSession.builder()
      .master(master)
      .appName(appName)
      .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)

    spark
  }

  /**
    * Get spark session.
    *
    * @param master
    * @param appName
    * @param configKey
    * @param configValue
    * @param logLevel
    * @return
    */
  def getSparkSession(master: String, appName: String, configKey: String, configValue: String, logLevel: String):
  SparkSession = {
    val spark = SparkSession.builder()
      .master(master)
      .appName(appName)
      .config(configKey, configValue)
      .getOrCreate()

    spark.sparkContext.setLogLevel(logLevel)

    spark
  }

}
