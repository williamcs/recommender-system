package com.datatypes

import java.util.UUID

case class Ratings(var id: String, var userId: Int, var movieId: Int, var rating: Double, var timeStamp: Long) {

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder

    sb.append(userId).append(",")
    sb.append(movieId).append(",")
    sb.append(rating).append(",")
    sb.append(timeStamp)

    sb.toString
  }

}

object Ratings {

  def fromString(line: String): Ratings = {
    val tokens: Array[String] = line.split(",")

    if (tokens.length != 4) {
      throw new RuntimeException("Invalid record: " + line)
    }

    try {
      // uuid can replaced with guava Hashing like farmHashFingerprint64 to improve the speed
      val id = UUID.randomUUID().toString
      val userId = tokens(0).toInt
      val movieId = tokens(1).toInt
      val rating = tokens(2).toDouble
      val timeStamp = tokens(3).toLong

      new Ratings(id, userId, movieId, rating, timeStamp)
    } catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
    }
  }
}

