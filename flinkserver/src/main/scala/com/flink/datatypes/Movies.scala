package com.flink.datatypes

import java.util.UUID

class Movies(var id: String, var movieId: Int, var title: String, var genres: String) {

  override def toString = s"Movies($id, $movieId, $title, $genres)"
}

object Movies {

  def fromString(line: String): Movies = {
    val tokens: Array[String] = line.split(",")

    if (tokens.length < 3) {
      throw new RuntimeException("Invalid record: " + line)
    }

    try {
      // uuid can replaced with guava Hashing like farmHashFingerprint64 to improve the speed
      val id = UUID.randomUUID().toString
      val movieId = tokens(0).toInt

      if (tokens.length == 4 && tokens(1).startsWith("\"") && tokens(2).endsWith("\"")) {
        val title = tokens(1) + "," + tokens(2)
        val genres = tokens(3)

        return new Movies(id, movieId, title, genres)
      }

      val title = tokens(1)
      val genres = tokens(2)

      new Movies(id, movieId, title, genres)
    } catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
    }
  }
}
