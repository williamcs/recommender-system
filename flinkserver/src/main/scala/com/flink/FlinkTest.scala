package com.flink

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

object FlinkTest {

  def main(args: Array[String]): Unit = {
    var reader: BufferedReader = null
    val ratingsFile = "data/ml-latest-small/ratings.csv"

    val fileStream = new FileInputStream(ratingsFile)
    reader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"))

    val line = reader.readLine
    println("The first line is: " + line)

    val secondLine = reader.readLine
    println("The second line is: " + secondLine)

    val thirdLine = reader.readLine
    println("The third line is: " + thirdLine)

    val testLine = "78,\"Crossing Guard, The (1995)\",Action|Crime|Drama|Thriller"
    val tokens: Array[String] = testLine.split(",")

    if (tokens.length == 4 && tokens(1).startsWith("\"") && tokens(2).endsWith("\"")) {
      println("start with \" ")
    }

    tokens.map(t => println(t))

  }
}
