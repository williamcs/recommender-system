package com.flink.sources

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import com.flink.datatypes.Ratings
import org.apache.flink.streaming.api.functions.source.SourceFunction

class RatingsSource(ratingsFilePath: String) extends SourceFunction[Ratings] {

  @transient
  private var reader: BufferedReader = null

  override def run(ctx: SourceFunction.SourceContext[Ratings]): Unit = {
    val fileStream = new FileInputStream(ratingsFilePath)
    reader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"))

    generateRatingStream(ctx)

    reader.close
    reader = null
  }

  override def cancel(): Unit = {
    try {
      if (reader != null) {
        reader.close
      }
    } finally {
      reader = null
    }
  }

  private def generateRatingStream(sourceContext: SourceFunction.SourceContext[Ratings]): Unit = {
    if (!reader.ready) return

    // read first line and skip the header
    val firstLine = reader.readLine
    println("skip first line: " + firstLine)

    // read all the following lines
    while (reader.ready) {
      val line = reader.readLine
      val rating = Ratings.fromString(line)

      sourceContext.collect(rating)

//      Thread.sleep(1000)
    }
  }
}
