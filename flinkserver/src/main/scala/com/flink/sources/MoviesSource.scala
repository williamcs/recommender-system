package com.flink.sources

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import com.flink.datatypes.Movies
import org.apache.flink.streaming.api.functions.source.SourceFunction

class MoviesSource(moviesFilePath: String) extends SourceFunction[Movies] {

  @transient
  private var reader: BufferedReader = null

  override def run(ctx: SourceFunction.SourceContext[Movies]): Unit = {
    val fileStream = new FileInputStream(moviesFilePath)
    reader = new BufferedReader(new InputStreamReader(fileStream, "UTF-8"))

    generateMovieStream(ctx)

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

  private def generateMovieStream(sourceContext: SourceFunction.SourceContext[Movies]): Unit = {
    if (!reader.ready) return

    // read first line and skip the header
    val firstLine = reader.readLine
    println("skip first line: " + firstLine)

    // read all the following lines
    while (reader.ready) {
      val line = reader.readLine
      val movie = Movies.fromString(line)

      sourceContext.collect(movie)

//      Thread.sleep(1000)
    }
  }
}
