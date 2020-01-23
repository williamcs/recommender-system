package com.spark.utils

import java.io.File

object CommonUtil {

  /**
    * Delete directory.
    *
    * @param path
    */
  def deleteDir(path: String): Unit = {
    val dir = new File(path)

    if (!dir.exists()) return

    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f.getPath)
      } else {
        f.delete()
      }
    })

    dir.delete()
  }

}
