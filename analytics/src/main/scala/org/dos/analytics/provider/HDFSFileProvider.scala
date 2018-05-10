package org.dos.analytics.provider

import scala.io.Source

import org.dos.analytics.constants.Constants

class HDFSFileProvider {

  /**
   * hdfsFiles conf file keeps path of all the files. This function returns all those paths in an array.
   */
  def getFile(): Array[String] = {

    val fileNames = Source.fromFile(Constants.CONF_PATH + "/" + Constants.HDFS_FILES)
      .getLines
      .toArray

    fileNames
  }

}