package org.dos.analytics.provider

import scala.io.StdIn.readInt
import org.dos.analytics.constants.Constants
import scala.io.Source
import com.typesafe.config.ConfigFactory
import java.io.File

class HDFSFileProvider {
  
  def getFile():String = {
      
      println("Enter data Files ")
      
      //TODO: to be automated : load from hdfsFiles conf file
      println("0. for data.csv" + "\n" +
              "1. for data1.csv")
              
      val option = readInt()
      
      val fileNames = Source.fromFile(Constants.CONF_PATH + "/" + Constants.HDFS_FILES )
                                .getLines
                                .toArray
                           
      val file = fileNames(option)
      
      file
  }
  
}