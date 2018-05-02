package org.dos.analytics.provider

import scala.io.StdIn.{readLine,readInt}
import scala.io.Source
import org.dos.analytics.constants.Constants

class DataSourceProvider {
  
  def getDataSource():String = {
      
      println("Enter Source of Input")
      
      //TODO: to be automated with sourceInput file
      println("0. for HDFS" + "\n" +
              "1. for MongoDb")
              
      val option = readInt()
      
      val dataSources = Source.fromFile(Constants.CONF_PATH + "/" + Constants.SOURCE_OF_INPUT )
                                .getLines
                                .toArray
                                
      val dataSource = dataSources(option)
      
      dataSource
  }
  
}