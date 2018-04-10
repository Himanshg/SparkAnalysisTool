package org.dos.analytics.provider

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.io.StdIn.{readInt}

import org.dos.analytics.constants.Constants
import scala.io.Source
import org.dos.analytics.dataSources.SourcesTrait


class DataProvider {
  
  def getData(sql: SparkSession):DataFrame = {
    
    
    val dataSourceClass = getSourceClass()
    
    dataSourceClass.getData(sql)
    
  }
    
  def getSourceClass() = {
      
    val x = new DataSourceProvider
    val action = Class.forName(Constants.DATA_SOURCE_IMPLEMENTATION + x.getDataSource()).newInstance()
    action.asInstanceOf[SourcesTrait]
    
    
  }
  
  
}