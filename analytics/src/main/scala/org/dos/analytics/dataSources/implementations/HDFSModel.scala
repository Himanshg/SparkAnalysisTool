package org.dos.analytics.dataSources.implementations

import org.dos.analytics.dataSources.SourcesTrait
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.dos.analytics.provider.HDFSFileProvider
import com.typesafe.config.ConfigFactory
import java.io.File
import org.dos.analytics.constants.Constants
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

class HDFSModel extends SourcesTrait {
  
  
  //TODO: Add support for multiple files
  
  override def getData():DataFrame = {
    
     /** SPARK CONNECTION **/
    val conf = new SparkConf().setMaster(Constants.SPARK_THREADS)
     
    val sql = SparkSession.builder()
                          .appName(Constants.APP_NAME)
                          .config(conf)
                          .getOrCreate()  
    
    
    val filePath = getFile() 
    val params = getParams(filePath)
    val filteredParams = getFilteredParams(params)  
    
    val hdfsHome = getHdfs()
    
   
    val fields = params.split(" ")
                    .map(f => StructField(f,DoubleType,true))
                          
    val schema = StructType(fields)
    
    //TODO: generalize it for multiple sources of i/p
    val dataDF = sql.read.schema(schema).csv(hdfsHome.+(filePath))
    
    dataDF.createOrReplaceTempView("data")
    
    val filterDF =  sql.sql("SELECT "+ filteredParams + " FROM data") 
    
    filterDF
    
  }
    
   def getHdfs():String = {
      val hdfsHome = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
                                  .getString(Constants.HDFS_HOME)
      hdfsHome
   }
    
   def getFile():String = { 
     val file = new HDFSFileProvider
     file.getFile()
  }
   
   def getParams(file: String):String = {
     //TODO: Read from a conf file => mapping of file with params   OR  take from the input file
     val str = "lat long zero alt days date time"
     str
   }
  
  def getFilteredParams(params: String):String = {
    
    //TODO:
    val params = "lat,long"
    params
  }
  
}