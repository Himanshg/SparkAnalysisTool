package org.dos.analytics.dataSources.implementations

import org.dos.analytics.dataSources.SourcesTrait
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class HDFSmodel extends SourcesTrait {
  
  
  //TODO: Add support for multiple files
  
  override def getData(sql: SparkSession):DataFrame = {
    
    val filePath = getFile() 
    val params =   getParams()  
    
    
    //TODO: Read from a conf file => mapping of file with params 
    val str = "lat long zero alt days date time" 
    
    val fields = str.split(" ")
                    .map(f => StructField(f,DoubleType,true))
                          
    val schema = StructType(fields)
    
    //TODO: generalize it for multiple sources of i/p
    val dataDF = sql.read.schema(schema).csv(filePath.toString())
    
    dataDF.createOrReplaceTempView("data")
    
    val filterDF =  sql.sql("SELECT "+ params + " FROM data") 
    
    filterDF
    
  }
  
   def getFile():String = { 
    //TODO:
    val file = "hdfs://master:9000/user/himanshu/input1/data.csv"
    file
    
  }
  
  def getParams():String = {
    
    //TODO:
    val params = "lat,long"
    params
  }
  
}