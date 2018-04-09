package org.dos.analytics.provider

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class DataProvider {
  
  //TODO: Add suppurt for multiple files
  
  def getData(filePath: String, params: String, sql: SparkSession):DataFrame = {
    
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
  
}