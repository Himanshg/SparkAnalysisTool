package org.dos.analytics.formatter.Implementations

import org.dos.analytics.formatter.InputFormatter
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class RDDVectors extends InputFormatter{
  
  
   override def format(data: DataFrame):RDD[Vector] = {
    
    
      val parsedData = data.rdd.map( f => {
      
      val arr:Array[Double] = new Array[Double](f.size)
      
      for (i<- 1 to f.size){
        arr.update(i-1, f.getDouble(i-1))
      }
      arr.foreach(println)
      Vectors.dense(arr)
      
    }) 
    
    parsedData
    
  }
  
  
}