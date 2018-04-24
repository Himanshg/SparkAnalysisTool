package org.dos.analytics.formatter.implementations

import org.dos.analytics.formatter.InputFormatter
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD


class RDDVectors extends InputFormatter{
  
  var inputData: RDD[Vector] = null
  
   override def format(data: DataFrame) {
    
    
      val parsedData = data.rdd.map( f => {
      
      val arr:Array[Double] = new Array[Double](f.size)
      
      for (i<- 1 to f.size){
        
        //TODO: remove the try catch block and handle manuakky
        try{
            arr.update(i-1, f.get(i-1).asInstanceOf[Double])  
        }catch{
          case e : Exception => {
                arr.update(i-1, f.getInt(i-1).toDouble)
             }
        }
        
        
      }
      arr.foreach(println)
      Vectors.dense(arr)
      
    }) 
    
    inputData = parsedData
  }
  
  override def getData():Any =  {
    inputData
  }
  
}