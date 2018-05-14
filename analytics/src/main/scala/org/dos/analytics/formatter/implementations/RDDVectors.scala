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
        
        
        arr.update(i-1, f.get(i-1).toString().trim().toDouble)    //TODO: typecasting to double is required because kafka gives all data in string. This overhead can be removed if data format is correct
        
        //TODO:(done) remove the try catch block and handle manually
        /*try{
            arr.update(i-1, f.get(i-1).toString().trim().asInstanceOf[Double])  
        }catch{
          case e : Exception => {
                //arr.update(i-1, f.getInt(i-1).toDouble)
            arr.update(i-1, f.get(i-1).toString().trim().toDouble)
             }
        }*/
        
        
      }
//      arr.foreach(println)
      Vectors.dense(arr)
      
    }) 
    
    inputData = parsedData
  }
  
  override def getData():Any =  {
    inputData
  }
  
}