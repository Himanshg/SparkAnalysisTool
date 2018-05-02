package org.dos.analytics.formatter.implementations

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.dos.analytics.formatter.InputFormatter
import org.apache.spark.sql.DataFrame

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector

import scala.io.StdIn.{readLine,readInt}

class RDDLabeledVectors extends InputFormatter{
  
   var inputData: RDD[LabeledPoint] = null
   
   override def getData():Any =  {
    inputData
  }
   
   override def format(data: DataFrame) {
     
     print("\n Enter feature number in Input to be used as classification (consider start form 0) : ")
     
     val classifier = readInt()
     
       val parsedData = data.rdd.map( f => {
         
         val arr:Array[Double] = new Array[Double](f.size - 1)
         
         for(i <- 0 until f.size){
           
             if( i != classifier){
             
               //TODO: remove the try catch block and handle manually
                try{
                    arr.update(i, f.get(i).asInstanceOf[Double])  
                }catch{
                  case e : Exception => {
                        arr.update(i-1, f.getInt(i).toDouble)
                     }
                }
             }
         }
         
         try{
             new LabeledPoint(f.getDouble(classifier), Vectors.dense(arr))  
         } catch {
             case e: Exception => {
                 new LabeledPoint(f.getInt(classifier).toDouble, Vectors.dense(arr))
             }
         }
         
         
       })
       
       inputData = parsedData
       
   }
   
   
}