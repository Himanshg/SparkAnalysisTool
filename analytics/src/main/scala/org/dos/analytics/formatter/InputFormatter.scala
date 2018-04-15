package org.dos.analytics.formatter

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

trait InputFormatter {
  
   def format(data: DataFrame){
     /*
      * To be implemented 
      */
   }
  
   def getData():Any = {
     
   }
  
}