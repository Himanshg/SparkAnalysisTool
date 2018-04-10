package org.dos.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.linalg.Vector

import scala.io.Source

import org.dos.analytics.constants.Constants
import org.dos.analytics.provider.AlgoProvider
import org.dos.analytics.formatter.Implementations.RDDVectors

import org.dos.analytics.utils.Utils
import org.dos.analytics.utils.implementations.{KMeansUtil,MultivariateStatsUtil}
import org.dos.analytics.provider.DataProvider
import org.apache.spark.sql.DataFrame
import org.dos.analytics.formatter.InputFormatter
import org.apache.spark.rdd.RDD

object Analytics {
   
  def main ( args: Array[String]){
    
    val conf = new SparkConf().setMaster("local[2]")
     
    val sql = SparkSession.builder()
                          .appName("analytics")
                          .config(conf)
                          .getOrCreate()        
                          
     /*
      * 1. Get Algo
      * 2. Get data file path / mongo file 
      * 3. Get Params
      * 
      */
    
     val algoClass = getAlgo()
     
    /* Data Provider start  */
     
     val data = getData(sql)
     
     val fomattedData = formatData(data,algoClass.getClass.toString())
     
     /*Data Provider Ends*/
     
     algoClass.analyse(fomattedData)
     
    
    
    
  }
  
  def getAlgo():Utils = {
    
    val x = new AlgoProvider()
    val action = Class.forName(Constants.UTILS_IMPLEMENTATION + x.getAlgo()).newInstance()
    action.asInstanceOf[Utils]
  }

  def getData(sql: SparkSession):DataFrame = {
     val x = new DataProvider()
     val data = x.getData(sql)
     data
     
  }

  def formatData(data: DataFrame, algoName: String):RDD[Vector] = {
    
     val formatInput = new RDDVectors
     val rddVector = formatInput.format(data)
     rddVector
     
  }
  

}