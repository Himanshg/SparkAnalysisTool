package org.dos.analytics.provider

import scala.io.StdIn.{readLine,readInt}
import scala.io.Source
import org.dos.analytics.constants.Constants

class AlgoProvider {
    
    def getAlgo():String = {
      
//      //TODO:(what else) To be automated using userInputAlgo File
//      println("0. Do KMeans Clustering" + "\n" + 
//              "1. Do Multivariate Statistics" )
//      
//      val option = readInt()
//      
      val algoClasses = Source.fromFile(Constants.CONF_PATH + "/" + Constants.USER_INPUT_TO_ALGO )
                              .getLines
                              .toArray
      
      println(" Enter the Algorithm you want to apply ")
                              
      for(i <- 1 to algoClasses.length){
        println(i-1 + " for " + algoClasses(i-1) )
      }
      
      val algoClass = algoClasses(readInt())
      
      algoClass.toString()
      
    }
    
}