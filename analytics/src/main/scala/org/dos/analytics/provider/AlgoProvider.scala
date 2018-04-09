package org.dos.analytics.provider

import scala.io.StdIn.{readLine,readInt}
import scala.io.Source
import org.dos.analytics.constants

class AlgoProvider {
    
    def getAlgo():String = {
      
      println("0. Do KMeans Clustering" + "\n" + 
              "1. Do Multivariate Statistics" )
      
      val option = readInt()
      
      val algoClasses = Source.fromFile(constants.Constants.CONF_PATH + "/" + constants.Constants.USER_INPUT_TO_ALGO )
                              .getLines
                              .toArray
      
      val algoClass = algoClasses(option)
      
      algoClass.toString()
      
    }
    
}