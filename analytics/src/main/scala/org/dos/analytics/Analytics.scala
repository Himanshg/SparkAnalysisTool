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
import org.dos.analytics.formatter.implementations.RDDVectors

import org.dos.analytics.utils.Utils
import org.dos.analytics.utils.implementations._
import org.dos.analytics.provider.DataProvider
import org.apache.spark.sql.DataFrame
import org.dos.analytics.formatter.InputFormatter
import org.apache.spark.rdd.RDD
import org.dos.analytics.provider.DataFormatProvider
import org.dos.analytics.formatter.InputFormatter
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkException

object Analytics {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]) {

    /*
      * 1. Get Algo
      * 2. Get data file path / mongo file
      * 3. Get Params
      *
      */
  //  try {
      val algoClass = getAlgo()
      /* Data Provider start  */
      val data = getData()
data.show()
      val fomattedInput = formatData(data, algoClass.getClass.getSimpleName())

      /*Data Provider Ends*/

      algoClass.analyse(fomattedInput.getData())
      
    /*} catch {
      case e:ArrayIndexOutOfBoundsException => println("The option you have entered does not exist. \nExiting the program.")
      case e:IndexOutOfBoundsException => println("The option you have entered does not exist. \nExiting the program.")
      case e:NumberFormatException => println("Wrong input format. Check the instructions before entering. \nExiting the program.")
      case e:SparkException => println("Ananlysis failed due to unexpected input data. \nIf you think input is correct and this error occurs again, contact the program Administrator. \nExiting the program.")
      case e:NullPointerException => println("Something went wrong. Choose your options carefully or contact the program Administrator. \nExiting the program.")
      case e:Exception => println("Something went wrong: " + e)
    }*/
  }

  def getAlgo() = {

    val x = new AlgoProvider()
    val action = Class.forName(Constants.UTILS_IMPLEMENTATION + x.getAlgo()).newInstance()
    action.asInstanceOf[Utils]
  }

  def getData() = {
    val x = new DataProvider()
    val data = x.getData()
    data

  }

  def formatData(data: DataFrame, algoName: String) = {

    val x = new DataFormatProvider(algoName)
    val action = Class.forName(Constants.DATA_FORMAT_IMPLEMENTATION + x.getDataFormat).newInstance()

    val formatInput = action.asInstanceOf[InputFormatter]

    formatInput.format(data)
    formatInput

  }

}