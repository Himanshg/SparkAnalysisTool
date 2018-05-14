package org.dos.analytics.provider

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.io.StdIn.{ readInt }

import org.dos.analytics.constants.Constants
import scala.io.Source
import org.dos.analytics.dataSources.SourcesTrait
import scala.io.StdIn.readInt

import scala.io.StdIn.readLine

class DataProvider {

  def getData(): DataFrame = {

    val dataSourceClass = getSourceClass()

    var df = dataSourceClass.getData()

    //df.printSchema()
    println("Below are the columns you have selected: ")
    val schema = df.schema.fieldNames

    for (i <- 1 to schema.length) {
      //val dispName = schema(i-1).split("T")(1)
      println(i + ". " + schema(i - 1))
      //println(i + ". " + dispName)
    }

    println("Enter column numbers (comma separated) if any to Drop, or else press Enter:")

    val columnsToDrop = readLine()

    if (columnsToDrop != "") {

      val droppingColArray = columnsToDrop.split(",").map(_.trim())

      droppingColArray.foreach(drpCol => {
        val colName = schema(drpCol.toInt - 1)
        df = df.drop(colName)
      })
    }
    //df.drop()

    df

  }

  def getSourceClass() = {

    val x = new DataSourceProvider
    val action = Class.forName(Constants.DATA_SOURCE_IMPLEMENTATION + x.getDataSource()).newInstance()
    action.asInstanceOf[SourcesTrait]

  }

}