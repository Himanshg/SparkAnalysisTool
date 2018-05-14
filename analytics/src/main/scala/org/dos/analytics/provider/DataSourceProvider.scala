package org.dos.analytics.provider

import scala.io.StdIn.{ readLine, readInt }
import scala.io.Source
import org.dos.analytics.constants.Constants
import com.typesafe.config.ConfigFactory
import java.io.File
import scala.collection.mutable.ListBuffer

class DataSourceProvider {

  def getDataSource(): String = {

    val dataSourceNames = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.DATA_SOURCE_CONF))
      .getStringList(Constants.DATA_SOURCE)

    val dataSourceAndClassList = new ListBuffer[(String, String)]()

    val itr = dataSourceNames.iterator()
    while (itr.hasNext()) {

      val srcNameAndClass = itr.next().split("=")
      dataSourceAndClassList += ((srcNameAndClass(0).trim(), srcNameAndClass(1).trim()))
    }

    println("Select your data source")
    var i = 0
    dataSourceAndClassList.foreach(sourceAndClass => {
      i = i + 1
      println(i + ". " + sourceAndClass._1)
      
    })

    val option = readInt()
    val dataSourceClass = dataSourceAndClassList(option - 1)._2 

    dataSourceClass
  }

}