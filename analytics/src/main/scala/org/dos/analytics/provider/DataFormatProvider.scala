package org.dos.analytics.provider

import com.typesafe.config.ConfigFactory
import org.dos.analytics.constants.Constants
import java.io.File

class DataFormatProvider(algo: String){
  
  def getDataFormat():String =  {
    
    val dataFormat = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.ALGO_TO_DATA))
                                  .getString("data." + algo)
                                  
    dataFormat
      
    
  }
  
}