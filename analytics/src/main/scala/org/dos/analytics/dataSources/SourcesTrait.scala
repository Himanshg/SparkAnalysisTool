package org.dos.analytics.dataSources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

trait SourcesTrait {
    
    def getData(sql: SparkSession):DataFrame = {
      /*
       * To be implemented in base classes
       */
      null
    }
}