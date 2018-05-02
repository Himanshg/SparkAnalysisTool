package org.dos.analytics.dataSources.implementations

import org.dos.analytics.dataSources.SourcesTrait
import org.dos.analytics.constants.Constants

import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.bson.Document

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import com.typesafe.config.ConfigFactory
import java.io.File

import scala.collection.JavaConversions._
import scala.io.StdIn._

class MongoDbModel extends SourcesTrait {
 
   override def getData():DataFrame = {
     
     /*	
      * get mongoHome
      * get database
      * get collection
      * 
      * TODO: Can add support for multiple databases and collections
      * 
      */
     
     val mongoHome = getMongo()
     val mongoDB = getDB()
     val collection = getCollection(mongoDB)
     
     //** MongoDB Connector **//    
     val spark = getSession(mongoHome, mongoDB, collection)
                   
     val df = MongoSpark.load(spark)
     
     val schema = df.schema
     
//     schema.printTreeString()   
//     df.select("*").where("vehicle= 'OR05V4512'").show()
//     df.select("*").where("temp < 34").show()
//     df.select("temp","humidity").where("vehicle='OR05V4512'").show()
     
     val fields = schema.fieldNames.toList
     
     /*
      * get filter param, value
      * 
      * get field
      */
     
     val filter = getFilter(fields)
     val columns = getColumns(fields)
     
     
     val filteredDF = df.select(columns.head, columns.tail:_*).where(filter).toDF()
        
     filteredDF
     
   }
   
   
   def getMongo():String = {
      val mongoHome = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
                                  .getString(Constants.MONGO_HOME)
      mongoHome
   }
   
   
   def getDB():String = {
     
     val databases = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
                                  .getStringList(Constants.MONGO_DBs)
     
     println("Select DataBase: ")
     
     for(i<- 1 to databases.length){
       println( i-1 + " for " + databases.get(i-1))
     }
                                  
     
     val db = databases.get(readInt())
     
     db
   }
   
   
   def getCollection(db: String):String = {
     
      val collections = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
                                     .getStringList(Constants.MONGO + "." + db + "." + Constants.MONGO_COLLECTIONS)
     
     println("Select Collection: ")
     
     for(i<- 1 to collections.length){
       println(i-1 + " for " + collections.get(i-1))
     }
     
     val collection = collections.get(readInt())
     collection
   }
   
   def getSession(mongoHome: String, mongoDB: String, collection: String) = {
     val conf = new SparkConf().setMaster(Constants.SPARK_THREADS)
                               
      
     val spark =  SparkSession.builder()
                              .appName(Constants.APP_NAME)
                              .config(conf)
                              .config(Constants.MONGO_INPUT_URI, mongoHome  + mongoDB + "." + collection)
                              .config(Constants.MONGO_OUTPUT_URI, mongoHome + mongoDB + "." + "spark_collection")
                              .getOrCreate()
                              
      spark
   }
   
   
   
   
   
   def getFilter(fields: List[String]) = {
     
     println("Parametrs are: ")
     fields.foreach(println)
     
     println(" Give the filter in the form: \n".+(
               "param = 'value' OR \n").+( 
               "param < 'value' OR \n").+(
               "param > 'value' "))
     
      
     
     val filter = readLine()
     filter
     
   }
   
   def getColumns(fields: List[String]) = {
     
     println("Enter ',' separated list of columns to select from the same options: ")
     
     val columns = readLine()
     
     val cols = columns.split(",")
     
     cols.map(_.trim()).toSeq
   }
   
   
}