package org.dos.analytics.dataSources.implementations

import org.dos.analytics.dataSources.SourcesTrait
import org.dos.analytics.constants.Constants

import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config.{ ReadConfig, WriteConfig }
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

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.types.LongType
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.annotation.meta.field
import scala.collection.mutable.ListBuffer

class MongoDbModel extends SourcesTrait {
  Logger.getLogger("org").setLevel(Level.OFF)
  //Logger.getLogger("INFO").setLevel(Level.OFF)

  var sparkSession: SparkSession = null

  //** MongoDB Connector **//
  def getSession(mongoHome: String, mongoDB: String, collection: String) = {
    val conf = new SparkConf().setMaster(Constants.SPARK_THREADS)

    val spark = SparkSession.builder()
      .appName(Constants.APP_NAME)
      .config(conf)
      .config(Constants.MONGO_INPUT_URI, mongoHome + mongoDB + "." + collection)
      .config(Constants.MONGO_OUTPUT_URI, mongoHome + mongoDB + "." + "spark_collection")
      .getOrCreate()

    spark
  }

  override def getData(): DataFrame = {

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
    val selectedCollectionList = getCollection(mongoDB)

    val collectionsArray = selectedCollectionList.split(",").map(_.trim()).toArray

    sparkSession = getSession(mongoHome, mongoDB, collectionsArray(0))

    var dataFrame = MongoSpark.load(sparkSession)
    dataFrame.show()
    var tableCount = 1
    var newFeatureNames = getNewFeatureNames(dataFrame.schema, tableCount)
    dataFrame = dataFrame.toDF(newFeatureNames: _*)
    dataFrame.show()
    var tempDF: DataFrame = null

    if (collectionsArray.size > 1) {
      val newCollectionArray = collectionsArray.drop(1)

      newCollectionArray.foreach(collection => {
        val readConfig = ReadConfig(Map("collection" -> collection, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkSession)))
        tempDF = MongoSpark.load(sparkSession, readConfig)
        tableCount = tableCount + 1
        newFeatureNames = getNewFeatureNames(tempDF.schema, tableCount)
        tempDF = tempDF.toDF(newFeatureNames: _*)
        tempDF.show()
        dataFrame = joinDFs(dataFrame, tempDF)

      })
    }
    
    dataFrame = getFilteredData(dataFrame)
    
    dataFrame
  }

  def getNewFeatureNames(schema: StructType, tableCount: Int): ListBuffer[String] = {
    
    var newNames = new ListBuffer[String]()
       
    var i = 1;
    val f = schema.fields.foreach(g => {
      newNames+= tableCount + "Table" + g.name
    })
    newNames
  }
  
  /**
   * @param dataframe
   * @param tempDF
   */
  def joinDFs(dataframe: DataFrame, tempDF: DataFrame): DataFrame = {
    /* In case of multiple files given, we are joining tables in each file */
    var dataDF = dataframe
    if (dataDF == null) {
      dataDF = tempDF
      dataDF.printSchema()
    } else {
      val dataDFInd = addColumnIndex(dataDF)
      val tempDFInd = addColumnIndex(tempDF)

      dataDF = dataDFInd.join(tempDFInd, Seq("columnindex")).drop("columnindex")
      dataDF.printSchema()
      //dataDFInd.show()
      //tempDFInd.show()
      //dataDF.show()
    }
    dataDF
  }

  /**
   * @param df
   * @return
   *  Column index is added as a phantom column to do the inner join correctly
   */
  def addColumnIndex(df: DataFrame) = sparkSession.createDataFrame(
    // Add Column index
    df.rdd.zipWithIndex.map { case (row, columnindex) => Row.fromSeq(row.toSeq :+ columnindex) },
    // Create schema
    StructType(df.schema.fields :+ StructField("columnindex", LongType, false)))

  def getFilteredData(df: DataFrame): DataFrame = {

    //df = null

    /*val df3 = sparkSession.loadFromMongoDB(ReadConfig(
      Map("uri" -> "mongodb://example.com/database.collection")
      )
    )
    */
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

    val columns = getColumns(fields)

    val filter = getFilter(columns)

    var filteredDF: DataFrame = null
    if (filter == "") {
      filteredDF = df.select(columns.head, columns.tail: _*).toDF()
    } else {
      filteredDF = df.select(columns.head, columns.tail: _*).where(filter).toDF()
    }
    //filteredDF.show()
    filteredDF
  }

  def getMongo(): String = {
    val mongoHome = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
      .getString(Constants.MONGO_HOME)
    mongoHome
  }

  def getDB(): String = {

    val databases = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
      .getStringList(Constants.MONGO_DBs)

    println("Select DataBase: ")

    for (i <- 1 to databases.length) {
      println(i + ". " + databases.get(i - 1))
    }

    val db = databases.get(readInt() - 1)

    db
  }

  def getCollection(db: String): String = {

    val collections = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
      .getStringList(Constants.MONGO + "." + db + "." + Constants.MONGO_COLLECTIONS)

    println("Collections inside database " + db + ": ")

    for (i <- 1 to collections.length) {
      println(i + ". " + collections.get(i - 1))
    }

    print("Enter the collection number you want to choose (comma separated): ")
    val selectedCollections = readLine()
    val collectionListArray = selectedCollections.split(",").map(_.trim()).toArray
    val collectionList = collectionListArray.map(collection => {
      val f = collections.get(collection.toInt - 1)
      f
    })
    val selectedCollectionList = collectionList.mkString(",")

    selectedCollectionList

  }

  def getFilter(fields: Seq[String]) = {

    println("Parametrs that you have selected are: ")
    //fields.foreach(println)
    for(i<- 1 to fields.length){
       var tableId = fields.get(i-1).split("T")(0)
       var displayName = "Collection " + tableId + " - " + fields.get(i-1).substring(tableId.length()+5)  //5 is for "Table"
       println(i + ". " + displayName)
     }

    println("\n Give the filter in the form: \n".+(
      "param = 'value' OR \n").+(
        "param < 'value' OR \n").+(
          "param > 'value' \n" + "Press Enter if all data should be considered."))

    val filter = readLine()
    filter

  }

  def getColumns(fields: List[String]) = {

    println("Features in the selected table are: ")

     for(i<- 1 to fields.length){
       var tableId = fields.get(i-1).split("T")(0)
       var displayName = "Collection " + tableId + " - " + fields.get(i-1).substring(tableId.length()+5)  //5 is for "Table"
       println(i + ". " + displayName)
     }

    print("Enter the Columns that you want to choose (comma separated): ")
    val selectedFeatures = readLine()

    val selectedFeaturesArray = selectedFeatures.split(",").map(_.trim()).toArray

    val featureListSeq = selectedFeaturesArray.map(feature => {
      val f = fields.get(feature.toInt-1)
      f
    }).toSeq

    featureListSeq
  }

}