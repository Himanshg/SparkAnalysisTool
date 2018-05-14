package org.dos.analytics.dataSources.implementations

import java.io.File

import scala.io.StdIn.readLine

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.dos.analytics.constants.Constants
import org.dos.analytics.dataSources.SourcesTrait
import org.dos.analytics.provider.HDFSFileProvider

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.log4j.Level

class HDFSModel extends SourcesTrait {

  Logger.getLogger("org").setLevel(Level.OFF)
  //Logger.getLogger("akka").setLevel(Level.OFF)
  
  var sql: SparkSession = null
  //TODO: (done)Add support for multiple files

  override def getData(): DataFrame = {

    /** SPARK CONNECTION **/
    val conf = new SparkConf().setMaster(Constants.SPARK_THREADS)

    sql = SparkSession.builder()
      .appName(Constants.APP_NAME)
      .config(conf)
      .getOrCreate()

    val filePaths = getFile()

    val hdfsHome = getHdfs()

    //TODO:(done) generalize it for multiple extensions of i/p
    val dataDFAndFilteredParam = getData(hdfsHome, filePaths)

    dataDFAndFilteredParam._1.createOrReplaceTempView("data")

    val filteredParams = dataDFAndFilteredParam._2

    val fields = filteredParams.split(",").toList
    val filter = getFilter(fields)

    var filterDF: DataFrame = null
    if (filter == "") {
      filterDF = sql.sql("SELECT " + filteredParams + " FROM data")
    } else {
      filterDF = sql.sql("SELECT " + filteredParams + " FROM data").where(filter)
    }
    
    sql.catalog.dropTempView("data")
    
    filterDF.printSchema()
    filterDF

  }

  def getFilter(fields: List[String]) = {

    println("Parametrs are: ")
    fields.foreach(println)

    println("Give the filter in the form: \n".+(
      "param = 'value' OR \n").+(
        "param < 'value' OR \n").+(
          "param > 'value' \n" + "Press Enter if all data should be considered."))

    val filter = readLine()

    filter

  }

  /**
   * @return the path of hdfs file from constant file
   */
  def getHdfs(): String = {
    val hdfsHome = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
      .getString(Constants.HDFS_HOME)
    hdfsHome
  }

  /**
   * @param hdfsHome
   * @param filePath
   * @return DataFrame: contains data from the sql query
   * String: Filtered Parameters
   */
  def getData(hdfsHome: String, filePaths: Array[String]): (DataFrame, String) = {

    var dataDF: DataFrame = null;
    var schemaAndFilteredParams: (StructType, String) = null
    var filteredParams: String = ""
    var tableCount: Int = 0

    filePaths.map(filePath => {
      var tempDF: DataFrame = null
      tableCount = tableCount + 1

      val fileExtension = filePath.split('.').last  /* parsing filePath to find the file extension*/

      /*TODO: read allParams from json*/
      // val allParams = sql.read.csv(filePath).head() //we assume that file header is always present
//      val allParams = "id,id_android,speed,time,distance,rating,rating_bus,rating_weather,car_or_bus,linha"
      val tableName = filePath.split("/").last.split('.')(0)
      val allParams = getSchema(tableName)//"lat,long,x,y,z,a,b"
      
      schemaAndFilteredParams = getSchemaAndFilteredParams(allParams.toString(), tableCount, tableName)

      /*fetch the data according to the format of the file in the hdfs*/
      if (fileExtension.equalsIgnoreCase("csv")) {
        tempDF = sql.read.schema(schemaAndFilteredParams._1).csv(hdfsHome.+(filePath))
//        tempDF = sql.read.schema(schemaAndFilteredParams._1).csv(filePath)
        //        tempDF.show()
      } else if (fileExtension.equalsIgnoreCase("json")) {
        tempDF = sql.read.schema(schemaAndFilteredParams._1).json(hdfsHome.+(filePath)) //TODO:this is not tested
      }
      
      tempDF = filterTempDF(tempDF, schemaAndFilteredParams._2)
      dataDF = joinDFs(dataDF, tempDF)

      filteredParams = filteredParams + "," + schemaAndFilteredParams._2
    })

    (dataDF, filteredParams.substring(1)) //filteredParams will be empty initially. This will put an extra comma in the beginnig. Hence substring(1).
  }

  def getSchema(fileName: String): String = {
    val hdfsHome = ConfigFactory.parseFile(new File(Constants.CONF_PATH + "/" + Constants.SYSTEM_CONF))
      .getString(Constants.HDFS_SCHEMA + "." + fileName)
    hdfsHome
  }
  
  /**
 * @param tempDF	dataframe of complete table in the memory
 * @param filteredParams parameters required by the user
 * @return a dataframe of the columns(parameters) required by the user
 */
def filterTempDF(tempDF: DataFrame, filteredParams: String): DataFrame = {
    tempDF.createOrReplaceTempView("tempView")
    val filterDF = sql.sql("SELECT " + filteredParams + " FROM tempView")
    //filterDF.show()
    sql.catalog.dropTempView("tempView")
    filterDF
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
  def addColumnIndex(df: DataFrame) = sql.createDataFrame(
    // Add Column index
    df.rdd.zipWithIndex.map { case (row, columnindex) => Row.fromSeq(row.toSeq :+ columnindex) },
    // Create schema
    StructType(df.schema.fields :+ StructField("columnindex", LongType, false)))

  def getFile(): Array[String] = {
    val file = new HDFSFileProvider
    file.getFile()
  }

  def getSchemaAndFilteredParams(allParams: String, tableCount: Int, tableName: String): (StructType, String) = {

    //indexing the parameters
    val paramsArray = allParams.split(",").map(_.trim())  //TODO: put trim here
    // paramsArray(0) = paramsArray(0).substring(1)
    // paramsArray(paramsArray.size - 1) = paramsArray.last.dropRight(1)

    /*var key = 0;
    val keyedParamMap = paramsArray.map(param => {
      key = key + 1
      (key, param)
    }).toMap*/

    for(i<- 1 to paramsArray.length){
       var displayName = "Table " + tableName + " - " + paramsArray(i-1)  //5 is for "Table"
       println(i + ". " + displayName)
     }
    
    //TODO: (done) display the list of params on the screen and ask user to choose which features he wants to analyse.
    //keyedParamMap.foreach(e => println(e._1 + ". " + e._2))

    print("Enter the feature number you want to choose (comma separated): ")
    val selectedFeatures = readLine()

    //TODO:(done) handle wrong input format

    val tableAlias = "Table" + tableCount

    //creating the list of selected features
    val selectedFeaturesArray = selectedFeatures.split(",")
    val featureList = selectedFeaturesArray.map(feature => {
      //val f = keyedParamMap.get(feature.toInt).get
      val f = paramsArray(feature.toInt-1)
      tableAlias + f
    })
    val features = featureList.mkString(",")

    //println(features)

    //creating the schema with selected features while making the datatype double
    val fields = paramsArray.map(f => StructField(tableAlias + f, DoubleType, true)).toSeq
    val schema = StructType(fields)

    (schema, features)

  }

}