package org.dos.analytics.constants

object Constants {
  
  final val SPARK_THREADS = "local[4]"
  
  final val APP_NAME = "analytics"
  
  final val HDFS = "env.mongo"
  
  final val MONGO = "env.mongo"
  
  final val HDFS_HOME = "env.hdfs.HDFS_HOME"
  
  final val MONGO_HOME = "env.mongo.MONGO_HOME"
  
  final val MONGO_DBs = "env.mongo.databases"
  
  final val MONGO_COLLECTIONS = "collections"
  
  final val MONGO_INPUT_URI = "spark.mongodb.input.uri"
  
  final val MONGO_OUTPUT_URI = "spark.mongodb.output.uri"
  
  final val CONF_PATH = "./src/main/scala/org/dos/conf"
  
  final val USER_INPUT_TO_ALGO = "userInputAlgo"
  
  final val SOURCE_OF_INPUT = "sourceInput"
  
  final val HDFS_FILES = "hdfsFiles"
  
  final val ALGO_TO_DATA = "algo_data.conf"
  
  final val SYSTEM_CONF = "system_env.conf"
  
  final val UTILS_IMPLEMENTATION = "org.dos.analytics.utils.implementations."
  
  final val DATA_SOURCE_IMPLEMENTATION = "org.dos.analytics.dataSources.implementations."
  
  final val DATA_FORMAT_IMPLEMENTATION = "org.dos.analytics.formatter.implementations."
  
  final val DATA_SOURCE_CONF = "dataSource.conf"
  
  final val DATA_SOURCE = "dataSource.source"
  
  final val HDFS_SCHEMA = "env.hdfs.schema"
  
  //final val HDFS_MODEL = "dataSource.sourceModel.hdfs"
  
  //final val MONGODB_MODEL = "dataSource.sourceModel.mongodb"
  
  
}