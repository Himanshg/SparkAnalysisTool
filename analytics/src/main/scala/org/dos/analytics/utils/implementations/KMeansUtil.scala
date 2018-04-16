package org.dos.analytics.utils.implementations

import org.dos.analytics.utils.Utils
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import scala.io.StdIn.{readLine,readInt}

class KMeansUtil extends Utils{
  
  override def analyse(parsedData: Any){
     // Cluster the data into two classes using KMeans
    
    val data: RDD[Vector] = parsedData.asInstanceOf[RDD[Vector]]
    
    println("Enter Number of Clusters: ")
    val numClusters = readInt()
    
    println("Enter Number of Iterations for Clustering: ")
    val numIterations = readInt()
    
    
    val clusters = KMeans.train(data, numClusters, numIterations)
    
    println("Final Centers: ")
//    println(clusters.toPMML())
  }
  
}