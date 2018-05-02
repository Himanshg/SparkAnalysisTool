package org.dos.analytics.utils.implementations

import org.dos.analytics.utils.Utils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import scala.io.StdIn.{readLine,readInt}
import org.apache.spark.mllib.clustering.GaussianMixture


class GaussianMixtureUtil extends Utils{
  
  override def analyse(parsedData: Any){
    
    // Load and parse the data
     val data: RDD[Vector] = parsedData.asInstanceOf[RDD[Vector]].cache()
     
     println("Enter Number of Clusters: ")
      val numClusters = readInt()
    
    /*
    println("Enter Number of Iterations for Clustering: ")
    val numIterations = readInt()
    */

    // Cluster the data into two classes using GaussianMixture
    val gmm = new GaussianMixture().setK(numClusters).run(data)
    
    /*// Save and load model
    gmm.save(sc, "target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel")
    val sameModel = GaussianMixtureModel.load(sc, "target/org/apache/spark/GaussianMixtureExample/GaussianMixtureModel")
    */
    
    //TODO: save the model somewhere
    // output parameters of max-likelihood model
    for (i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format(gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
    }
        
  }
  
}