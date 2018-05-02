package org.dos.analytics.utils.implementations

import org.dos.analytics.utils.Utils
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import scala.io.StdIn.{readDouble,readInt}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

class LinearRegressionUtil extends Utils{
    
      override def analyse(parsedData: Any){
     // Cluster the data into two classes using KMeans
    
    //TODO: call I/p converter from any to required format
    val data: RDD[LabeledPoint] = parsedData.asInstanceOf[RDD[LabeledPoint]].cache()
    
    println("Enter Step size: ")
    val stepSize = readDouble()
    
    println("Enter Number of Iterations: ")
    val numIterations = readInt()
    
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize)
    
    // Evaluate model on training examples and compute training error
    
    val valuesAndPreds = data.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    
    println(s"training Mean Squared Error $MSE")
    
    /*
    // Save and load model
      model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
      val sameModel = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
    */
    
   
  }
  
}