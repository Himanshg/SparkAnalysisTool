package org.dos.analytics.utils.implementations

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.stat.Statistics
import org.dos.analytics.utils.Utils

object MultivariateStatsUtil extends Utils{
  
  override def analyse(rdd: Any) {
    
    
    val data: RDD[Vector] = rdd.asInstanceOf[RDD[Vector]]
    
    val summary: MultivariateStatisticalSummary = Statistics.colStats(data)

    println("mean: " + summary.mean) // a dense vector containing the mean value for each column
    println("variance: " + summary.variance) // column-wise variance
    println("non Zero count: " + summary.numNonzeros)
  }
}