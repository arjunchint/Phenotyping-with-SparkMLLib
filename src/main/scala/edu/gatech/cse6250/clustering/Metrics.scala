/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse6250.clustering

import org.apache.spark.rdd.RDD

object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the purity of clustering.
   * Purity is defined as
   * \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
   *
   * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return
   */
  def purity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    /**
     * TODO: Remove the placeholder and implement your code here
     */
    // 0.0

    clusterAssignmentAndLabel.map((_, 1L)).keyBy(_._1).reduceByKey((a, b) => (a._1, a._2 + b._2)).map(p => (p._2._1._1, p._2._2)).keyBy(_._1).reduceByKey((a, b) => (1, Math.max(a._2, b._2))).map(_._2._2).reduce(_ + _) / clusterAssignmentAndLabel.count().toDouble
  }
}
