package edu.gatech.cse6250.features

import edu.gatech.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD

/**
 * @author Hang Su
 */
object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   *
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    diagnostic.map(p => ((p.patientID, p.code), 1.0)).keyBy(_._1).reduceByKey((x, y) => (x._1, x._2 + y._2)).map(_._2)
    // diagnostic.sparkContext.parallelize(List((("patient", "diagnostics"), 1.0)))
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   *
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    medication.map(p => ((p.patientID, p.medicine), 1.0)).keyBy(_._1).reduceByKey((x, y) => (x._1, x._2 + y._2)).map(_._2)
    // medication.sparkContext.parallelize(List((("patient", "med"), 1.0)))
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   *
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    labResult.map(p => ((p.patientID, p.testName), p.value, 1.0)).keyBy(_._1).reduceByKey((x, y) => (x._1, x._2 + y._2, x._3 + y._3)).map(p => (p._1, p._2._2 / p._2._3))

    // labResult.sparkContext.parallelize(List((("patient", "lab"), 1.0)))
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   *
   * @param diagnostic   RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */

  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    constructDiagnosticFeatureTuple(diagnostic.filter(p => candiateCode.contains(p.code.toLowerCase)))
    // diagnostic.sparkContext.parallelize(List((("patient", "diagnostics"), 1.0)))
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   *
   * @param medication          RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    constructMedicationFeatureTuple(medication.filter(p => candidateMedication.contains(p.medicine.toLowerCase)))

    // medication.sparkContext.parallelize(List((("patient", "med"), 1.0)))
  }

  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   *
   * @param labResult    RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    constructLabFeatureTuple(labResult.filter(p => candidateLab.contains(p.testName.toLowerCase)))
    // labResult.sparkContext.parallelize(List((("patient", "lab"), 1.0)))
  }

  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   *
   * @param sc      SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map */
    // val feature_id_mapping = feature.map(_._1._2).distinct().zipWithIndex().map { case (k, v) => (v, k.toString) }
    val feature_id_mapping = feature.map(_._1._2).distinct().zipWithIndex().collect().toMap

    /** transform input feature */

    /**
     * Functions maybe helpful:
     * collect
     * groupByKey
     */

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    // val result = sc.parallelize(Seq(
    //   ("Patient-NO-1", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-2", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-3", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-4", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-5", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-6", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-7", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-8", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-9", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
    //   ("Patient-NO-10", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))))

    val result = feature.map(p => (p._1._1, feature_id_mapping(p._1._2), p._2)).groupBy(p => p._1).map(p => (p._1, Vectors.sparse(feature_id_mapping.size, p._2.toList.map(x => (x._2.toInt, x._3)))))

    // val result = feature.groupBy(p => p._1._1).map(p => (p._1._1, Vectors.sparse(feature_id_mapping.size, feature_id_mapping(p._1._2).toList.map(x => (x._2.toInt, x._3)))))

    // val result = feature.keyBy(_._1._1).reduceByKey().map(p => (p._1._1, Vectors.sparse(feature_id_mapping.size, feature_id_mapping(p._1._2).toList.map(x => (x._2.toInt, x._3)))))

    result

    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */

  }
}

