package edu.gatech.cse6250.phenotyping

import edu.gatech.cse6250.model.{ Diagnostic, LabResult, Medication }
import org.apache.spark.rdd.RDD

/**
 * @author Hang Su <hangsu@gatech.edu>,
 * @author Sungtae An <stan84@gatech.edu>,
 */
object T2dmPhenotype {

  /** Hard code the criteria */
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
    "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
    "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
    "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
    "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
    "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
    "avandia", "actos", "actos", "glipizide")

  /**
   * Transform given data set to a RDD of patients and corresponding phenotype
   *
   * @param medication medication RDD
   * @param labResult  lab result RDD
   * @param diagnostic diagnostic code RDD
   * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
   */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
     * Remove the place holder and implement your code here.
     * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
     * When testing your code, we expect your function to have no side effect,
     * i.e. do NOT read from file or write file
     *
     * You don't need to follow the example placeholder code below exactly, but do have the same return type.
     *
     * Hint: Consider case sensitivity when doing string comparisons.
     */

    val sc = medication.sparkContext

    /** Hard code the criteria */
    // use the given criteria above like T1DM_DX, T2DM_DX, T1DM_MED, T2DM_MED and hard code DM_RELATED_DX criteria as well

    val DM_RELATED_DX = Set("790.21", "790.22", "790.2", "790.29", "648.81", "648.82", "648.83", "648.84", "648.0", "648.00", "648.01", "648.02", "648.03", "648.04", "791.5", "277.7", "V77.1", "256.4")

    /** Find CASE Patients */

    val all_patients = labResult.map(p => p.patientID).union(diagnostic.map(_.patientID)).union(medication.map(_.patientID)).distinct()
    // println(all_patients.count())

    val dm_dx = diagnostic.filter(p => !T1DM_DX.contains(p.code) && T2DM_DX.contains(p.code)).map(_.patientID).distinct()

    val T1DM_patients = medication.filter(p => T1DM_MED.contains(p.medicine))
    // println(T1DM_patients.count())

    val T2DM_patients = medication.filter(p => T2DM_MED.contains(p.medicine))
    // println(T2DM_patients.count())

    val T1DM_T2DM_patients = T1DM_patients.map(_.patientID).intersection(dm_dx).subtract(T2DM_patients.map(_.patientID))

    val T1_minDate = T1DM_patients.keyBy(_.patientID).reduceByKey((a, b) => if (a.date.before(b.date)) a else b).map(t => (t._1, t._2))
    // T1_minDate.take(10).foreach(println)

    val T2_minDate = T2DM_patients.keyBy(_.patientID).reduceByKey((a, b) => if (a.date.before(b.date)) a else b).map(t => (t._1, t._2))
    // T2_minDate.take(10).foreach(println)
    // WRONG: rdd.filter(r  => r ._2._1.before(r._2._1))
    // rdd.filter(r  => r ._2.<strong>_2</strong>.before(r._2._1))
    //.toString.asInstanceOf[java.sql.Date]

    val T2_before_T1 = T2_minDate.join(T1_minDate).filter(p => p._2._1.date.before(p._2._2.date)).map(_._1)
    // val T2_before_T1 = T2_minDate.join(T1_minDate).map(p => p._1)
    // .filter(p => p._2._1.before(p._2._2.toString.asInstanceOf[java.sql.Date]))
    // println(T2_before_T1.count())

    val not_DM1_patients = dm_dx.subtract(T1DM_patients.map(_.patientID))
    val casePatients = not_DM1_patients.union(T1DM_T2DM_patients).union(T2_before_T1).distinct().map((_, 1))
    // val casePatients = sc.parallelize(Seq(("casePatient-one", 1), ("casePatient-two", 1), ("casePatient-three", 1)))
    // println(casePatients.count())

    /** Find CONTROL Patients */
    val glucose_patients = labResult.filter(p => p.testName.contains("glucose"))

    val abnormal_a1c = labResult.filter(p => p.testName.contains("a1c")).filter(p => p.value >= 6)
    val abnormal_glucose = glucose_patients.filter(p => p.value >= 110)

    val fine_patients = glucose_patients.subtract(abnormal_glucose).subtract(abnormal_a1c).map(p => p.patientID)

    val mellitus_patients = diagnostic.filter(p => DM_RELATED_DX.contains(p.code) || p.code.contains("250.")).map(_.patientID)

    val controlPatients = fine_patients.subtract(mellitus_patients).distinct().map((_, 2))
    // val controlPatients = sc.parallelize(Seq(("controlPatients-one", 2), ("controlPatients-two", 2), ("controlPatients-three", 2)))

    /** Find OTHER Patients */
    val others = all_patients.subtract(controlPatients.map(_._1)).subtract(casePatients.map(_._1)).map((_, 3))
    // println(others.count())

    // val others = sc.parallelize(Seq(("others-one", 3), ("others-two", 3), ("others-three", 3)))

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }
}