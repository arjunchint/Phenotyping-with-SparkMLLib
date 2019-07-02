package edu.gatech.cse6250.main
// import edu.gatech.cse6250.main
import java.text.SimpleDateFormat

import edu.gatech.cse6250.clustering.Metrics
import edu.gatech.cse6250.features.FeatureConstruction
import edu.gatech.cse6250.helper.{ CSVHelper, SparkHelper }
import edu.gatech.cse6250.model.{ Diagnostic, LabResult, Medication }
import edu.gatech.cse6250.phenotyping.T2dmPhenotype
import org.apache.spark.mllib.clustering.{ GaussianMixture, KMeans, StreamingKMeans }
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{ DenseMatrix, Matrices, Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * @author Hang Su <hangsu@gatech.edu>,
 * @author Yu Jing <yjing43@gatech.edu>,
 * @author Ming Liu <mliu302@gatech.edu>
 */
object Main {
  def main(args: Array[String]) {
    import org.apache.log4j.{ Level, Logger }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val spark = SparkHelper.spark
    val sc = spark.sparkContext
    //  val sqlContext = spark.sqlContext

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(spark)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication)
    )

    // =========== USED FOR AUTO GRADING CLUSTERING GRADING =============
    // phenotypeLabel.map{ case(a,b) => s"$a\t$b" }.saveAsTextFile("data/phenotypeLabel")
    // featureTuples.map{ case((a,b),c) => s"$a\t$b\t$c" }.saveAsTextFile("data/featureTuples")
    // return
    // ==================================================================

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    val (kMeansPurity, gaussianMixturePurity, streamingPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] purity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] purity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] purity of StreamingKmeans is: $streamingPurity%.5f")

    /** feature construction with filtered features */
    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication)
    )

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)

    val (kMeansPurity2, gaussianMixturePurity2, streamingPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] purity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] purity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] purity of StreamingKmeans is: $streamingPurity2%.5f")
  }

  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures: RDD[(String, Vector)]): (Double, Double, Double) = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    println("phenotypeLabel: " + phenotypeLabel.count)
    /** scale features */
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray))) })
    println("features: " + features.count)
    val rawFeatureVectors = features.map(_._2).cache()
    println("rawFeatureVectors: " + rawFeatureVectors.count)

    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows

    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]

    def transform(feature: Vector): Vector = {
      val scaled = scaler.transform(Vectors.dense(feature.toArray))
      Vectors.dense(Matrices.dense(1, scaled.size, scaled.toArray).multiply(densePc).toArray)
    }
    val numberClusters = 3
    val maxIterations = 20
    /**
     * TODO: K Means Clustering using spark mllib
     * Train a k means model using the variabe featureVectors as input
     * Set maxIterations =20 and seed as 6250L
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.purity
     * Remove the placeholder below after your implementation
     */
    // features.zip(featureVectors).map(...to get patient ID).join(phenotypeLabel).map(....predict(feature))

    // val kMeansModel = KMeans.train(featureVectors,3,20,1,"k-means||",6250L)
    // val kMeansClusterAssignmentAndLabel = features.join(phenotypeLabel).map({ case (patientID, (feature, realClass)) => (kMeansModel.predict(transform(feature)), realClass)})
    // val kMeansPurity = purity(kMeansClusterAssignmentAndLabel)

    // val km = new KMeans().setK(numberClusters).setMaxIterations(20).setSeed(6250L)
    // val kMeanCluster = km.run(featureVectors)

    featureVectors.cache()
    val kmean = KMeans.train(featureVectors, numberClusters, maxIterations, "k-means||", 6250L).predict(featureVectors)
    val feature_id = features.map(_._1)
    val kmean_result = feature_id.zip(kmean)
    val kmean_AssignmentAndLabel = kmean_result.join(phenotypeLabel).map(_._2)
    val kMeansPurity = Metrics.purity(kmean_AssignmentAndLabel)

    // - Map the RDD to ((clusterId, classId), 1.0)
    // - Use reduceByKey to sum all the values together
    // - Use take() to get all the values
    // - print out all the values of the RDD
    // val kMeansPurity = 0.0
    val kmean_distribution = kmean_AssignmentAndLabel.map(f => (f, 1))
      .keyBy(_._1)
      .reduceByKey((x, y) => (x._1, x._2 + y._2))
    kmean_distribution.collect().foreach(println)
    println(kmean_AssignmentAndLabel.count().toDouble)

    /**
     * TODO: GMMM Clustering using spark mllib
     * Train a Gaussian Mixture model using the variabe featureVectors as input
     * Set maxIterations =20 and seed as 6250L
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.purity
     * Remove the placeholder below after your implementation
     */
    val gmm = new GaussianMixture().setK(numberClusters).setMaxIterations(maxIterations).setSeed(6250L).run(featureVectors).predict(featureVectors)
    val gmm_result = feature_id.zip(gmm)
    val gmm_AssignmentAndLabel = gmm_result.join(phenotypeLabel).map(_._2)
    val gaussianMixturePurity = Metrics.purity(gmm_AssignmentAndLabel)

    val gaussian_distribution = gmm_AssignmentAndLabel.map(f => (f, 1))
      .keyBy(_._1)
      .reduceByKey((x, y) => (x._1, x._2 + y._2))

    gaussian_distribution.collect().foreach(println)
    println(gmm_AssignmentAndLabel.count().toDouble)

    // val gaussianMixturePurity = 0.0

    /**
     * TODO: StreamingKMeans Clustering using spark mllib
     * Train a StreamingKMeans model using the variabe featureVectors as input
     * Set the number of cluster K = 3, DecayFactor = 1.0, number of dimensions = 10, weight for each center = 0.5, seed as 6250L
     * In order to feed RDD[Vector] please use latestModel, see more info: https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.mllib.clustering.StreamingKMeans
     * To run your model, set time unit as 'points'
     * Assign each feature vector to a cluster(predicted Class)
     * Obtain an RDD[(Int, Int)] of the form (cluster number, RealClass)
     * Find Purity using that RDD as an input to Metrics.purity
     * Remove the placeholder below after your implementation
     */
    val streamK = new StreamingKMeans().setK(numberClusters).setRandomCenters(10, 0.5, 6250L).latestModel().update(featureVectors, 1.0, "points").predict(featureVectors)
    val streamK_result = feature_id.zip(streamK)
    val streamK_AssignmentAndLabel = streamK_result.join(phenotypeLabel).map(_._2)
    val streamKmeansPurity = Metrics.purity(streamK_AssignmentAndLabel)

    val streamK_distribution = streamK_AssignmentAndLabel.map(f => (f, 1))
      .keyBy(_._1)
      .reduceByKey((x, y) => (x._1, x._2 + y._2))

    streamK_distribution.collect().foreach(println)
    println(streamK_AssignmentAndLabel.count().toDouble)

    // val streamingKmeansModel = new StreamingKMeans().setK(3).setDecayFactor(1.0).setRandomCenters(10,0.5,8803L).latestModel().update(featureVectors,1.0,"points")
    // val streamingClusterAssignmentAndLabel = features.join(phenotypeLabel).map({case (patientID, (feature, realClass)) => (streamingKmeansModel.predict(transform(feature)), realClass)})
    // val streamingPurity = purity(streamingClusterAssignmentAndLabel)

    // val streamKmeansPurity = 0.0

    (kMeansPurity, gaussianMixturePurity, streamKmeansPurity)
  }

  /**
   * load the sets of string for filtering of medication
   * lab result and diagnostics
   *
   * @return
   */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def sqlDateParser(input: String, pattern: String = "yyyy-MM-dd'T'HH:mm:ssX"): java.sql.Date = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    new java.sql.Date(dateFormat.parse(input).getTime)
  }

  def loadRddRawData(spark: SparkSession): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /* the sql queries in spark required to import sparkSession.implicits._ */
    import spark.implicits._
    val sqlContext = spark.sqlContext

    /* a helper function sqlDateParser may useful here */

    /**
     * load data using Spark SQL into three RDDs and return them
     * Hint:
     * You can utilize edu.gatech.cse6250.helper.CSVHelper
     * through your sparkSession.
     *
     * This guide may helps: https://bit.ly/2xnrVnA
     *
     * Notes:Refer to model/models.scala for the shape of Medication, LabResult, Diagnostic data type.
     * Be careful when you deal with String and numbers in String type.
     * Ignore lab results with missing (empty or NaN) values when these are read in.
     * For dates, use Date_Resulted for labResults and Order_Date for medication.
     *
     */

    /**
     * TODO: implement your own code here and remove
     * existing placeholder code below
     */

    // READ IN CSV's
    // ignore NANS!

    // SELECT * FROM table
    // WHERE Numeric_Result IS NOT NULL

    // val sql_lab_a = sqlContext.sql("SELECT Member_ID AS patientID, Date_Resulted AS date, Result_Name AS testName, Numeric_Result as value FROM Lab WHERE Numeric_Result is not null")

    val med_orders = CSVHelper.loadCSVAsTable(spark, "data/medication_orders_INPUT.csv", "med_orders")
    val encounter_dx = CSVHelper.loadCSVAsTable(spark, "data/encounter_dx_INPUT.csv", "encounter_dx")
    val all_encounters = CSVHelper.loadCSVAsTable(spark, "data/encounter_INPUT.csv", "all_encounters")
    val lab_results = CSVHelper.loadCSVAsTable(spark, "data/lab_results_INPUT.csv", "lab_results")

    val sql_med = sqlContext.sql("SELECT Member_ID AS patientID, Order_Date AS date, Drug_Name AS medicine FROM med_orders")
    val sql_dx = sqlContext.sql("SELECT all_encounters.Member_ID AS patientID, all_encounters.Encounter_DateTime AS date, encounter_dx.code AS code FROM all_encounters JOIN encounter_dx ON all_encounters.Encounter_ID = encounter_dx.Encounter_ID")
    val sql_lab = sqlContext.sql("SELECT Member_ID AS patientID, Date_Resulted AS date, Result_Name AS testName, Numeric_Result as value FROM lab_results WHERE Numeric_Result != ''")

    val medication: RDD[Medication] = sql_med.map(p => Medication(p(0).toString, sqlDateParser(p(1).toString), p(2).toString.toLowerCase)).as[Medication].rdd
    val labResult: RDD[LabResult] = sql_lab.map(p => LabResult(p(0).toString, sqlDateParser(p(1).toString), p(2).toString.toLowerCase, p(3).toString.filter(c => c != ',').toDouble)).as[LabResult].rdd
    val diagnostic: RDD[Diagnostic] = sql_dx.map(p => Diagnostic(p(0).toString, sqlDateParser(p(1).toString), p(2).toString.toLowerCase)).as[Diagnostic].rdd

    // val medication: RDD[Medication] = spark.sparkContext.emptyRDD
    // val labResult: RDD[LabResult] = spark.sparkContext.emptyRDD
    // val diagnostic: RDD[Diagnostic] = spark.sparkContext.emptyRDD

    // case class Medication(patientID: String, date: Date, medicine: String)
    // case class LabResult(patientID: String, date: Date, testName: String, value: Double)
    // case class Diagnostic(patientID: String, date: Date, code: String)

    (medication, labResult, diagnostic)
  }

}
