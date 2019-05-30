package review.sentiment.analysis.classifier

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import review.sentiment.analysis.Spark
import review.sentiment.analysis.classifier.ClassificationManager.{CalculateMarkRequest, CalculateMarkResponse, TrainRequest, TrainResponse}

import scala.math.min

import Spark.session.implicits._

abstract class BinaryClassifier(targetMark: Double) extends AbstractClassifier {

    //
    // Abstract methods
    //

    def calculateMark(df: DataFrame): Double
    def train(trainingData: DataFrame, testData: DataFrame): Double

    //
    // Public methods
    //

    def calculateMark(vec: SparseVector): Double = {
        val df = preparePredictData(vec)
        calculateMark(df)
    }

    def train(reviews: RDD[LabeledPoint]): Double = {
        val (trainingData, testData) = prepareTrainData(reviews, targetMark)
        train(trainingData, testData)
    }

    //
    // Private functions
    //

    private val preparePredictData = (vec: SparseVector) => {
        // Convert vector to RDD
        val rdd = Spark.ctx.makeRDD(Seq(Row(vec.asML)))

        // Convert rdd to dataframe
        val schema = new StructType()
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rdd, schema)

        // Return prepared data
        df
    }

    private val prepareTrainData = (reviews: RDD[LabeledPoint], targetMark: Double) => {
        log.info("Preparing data set for training...")

        // Convert reviews to sql rows
        val rows = reviews.map(review => Row(review.label, review.features.asML))

        // Convert reviews RDD to dataframe
        val schema = new StructType()
            .add("label", DoubleType)
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rows, schema)

        // Split dataset to positive and negative according to targetMark
        val posRows = df.filter($"label" === targetMark)
        val negRows = df.filter(!($"label" === targetMark))

        // Reduce datasets to have the same number of negative and positive records
        val recordsCount = min(posRows.count, negRows.count).toInt

        // Set posRows with label "1" and negRows with label "0"
        val pos = posRows.limit(recordsCount).withColumn("label", lit(1.0))
        val neg = negRows.limit(recordsCount).withColumn("label", lit(0.0))

        log.info(s"Dataset consists of ${pos.count} positive records and ${neg.count} negative records")

        // Split both pos and neg into training and test sets
        // val Array(posTraining, posTest) = pos.randomSplit(Array(0.6, 0.4), seed=1234)
        // val Array(negTraining, negTest) = neg.randomSplit(Array(0.6, 0.4), seed=1234)

        val Array(posTraining, posTest) = Array(pos, pos)
        val Array(negTraining, negTest) = Array(neg, neg)

        // Union both training and test sets and shuffle them
        val trainingData = posTraining.union(negTraining).orderBy(rand(seed=1234))
        val testData = posTest.union(negTest).orderBy(rand(seed=1234))

        log.info(s"Training dataset consists of ${posTraining.count} positive records and ${negTraining.count} negative records")
        log.info(s"Test dataset consists of ${posTest.count} positive records and ${negTest.count} negative records")

        // Return prepared data
        (trainingData, testData)
    }
}
