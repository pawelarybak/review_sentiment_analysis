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

import scala.math.min

import Spark.session.implicits._

abstract class MultilabelClassifier extends AbstractClassifier {

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
        calculateMark(df) + 1.0
    }

    def train(reviews: RDD[LabeledPoint]): Double = {
        val (trainingData, testData) = prepareTrainData(reviews)
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

    private val prepareTrainData = (reviews: RDD[LabeledPoint]) => {
        log.info("Preparing data set for training...")

        // Convert reviews to sql rows
        val rows = reviews.map(review => Row(review.label-1.0, review.features.asML))

        // Convert reviews RDD to dataframe
        val schema = new StructType()
            .add("label", DoubleType)
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rows, schema)

        // Split both pos and neg into training and test sets
        val Array(trainingData, testData) = df.randomSplit(Array(0.6, 0.4))

        log.info(s"Dataset consists of ${trainingData.count} training records and ${testData.count} test records")

        // Return prepared data
        (trainingData, testData)
    }
}
