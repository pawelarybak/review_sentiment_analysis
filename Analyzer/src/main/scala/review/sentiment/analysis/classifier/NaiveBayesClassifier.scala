package review.sentiment.analysis.classifier

import akka.actor.Props

import review.sentiment.analysis.Spark

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.SparseVector

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import Spark.session.implicits._
import org.apache.spark.sql.functions._

import scala.math.min

object NaiveBayesClassifier {
    def props: Props = Props[NaiveBayesClassifier]
}

class NaiveBayesClassifier(targetMark: Double) extends AbstractClassifier with Serializable {

    private var model: Option[NaiveBayesModel] = None

    override def train(reviews: RDD[LabeledPoint]): Double = {
        val result = doTrain(reviews, targetMark)
        model = Some(result._1)
        result._2
    }

    override def calculateMark(vec: SparseVector): Double = {
        // Convert vector to RDD
        val rdd = Spark.ctx.makeRDD(Seq(Row(vec.asML)))

        // Convert rdd to dataframe
        val schema = new StructType()
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rdd, schema)

        // Predict value
        val predictions = model.get.transform(df)
        val prob = predictions.select("probability").head
        log.info(s"probability: ${prob}")

        // Get final prediction
        predictions.head.getDouble(3)
    }

    val doTrain = (reviews: RDD[LabeledPoint], targetMark: Double) => {
        log.info("Preparing data set to train...")

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

        log.info("Training a model...")
        val model = new NaiveBayes()
            .setModelType("multinomial") // Note that bernoulli could not be used here, since occurencies may be greater than "1"
            .setSmoothing(0.1875)
            .fit(trainingData)

        log.info("Veryfing model...")
        val predictions = model.transform(testData)
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)

        (model, accuracy)
    }
}
