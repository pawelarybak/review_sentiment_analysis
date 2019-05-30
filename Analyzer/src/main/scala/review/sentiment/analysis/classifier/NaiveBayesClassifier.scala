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
        // Convert reviews to sql rows
        val rows = reviews.map(review => Row(review.label, review.features.asML))

        // Convert reviews RDD to dataframe
        val schema = new StructType()
            .add("label", DoubleType)
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rows, schema)

        // Mark all rows with label equal to target mark as 1.0 and all other to 0.0 (binary classification)
        val mdf = df.withColumn("label", when($"label" === targetMark, 1.0).otherwise(0.0))

        // Split dataframe into training and test set
        val Array(trainingData, testData) = mdf.randomSplit(Array(0.6, 0.4), seed=1234)

        // Train model
        val model = new NaiveBayes()
            .setModelType("multinomial") // Note that bernoulli could not be used here, since occurencies may be greater than "1"
            .setSmoothing(0.85)
            .fit(trainingData)

        // Calculate accuracy
        val predictions = model.transform(testData)
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)

        (model, accuracy)
    }
}
