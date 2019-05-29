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

object NaiveBayesClassifier {
    def props: Props = Props[NaiveBayesClassifier]
}

class NaiveBayesClassifier extends AbstractClassifier with Serializable {

    private var model: Option[NaiveBayesModel] = None

    override def train(reviews: RDD[LabeledPoint]): Double = {
        val result = doTrain(reviews)
        model = Some(result._1)
        result._2
    }

    override def calculateMark(vec: SparseVector): Double = {
        model.get.predict(vec.asML)
    }

    val doTrain = (reviews: RDD[LabeledPoint]) => {
        // Convert reviews to sql rows
        val rows = reviews.map(review => Row(review.label, review.features.asML))

        // Convert reviews RDD to dataframe
        val schema = new StructType()
            .add("label", DoubleType)
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rows, schema)

        // Split reviews into training and test set
        val Array(trainingData, testData) = df.randomSplit(Array(0.6, 0.4))

        // Train model
        val model = new NaiveBayes()
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
