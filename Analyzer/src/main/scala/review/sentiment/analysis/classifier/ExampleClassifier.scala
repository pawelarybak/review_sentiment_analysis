package review.sentiment.analysis.classifier

import akka.actor.Props

import review.sentiment.analysis.Spark

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils

object ExampleClassifier {
    def props: Props = Props[ExampleClassifier]
}

class ExampleClassifier() extends AbstractClassifier with Serializable {

    private implicit val lambda = 1.0
    private implicit val modelType = "multinomial"

    private var model: Option[NaiveBayesModel] = None

    override def train(reviews: RDD[LabeledPoint]): Double = {
        val result = doTrain(reviews)
        model = result._1
        result._2
    }

    override def calculateMark(vec: SparseVector): Double = {
        model.get.predict(vec)
    }

    val doTrain = (reviews: RDD[LabeledPoint]) => {
        // Split reviews into training and test set
        val Array(training, test) = reviews.randomSplit(Array(0.5, 0.5))

        // Train model and get accuracy
        val model = Some(NaiveBayes.train(training))
        val predictionAndLabel = test.map(p => (model.get.predict(p.features), p.label))
        val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

        (model, accuracy)
    }
}
