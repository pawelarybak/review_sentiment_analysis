package review.sentiment.analysis.classifier

import akka.actor.Props

import review.sentiment.analysis.Spark

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

object ExampleClassifier {
    def props: Props = Props[ExampleClassifier]
}

class ExampleClassifier() extends AbstractClassifier with Serializable {

    private implicit val lambda = 1.0
    private implicit val modelType = "multinomial"

    private var model: Option[NaiveBayesModel] = None

    override def train(reviews: Array[(Array[Int], Int)]): Float = {
        val result = doTrain(reviews)
        model = result._1
        result._2
    }

    override def calculateMark(text: Array[Int]): Int = {
        val data = new DenseVector(text.map(_.toDouble))
        model.get.predict(data).toInt
    }

    val doTrain = (reviews: Array[(Array[Int], Int)]) => {
        // Convert reviews into RDD
        val nsplits = 50
        val labeledPoints = reviews.map(rev => LabeledPoint(rev._2, new DenseVector(rev._1.map(_.toDouble))))
        val data = Spark.ctx.makeRDD(labeledPoints, nsplits)

        // Split reviews into training and test set
        val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

        // Train model and get accuracy
        val model = Some(NaiveBayes.train(training))
        val predictionAndLabel = test.map(p => (model.get.predict(p.features), p.label))
        val accuracy = 1.0f * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

        (model, accuracy)
    }
}
