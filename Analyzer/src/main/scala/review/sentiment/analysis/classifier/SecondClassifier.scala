package review.sentiment.analysis.classifier

import akka.actor.Props

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD

object SecondClassifier {
    def props: Props = Props[SecondClassifier]
}

class SecondClassifier() extends AbstractClassifier  {

    override def calculateMark(vec: SparseVector): Double = 5

    def train(reviews: RDD[LabeledPoint]): Double = {
    	val accuracy = 1.0f
    	accuracy
    }
}
