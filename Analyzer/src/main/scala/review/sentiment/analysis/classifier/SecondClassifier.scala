package review.sentiment.analysis.classifier

import akka.actor.Props

import org.apache.spark.SparkContext

object SecondClassifier {
    def props: Props = Props[SecondClassifier]
}

class SecondClassifier(sc: SparkContext) extends AbstractClassifier  {

    override def calculateMark(vec: Array[Int]): Int = 5

    def train(reviews: Array[(Array[Int], Int)]): Float = {
    	val accuracy = 1.0f
    	accuracy
    }
}
