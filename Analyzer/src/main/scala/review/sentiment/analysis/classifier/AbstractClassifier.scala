package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint

import review.sentiment.analysis.classifier.ClassificationManager.{CalculateMarkRequest, CalculateMarkResponse, TrainRequest, TrainResponse}

abstract class AbstractClassifier extends Actor with ActorLogging {

    override def receive: Receive = {
        case CalculateMarkRequest(vec) =>
            log.info(s"Calculating mark of vector...")
            val mark = calculateMark(vec)
            log.info(s"Mark calculation finished: $mark")
            sender() ! CalculateMarkResponse(mark)

        case TrainRequest(reviews) =>
        	log.info(s"Training with reviews...")
        	val accuracy = train(reviews)
        	log.info(s"Train finished. Accuracy: $accuracy")
        	sender() ! TrainResponse(accuracy)
    }

    def calculateMark(vec: SparseVector): Double

    def train(reviews: RDD[LabeledPoint]): Double
}
