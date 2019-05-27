package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging}
import review.sentiment.analysis.classifier.ClassificationManager.{CalculateMarkRequest, CalculateMarkResponse, TrainRequest, TrainResponse}

abstract class AbstractClassifier extends Actor with ActorLogging {

    override def receive: Receive = {
        case CalculateMarkRequest(vec) =>
            log.info(s"Calculating mark of vector with size ${vec.size}...")
            val mark : Int = calculateMark(vec)
            log.info(s"Mark calculation finished: $mark")
            sender() ! CalculateMarkResponse(mark)

        case TrainRequest(reviews: Array[(Array[Int], Int)]) =>
        	log.info(s"Training with ${reviews.size} reviews...")
        	val accuracy = train(reviews)
        	log.info(s"Train finished. Accuracy: $accuracy")
        	sender() ! TrainResponse(accuracy)
    }

    def calculateMark(vec: Array[Int]): Int

    def train(reviews: Array[(Array[Int], Int)]): Float
}
