package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging}
import review.sentiment.analysis.classifier.ClassificationManager.{CalculateMarkRequest, CalculateMarkResponse}

abstract class AbstractClassifier extends Actor with ActorLogging {

    override def receive: Receive = {
        case CalculateMarkRequest(vec) =>
            val mark : Int = calculateMark(vec)

            log.info(s"Received vec: ${vec.mkString(" ")}, given mark: $mark")

            sender() ! CalculateMarkResponse(mark)
    }

    def calculateMark(vec: Array[Int]): Int
}
