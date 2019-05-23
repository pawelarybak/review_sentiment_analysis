package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging}
import review.sentiment.analysis.manager.AnalysisManager.{CalculateMarkRequest, CalculateMarkResponse}

abstract class AbstractClassifier extends Actor with ActorLogging {

    override def receive: Receive = {
        case CalculateMarkRequest(text) =>
            val mark : Int = calculateMark(text)

            log.info(s"Received text: $text, given mark: $mark")

            sender() ! CalculateMarkResponse(mark)
    }

    def calculateMark(text: String): Int

}
