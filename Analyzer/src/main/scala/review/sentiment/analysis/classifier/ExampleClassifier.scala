package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging, Props}
import review.sentiment.analysis.manager.AnalysisManager.{CalculateMarkRequest, CalculateMarkResponse}

object ExampleClassifier {
    def props : Props = Props[ExampleClassifier]
}

class ExampleClassifier extends Actor with ActorLogging  {

    override def receive: Receive = {
        case CalculateMarkRequest(text) =>
            log.info(s"Received text: $text")

            val mark : Int = 5

            sender() ! CalculateMarkResponse(mark)
    }

}
