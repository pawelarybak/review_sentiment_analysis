package review.sentiment.analysis.manager

import akka.actor.{Actor, ActorLogging}
import review.sentiment.analysis.classifier.ExampleClassifier
import review.sentiment.analysis.manager.AnalysisManager.{AnalyseTextRequest, CalculateMarkRequest, CalculateMarkResponse}

object AnalysisManager {
    final case class AnalyseTextRequest(text: String)
    final case class CalculateMarkRequest(text: String)
    final case class CalculateMarkResponse(mark: Int)
}

class AnalysisManager extends Actor with ActorLogging {

    override def receive: Receive = {
        case AnalyseTextRequest(text) =>
            val classifierRef = context.actorOf(ExampleClassifier.props, "example_classifier")

            classifierRef ! CalculateMarkRequest(text)

        case CalculateMarkResponse(mark) =>
            log.info(s"Received mark: $mark")
    }

}
