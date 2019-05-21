package review.sentiment.analysis.manager

import akka.actor.{Actor, ActorLogging}
import review.sentiment.analysis.classifier.ExampleClassifier
import review.sentiment.analysis.preprocessing.Stemmer
import review.sentiment.analysis.preprocessing.Stemmer.{StemmingRequest, StemmingResponse}
import review.sentiment.analysis.manager.AnalysisManager.{AnalyseTextRequest, CalculateMarkRequest, CalculateMarkResponse}

object AnalysisManager {
    final case class AnalyseTextRequest(text: String)
    final case class CalculateMarkRequest(text: String)
    final case class CalculateMarkResponse(mark: Int)
}

class AnalysisManager extends Actor with ActorLogging {

    override def receive: Receive = {
        case AnalyseTextRequest(text) =>
            val preprocessorRef = context.actorOf(Stemmer.props, "example_preprocessor")

            preprocessorRef ! StemmingRequest(text)

        case StemmingResponse(preprocessedText) =>
            println(preprocessedText)
            val classifierRef = context.actorOf(ExampleClassifier.props, "example_classifier")

            classifierRef ! CalculateMarkRequest(preprocessedText)

        case CalculateMarkResponse(mark) =>
            log.info(s"Received mark: $mark")
    }

}
