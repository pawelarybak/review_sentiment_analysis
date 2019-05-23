package review.sentiment.analysis.manager

import akka.actor.{Actor, ActorLogging}
import review.sentiment.analysis.classifier.ClassificationManager
import review.sentiment.analysis.manager.AnalysisManager.{AnalyseTextRequest, CalculateMarkRequest, CalculateMarkResponse}
import review.sentiment.analysis.preprocessing.Stemmer
import review.sentiment.analysis.preprocessing.Stemmer.{StemmingRequest, StemmingResponse}

object AnalysisManager {
    final case class AnalyseTextRequest(text: String)
    final case class CalculateMarkRequest(text: String)
    final case class CalculateMarkResponse(mark: Int)
}

class AnalysisManager extends Actor with ActorLogging {

    private val classificationManager = context.actorOf(ClassificationManager.props, "classification_manager")
    private val preprocessor = context.actorOf(Stemmer.props, "example_preprocessor")

    override def receive: Receive = {
        case AnalyseTextRequest(text) =>
            preprocessor ! StemmingRequest(text)

        case StemmingResponse(preprocessedText) =>
            classificationManager ! CalculateMarkRequest(preprocessedText)

        case CalculateMarkResponse(mark) =>
            log.info(s"Received mark: $mark")
    }

}
