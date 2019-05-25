package review.sentiment.analysis.manager

import akka.actor.{Actor, ActorLogging}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import scala.concurrent.duration._
import review.sentiment.analysis.classifier.ClassificationManager
import review.sentiment.analysis.classifier.ClassificationManager.{CalculateMarkRequest, CalculateMarkResponse}
import review.sentiment.analysis.preprocessing.Stemmer
import review.sentiment.analysis.preprocessing.Stemmer.{StemmingRequest, StemmingResponse}

import scala.concurrent.ExecutionContext

object AnalysisManager {
    final case class AnalyseTextRequest(text: String)
    final case class AnalyseTextResponse(mark: Int)
}

class AnalysisManager extends Actor with ActorLogging {

    import AnalysisManager._

    private implicit val timeout: Timeout = Timeout(5 seconds)
    private implicit val ec = ExecutionContext.global
    private val classificationManager = context.actorOf(ClassificationManager.props, "classification_manager")
    private val preprocessor = context.actorOf(Stemmer.props, "example_preprocessor")

    override def receive: Receive = {
        case AnalyseTextRequest(text) =>
            preprocessor.ask(StemmingRequest(text))
                        .mapTo[StemmingResponse]
                        .map(_.processedText)
                        .flatMap(processedText => classificationManager.ask(CalculateMarkRequest(processedText)))
                        .mapTo[CalculateMarkResponse]
                        .map(_.mark)
                        .map(AnalyseTextResponse)
                        .pipeTo(sender())
    }

}
