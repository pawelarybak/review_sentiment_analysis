package review.sentiment.analysis.manager

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}

import akka.actor.{Actor, ActorLogging}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import review.sentiment.analysis.classifier.ClassificationManager
import review.sentiment.analysis.classifier.ClassificationManager.{CalculateMarkRequest, CalculateMarkResponse, TrainRequest, TrainResponse}
import review.sentiment.analysis.preprocessing.Stemmer
import review.sentiment.analysis.preprocessing.Stemmer.{StemmingRequest, StemmingResponse, StemmingsRequest, StemmingsResponse}
import review.sentiment.analysis.bowgen.BOWGenerator
import review.sentiment.analysis.bowgen.BOWGenerator.{AddTextsRequest, AddTextsResponse}

object AnalysisManager {
    final case class AddReviewsRequest(reviews: Array[(String, Int)])
    final case class AddReviewsResponse(newWordsCount: Int, accuracy: Float)
    final case class AnalyseTextRequest(text: String)
    final case class AnalyseTextResponse(mark: Int)
}

class AnalysisManager extends Actor with ActorLogging {

    import AnalysisManager._

    private implicit val timeout = Timeout(5 seconds)
    private implicit val ec = ExecutionContext.global

    private val classificationManager = context.actorOf(ClassificationManager.props, "classification_manager")
    private val preprocessor = context.actorOf(Stemmer.props, "example_preprocessor")
    private val bowGenerator = context.actorOf(BOWGenerator.props, "bow_generator")

    override def receive: Receive = {
        case AddReviewsRequest(reviews) =>
            log.info(s"Adding ${reviews.size} reviews...")
            val texts = reviews.map(_._1)
            val marks = reviews.map(_._2)
            val requestSender = sender()
            val textAdding =
                preprocessor.ask(StemmingsRequest(texts))
                            .mapTo[StemmingsResponse]
                            .map(_.processedTexts)
                            .flatMap(processedTexts => bowGenerator.ask(AddTextsRequest(processedTexts)))
                            .mapTo[AddTextsResponse]
                            .map(response => (response.newWordsCount, response.vecs))

            textAdding onComplete {
                case Success((newWordsCount, vecs)) =>
                    val processedReviews = vecs.zip(marks)
                    classificationManager.ask(TrainRequest(processedReviews))
                        .mapTo[TrainResponse]
                        .map(response => response.accuracy)
                        .map(accuracy => AddReviewsResponse(newWordsCount, accuracy))
                        .pipeTo(requestSender)

                case Failure(t) =>
                    println("Could not add texts")
                    // What the fuck to do now?
            }

        case AnalyseTextRequest(text) =>
            log.info(s"Analysing text of size ${text.size}...")
            preprocessor.ask(StemmingRequest(text))
                        .mapTo[StemmingResponse]
                        .map(_.processedText)
                        .flatMap(processedText => classificationManager.ask(CalculateMarkRequest(processedText)))
                        .mapTo[CalculateMarkResponse]
                        .map(response => AnalyseTextResponse(response.mark))
                        .pipeTo(sender())
    }

}
