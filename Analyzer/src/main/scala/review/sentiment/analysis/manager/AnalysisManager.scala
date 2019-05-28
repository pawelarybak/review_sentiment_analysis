package review.sentiment.analysis.manager

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import review.sentiment.analysis.bowgen.BOWManager
import review.sentiment.analysis.bowgen.BOWManager.{AddTextsRequest, AddTextsResponse, AnnotateTextsRequest, AnnotateTextsResponse}
import review.sentiment.analysis.classifier.ClassificationManager
import review.sentiment.analysis.classifier.ClassificationManager.{CalculateMarkRequest, CalculateMarkResponse, TrainRequest, TrainResponse}
import review.sentiment.analysis.preprocessing.Stemmer
import review.sentiment.analysis.preprocessing.Stemmer.{StemmingsRequest, StemmingsResponse}
import review.sentiment.analysis.training.ReviewsDB
import review.sentiment.analysis.training.ReviewsDB.{GetReviewsRequest, GetReviewsResponse}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object AnalysisManager {
    def props: Props = Props[AnalysisManager]

    final case class InitializeRequest()
    final case class InitializeResponse()
    final case class AnalyseTextRequest(text: String)
    final case class AnalyseTextResponse(mark: Int)
}

class AnalysisManager() extends Actor with ActorLogging {

    import AnalysisManager._

    private implicit val timeout = Timeout(99999 seconds)
    private implicit val ec = ExecutionContext.global

    private val classificationManager = context.actorOf(ClassificationManager.props, "classification_manager")
    private val preprocessor = context.actorOf(Stemmer.props, "example_preprocessor")
    private val reviewsDB = context.actorOf(ReviewsDB.props, "reviews_db")
    private val bowManager = context.actorOf(BOWManager.props, "bow_generator")

    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
        case _ => Restart
    }

    override def receive: Receive = {
        case InitializeRequest() =>
            log.info("Initializing...")

            val requestSender = sender()
            getReviews()
                .flatMap(processReviews)
                .map(_ => {
                    log.info("Initialization finished")
                    requestSender ! InitializeResponse()
                })

        case AnalyseTextRequest(text) =>
            log.info(s"Analysing text of size ${text.size}...")

            preprocessor
                .ask(StemmingsRequest(Array(text)))
                .mapTo[StemmingsResponse]
                .map(_.processedTexts.head)
                .flatMap(processedText => bowManager.ask(AnnotateTextsRequest(Array(processedText))))
                .mapTo[AnnotateTextsResponse]
                .map(_.vecs.head)
                .flatMap(vec => classificationManager.ask(CalculateMarkRequest(vec)))
                .mapTo[CalculateMarkResponse]
                .map(response => AnalyseTextResponse(response.mark))
                .pipeTo(sender())
    }

    private def getReviews(): Future[Array[(String, Int)]] = {
        log.info(s"Getting rawReviews from DB...")

        reviewsDB
            .ask(GetReviewsRequest())
            .mapTo[GetReviewsResponse]
            .map(_.reviews)
            .map(reviews => { log.info(s"Got ${reviews.size} reviews from DB"); reviews })
    }

    private def processReviews(rawReviews: Array[(String, Int)]): Future[Unit] = {
        log.info(s"Processing ${rawReviews.size} raw reviews...");

        val (rawTexts, marks) = rawReviews.unzip

        stemTexts(rawTexts)
            .flatMap(addProcessedTextsToBOW)
            .flatMap(vecs => { trainClassifiers(vecs.zip(marks)) })
            .map(_ => {
                log.info("Processing finished")
                Future.successful(Unit)
            })
    }

    private def stemTexts(rawTexts: Array[String]): Future[Array[Array[String]]] = {
        log.info(s"Stemming ${rawTexts.size} raw texts...")

        preprocessor
            .ask(StemmingsRequest(rawTexts))
            .mapTo[StemmingsResponse]
            .map(_.processedTexts)
            .map(processedTexts => { log.info(s"Successfully stemmed ${processedTexts.size} raw texts"); processedTexts })
    }

    private def addProcessedTextsToBOW(processedTexts: Array[Array[String]]): Future[Array[Array[Int]]] = {
        log.info(s"Adding ${processedTexts.size} processed texts to BOW...")

        bowManager
            .ask(AddTextsRequest(processedTexts))
            .mapTo[AddTextsResponse]
            .map(response => (response.newWordsCount, response.vecs))
            .map({ case (newWordsCount, vecs) =>
                log.info(s"Successfully added ${processedTexts.size} texts to BOW. New words count: ${newWordsCount}")
                vecs
            })
    }

    private def trainClassifiers(processedReviews: Array[(Array[Int], Int)]): Future[Unit] = {
        log.info(s"Training classifiers using ${processedReviews.size} processed reviews...")

        classificationManager
            .ask(TrainRequest(processedReviews))
            .mapTo[TrainResponse]
            .map(_.accuracy)
            .map(accuracy => {
                log.info(s"Training with ${processedReviews.size} reviews finished. Accuracy: $accuracy");
                Future.successful(Unit)
            })
    }
}
