package review.sentiment.analysis.manager

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint

import review.sentiment.analysis.Spark
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
    final case class AnalyseTextResponse(mark: Double)
}

class AnalysisManager() extends Actor with ActorLogging {

    import AnalysisManager._

    private implicit val timeout = Timeout(99999 seconds)
    private implicit val ec = ExecutionContext.global

    private val classificationManager = context.actorOf(ClassificationManager.props, "classification_manager")
    private val preprocessor = context.actorOf(Stemmer.props, "example_preprocessor")
    private val reviewsDB = context.actorOf(ReviewsDB.props, "reviews_db")
    private val bowManager = context.actorOf(BOWManager.props, "bow_manager")

    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries=10, withinTimeRange=1 minute) {
        case _ => Restart
    }

    override def receive: Receive = {
        case InitializeRequest() =>
            log.info("Initializing...")
            val requestSender = sender()
            getReviews()
                .map(reviews => {
                    val marks = reviews.map(_._1)
                    val rawTexts = reviews.map(_._2)
                    stemTexts(rawTexts)
                        .flatMap(addProcessedTextsToBOW)
                        .map(vecs => zipRdds(marks, vecs).map(x => new LabeledPoint(x._1, x._2)))
                        .flatMap(reviews => { trainClassifiers(reviews) })
                        .map(_ => log.info("Initialization finished"))
                        .map(_ => InitializeResponse())
                        .pipeTo(requestSender)
                })

        case AnalyseTextRequest(text) =>
            log.info(s"Analysing text of size ${text.size}...")
            val texts = Spark.ctx.makeRDD(Array(text))
            preprocessor
                .ask(StemmingsRequest(texts))
                .mapTo[StemmingsResponse]
                .map(_.processedTexts)
                .flatMap(processedTexts => bowManager.ask(AnnotateTextsRequest(processedTexts)))
                .mapTo[AnnotateTextsResponse]
                .map(_.vecs.first)
                // .map(vecs => { log.info(s"Vector size: ${vecs.size}"); vecs})
                .flatMap(vec => classificationManager.ask(CalculateMarkRequest(vec)))
                .mapTo[CalculateMarkResponse]
                .map(response => AnalyseTextResponse(response.mark))
                .pipeTo(sender())
    }

    private def getReviews(): Future[RDD[(Double, String)]] = {
        log.info(s"Getting rawReviews from DB...")

        reviewsDB
            .ask(GetReviewsRequest())
            .mapTo[GetReviewsResponse]
            .map(_.reviews)
            // .map(reviews => { log.info(s"Got reviews from DB"); reviews })
    }

    private def zipRdds(marks: RDD[Double], vecs: RDD[SparseVector]): RDD[(Double, SparseVector)] = {
        log.info(s"Joining processed texts with their marks...")

        // Workaround against inability to zip two RDD with different numbers of partitions
        val newMarks = marks.zipWithIndex.map(_.swap)
        val newVecs = vecs.zipWithIndex.map(_.swap)
        newMarks.join(newVecs).map(_._2)
    }

    private def stemTexts(rawTexts: RDD[String]): Future[RDD[Array[String]]] = {
        log.info(s"Stemming raw texts...")
        preprocessor
            .ask(StemmingsRequest(rawTexts))
            .mapTo[StemmingsResponse]
            .map(_.processedTexts)
            // .map(processedTexts => { log.info(s"Successfully stemmed raw texts"); processedTexts })
    }

    private def addProcessedTextsToBOW(processedTexts: RDD[Array[String]]): Future[RDD[SparseVector]] = {
        log.info(s"Adding processed texts to BOW...")
        bowManager
            .ask(AddTextsRequest(processedTexts))
            .mapTo[AddTextsResponse]
            .map(_.vecs)
            // .map(response => (response.newWordsCount, response.vecs))
            // .map({ case (newWordsCount, vecs) =>
            //     log.info(s"Successfully added texts to BOW. New words count: ${newWordsCount}")
            //     vecs
            // })
    }

    private def trainClassifiers(processedReviews: RDD[LabeledPoint]): Future[Unit] = {
        log.info(s"Training classifiers using processed reviews...")
        classificationManager
            .ask(TrainRequest(processedReviews))
            // .mapTo[TrainResponse]
            // .map(_.accuracy)
            // .map(accuracy => log.info(s"Training with reviews finished. Accuracy: $accuracy"))
            .map(_ => Future.successful(Unit))
    }
}
