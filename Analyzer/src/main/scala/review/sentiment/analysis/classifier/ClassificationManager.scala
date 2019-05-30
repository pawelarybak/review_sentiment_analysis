package review.sentiment.analysis.classifier

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

object ClassificationManager {
    def props: Props = Props[ClassificationManager]

    final case class TrainRequest(reviews: RDD[LabeledPoint])
    final case class TrainResponse(accuracy: Double)
    final case class CalculateMarkRequest(vec: SparseVector)
    final case class CalculateMarkResponse(mark: Double)
}

class ClassificationManager() extends Actor with ActorLogging {

    import ClassificationManager._

    private val classifiers = List(
        context.actorOf(Props(new NaiveBayesClassifier(1.0)), "nb_classifier_1"),
        context.actorOf(Props(new NaiveBayesClassifier(2.0)), "nb_classifier_2"),
        context.actorOf(Props(new NaiveBayesClassifier(3.0)), "nb_classifier_3"),
        context.actorOf(Props(new NaiveBayesClassifier(4.0)), "nb_classifier_4"),
        context.actorOf(Props(new NaiveBayesClassifier(5.0)), "nb_classifier_5"),
        context.actorOf(Props(new NaiveBayesClassifier(6.0)), "nb_classifier_6"),
        context.actorOf(Props(new NaiveBayesClassifier(7.0)), "nb_classifier_7"),
        context.actorOf(Props(new NaiveBayesClassifier(8.0)), "nb_classifier_8"),
        context.actorOf(Props(new NaiveBayesClassifier(9.0)), "nb_classifier_9"),
        context.actorOf(Props(new NaiveBayesClassifier(10.0)), "nb_classifier_10")
    )

    private implicit val timeout: Timeout = Timeout(1000 seconds)
    private implicit val ec = ExecutionContext.global

    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
        case _ => Stop
    }

    override def receive: Receive = {
        case CalculateMarkRequest(vec) =>
            log.info(s"Calculating mark for vector...")

            val marks = performClassificationRequests(vec)
            val finalMark = marks.map(a => calculateFinalMark(a))

            finalMark.map(mark => {log.info(s"Final mark: $mark"); mark})
                     .map(a => CalculateMarkResponse(a))
                     .pipeTo(sender())

        case TrainRequest(reviews) =>
            log.info(s"Training using reviews...")

            val accuracies = performTrainRequests(reviews)
            val finalAccuracy = accuracies.map(a => calculateFinalAccuracy(a))

            finalAccuracy.map(accuracy => { log.info(s"Train complete. Final accuracy: $accuracy"); accuracy } )
                         .map(accuracy => TrainResponse(accuracy))
                         .pipeTo(sender())
    }

    private def performTrainRequests(reviews: RDD[LabeledPoint]): Future[List[Double]] = {
        val trainRequest = TrainRequest(reviews)
        val futureAccuracies = classifiers.map(_.ask(trainRequest).mapTo[TrainResponse])

        Future.sequence(futureAccuracies).map(_.map(_.accuracy))
    }

    private def calculateFinalAccuracy(accuracies: List[Double]): Double = {
        accuracies.sum / accuracies.size
    }

    private def performClassificationRequests(vec: SparseVector): Future[List[Double]] = {
        val calculateMarkRequest = CalculateMarkRequest(vec)
        val futureMarks = classifiers.map(_.ask(calculateMarkRequest).recover( {
            case e : Exception => CalculateMarkResponse(-1)
        }).mapTo[CalculateMarkResponse])

        Future.sequence(futureMarks)
            .map(_.map(_.mark)
                    .filter(_ != -1)
            )
    }

    private def calculateFinalMark(marks: List[Double]): Double = {
        val marksGrouped = marks.groupBy(identity).mapValues(_.size)
        val maxOccurrences = marksGrouped.maxBy(_._2)._2
        val mostOccurringMarks = marksGrouped.filter(_._2 == maxOccurrences).keys

        mostOccurringMarks.sum / mostOccurringMarks.size
    }
}
