package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.{ask, pipe}
import scala.language.postfixOps
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object ClassificationManager {
    def props: Props = Props[ClassificationManager]

    final case class TrainRequest(reviews: Array[(Array[Int], Int)])
    final case class TrainResponse(accuracy: Float)
    final case class CalculateMarkRequest(vec: Array[Int])
    final case class CalculateMarkResponse(mark: Int)
}

class ClassificationManager() extends Actor with ActorLogging {

    import ClassificationManager._

    private val classifiers = List(
        context.actorOf(ExampleClassifier.props, "example_classifier"),
        context.actorOf(ExampleClassifier.props, "second_classifier"),
        context.actorOf(ExampleClassifier.props, "third_classifier"),
        context.actorOf(ExampleClassifier.props, "fourth_classifier")
    )

    private implicit val timeout: Timeout = Timeout(1000 seconds)
    private implicit val ec = ExecutionContext.global

    override def receive: Receive = {
        case CalculateMarkRequest(vec) =>
            log.info(s"Received vector of size ${vec.size}")

            val marks: Future[List[Int]] = performClassificationRequests(vec)
            val finalMark: Future[Int] = marks.map(a => calculateFinalMark(a))

            finalMark.map(mark => {log.info(s"Final mark: $mark"); mark})
                     .map(a => CalculateMarkResponse(a))
                     .pipeTo(sender())

        case TrainRequest(reviews) =>
            log.info(s"Training using ${reviews.size} reviews...")

            val accuracies = performTrainRequests(reviews)
            val finalAccuracy = accuracies.map(a => calculateFinalAccuracy(a))

            finalAccuracy.map(accuracy => { log.info(s"Train complete. Final accuracy: $accuracy"); accuracy } )
                         .map(accuracy => TrainResponse(accuracy))
                         .pipeTo(sender())
    }

    private def performTrainRequests(reviews: Array[(Array[Int], Int)]): Future[List[Float]] = {
        val trainRequest = TrainRequest(reviews)
        val futureAccuracies = classifiers.map(_.ask(trainRequest).mapTo[TrainResponse])

        Future.sequence(futureAccuracies).map(_.map(_.accuracy))
    }

    private def calculateFinalAccuracy(accuracies: List[Float]): Float = {
        accuracies.sum / accuracies.size
    }

    private def performClassificationRequests(vec: Array[Int]): Future[List[Int]] = {
        val calculateMarkRequest = CalculateMarkRequest(vec)
        val futureMarks = classifiers.map(_.ask(calculateMarkRequest).mapTo[CalculateMarkResponse])

        Future.sequence(futureMarks).map(_.map(_.mark))
    }

    private def calculateFinalMark(marks: List[Int]): Int = {
        val marksGrouped = marks.groupBy(identity).mapValues(_.size)
        val maxOccurrences = marksGrouped.maxBy(_._2)._2
        val mostOccurringMarks = marksGrouped.filter(_._2 == maxOccurrences).keys

        mostOccurringMarks.sum / mostOccurringMarks.size
    }

}
