package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object ClassificationManager {
    def props: Props = Props[ClassificationManager]

    final case class TrainRequest(reviews: Array[(Array[Int], Int)])
    final case class TrainResponse(accuracy: Float)
    final case class CalculateMarkRequest(text: Array[String])
    final case class CalculateMarkResponse(mark: Int)
}

class ClassificationManager extends Actor with ActorLogging {

    import ClassificationManager._

    private val classifiers = List(
        context.actorOf(ExampleClassifier.props, "example_classifier"),
        context.actorOf(SecondClassifier.props, "second_classifier"),
        context.actorOf(SecondClassifier.props, "third_classifier"),
        context.actorOf(ExampleClassifier.props, "fourth_classifier")
    )

    private implicit val timeout: Timeout = Timeout(5 seconds)
    private implicit val ec = ExecutionContext.global

    override def receive: Receive = {
        case CalculateMarkRequest(text) =>
            log.info(s"Received text: ${text.mkString(" ")}")

            val marks: Future[List[Int]] = performClassificationRequests(text)
            val finalMark: Future[Int] = marks.map(a => calculateFinalMark(a))

            finalMark.map(mark => {log.info(s"Final mark: $mark"); mark})
                     .map(a => CalculateMarkResponse(a))
                     .pipeTo(sender())

        case TrainRequest(reviews) =>
            log.info(s"Training using ${reviews.size} reviews...")

            sender() ! TrainResponse(1.0f)
    }

    private def performClassificationRequests(requestText: Array[String]): Future[List[Int]] = {
        val calculateMarkRequest = CalculateMarkRequest(requestText)
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
