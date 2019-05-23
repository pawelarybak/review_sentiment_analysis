package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import review.sentiment.analysis.manager.AnalysisManager.{CalculateMarkRequest, CalculateMarkResponse}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object ClassificationManager {
    def props : Props = Props[ClassificationManager]
}

class ClassificationManager extends Actor with ActorLogging {

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
            log.info(s"Received text: $text")

            val marks : Future[List[Int]] = performClassificationRequests(text)
            val finalMark : Future[Int] = marks.map(a => calculateFinalMark(a))

            finalMark.map(a => CalculateMarkResponse(a))
                     .pipeTo(sender())
    }

    private def performClassificationRequests(requestText : String) : Future[List[Int]] = {
        val request = CalculateMarkRequest(requestText)
        val futureMarks : List[Future[CalculateMarkResponse]] = classifiers.map(_.ask(request)
                                                                            .mapTo[CalculateMarkResponse])
        Future.sequence(futureMarks)
            .map(_.map(_.mark))
    }

    private def calculateFinalMark(marks: List[Int]): Int = {
        val marksGrouped = marks.groupBy(identity).mapValues(_.size)
        val maxOccurrences = marksGrouped.maxBy(_._2)._2
        val mostOccurringMarks = marksGrouped.filter(_._2 == maxOccurrences).keys
        mostOccurringMarks.sum / mostOccurringMarks.size
    }


}
