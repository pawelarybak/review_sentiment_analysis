package review.sentiment.analysis.classifier

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
import review.sentiment.analysis.manager.AnalysisManager.{CalculateMarkRequest, CalculateMarkResponse}

import scala.concurrent.Await
import scala.concurrent.duration._

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

    implicit val timeout: Timeout = Timeout(5 seconds)

    override def receive: Receive = {
        case CalculateMarkRequest(text) =>
            log.info(s"Received text: $text")

            val marks : List[Int] = performClassificationRequests(text)
            val finalMark : Int = calculateFinalMark(marks)

            sender() ! CalculateMarkResponse(finalMark)
    }

    private def performClassificationRequests(requestText : String) : List[Int] = {
        val request = CalculateMarkRequest(requestText)
        classifiers.map(_ ? request)
                    .map(futureResult => Await.result(futureResult, timeout.duration).asInstanceOf[CalculateMarkResponse])
                    .map(_.mark)
    }

    private def calculateFinalMark(marks: List[Int]): Int = {
        val marksGrouped = marks.groupBy(identity).mapValues(_.size)
        val maxOccurrences = marksGrouped.maxBy(_._2)._2
        val mostOccurringMarks = marksGrouped.filter(_._2 == maxOccurrences).keys
        mostOccurringMarks.sum / mostOccurringMarks.size
    }


}
