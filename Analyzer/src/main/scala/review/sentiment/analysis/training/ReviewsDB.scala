package review.sentiment.analysis.training

import scala.io.Source

import akka.actor.{Actor, ActorLogging, Props}

object ReviewsDB {
  def props: Props = Props[ReviewsDB]

  final case class GetReviewsRequest()
  final case class GetReviewsResponse(reviews: Array[(String, Int)])
}

class ReviewsDB extends Actor with ActorLogging {

  import ReviewsDB._

  private val dataPath = "training_data.csv";

  override def receive: Receive = {
    case GetReviewsRequest() =>
      log.info(s"Loading training data from: ${dataPath}...")

      val reviews = Source.fromResource(dataPath)
          .getLines
          .drop(1) // csv header
          .take(1000)
          .map(row => {
            val value = row.split(",", 2)
            (value(1), value(0).toFloat.toInt)
          })
          .toArray

      sender() ! GetReviewsResponse(reviews)
  }

}
