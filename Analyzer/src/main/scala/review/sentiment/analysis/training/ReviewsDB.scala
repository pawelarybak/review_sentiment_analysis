package review.sentiment.analysis.training

import scala.io.Source

import review.sentiment.analysis.Spark

import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

import akka.actor.{Actor, ActorLogging, Props}

import Spark.session.implicits._

object ReviewsDB {
    def props: Props = Props[ReviewsDB]

    final case class GetReviewsRequest()
    final case class GetReviewsResponse(reviews: RDD[(Double, String)])
}

class ReviewsDB extends Actor with ActorLogging {

    import ReviewsDB._

    private val dataPath = "training_data.csv";

    override def receive: Receive = {
        case GetReviewsRequest() =>
            log.info(s"Loading training data from: ${dataPath}...")

            val csvSchema = new StructType()
                .add("rating", DoubleType, true)
                .add("text", StringType, true)
            val csv = Spark.sql.read
                .option("header", "true")
                .schema(csvSchema)
                // .limit(100)
                .csv("src/main/resources/training_data.csv")
            val reviews = csv
                .map(row => (row.getDouble(0), row.getString(1)))
                .rdd

            sender() ! GetReviewsResponse(reviews)
    }

}
