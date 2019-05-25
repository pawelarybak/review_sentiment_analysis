package review.sentiment.analysis

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Failure}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import review.sentiment.analysis.api.HttpServerActor
import review.sentiment.analysis.api.HttpServerActor.StartServer
import review.sentiment.analysis.manager.AnalysisManager
import review.sentiment.analysis.manager.AnalysisManager.{AddReviewsRequest, AddReviewsResponse}

object Main extends App {
    println("Initializing system...")
    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)
    val analysisManager = system.actorOf(Props(new AnalysisManager()), "analysis_manager")
    val httpServer = system.actorOf(Props(new HttpServerActor(analysisManager)), "http_server")

    private implicit val timeout = Timeout(5 seconds)
    private implicit val ec = ExecutionContext.global

    println("Training AnalysisManager...")
    val reviews = fetchReviews()
    val training = analysisManager.ask(AddReviewsRequest(reviews))
                                  .mapTo[AddReviewsResponse]
    training onComplete {
        case Success(response) =>
            println(s"Train complete. New words count: ${response.newWordsCount}, accuracy: ${response.accuracy}")

            println("Starting HttpServer...")
            httpServer ! StartServer

        case Failure(t) =>
            println(s"Train error: ${t.getMessage()}")
    }

    def fetchReviews(): Array[(String, Int)] = {
        Array(
            ("Spoko spoko", 10),
            ("Fajne to", 8),
            ("Eeee kiepskie", 2),
            ("Nuda", 1)
        )
    }
}
