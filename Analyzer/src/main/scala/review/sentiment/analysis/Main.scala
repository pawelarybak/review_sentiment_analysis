package review.sentiment.analysis

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Success, Failure}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import review.sentiment.analysis.api.HttpServerActor
import review.sentiment.analysis.api.HttpServerActor.StartServer
import review.sentiment.analysis.manager.AnalysisManager
import review.sentiment.analysis.manager.AnalysisManager.{InitializeRequest, InitializeResponse}

object Main extends App {

    println("Initializing system...")
    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)

    val analysisManager = system.actorOf(Props(new AnalysisManager()), "analysis_manager")
    val httpServer = system.actorOf(Props(new HttpServerActor(analysisManager)), "http_server")

    private implicit val timeout = Timeout(99999 seconds)
    private implicit val ec = ExecutionContext.global

    analysisManager
        .ask(InitializeRequest())
        .mapTo[InitializeResponse]
        .map(_ => httpServer ! StartServer)
        .map(_ => println("Initialization finished!"))
}
