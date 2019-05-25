package review.sentiment.analysis

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import review.sentiment.analysis.api.HttpServerActor
import review.sentiment.analysis.api.HttpServerActor.StartServer
import review.sentiment.analysis.manager.AnalysisManager

object Main extends App {

    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)

    println("Hello")

    val analysisManager : ActorRef = system.actorOf(Props[AnalysisManager], "analysis_manager")
    val httpServer = system.actorOf(Props(new HttpServerActor(analysisManager)), "http_server")

    httpServer ! StartServer
}
