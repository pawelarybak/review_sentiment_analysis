package review.sentiment.analysis

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import review.sentiment.analysis.api.HttpServerActor
import review.sentiment.analysis.api.HttpServerActor.StartServer
import review.sentiment.analysis.manager.AnalysisManager
import review.sentiment.analysis.manager.AnalysisManager.AnalyseTextRequest

object Main extends App {

    println("Hello")

    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)

    val httpServer = system.actorOf(Props[HttpServerActor], "http_server")
    httpServer ! StartServer
    val analysisManager = system.actorOf(Props[AnalysisManager], "manager")

    val reviewText : String = "Litwo! Ojczyzno moja! Ty jesteś jak zdrowie"

    analysisManager ! AnalyseTextRequest(reviewText)


    Thread.sleep(1000)

    system.terminate()

}
