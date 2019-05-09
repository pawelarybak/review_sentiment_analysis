package review.sentiment.analysis

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import review.sentiment.analysis.manager.AnalysisManager
import review.sentiment.analysis.manager.AnalysisManager.AnalyseTextRequest

object Main extends App {

    println("Hello")

    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)

    val analysisManager = system.actorOf(Props[AnalysisManager], "manager")

    val reviewText : String = "Example review text."
    analysisManager ! AnalyseTextRequest(reviewText)

    system.terminate()

}
