package review.sentiment.analysis.api

import akka.actor.{Actor, ActorLogging}
import akka.stream.ActorMaterializer
import review.sentiment.analysis.Main
import review.sentiment.analysis.api.HttpServerActor.StartServer

object HttpServerActor {
    final case class StartServer()
}

class HttpServerActor extends Actor with ActorLogging {

    implicit val materializer = ActorMaterializer()
    implicit val system = Main.system
    implicit val executionContext = system.dispatcher

    val httpServer : HttpServer = new HttpServer

    override def receive: Receive = {
        case StartServer => start()
    }

    def start(): Unit = {
        httpServer.start(analyze)
    }

    def analyze(text : String) : Int = {
        5
    }

}
