package review.sentiment.analysis.api

import akka.actor.{Actor, ActorLogging, ActorRef, Kill}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import review.sentiment.analysis.Main
import review.sentiment.analysis.api.HttpServerActor.StartServer
import review.sentiment.analysis.manager.AnalysisManager.{AnalyseTextRequest, AnalyseTextResponse}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

object HttpServerActor {
    final case class StartServer()
}

class HttpServerActor(analysisManager: ActorRef) extends Actor with ActorLogging {

    implicit val materializer = ActorMaterializer()
    implicit val system = Main.system
    implicit val executionContext = system.dispatcher
    implicit val timeout = Timeout(5 seconds)

    val httpServer : HttpServer = new HttpServer

    override def receive: Receive = {
        case StartServer => start()
    }

    def start(): Unit = {
        httpServer.start(analyze, kill)
    }

    def kill(actor: String) : Unit = {
        system.actorSelection(actor).resolveOne().onComplete {
            case Success(actorRef) =>
                actorRef ! Kill
            case Failure(ex) =>
                log.error(actor + " does not exist")
        }
    }

    def analyze(text: String): Future[Int] = {
        analysisManager.ask(AnalyseTextRequest(text))
                       .mapTo[AnalyseTextResponse]
                       .map(_.mark)
    }

}
