package review.sentiment.analysis

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import akka.pattern.ask
import akka.util.Timeout
import review.sentiment.analysis.api.HttpServerActor
import review.sentiment.analysis.api.HttpServerActor.StartServer
import review.sentiment.analysis.manager.AnalysisManager
import review.sentiment.analysis.manager.AnalysisManager.{InitializeRequest, InitializeResponse}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


class MainSupervisor extends Actor with ActorLogging {

    private implicit val timeout = Timeout(99999 seconds)
    private implicit val ec = ExecutionContext.global

    var analysisManager : ActorRef =_
    var httpServer : ActorRef = _

    override def preStart(): Unit = {
        super.preStart()
        analysisManager = context.actorOf(Props(new AnalysisManager()), "analysis_manager")
        httpServer = context.actorOf(Props(new HttpServerActor(analysisManager)), "http_server")

        context.watch(analysisManager)
        context.watch(httpServer)
    }

    override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
        case _ => Restart
    }

    override def receive: Receive = {
        case InitializeRequest =>
            println("Initializing system...")
            analysisManager
                .ask(InitializeRequest())
                .mapTo[InitializeResponse]
                .map(_ => httpServer ! StartServer)
                .map(_ => println("Initialization finished!"))
    }

}
