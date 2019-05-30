package review.sentiment.analysis

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.sql.SparkSession

import review.sentiment.analysis.api.HttpServerActor
import review.sentiment.analysis.api.HttpServerActor.StartServer
import review.sentiment.analysis.manager.AnalysisManager
import review.sentiment.analysis.manager.AnalysisManager.{InitializeRequest, InitializeResponse}

object Spark {
    val session = SparkSession.builder()
        .master("local[*]")
        .appName("rsa-system")
        .config("spark.jars", "target/scala-2.11/analyzer_2.11-0.1.jar")
        .config("spark.executor.memory", "8g")
        .getOrCreate()

    val ctx = session.sparkContext
    val sql = session.sqlContext
}

object Main extends App {
    println("Initializing system...")
    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)
    Spark.ctx.setLogLevel("WARN")

    val supervisor = system.actorOf(Props[MainSupervisor], "supervisor")
    val analysisManager = system.actorOf(AnalysisManager.props, "analysis_manager")
    val httpServer = system.actorOf(Props(new HttpServerActor(analysisManager)), "http_server")

    private implicit val timeout = Timeout(99999 seconds)
    private implicit val ec = ExecutionContext.global

    supervisor ! InitializeRequest

    analysisManager
        .ask(InitializeRequest())
        .mapTo[InitializeResponse]
        .map(_ => httpServer ! StartServer)
        .map(_ => println("Initialization finished!"))
}
