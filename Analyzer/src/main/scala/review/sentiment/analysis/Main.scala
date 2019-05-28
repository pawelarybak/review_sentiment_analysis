package review.sentiment.analysis

import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import review.sentiment.analysis.manager.AnalysisManager.InitializeRequest

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

object Spark {
    private val sparkConf = new SparkConf()
        .setAppName("rsa-system")
        .setMaster("local[*]")
        .set("spark.cores.max", "6")

    val ctx = new SparkContext(sparkConf)
}

object Main extends App {

    Spark.ctx.setLogLevel("WARN")

    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)

    val supervisor = system.actorOf(Props[MainSupervisor], "supervisor")

    private implicit val timeout = Timeout(99999 seconds)
    private implicit val ec = ExecutionContext.global


    // spark.stop()
    supervisor ! InitializeRequest

}
