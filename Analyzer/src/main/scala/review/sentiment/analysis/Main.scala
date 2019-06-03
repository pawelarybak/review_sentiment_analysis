package review.sentiment.analysis

import akka.actor.{ActorSystem, Props}

import com.typesafe.config.ConfigFactory

import review.sentiment.analysis.manager.AnalysisManager.InitializeRequest

import org.apache.spark.sql.SparkSession

object Spark {
    val session = SparkSession.builder()
        .master("local[*]")
        .appName("rsa-system")
        .config("spark.jars", "target/scala-2.11/analyzer_2.11-0.1.jar")
        .config("spark.executor.memory", "11G")
        .config("spark.driver.maxResultSize", "1024M")
        .getOrCreate()

    val ctx = session.sparkContext
    val sql = session.sqlContext
}

object Main extends App {

    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)

    Spark.ctx.setLogLevel("WARN")

    val supervisor = system.actorOf(Props[MainSupervisor], "supervisor")

    supervisor ! InitializeRequest
}
