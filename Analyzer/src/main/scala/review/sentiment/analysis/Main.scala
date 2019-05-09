package review.sentiment.analysis

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

object Main extends App {

    println("Hello")

    val config = ConfigFactory.load()
    val system = ActorSystem("rsa-system", config)

    system.terminate()

}
