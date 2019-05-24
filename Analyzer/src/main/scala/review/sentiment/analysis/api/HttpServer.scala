package review.sentiment.analysis.api

import akka.http.scaladsl.model.{ ContentTypes, HttpEntity }
import akka.http.scaladsl.server.HttpApp
import akka.http.scaladsl.server.Route

class HttpServer extends HttpApp {

    var analyzeMethod : String => Int = _

    def start(analyzeMethod : String => Int): Unit = {
        this.analyzeMethod = analyzeMethod

        startServer("localhost", 8080)
    }

    override def routes: Route = {
        path("analyze") {
            get {
                decodeRequest {
                    entity(as[String]) { body =>
                        val result = analyzeMethod.apply(body)
                        complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, result.toString))
                    }
                }
            }
        }
    }

}

