package review.sentiment.analysis.api

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.HttpApp
import akka.http.scaladsl.server.Route

import scala.concurrent.Future

class HttpServer extends HttpApp {

    var analyzeMethod : String => Future[Int] = _
    var killMethod : String => Unit = _

    def start(analyzeMethod: String => Future[Int],
              killMethod: String => Unit): Unit = {
        this.analyzeMethod = analyzeMethod
        this.killMethod = killMethod

        startServer("localhost", 8080)
    }

    override def routes: Route = {
        path("analyze") {
            get {
                decodeRequest {
                    entity(as[String]) { body =>
                        onSuccess(analyzeMethod.apply(body)) { result =>
                            complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, result.toString))
                        }
                    }
                }
            }
        } ~
        path("kill") {
            delete {
                parameters('actorId) { (actorId) =>
                    killMethod.apply(actorId)
                    complete(HttpEntity.Empty)
                }
            }
        }
    }

}

