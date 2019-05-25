package review.sentiment.analysis.bowgen

import scala.io.Source
import scala.collection.mutable.{HashSet, HashMap}

import akka.actor.{Actor, ActorLogging, Props}

object BOWGenerator {
  def props: Props = Props[BOWGenerator]

  final case class AddTextRequest(text: Array[String])
  final case class AddTextResponse(newWordsCount: Int)
  final case class AnnotateRequest(text: Array[String])
  final case class AnnotateResponse(vec: Array[Int])
}

class BOWGenerator extends Actor with ActorLogging {

  import BOWGenerator._

  private var bow = new collection.mutable.HashSet[String]

  override def receive: Receive = {
    case AddTextRequest(text: Array[String]) =>
      log.info(s"Adding text to BOW: ${text.mkString(" ")}...")

      // Remove known words from text and add them to BOW
      val notKnownWords = text.filterNot(bow.contains)
      bow ++= notKnownWords

      // Return new words count to sender
      val newWordsCount = notKnownWords.size
      log.info(s"New words count: $newWordsCount")

      sender() ! AddTextResponse(newWordsCount)

    case AnnotateRequest(text: Array[String]) =>
      log.info(s"Annotating with BOW text: ${text.mkString(" ")}...")

      // Create integer vector for occurencies
      var vec = collection.mutable.HashMap(bow.toVector.map(w => (w, 0)): _*)

      // Remove not known words from text
      val knownWords = text.filter(bow.contains)

      // For each known word increment vec's counter
      knownWords.foreach(w => vec(w) += 1)

      // Return final vector to sender
      sender() ! AnnotateResponse(vec.map(v => v._2).toArray)
  }

}
