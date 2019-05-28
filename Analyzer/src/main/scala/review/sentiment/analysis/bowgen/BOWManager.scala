package review.sentiment.analysis.bowgen

import scala.io.Source
import scala.collection.mutable.{HashSet, HashMap}
import akka.actor.{Actor, ActorLogging, Props}

object BOWManager {
    def props: Props = Props[BOWManager]

    final case class AddTextsRequest(texts: Array[Array[String]])
    final case class AddTextsResponse(newWordsCount: Int, vecs: Array[Array[Int]])
    final case class AnnotateTextsRequest(texts: Array[Array[String]])
    final case class AnnotateTextsResponse(vecs: Array[Array[Int]])
}

class BOWManager extends Actor with ActorLogging {

    import BOWManager._

    private var bow = new collection.mutable.HashSet[String]

    override def receive: Receive = {
        case AddTextsRequest(texts: Array[Array[String]]) =>
            log.info(s"Adding ${texts.size} texts to BOW...")

            val newWordsCount = texts.map(addText).sum
            val vecs = texts.map(annotateText)

            log.info(s"Successfully added ${texts.size} texts to BOW. New words count: $newWordsCount")

            sender() ! AddTextsResponse(newWordsCount, vecs)

        case AnnotateTextsRequest(texts: Array[Array[String]]) =>
            log.info(s"Annotating ${texts.size} texts using BOW...")

            val vecs = texts.map(annotateText)

            log.info(s"Texts annotation complete")
            sender() ! AnnotateTextsResponse(vecs)
    }

    def addText(text: Array[String]): Int = {
        log.debug(s"Adding to BOW text: ${text.mkString(" ")}")

        // Remove known words from text and add them to BOW
        val notKnownWords = text.filterNot(bow.contains)
        val prevWordsCount = bow.size
        bow ++= notKnownWords

        val newWordsCount = (bow.size - prevWordsCount)
        log.debug(s"New words count: $newWordsCount")
        newWordsCount
    }

    def annotateText(text: Array[String]): Array[Int] = {
        log.debug(s"Annotating using BOW text: ${text.mkString(" ")}")

        // Create integer vector for words occurencies
        var vec = collection.mutable.HashMap(bow.toVector.map(w => (w, 0)): _*)

        // Remove not known words from text
        val knownWords = text.filter(bow.contains)

        // For each known word increment vec's counter
        knownWords.foreach(w => vec(w) += 1)

        val finalVec = vec.map(v => v._2).toArray
        log.debug(s"Final vector: ${finalVec.mkString(" ")}")
        finalVec
    }
}