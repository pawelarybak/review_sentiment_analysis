package review.sentiment.analysis.preprocessing

import scala.io.Source
import scala.collection.JavaConverters._

import akka.actor.{Actor, ActorLogging, Props}

import morfologik.stemming.IStemmer
import morfologik.stemming.polish.PolishStemmer

object Stemmer {
    def props: Props = Props[Stemmer]

    final case class StemmingsRequest(rawTexts: Array[String])
    final case class StemmingsResponse(processedTexts: Array[Array[String]])
}

class Stemmer extends Actor with ActorLogging {

    import Stemmer._

    private val tokenizer = """(?u)\b\p{L}+\b""".r
    private val stemmer = new PolishStemmer()
    private val stopWords = loadStopWords()

    override def receive: Receive = {
        case StemmingsRequest(rawTexts) =>
            log.info(s"Stemming ${rawTexts.size} raw texts...")

            val processedTexts = rawTexts.map(processText)

            log.info(s"Stemming complete")
            sender() ! StemmingsResponse(processedTexts)
    }

    private def processText(rawText: String): Array[String] = {
        log.debug(s"Processing raw text with size ${rawText.size}...")

        val tokens = tokenizer.findAllIn(rawText).toArray
        val processedText = tokens
            .map(token => token.toLowerCase())
            .map(token => stem(token))
            .filter(token => !isStopWord(token))

        log.debug(s"Processed text: ${processedText.mkString(" ")}")
        processedText
    }

    private def stem(token: String): String = {
        stemmer
            .lookup(token)
            .asScala
            .headOption
            .map(token => token.getStem.toString)
            .getOrElse(token)
    }

    private def isStopWord(token: String): Boolean = {
        stopWords.contains(token)
    }

    private def loadStopWords(): Set[String] = {
        Source.fromResource("polish-stopwords.txt")
            .getLines
            .toSet
    }
}
