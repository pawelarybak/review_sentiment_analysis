package review.sentiment.analysis.preprocessing

import scala.io.Source
import scala.collection.JavaConverters._

import akka.actor.{Actor, ActorLogging, Props}

import morfologik.stemming.IStemmer
import morfologik.stemming.polish.PolishStemmer

object Stemmer {
  def props: Props = Props[Stemmer]

  final case class StemmingRequest(rawText: String)
  final case class StemmingResponse(processedText: Array[String])
}

class Stemmer extends Actor with ActorLogging {

  import Stemmer._

  private val tokenizer = """(?u)\b\p{L}+\b""".r
  private val stemmer = new PolishStemmer()
  private val stopWords = loadStopWords()

  override def receive: Receive = {
    case StemmingRequest(rawText: String) =>
      log.info(s"Stemming raw text of size: ${rawText.size}...")
      val tokens = tokenizer.findAllIn(rawText).toArray
      val processedText = tokens
        .map(token => token.toLowerCase())
        .map(token => stem(token))
        .filter(token => !isStopWord(token))

      log.info(s"Processed text: ${processedText.mkString(" ")}")
      sender() ! StemmingResponse(processedText)
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
