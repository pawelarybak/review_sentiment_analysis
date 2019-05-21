package review.sentiment.analysis.preprocessing

import scala.io.Source
import scala.collection.JavaConverters._

import akka.actor.{Actor, ActorLogging, Props}

import morfologik.stemming.IStemmer
import morfologik.stemming.polish.PolishStemmer


object Stemmer {
  def props: Props = Props[Stemmer]

  final case class StemmingRequest(rawText: String)

  final case class StemmingResponse(processedText: String)

}

class Stemmer extends Actor with ActorLogging {

  import Stemmer._

  private val tokenizer = """(?u)\b\p{L}+\b""".r
  private val stemmer = new PolishStemmer()
  private val stopWords = Source.fromResource("polish-stopwords.txt")
    .getLines
    .toSet

  override def receive: Receive = {
    case StemmingRequest(rawText: String) =>

      val tokens = tokenizer.findAllIn(rawText).toArray
      val processedTokens = tokens
        .map(token => token.toLowerCase())
        .map(token => stem(token))
        .filter(token => !isStopWord(token))

      val processedText = processedTokens.mkString(" ")

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
}