package review.sentiment.analysis.preprocessing

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
  //  private var stemmer = new PolishStemmer()

  override def receive: Receive = {
    case StemmingRequest(rawText: String) =>

      val stemmer = new PolishStemmer()

      val tokens = tokenizer.findAllIn(rawText).toArray
      val processedTokens = tokens
        .map(token => token.toLowerCase())
        .map(token => stem(token, stemmer))

      val processedText = processedTokens.mkString(" ")

      sender() ! StemmingResponse(processedText)
  }

  private def stem(token: String, stemmer: IStemmer): String = {
    stemmer.lookup(token).get(0).getStem().toString()
  }
}