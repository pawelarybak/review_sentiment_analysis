package review.sentiment.analysis.preprocessing

import scala.io.Source
import scala.collection.JavaConverters._

import akka.actor.{Actor, ActorLogging, Props}

import org.apache.spark.rdd.RDD

import morfologik.stemming.IStemmer
import morfologik.stemming.polish.PolishStemmer

import review.sentiment.analysis.Spark

object Stemmer {
    def props: Props = Props[Stemmer]

    final case class StemmingsRequest(rawTexts: RDD[String])
    final case class StemmingsResponse(processedTexts: RDD[Array[String]])
}

class Stemmer extends Actor with ActorLogging {

    import Stemmer._

    private val tokenizer = """(?u)\b\p{L}+\b""".r
    private val stemmer = new PolishStemmer()
    private val stopWords = loadStopWords()

    override def receive: Receive = {
        case StemmingsRequest(texts) =>
            log.info(s"Stemming ${texts.count} raw texts...")

            val preprocessedTexts = preprocessTexts(texts)
            val processedTexts = processTexts(preprocessedTexts.collect)
            val postprocessedTexts = postprocessTexts(Spark.ctx.makeRDD(processedTexts), stopWords)

            log.info(s"Stemming of ${texts.count} raw texts complete")
            sender() ! StemmingsResponse(postprocessedTexts)
    }

    val preprocessTexts = (texts: RDD[String]) => {
        val tokenizer = """(?u)\b\p{L}+\b""".r
        texts
            .map(text => tokenizer.findAllIn(text).toArray)
            .map(tokens => tokens.map(_.toLowerCase))
    }

    private def processTexts(preprocessedTexts: Array[Array[String]]): Array[Array[String]] = {
        preprocessedTexts
            .map(tokens => {
                tokens.map(token => {
                    stemmer
                        .lookup(token)
                        .asScala
                        .headOption
                        .map(token => token.getStem.toString)
                        .getOrElse(token)
                })
            })
    }

    val postprocessTexts = (processedTexts: RDD[Array[String]], stopWords: Set[String]) => {
        processedTexts
            .map(text => text.filterNot(stopWords.contains))
    }

    private def loadStopWords(): Set[String] = {
        val stream = getClass.getResourceAsStream("/polish-stopwords.txt")
        val lines = scala.io.Source.fromInputStream(stream).getLines
        val stopWords = lines.toSet
        stopWords
    }
}
