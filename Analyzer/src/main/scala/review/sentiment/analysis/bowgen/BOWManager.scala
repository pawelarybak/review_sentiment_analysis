package review.sentiment.analysis.bowgen

import akka.actor.{Actor, ActorLogging, Props}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector

import scala.io.Source
import scala.collection.immutable.Set

object BOWManager {
    def props: Props = Props[BOWManager]

    final case class AddTextsRequest(texts: RDD[Array[String]])
    final case class AddTextsResponse(newWordsCount: Int, vecs: RDD[SparseVector])
    final case class AnnotateTextsRequest(texts: RDD[Array[String]])
    final case class AnnotateTextsResponse(vecs: RDD[SparseVector])
}

class BOWManager extends Actor with ActorLogging {

    import BOWManager._

    private var bow = Set[String]()

    override def receive: Receive = {
        case AddTextsRequest(texts) =>
            log.info(s"Adding ${texts.count} texts to BOW...")

            bow = buildBOW(texts)
            val newWordsCount = bow.size
            val vecs = annotateTexts(texts, bow)

            log.info(s"Successfully added ${texts.count} texts to BOW. New words count: $newWordsCount")
            sender() ! AddTextsResponse(newWordsCount, vecs)

        case AnnotateTextsRequest(texts) =>
            val textsCount = texts.count
            log.info(s"Annotating $textsCount texts using BOW...")

            val vecs = annotateTexts(texts, bow)

            log.info(s"Annotation of $textsCount texts complete")
            sender() ! AnnotateTextsResponse(vecs)
    }

    val buildBOW = (texts: RDD[Array[String]]) => {
        texts
            .flatMap(x => x) // flatten
            .collect
            .toSet // duplicates will be removed automatically
    }

    val annotateTexts = (texts: RDD[Array[String]], bow: Set[String]) => {
        // Filter out words, which are not present in BOW
        val filteredTexts = texts.map(_.filter(bow.contains))

        // Count occurencies of each word in each text
        val textsCounts = filteredTexts.map(_.groupBy(identity).mapValues(x => x.length))

        // Generate full vectors of occurencies
        val vecs = textsCounts.map(counts => bow.toArray.map(word => counts.getOrElse(word, 0)))

        // Find indexes of only non-zero occurencies
        val val_idxs = vecs.map(vec => vec.zipWithIndex.filter(v_idx => v_idx._1 > 0).unzip)

        // Make sparse vectors from them
        val_idxs.map(val_idx => new SparseVector(bow.size, val_idx._2, val_idx._1.map(_.toDouble)))
    }
}
