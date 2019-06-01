package review.sentiment.analysis.bowgen

import akka.actor.{Actor, ActorLogging, Props}

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.DataFrame

import scala.io.Source
import scala.collection.immutable.Set

import review.sentiment.analysis.Spark

import Spark.session.implicits._

object BOWManager {
    def props: Props = Props[BOWManager]

    final case class AddTextsRequest(texts: RDD[Array[String]])
    final case class AddTextsResponse(newWordsCount: Int, vecs: RDD[SparseVector])
    final case class AnnotateTextsRequest(texts: RDD[Array[String]])
    final case class AnnotateTextsResponse(vecs: RDD[SparseVector])
}

class BOWManager extends Actor with ActorLogging {

    import BOWManager._

    private var cvm: CountVectorizerModel = _
    implicit val datasetEncoder1 = org.apache.spark.sql.Encoders.kryo[org.apache.spark.mllib.linalg.SparseVector]
    implicit val datasetEncoder2 = org.apache.spark.sql.Encoders.kryo[org.apache.spark.ml.linalg.SparseVector]

    override def receive: Receive = {
        case AddTextsRequest(texts) =>
            log.info(s"Adding ${texts.count} texts to BOW...")

            val df = texts.toDF("words")
            cvm = buildBOW(df)
            val newWordsCount = cvm.vocabulary.size
            val vecs = annotateTexts(df, cvm)

            log.info(s"Successfully added ${texts.count} texts to BOW. New words count: $newWordsCount")
            sender() ! AddTextsResponse(newWordsCount, vecs)

        case AnnotateTextsRequest(texts) =>
            val textsCount = texts.count
            log.info(s"Annotating $textsCount texts using BOW...")

            val df = texts.toDF("words")
            val vecs = annotateTexts(df, cvm)

            log.info(s"Annotation of $textsCount texts complete.")
            sender() ! AnnotateTextsResponse(vecs)
    }

    val buildBOW = (df: DataFrame) => {
        new CountVectorizer()
            .setInputCol("words")
            .setOutputCol("features")
            .fit(df)
    }

    val annotateTexts = (df: DataFrame, cvm: CountVectorizerModel) => {
        cvm
            .transform(df)
            .map(row => row.getAs[org.apache.spark.ml.linalg.SparseVector](1))
            .map(vec => org.apache.spark.mllib.linalg.SparseVector.fromML(vec))
            .rdd
    }
}
