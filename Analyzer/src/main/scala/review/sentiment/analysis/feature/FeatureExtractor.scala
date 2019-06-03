package review.sentiment.analysis.feature

import akka.actor.{Actor, ActorLogging, Props}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.DataFrame

import review.sentiment.analysis.Spark

import Spark.session.implicits._

object FeatureExtractor {
    def props: Props = Props[IDFExtractor]

    final case class AddTextsRequest(texts: RDD[Array[String]])
    final case class AddTextsResponse(vecs: RDD[SparseVector])
    final case class ExtractFeaturesRequest(texts: RDD[Array[String]])
    final case class ExtractFeaturesResponse(vecs: RDD[SparseVector])
}

abstract class FeatureExtractor extends Actor with ActorLogging {

    import FeatureExtractor._

    implicit val datasetEncoder1 = org.apache.spark.sql.Encoders.kryo[org.apache.spark.mllib.linalg.SparseVector]
    implicit val datasetEncoder2 = org.apache.spark.sql.Encoders.kryo[org.apache.spark.ml.linalg.SparseVector]

    //
    // Abstract methods
    //

    def addTexts(df: DataFrame): DataFrame
    def extractFeatures(df: DataFrame): DataFrame

    //
    // Public methods
    //

    override def receive: Receive = {
        case AddTextsRequest(texts) =>
            log.info(s"Adding ${texts.count} texts...")

            val df = addTexts(texts.toDF("words"))
            val vecs = convertResults(df)

            log.info(s"Added ${texts.count} texts")
            sender() ! AddTextsResponse(vecs)

        case ExtractFeaturesRequest(texts) =>
        	log.info(s"Extracting features from ${texts.count} texts...")

        	val df = extractFeatures(texts.toDF("words"))
            val vecs = convertResults(df)

        	log.info(s"Extracted features from ${texts.count} texts")
        	sender() ! ExtractFeaturesResponse(vecs)
    }

    //
    // Private methods
    //

    private val convertResults = (df: DataFrame) => {
        df
            .map(row => row.getAs[org.apache.spark.ml.linalg.SparseVector](1))
            .map(vec => org.apache.spark.mllib.linalg.SparseVector.fromML(vec))
            .rdd
    }
}
