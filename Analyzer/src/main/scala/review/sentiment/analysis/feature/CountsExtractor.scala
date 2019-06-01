package review.sentiment.analysis.feature

import akka.actor.{Actor, ActorLogging, Props}

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.DataFrame

import review.sentiment.analysis.Spark

import Spark.session.implicits._

object CountsExtractor {
    def props: Props = Props[CountsExtractor]
}

class CountsExtractor extends FeatureExtractor {

    private var cvModel: CountVectorizerModel = _

    override def addTexts(df: DataFrame): DataFrame = {
        cvModel = new CountVectorizer()
            .setInputCol("words")
            .setOutputCol("features")
            .fit(df)

        extractFeatures(df)
    }

    override def extractFeatures(df: DataFrame): DataFrame = {
        cvModel.transform(df)
    }
}
