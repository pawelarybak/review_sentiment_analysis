package review.sentiment.analysis.feature

import akka.actor.{Actor, ActorLogging, Props}

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.DataFrame

import review.sentiment.analysis.Spark

import Spark.session.implicits._

object IDFExtractor {
    def props: Props = Props[IDFExtractor]
}

class IDFExtractor extends FeatureExtractor {

    private var idfModel: IDFModel = _
    private var hashingTF: HashingTF = _

    override def addTexts(df: DataFrame): DataFrame = {
        // Prepare matchers
        hashingTF = new HashingTF()
            .setInputCol("words")
            .setOutputCol("rawFeatures")
        val idf = new IDF()
            .setInputCol("rawFeatures")
            .setOutputCol("features")

        // Build IDF model
        val featurizedData = hashingTF.transform(df)
        idfModel = idf.fit(featurizedData)

        // Calculate features
        idfModel
            .transform(featurizedData)
            .select("words", "features")
    }

    override def extractFeatures(df: DataFrame): DataFrame = {
        val featurizedData = hashingTF.transform(df)
        val features = idfModel.transform(featurizedData)
        features.select("words", "features")
    }
}
