package review.sentiment.analysis.classifier

import akka.actor.Props

import review.sentiment.analysis.Spark

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.SparseVector

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object MLPClassifier {
    def props: Props = Props[MLPClassifier]
}

class MLPClassifier extends AbstractClassifier with Serializable {

    private var model: Option[MultilayerPerceptronClassificationModel] = None

    override def train(reviews: RDD[LabeledPoint]): Double = {
        val result = doTrain(reviews)
        model = Some(result._1)
        result._2
    }

    override def calculateMark(vec: SparseVector): Double = {
        model.get.predict(vec.asML)
    }

    val doTrain = (reviews: RDD[LabeledPoint]) => {
        val nfeatures = reviews.first.features.size

        // Convert reviews to sql rows
        val rows = reviews.map(review => Row(review.label, review.features.asML))

        // Convert reviews RDD to dataframe
        val schema = new StructType()
            .add("label", DoubleType)
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rows, schema)

        // Split reviews into training and test set
        val Array(trainingData, testData) = df.randomSplit(Array(0.6, 0.4))

        // Create the trainer and set its parameters
        val layers = Array[Int](nfeatures, 5, 11)
        val trainer = new MultilayerPerceptronClassifier()
            .setLayers(layers)
            .setBlockSize(128)
            .setMaxIter(100)

        // Train the model
        val model = trainer.fit(trainingData)

        // Compute accuracy on the test set
        val result = model.transform(testData)
        val predictionAndLabels = result.select("prediction", "label")
        val evaluator = new MulticlassClassificationEvaluator()
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictionAndLabels)

        (model, accuracy)
    }
}
