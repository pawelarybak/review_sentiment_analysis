package review.sentiment.analysis.classifier

import akka.actor.Props

import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame

object SVMClassifier {
    def props: Props = Props[SVMClassifier]
}

class SVMClassifier(targetMark: Double) extends BinaryClassifier(targetMark) {

    //
    // Private members
    //

    private var model: Option[LinearSVCModel] = None

    //
    // Public methods
    //

    override def calculateMark(df: DataFrame): Double = {
        predictMark(df)
    }

    override def train(trainingData: DataFrame, testData: DataFrame): Double = {
        val (trainedModel, accuracy) = trainModel(trainingData, testData)
        model = Some(trainedModel)
        accuracy
    }

    //
    // Private functions
    //

    private val predictMark = (df: DataFrame) => {
        // Predict value
        val predictions = model.get.transform(df)

        // Get final prediction
        predictions.head.getDouble(2)
    }

    private val trainModel = (trainingData: DataFrame, testData: DataFrame) => {
        log.info("Training a model...")
        val model = new LinearSVC()
              .setMaxIter(10)
              .setRegParam(0.1)
              .fit(trainingData)

        log.info("Veryfing model...")
        val predictions = model.transform(testData)
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)

        (model, accuracy)
    }
}
