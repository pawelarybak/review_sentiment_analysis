package review.sentiment.analysis.classifier

import akka.actor.Props

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame

object MultilabelNaiveBayesClassifier {
    def props: Props = Props[MultilabelNaiveBayesClassifier]
}

class MultilabelNaiveBayesClassifier extends MultilabelClassifier {

    //
    // Private members
    //

    private var nbModel: NaiveBayesModel = _

    //
    // Public methods
    //

    override def calculateMark(df: DataFrame): Double = {
        predictMark(df, nbModel)
    }

    override def train(trainingData: DataFrame, testData: DataFrame): Double = {
        val (trainedModel, accuracy) = trainModel(trainingData, testData)
        nbModel = trainedModel
        accuracy
    }

    //
    // Private functions
    //

    private val predictMark = (df: DataFrame, nbModel: NaiveBayesModel) => {
        // Predict value
        val predictions = nbModel.transform(df)
        val prob = predictions.select("probability")

        // Get final prediction
        predictions.head.getDouble(3)
    }

    private val trainModel = (trainingData: DataFrame, testData: DataFrame) => {
        log.info("Training a nbModel...")
        val nbModel = new NaiveBayes()
            .setModelType("multinomial") // Note that bernoulli could not be used here, since occurencies may be greater than "1"
            .setSmoothing(0.1875)
            .fit(trainingData)

        log.info("Veryfing model...")
        val predictions = nbModel.transform(testData)
        val evaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)

        (nbModel, accuracy)
    }
}
