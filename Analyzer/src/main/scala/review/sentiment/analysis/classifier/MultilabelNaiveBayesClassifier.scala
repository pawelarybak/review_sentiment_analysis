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

    private var model: Option[NaiveBayesModel] = None

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
        val prob = predictions.select("probability")
        prob.show

        // Get final prediction
        predictions.head.getDouble(3)
    }

    private val trainModel = (trainingData: DataFrame, testData: DataFrame) => {
        log.info("Training a model...")
        val model = new NaiveBayes()
            .setModelType("multinomial") // Note that bernoulli could not be used here, since occurencies may be greater than "1"
            .setSmoothing(0.1875)
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
