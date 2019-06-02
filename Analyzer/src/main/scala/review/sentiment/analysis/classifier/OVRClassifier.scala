package review.sentiment.analysis.classifier

import akka.actor.Props

import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest, OneVsRestModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object OVRClassifier {
    def props: Props = Props[OVRClassifier]
}

class OVRClassifier extends MultilabelClassifier {

    //
    // Private members
    //

    private var ovrModel: OneVsRestModel = _

    //
    // Public methods
    //

    override def calculateMark(df: DataFrame): Double = {
        predictMark(df, ovrModel)
    }

    override def train(trainingData: DataFrame, testData: DataFrame): Double = {
        val (trainedModel, accuracy) = trainModel(trainingData, testData)
        ovrModel = trainedModel
        accuracy
    }

    //
    // Private functions
    //

    private val predictMark = (df: DataFrame, ovrModel: OneVsRestModel) => {
        // Predict value
        val predictions = ovrModel.transform(df)

        // Extract prediction
        predictions.head.getDouble(2)
    }

    private val trainModel = (trainingData: DataFrame, testData: DataFrame) => {
        // instantiate the base classifier
        val classifier = new LogisticRegression()
            .setMaxIter(10)
            .setTol(1E-6)
            .setFitIntercept(false)

        // Instantiate the One Vs Rest Classifier.
        val ovr = new OneVsRest()
        	.setClassifier(classifier)

        // Train the multiclass model.
        val ovrModel = ovr.fit(trainingData)

        // Score the model on test data.
        val predictions = ovrModel.transform(testData)

        // Obtain evaluator
        val evaluator = new MulticlassClassificationEvaluator()
          .setMetricName("accuracy")

        // Compute the classification error on test data.
        val accuracy = evaluator.evaluate(predictions)

        (ovrModel, accuracy)
    }
}
