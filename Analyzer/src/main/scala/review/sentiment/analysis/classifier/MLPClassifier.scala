package review.sentiment.analysis.classifier

import akka.actor.Props

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.DataFrame

object MLPClassifier {
    def props: Props = Props[MLPClassifier]
}

class MLPClassifier extends MultilabelClassifier with Serializable {

    //
    // Private members
    //

    private var mlpcModel: MultilayerPerceptronClassificationModel = _

    //
    // Public methods
    //

    override def calculateMark(df: DataFrame): Double = {
        mlpcModel.predict(df.head.getAs[SparseVector](0))
    }

    override def train(trainingData: DataFrame, testData: DataFrame): Double = {
        val (trainedModel, accuracy) = trainModel(trainingData, testData)
        mlpcModel = trainedModel
        accuracy
    }

    //
    // Private functions
    //

    val trainModel = (trainingData: DataFrame, testData: DataFrame) => {
        // Obtain number total number of features in datasets (aka size of features vectors)
        val nfeatures = trainingData.head.getAs[SparseVector](1).size

        // Create the trainer and set its parameters
        val layers = Array(nfeatures, 5, 11)
        val trainer = new MultilayerPerceptronClassifier()
            .setLayers(layers)
            .setBlockSize(128)
            .setMaxIter(100)

        // Train the mlpcModel
        val mlpcModel = trainer.fit(trainingData)

        // Compute accuracy on the test set
        val result = mlpcModel.transform(testData)
        val predictionAndLabels = result.select("prediction", "label")
        val evaluator = new MulticlassClassificationEvaluator()
            .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictionAndLabels)

        (mlpcModel, accuracy)
    }
}
