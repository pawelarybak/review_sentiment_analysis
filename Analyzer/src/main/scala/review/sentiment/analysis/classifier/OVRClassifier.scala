package review.sentiment.analysis.classifier

import akka.actor.Props

import review.sentiment.analysis.Spark

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.SparseVector

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest, OneVsRestModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object OVRClassifier {
    def props: Props = Props[OVRClassifier]
}

class OVRClassifier extends AbstractClassifier with Serializable {

    private var model: Option[OneVsRestModel] = None

    override def train(reviews: RDD[LabeledPoint]): Double = {
        val result = doTrain(reviews)
        model = Some(result._1)
        result._2
    }

    override def calculateMark(vec: SparseVector): Double = {
    	// Convert vector to RDD
    	val rdd = Spark.ctx.makeRDD(Seq(Row(vec.asML)))

    	// Convert rdd to dataframe
        val schema = new StructType()
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rdd, schema)

        // Predict value
        val predictions = model.get.transform(df)

        // Extract prediction
        predictions.head.getDouble(2)+1.0
    }

    val doTrain = (reviews: RDD[LabeledPoint]) => {
        // Convert reviews to sql rows
        val rows = reviews.map(review => Row(review.label-1.0, review.features.asML))

        // Convert reviews RDD to dataframe
        val schema = new StructType()
            .add("label", DoubleType)
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rows, schema)

        // Split reviews into training and test set
        val Array(trainingData, testData) = df.randomSplit(Array(0.6, 0.4))

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
