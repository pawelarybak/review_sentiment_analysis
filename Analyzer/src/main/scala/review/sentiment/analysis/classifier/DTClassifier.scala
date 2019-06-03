package review.sentiment.analysis.classifier

import akka.actor.Props

import review.sentiment.analysis.Spark

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.SparseVector

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}


import org.apache.spark.rdd.RDD

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object DTClassifier {
    def props: Props = Props[DTClassifier]
}

class DTClassifier extends AbstractClassifier with Serializable {

    private var model: Option[PipelineModel] = None

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
        predictions.head.getString(5).toDouble+1.0
    }

    val doTrain = (reviews: RDD[LabeledPoint]) => {
        // Convert reviews to sql rows
        val rows = reviews.map(review => Row(review.label-1.0, review.features.asML))

        // Convert reviews RDD to dataframe
        val schema = new StructType()
            .add("label", DoubleType)
            .add("features", VectorType)
        val df = Spark.session.createDataFrame(rows, schema)

        // Index labels, adding metadata to the label column.
        // Fit on whole dataset to include all labels in index.
        val labelIndexer = new StringIndexer()
          .setInputCol("label")
          .setOutputCol("indexedLabel")
          .fit(df)

        // Automatically identify categorical features, and index them.
        val featureIndexer = new VectorIndexer()
          .setInputCol("features")
          .setOutputCol("indexedFeatures")
          .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
          .setHandleInvalid("keep")
          .fit(df)

        // Split reviews into training and test set
        val Array(trainingData, testData) = df.randomSplit(Array(0.6, 0.4))

        // Train a DecisionTree model.
        val dt = new DecisionTreeClassifier()
          .setLabelCol("indexedLabel")
          .setFeaturesCol("indexedFeatures")

        // Convert indexed labels back to original labels.
        val labelConverter = new IndexToString()
          .setInputCol("prediction")
          .setOutputCol("predictedLabel")
          .setLabels(labelIndexer.labels)

        // Chain indexers and tree in a Pipeline.
        val pipeline = new Pipeline()
          .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

        // Train model. This also runs the indexers.
        val model = pipeline.fit(trainingData)

        // Make predictions.
        val predictions = model.transform(testData)

        // Select (prediction, true label) and compute test error.
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("indexedLabel")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)

        (model, accuracy)
    }
}
