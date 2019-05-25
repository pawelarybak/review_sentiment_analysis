package review.sentiment.analysis.classifier

import akka.actor.Props

object ExampleClassifier {
    def props: Props = Props[ExampleClassifier]
}

class ExampleClassifier extends AbstractClassifier  {

    override def calculateMark(text: Array[Int]): Int = 7

}
