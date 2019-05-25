package review.sentiment.analysis.classifier

import akka.actor.Props

object SecondClassifier {
    def props: Props = Props[SecondClassifier]
}

class SecondClassifier extends AbstractClassifier  {

    override def calculateMark(text: Array[String]): Int = 5

}
