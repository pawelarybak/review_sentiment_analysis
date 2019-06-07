# review_sentiment_analysis

The aim of this project was to write actor-based application, which will be used to perform sentiment analysis of films reviews, written in Polish. It marks given reviews with a labels in scale between '1' (very bad) to '10' (very good). Application is written in Scala with help of Akka and Spark frameworks. Communication with it is realized using an HTTP server. There is also implemented a basic supervisioning strategy. It means, that killing some of the actors doesn't break the application and killed actors are going to be restarted.

# How it works

Application is divided into two subprojects: `Analyzer` and `scraper`. First one is the agent system which works as a sentiment analyzer, and the seconds contains utilities to perform web scraping and end testing of reviews analyzer.

The diagram of complete system is presented below:
![RSA system diagram](https://raw.githubusercontent.com/pawelarybak/review_sentiment_analysis/master/docs/img/rsa-diagram.png)

The main functionality of the application sits in `AnalysisManager` actor. It works as a main coordinator, by accepting input data and transfering it between other components. Among other actors there are:
  - `HttpServerActor` - accepts HTTP requests
  - `ReviewsDB` - works as a database of films reviews (currently implemented as CSV files),
  - `Stemmer` - performs preprocessing of reviews' by tokenizing them, removing stop words and stemming (achieved with [morfologik-stemming library](https://github.com/morfologik/morfologik-stemming)),
  - `CountsExtractor` - feature extractor: implements Bag-Of-Words algorithm
  - `ClassificationManager` - manages process of classification by talking with classifiers. There are several multiclass classifiers (also implemented as actors):
    * `MultilabelNaiveBayesClassifier`,
    * `OVRClassifier` (aka One vs Rest),
    * `MLPClassifier` (aka Multilayer perceptron),
    * `DTClassifier` (aka Decision tree)

All classifiers and feature extractor are written with a help of `spark.ml` library. Classifiers are trained using supervised learning. Each of them receives whole reviews database, which is then individually and randomly splited into training and test sets. Each classifier has its own knowledge and calculates marks in its way. `ClassificationManager` then collects all marks from them and forms final mark, using plurality voting (that means, most rated mark wins and in case of conflict mean of marks is calculated).

# Requirements

In order to run the project, you need to have the following:
  * For the `Analyzer` subproject:
    - `java` - version 1.8
    - `sbt` - tested on version 1.0
    - `scala` - version 2.11.12
    - `spark` - version 2.4.3
  * For the `scrapper` subproject:
    - `python` - tested on version 3.6.7
    - `pandas` - version 0.24.2
    - `beautifulsoup4` - version 4.7.1
    - `numpy` - version 1.16.2

# Running

All that you need to run the project is to just execute it through SBT:

```sh
cd Analyzer/
sbt run
```

Project will be compiled and started. After successful training on the training set, an HTTP server will be opened (by default on port 8000).

# Testing

You can test it either by sending GET request with a review text as a body at `$HOST:$PORT/analyze` (e.g. using cURL):
```sh
curl -X GET http://localhost:8000/analyze -d "What a funny movie!"
```

or just by executing testing application from `scrapper` submodule:

```sh
cd scrapper/
python3 test_api.py
```

# Getting more data

At the moment, there are some training data (fetched from [Filmweb portal](https://www.filmweb.pl/)) stored in `Analyzer/src/main/resources/` directory. In order to get more latest (if there are any), you have to fetch it again:

```sh
cd scrapper/
python3 fetch_filmweb_data.py
```

# Running in a cluster

Since internal computations are implemented using Spark framework, application may work (and should be) on a cluster. In order to do it, first make sure, that you have the same version of Spark on workers as used in application (currently it is 2.4.3). Then start your Spark master, find its URL (kinda like `spark://<some_ip>:7077`) and set it in `setMaster(...)` call in `Main.scala` module:

```scala
    val session = SparkSession.builder()
        .master("spark://<some_ip>:7077")
        ...
```
