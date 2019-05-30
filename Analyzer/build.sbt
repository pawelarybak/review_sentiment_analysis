name := "Analyzer"

version := "0.1"
scalaVersion := "2.11.12"
cancelable in Global := true

scalacOptions := Seq("-unchecked", "-deprecation", "-feature")

// Pass more memory to JVM
javaOptions in run += "-Xmx8G"

// Run spark application in forked mode
fork in (Compile, run) := true

// Don't prefix stdouts of forked applications
outputStrategy := Some(StdoutOutput)

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.22"
libraryDependencies += "com.typesafe.akka" %% "akka-http"   % "10.1.8"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.22"
libraryDependencies += "org.carrot2" % "morfologik-polish" % "2.1.6"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.3"
