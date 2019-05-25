name := "Analyzer"

version := "0.1"
scalaVersion := "2.12.8"
cancelable in Global := true

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.22"
libraryDependencies += "com.typesafe.akka" %% "akka-http"   % "10.1.8"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.22"
libraryDependencies += "org.carrot2" % "morfologik-polish" % "2.1.6"
