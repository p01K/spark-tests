name         := "spark-tests"
version      := "1.0"
scalaVersion := "2.10.5"
// libraryDependencies += org.apache.spark" %% "spark-core" % "1.6.0-SNAPSHOT"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.5.2",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)
compileOrder := CompileOrder.JavaThenScala
// libraryDependencies +=
// "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test"
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
