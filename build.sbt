name := "spark-tests"

version := "1.0"

scalaVersion := "2.10.5"

scalacOptions ++= Seq("-unchecked",
                      // "-deprecation","on",
                      // "-optimize",
                      "-Xlint",
                      "-Ywarn-dead-code"
                      )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "org.scalatest"    %% "scalatest"   % "2.2.4" % "test"
)

compileOrder := CompileOrder.JavaThenScala

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
