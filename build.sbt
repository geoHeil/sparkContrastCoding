name := "estimatorSpeedProblem"
organization := "geoheil"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation", "-feature")

//The default SBT testing java options are too small to support running many of the tests
// due to the need to launch Spark in local mode.
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

lazy val spark = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark % "provided",
  "org.apache.spark" %% "spark-sql" % spark % "provided",
  "org.apache.spark" %% "spark-mllib" % spark % "provided",
  "org.apache.spark" %% "spark-hive" % spark % "provided",
  "com.holdenkarau" %% "spark-testing-base" % "2.0.2_0.4.7" % "test",
  "org.json4s" %% "json4s-native" % "3.5.0"
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

assemblyMergeStrategy in assembly := {
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

mainClass := Some("Percentage")
