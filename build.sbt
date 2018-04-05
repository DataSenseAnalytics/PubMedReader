name := "neodm"

organization := "com.twineanalytics"

version := "1.5-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-deprecation", "-feature")

val sparkVersion = "2.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.tresamigos" %% "smv" % "2.1-SNAPSHOT",
  "com.databricks" %% "spark-xml" % "0.4.1",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test",
  "com.javadocmd" % "simplelatlng" % "1.3.0",
  "com.rockymadden.stringmetric" % "stringmetric_2.10" % "0.27.3"
)

parallelExecution in Test := false

mainClass in assembly := Some("org.tresamigos.smv.SmvApp")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := s"${name.value}-${version.value}-jar-with-dependencies.jar"

val smvInit = if (sys.props.contains("smvInit")) {
    val files = sys.props.get("smvInit").get.split(",")
    files.map{f=> IO.read(new File(f))}.mkString("\n")
  } else ""

initialCommands in console := s"""
val sc = new org.apache.spark.SparkContext("local", "shell")
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
${smvInit}
"""

assemblyMergeStrategy in assembly := {
  case PathList("org", "w3c", "dom", "events", xs @ _ *) => MergeStrategy.last
  case x =>
    val oldStrat = (assemblyMergeStrategy in assembly).value
    oldStrat(x)
}
