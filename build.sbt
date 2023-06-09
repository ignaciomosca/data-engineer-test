name := "data-engineer-test"

version := "0.0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-hadoop-cloud" % "3.3.2",
  "com.github.mrpowers" %% "spark-daria" % "1.2.3",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % "test",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "com.lihaoyi" %% "os-lib" % "0.9.1",
  "com.amazonaws" % "aws-java-sdk" % "1.11.698"
)

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

// JAR file settings

// don't include Scala in the JAR file
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
// You can add the JAR file naming conventions by running the shell script
