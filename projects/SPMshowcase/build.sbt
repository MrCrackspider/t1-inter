name := "SPMshowcase"

version := "0.1"

scalaVersion := "2.11.8"
// 2.12.15 // 2.3.1 // 2.11.8

val sparkVersion = "2.3.1"
// 2.4.7

//scalacOptions ++= Seq("-unchecked", "-Xmax-classfile-name", "75")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.postgresql" % "postgresql" % "42.1.1",
  "org.json4s" %% "json4s-jackson" % "3.2.10"
)

//assembly / mainClass := Some("SPMshowcase")
assembly / assemblyJarName := s"SPMshowcase.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

