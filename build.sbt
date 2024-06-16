name := "MySparkProject"
organization := "Scala.Spark.Project"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.18"
val sparkVersion = "3.5.1"
val circeVersion = "0.14.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion, // Spark Core
  "org.apache.spark" %% "spark-sql" % sparkVersion, // Spark SQL
  "org.scalatest" %% "scalatest" % "3.2.15",
  "org.scalatestplus" %% "mockito-4-6" % "3.2.14.0" % Test,
  "com.typesafe" % "config" % "1.4.1",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion
)

