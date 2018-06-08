name := "ChangeFileFormat"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-hive" % "1.6.1")

libraryDependencies += "com.databricks" %% "spark-csv" % "1.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"