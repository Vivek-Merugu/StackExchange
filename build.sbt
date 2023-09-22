organization := "com.vivek"
scalaVersion := "2.12.15"
name := "StackExchange"
version := "0.0.1-SNAPSHOT"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "3.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
  "org.apache.spark" %% "spark-avro" % "3.2.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "3.2.1" % "provided",
  "org.apache.hadoop" % "hadoop-yarn-client" % "3.2.1" % "provided")