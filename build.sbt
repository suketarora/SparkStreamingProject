name := "Spark_Kafka_ScyllaDB_ETL"

version := "0.1"



scalaVersion := "2.11.8"
// Spark Information
val sparkVersion = "2.4.0"
// allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"
resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"
resolvers += "MavenRepository" at
  "https://mvnrepository.com/"
libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion,
  "org.apache.commons" % "commons-configuration2" % "2.3",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test

 // the rest of the file
 

)

 val projectMainClass = "Main"

mainClass in (Compile, run) := Some(projectMainClass)

