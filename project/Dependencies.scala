import Versions._
import sbt._

object Dependencies {

  val kafka                 = "org.apache.kafka"        %% "kafka"                              % kafkaVersion
  val curator               = "org.apache.curator"      % "curator-test"                        % curatorVersion                 // ApacheV2
  val commonIO              = "commons-io"              % "commons-io"                          % commonIOVersion

  val flinkScala            = "org.apache.flink"        %% "flink-scala"                        % flinkVersion
  val flinkStreaming        = "org.apache.flink"        %% "flink-streaming-scala"              % flinkVersion
  val flinkKafka            = "org.apache.flink"        %% "flink-connector-kafka"              % flinkVersion
  val flinkQueryableRuntime = "org.apache.flink"        %% "flink-queryable-state-runtime"      % flinkVersion
  val flinkCassandra        = "org.apache.flink"        %% "flink-connector-cassandra"          % flinkVersion

  val sparkcore             = "org.apache.spark"        %% "spark-core"                         % sparkVersion
  val sparkstreaming        = "org.apache.spark"        %% "spark-streaming"                    % sparkVersion
  val sparkSQLkafka         = "org.apache.spark"        %% "spark-sql-kafka-0-10"               % sparkVersion
  val sparkSQL              = "org.apache.spark"        %% "spark-sql"                          % sparkVersion
  val sparkkafka            = "org.apache.spark"        %% "spark-streaming-kafka-0-10"         % sparkVersion
  val sparkMLLib            = "org.apache.spark"        %% "spark-mllib"                        % sparkVersion
  val sparkCassandra        = "com.datastax.spark"      %% "spark-cassandra-connector"          % sparkVersion

  val kryo                  = "com.esotericsoftware.kryo" % "kryo"                              % kryoVersion

  val alpakkaKafka          = "com.typesafe.akka"       %% "akka-stream-kafka"                  % alpakkaKafkaVersion

  val akkaStreamTyped       = "com.typesafe.akka"       %% "akka-stream-typed"                  % akkaVersion
  val akkaHttp              = "com.typesafe.akka"       %% "akka-http"                          % akkaHttpVersion
  val akkaHttpJsonJackson   = "de.heikoseeberger"       %% "akka-http-jackson"                  % akkaHttpJsonVersion
  val akkatyped             = "com.typesafe.akka"       %% "akka-actor-typed"                   % akkaVersion
  
  val joda                  = "joda-time"               % "joda-time"                           % jodaVersion
  
  val gson                  = "com.google.code.gson"     % "gson"                               % gsonVersion

  val slf4jlog4j            = "org.slf4j"                % "slf4j-log4j12"                      % slf4jlog4jVersion

  // Adds the @silencer annotation for suppressing deprecation warnings we don't care about.
  val silencer              = "com.github.ghik"         %% "silencer-lib"                       % silencerVersion     % Provided
  val silencerPlugin        = "com.github.ghik"         %% "silencer-plugin"                    % silencerVersion     % Provided

  val silencerDependencies = Seq(compilerPlugin(silencerPlugin), silencer)

  val modelsDependencies = silencerDependencies
  val clientDependencies = Seq(kafka, curator, commonIO, slf4jlog4j) ++ silencerDependencies
  val flinkDependencies = Seq(flinkScala, flinkStreaming, flinkKafka, flinkQueryableRuntime, flinkCassandra, joda, slf4jlog4j) ++ silencerDependencies
  val sparkDependencies = Seq(sparkcore, sparkstreaming, sparkkafka, sparkSQL, sparkSQLkafka, sparkMLLib, sparkCassandra, slf4jlog4j) ++ silencerDependencies
  val akkaServerDependencies = Seq(alpakkaKafka, akkaStreamTyped, akkatyped, akkaHttp, akkaHttpJsonJackson, slf4jlog4j) ++ silencerDependencies

}
