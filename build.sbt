name := "recommender-system"

version := "0.1"

scalaVersion in ThisBuild := "2.12.9"
scalacOptions in ThisBuild ++= Seq(
  "-feature",
  "-unchecked",
  "-language:higherKinds",
  "-language:postfixOps",
  "-deprecation")

lazy val protobufs = (project in file("./protobufs"))
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value))
  .settings(libraryDependencies ++= Dependencies.scalapbDependencies)

lazy val configuration = (project in file("./configuration"))
  .settings(libraryDependencies ++= Dependencies.configDependencies)

lazy val client = (project in file("./client"))
  .settings(libraryDependencies ++= Dependencies.clientDependencies)
  .dependsOn(model, configuration)

lazy val model = (project in file("./model"))
  .settings(libraryDependencies ++= Dependencies.modelsDependencies)

lazy val flinkserver = (project in file("./flinkserver"))
  .settings(libraryDependencies ++= Dependencies.flinkDependencies)
  .dependsOn(model, configuration)

lazy val sparkserver = (project in file("./sparkserver"))
  .settings(libraryDependencies ++= Dependencies.sparkDependencies)
  .dependsOn(model, configuration)

lazy val akkaserver = (project in file("./akkaserver"))
  .settings(libraryDependencies ++= Dependencies.akkaServerDependencies)
  .dependsOn(sparkserver, model, configuration, protobufs)

lazy val root = (project in file(".")).
  aggregate(protobufs, client, model, configuration, flinkserver, sparkserver, akkaserver)