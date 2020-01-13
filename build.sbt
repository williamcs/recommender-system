name := "recommender-system"

version := "0.1"

scalaVersion in ThisBuild := "2.12.9"
scalacOptions in ThisBuild ++= Seq(
  "-feature",
  "-unchecked",
  "-language:higherKinds",
  "-language:postfixOps",
  "-deprecation")

lazy val configuration = (project in file("./configuration"))

lazy val flinkserver = (project in file("./flinkserver"))
  .settings(libraryDependencies ++= Dependencies.flinkDependencies)
  .dependsOn(configuration)

lazy val sparkserver = (project in file("./sparkserver"))
  .settings(libraryDependencies ++= Dependencies.sparkDependencies)
  .dependsOn(configuration)

lazy val root = (project in file(".")).
  aggregate(configuration, flinkserver, sparkserver)