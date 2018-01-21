import Dependencies._

lazy val root = (project in file("."))
  .enablePlugins(JmhPlugin)
  .settings(
    inThisBuild(List(
      organization := "com.yannmoisan",
      scalaVersion := "2.11.12",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "spark-jmh",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.1",
      scalaTest % Test
    )
  )
