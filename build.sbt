lazy val root = (project in file("."))
  .settings(
    name := "SPARKTEST1",
    scalaVersion := "2.11.8",
    javaOptions += "-Xmx3g",
    fork in run := true,
    javaOptions in run += "-Xmx3G",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.1.1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
  )
