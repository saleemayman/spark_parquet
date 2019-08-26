lazy val root = (project in file("."))
  .settings(
    name := "SPARKTEST1",
    scalaVersion := "2.12.9",
    javaOptions += "-Xmx8g",
    fork in run := true,
    javaOptions in run += "-Xmx8G",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.1.1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
  )
