name := "rotate-keys"
version := "1.0"
scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.11.46",
  ("org.apache.spark" %% "spark-core" % "2.0.0").
    exclude("org.mortbay.jetty","servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.google.guava", "guava").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("org.glassfish.hk2.external", "javax.inject").
    exclude("org.glassfish.hk2.external", "aopalliance-repackaged"),
  "org.json4s" %% "json4s-native" % "3.3.0"
)

mainClass in (Compile, run) := Some("com.aws.rekey.RotateKeys")

