name := "readORC"

version := "1.5.regex.demo.rlike"

scalaVersion := "2.11.8"

val scalaStringVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % scalaStringVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % scalaStringVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % scalaStringVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % scalaStringVersion
libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "1.102.0"


assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "lz4", xs @ _*) => MergeStrategy.last
  case PathList("net", "jpountz", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "git.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
