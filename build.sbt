name := "GDB"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies  ++= Seq(

  "org.apache.spark" % "spark-core_2.10" % "1.5.1" %"provided",
  "org.apache.spark" % "spark-mllib_2.10" %"1.5.1" %"provided",
  "org.scalanlp" % "breeze_2.10" % "0.10" %"provided"
)


resolvers ++= Seq(
  // other resolvers here
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

assemblyMergeStrategy in assembly := {
  case PathList("com", "esotericsoftware", xs@_ *) => MergeStrategy.first // For Log$Logger.class
  case PathList("com", "apache", "spark") => MergeStrategy.first // For Log$Logger.class
  case PathList("com", "apache", "hadoop") => MergeStrategy.first // For Log$Logger.class
  case x if x.contains("javax") => MergeStrategy.first
  case x if x.contains("beanutils") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

    