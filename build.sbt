name := "GDB"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies  ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.5.1" , //%"provided",
  "org.apache.spark" %% "spark-graphx" % "1.5.1"

)


resolvers ++= Seq(
  // other resolvers here
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF","MANIFEST.MF")=>MergeStrategy.discard
  case _ => MergeStrategy.first

}

    