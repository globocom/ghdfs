val compilerVersion = "2.11.11"

val buildSettings = Seq(
  name := "ghdfs",
  scalaVersion := compilerVersion,
  description := "ghdfs",
  organization := "com.globo.bigdata",
  organizationName := "Globo.com",
  homepage := Some(url("http://docs.bigdata.globoi.com/")),
  resolvers := Resolvers.defaultResoulvers,
  libraryDependencies ++= Dependencies.rootDependencies,
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated,
  publishTo := Some("Artifactory Realm" at "https://artifactory.globoi.com/artifactory/libs-release-local"),
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  publishMavenStyle := true 
)

lazy val root = (project in file(".")).settings(buildSettings: _*)